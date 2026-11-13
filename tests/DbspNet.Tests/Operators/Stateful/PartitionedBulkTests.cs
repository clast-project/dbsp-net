// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
// The bulk-load fast path of blocked partitioned TOP-K and rank (docs/design-bounded-memory.md §5.52):
// a partition the store held nothing for when the step began is sorted once and written to the record as
// one run. Required, on every tick: the fast path equals itself off, sliced equals unsliced, blocked equals
// managed, and each view equals a batch recompute over the inputs integrated so far; parallel equals
// serial; and a checkpoint taken right after a bulk tick restores into a program that keeps matching as
// later ticks retract bulk-loaded rows. The data covers a bulk tick into an empty store (N1), one into a
// non-empty store with new and existing partitions mixed (N2), negative weights in new partitions,
// weight-2 rows, NULL partition keys and ties.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Core.Operators.Stateful.Blocks;
using DbspNet.Persistence;
using DbspNet.Persistence.IO;
using DbspNet.Persistence.Runs;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;
using Xunit;

namespace DbspNet.Tests.Operators.Stateful;

public class PartitionedBulkTests
{
    private const string Table = "CREATE TABLE t (a INT NOT NULL, p INT, b INT, c VARCHAR)";

    private static readonly (string Name, string Query)[] Queries =
    [
        // TOP-K: ROW_NUMBER k = 1 and k = 2, RANK with ties, DENSE_RANK over a VARCHAR key with NULLs.
        ("k1", "SELECT a, p, b, c FROM (SELECT a, p, b, c, ROW_NUMBER() OVER (PARTITION BY p ORDER BY b DESC, a) AS r FROM t) x WHERE r <= 1"),
        ("k2", "SELECT a, p, b, c FROM (SELECT a, p, b, c, ROW_NUMBER() OVER (PARTITION BY p ORDER BY b, c, a) AS r FROM t) x WHERE r <= 2"),
        ("rk", "SELECT a, p, b FROM (SELECT a, p, b, RANK() OVER (PARTITION BY p ORDER BY b) AS r FROM t) x WHERE r <= 1"),
        ("dr", "SELECT a, c, b FROM (SELECT a, c, b, DENSE_RANK() OVER (PARTITION BY c ORDER BY b DESC) AS r FROM t) x WHERE r <= 2"),

        // Rank in the output: ROW_NUMBER, RANK with ties, DENSE_RANK over the VARCHAR key.
        ("rn", "SELECT a, p, b, c, ROW_NUMBER() OVER (PARTITION BY p ORDER BY b, a) AS r FROM t"),
        ("rr", "SELECT a, p, b, RANK() OVER (PARTITION BY p ORDER BY b) AS r FROM t"),
        ("rd", "SELECT a, c, b, DENSE_RANK() OVER (PARTITION BY c ORDER BY b DESC) AS r FROM t"),
    ];

    private static string[] Sql => [Table, .. Queries.Select(q => $"CREATE VIEW {q.Name} AS {q.Query}")];

    private static HashSet<string> Views => new(Queries.Select(q => q.Name), StringComparer.Ordinal);

    // Blocked (window stores on blocks, a zero write-back budget so caches drop every step) or managed;
    // the fast path on or off. The sliced arms step under UseStepSpill, set by the caller.
    private static CompiledProgram Compile(bool bulk, bool blocked = true)
    {
        PartitionedBulkMode.Enabled = bulk;
        try
        {
            using var blocks = TraceBlocksMode.Use(join: blocked, freezeEntries: 16, blockEntries: 4, window: blocked);
            using var writeBack = SpillSettings.UseWindowWriteBack(0);
            return SqlProgram.Compile(Sql, Views, ArrowSqlSnapshotCodecs.Instance);
        }
        finally
        {
            PartitionedBulkMode.Reset();
        }
    }

    private static List<PartitionedTopKCompactOp<StructuralRow, StructuralRow>> TopKs(IEnumerable<Core.Circuit.IOperator> ops) =>
        [.. ops.OfType<PartitionedTopKCompactOp<StructuralRow, StructuralRow>>()];

    private static List<PartitionedRankOp<StructuralRow, StructuralRow, StructuralRow>> Ranks(IEnumerable<Core.Circuit.IOperator> ops) =>
        [.. ops.OfType<PartitionedRankOp<StructuralRow, StructuralRow, StructuralRow>>()];

    private static long BulkPartitions(IEnumerable<Core.Circuit.IOperator> ops) =>
        TopKs(ops).Sum(o => o.BulkPartitions) + Ranks(ops).Sum(o => o.BulkPartitions);

    // ---- Data ------------------------------------------------------------------------------------------

    private static object?[] R(int a, int? p, int? b, string? c) => [a, p, b, c];

    // tick 0: a bulk load into the empty store (N1): 400 partitions of 1–4 rows with ties, some rows twice,
    //   a NULL partition, a partition holding only a −1 row and one holding a −1 and a +1 row.
    // tick 1: the first rows of some partitions retracted (at their weights), rows into existing partitions,
    //   50 new partitions, one of the −1 rows cancelled.
    // tick 2: a second bulk into the non-empty store (N2): 600 new partitions, rows into a third of the old
    //   ones, new c values, a new partition with a −1 row.
    // tick 3: tick 2's bulk partitions lose their first rows; the other −1 rows cancelled.
    // ticks 4–9: churn: inserts anywhere, deletes of live rows, now and then a delete of an absent row.
    private static List<(object?[] Row, long Weight)> Tick(int tick, Dictionary<string, (object?[] Row, long Weight)> live, Random rng)
    {
        var d = new List<(object?[], long)>();
        void Add(object?[] row, long w) => d.Add((row, w));
        object?[] Row0(int p) => R(p * 10, p, (p * 7) % 5, "c" + (p % 4));

        switch (tick)
        {
            case 0:
                for (var p = 0; p < 400; p++)
                {
                    for (var j = 0; j <= p % 4; j++)
                    {
                        var row = R((p * 10) + j, p, ((p * 7) + (j * 3)) % 5, j % 3 == 1 ? null : "c" + ((p + j) % 4));
                        Add(row, (p + j) % 9 == 0 ? 2 : 1);
                    }
                }

                Add(R(9_000, null, 3, "n"), 1);
                Add(R(9_001, null, 1, null), 1);
                Add(R(9_100, 1_000, 1, "neg"), -1);
                Add(R(9_101, 1_001, 0, "neg"), -1);
                Add(R(9_102, 1_001, 2, "pos"), 1);
                break;

            case 1:
                for (var p = 0; p < 400; p += 5)
                {
                    var row = Row0(p);
                    if (live.TryGetValue(Key(row), out var e))
                    {
                        Add(e.Row, -e.Weight);
                    }
                }

                for (var p = 0; p < 400; p += 7)
                {
                    Add(R(20_000 + p, p, 9, "x"), 1);
                }

                for (var p = 400; p < 450; p++)
                {
                    Add(R(p * 10, p, p % 3, "c" + (p % 4)), 1);
                }

                Add(R(9_100, 1_000, 1, "neg"), 1);
                break;

            case 2:
                for (var p = 450; p < 1_050; p++)
                {
                    for (var j = 0; j <= p % 3; j++)
                    {
                        Add(R((p * 10) + j, p, ((p * 3) + j) % 4, "d" + (p % 6)), (p + j) % 11 == 0 ? 2 : 1);
                    }
                }

                for (var p = 0; p < 450; p += 3)
                {
                    Add(R(30_000 + p, p, -1, "y"), 1);
                }

                Add(R(9_200, 2_000, 5, "neg"), -1);
                break;

            case 3:
                for (var p = 450; p < 1_050; p += 4)
                {
                    var row = R(p * 10, p, (p * 3) % 4, "d" + (p % 6));
                    if (live.TryGetValue(Key(row), out var e))
                    {
                        Add(e.Row, -e.Weight);
                    }
                }

                Add(R(9_101, 1_001, 0, "neg"), 1);
                Add(R(9_200, 2_000, 5, "neg"), 1);
                break;

            default:
                for (var i = 0; i < 60; i++)
                {
                    Add(R(40_000 + (tick * 100) + i, rng.Next(0, 1_100), rng.Next(-2, 10), rng.Next(4) == 0 ? null : "c" + rng.Next(4)), rng.Next(6) == 0 ? 2 : 1);
                }

                var keys = live.Keys.Order(StringComparer.Ordinal).ToList();
                for (var i = 0; i < 40 && keys.Count > 0; i++)
                {
                    var e = live[keys[rng.Next(keys.Count)]];
                    if (e.Weight > 0)
                    {
                        Add(e.Row, -1);
                    }
                }

                if (tick % 3 == 0)
                {
                    Add(R(50_000 + tick, rng.Next(0, 1_100), 4, "gone"), -1);
                }

                break;
        }

        // Consolidate (a delta is a Z-set) and fold into the live inputs.
        var net = new Dictionary<string, (object?[] Row, long Weight)>(StringComparer.Ordinal);
        foreach (var (row, w) in d)
        {
            var k = Key(row);
            net[k] = (row, (net.TryGetValue(k, out var e) ? e.Weight : 0) + w);
        }

        foreach (var (k, (row, w)) in net)
        {
            var next = (live.TryGetValue(k, out var e) ? e.Weight : 0) + w;
            if (next == 0)
            {
                live.Remove(k);
            }
            else
            {
                live[k] = (row, next);
            }
        }

        return [.. net.Values.Where(e => e.Weight != 0)];
    }

    private static string Key(object?[] row) => string.Join("|", row.Select(v => v?.ToString() ?? "<null>"));

    // ---- Oracles -----------------------------------------------------------------------------------------

    private static string Rows(IEnumerable<KeyValuePair<StructuralRow, Z64>> rows, int width)
    {
        var d = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach (var kv in rows)
        {
            var key = string.Join("|", Enumerable.Range(0, width).Select(i => kv.Key[i] is { } x ? FormattableString.Invariant($"{x}") : "<null>"));
            d[key] = d.GetValueOrDefault(key) + kv.Value.Value;
        }

        return string.Join("; ", d.Where(e => e.Value != 0).Select(e => e.Key + "#" + e.Value).Order(StringComparer.Ordinal));
    }

    private static string Digest(Func<string, (ZSet<StructuralRow, Z64> View, int Width)> output) =>
        string.Join("\n", Queries.Select(q =>
        {
            var (view, width) = output(q.Name);
            return q.Name + ": " + Rows(view, width);
        }));

    private static string Digest(CompiledProgram p) => Digest(v => (p.Outputs[v].CurrentView, p.Outputs[v].Schema.Count));

    private static string Digest(ParallelCompiledProgram p) => Digest(v => (p.Outputs[v].CurrentView, p.Outputs[v].Schema.Count));

    // Each view's query resolved on its own, not optimized, evaluated over the live inputs.
    private static string Batch(Dictionary<string, (object?[] Row, long Weight)> live)
    {
        var resolver = new Resolver(new Catalog());
        resolver.Resolve(Parser.ParseStatement(Table));
        var b = new ZSetBuilder<StructuralRow, Z64>();
        foreach (var (row, w) in live.Values)
        {
            b.Add(new StructuralRow((object?[])row.Clone()), new Z64(w));
        }

        var tables = new Dictionary<string, ZSet<StructuralRow, Z64>>(StringComparer.Ordinal) { ["t"] = b.Build() };
        return Digest(v =>
        {
            var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(Oracle(Queries.Single(q => q.Name == v).Query)))).Query;
            return (BatchPlanEvaluator.Evaluate(plan, new BatchEvalContext(tables, new Dictionary<CteRef, ZSet<StructuralRow, Z64>>())), plan.Schema.Count);
        });
    }

    // The resolver lowers `SELECT .. FROM (.. rank ..) x WHERE r <= k` straight to a TOP-K plan, which the
    // batch evaluator has no case for: the oracle reads the same query as a CTE, which resolves to the rank
    // and the filter (the §5.51 rewrite runs only in the optimizer, which the oracle skips).
    private static string Oracle(string query)
    {
        const string from = " FROM (";
        var open = query.IndexOf(from, StringComparison.Ordinal);
        var close = query.LastIndexOf(") x WHERE ", StringComparison.Ordinal);
        return open < 0 || close < 0
            ? query
            : "WITH x AS (" + query[(open + from.Length)..close] + ") " + query[..open] + " FROM x" + query[(close + 3)..];
    }

    private static void Push(CompiledProgram p, List<(object?[] Row, long Weight)> delta)
    {
        p.Table("t").Push(delta.Select(e => ((object?[])e.Row.Clone(), e.Weight)));
        p.Step();
    }

    // ---- Tests -------------------------------------------------------------------------------------------

    [Theory]
    [InlineData(0L, 7)] // forced: every step sliced, 7-row chunks
    [InlineData(64L, 4096)] // sliced past 64 rows
    public void Fast_path_equals_off_unsliced_managed_and_a_recompute_on_every_tick(long stepRows, int forcedChunk)
    {
        CompiledProgram on, off, plain, managed;
        using (SpillSettings.UseStepSpill(stepRows, forcedChunk))
        {
            on = Compile(bulk: true);
            off = Compile(bulk: false);
        }

        using (SpillSettings.UseStepSpill(null))
        {
            plain = Compile(bulk: true);
            managed = Compile(bulk: true, blocked: false);
        }

        Assert.Equal(4, TopKs(on.Circuit.Operators).Count);
        Assert.Equal(3, Ranks(on.Circuit.Operators).Count);
        Assert.All(TopKs(on.Circuit.Operators), op => Assert.True(op.IsBlocked && op.BulkEnabled));
        Assert.All(Ranks(on.Circuit.Operators), op => Assert.True(op.IsBlocked && op.BulkEnabled));
        Assert.All(TopKs(off.Circuit.Operators), op => Assert.False(op.BulkEnabled));
        Assert.All(TopKs(managed.Circuit.Operators), op => Assert.False(op.IsBlocked));

        var live = new Dictionary<string, (object?[] Row, long Weight)>(StringComparer.Ordinal);
        var rng = new Random(11);
        var nonEmpty = new HashSet<string>(StringComparer.Ordinal);
        for (var tick = 0; tick < 10; tick++)
        {
            var delta = Tick(tick, live, rng);
            var bulk0 = BulkPartitions(on.Circuit.Operators);
            using (SpillSettings.UseStepSpill(stepRows, forcedChunk))
            {
                Push(on, delta);
                Push(off, delta);
            }

            using (SpillSettings.UseStepSpill(null))
            {
                Push(plain, delta);
                Push(managed, delta);
            }

            var expected = Batch(live);
            Assert.Equal(expected, Digest(managed));
            Assert.Equal(expected, Digest(off));
            Assert.Equal(expected, Digest(on));
            Assert.Equal(expected, Digest(plain));
            if (tick is 0 or 2)
            {
                // Both bulk ticks take the fast path (tick 2 alongside the old path for existing partitions).
                Assert.True(BulkPartitions(on.Circuit.Operators) > bulk0, $"tick {tick}: no partition took the fast path");
            }

            foreach (var q in Queries.Where(q => on.Outputs[q.Name].CurrentView.Count > 0))
            {
                nonEmpty.Add(q.Name);
            }
        }

        Assert.Equal(Queries.Length, nonEmpty.Count);
        Assert.Equal(0, BulkPartitions(off.Circuit.Operators));
        Assert.All(TopKs(on.Circuit.Operators), op => Assert.True(op.SlicedSteps > 0));
    }

    [Fact]
    public void Parallel_equals_serial_on_every_tick()
    {
        var serial = Compile(bulk: false);
        ParallelCompiledProgram parallel;
        PartitionedBulkMode.Enabled = true;
        try
        {
            using var blocks = TraceBlocksMode.Use(join: true, freezeEntries: 16, blockEntries: 4, window: true);
            using var writeBack = SpillSettings.UseWindowWriteBack(0);
            Assert.True(SqlProgram.TryCompileParallel(Sql, Views, 4, out var p, ArrowSqlSnapshotCodecs.Instance));
            parallel = p!;
        }
        finally
        {
            PartitionedBulkMode.Reset();
        }

        using (parallel)
        {
            var ops = parallel.Circuit.Replicas.SelectMany(r => r.Operators).ToList();
            Assert.NotEmpty(TopKs(ops));
            Assert.All(TopKs(ops), op => Assert.True(op.BulkEnabled));
            Assert.All(Ranks(ops), op => Assert.True(op.BulkEnabled));
            var live = new Dictionary<string, (object?[] Row, long Weight)>(StringComparer.Ordinal);
            var rng = new Random(5);
            for (var tick = 0; tick < 10; tick++)
            {
                var delta = Tick(tick, live, rng);
                Push(serial, delta);
                parallel.Table("t").Push(delta.Select(e => ((object?[])e.Row.Clone(), e.Weight)));
                parallel.Step();
                Assert.Equal(Digest(serial), Digest(parallel));
            }

            Assert.True(BulkPartitions(ops) > 0);
        }
    }

    // A checkpoint right after each bulk tick (0: N1, 2: N2), in both state layouts, sliced; the restored
    // program then retracts bulk-loaded rows (ticks 1 and 3) and keeps matching an uninterrupted twin.
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task A_checkpoint_after_a_bulk_tick_restores_exactly(bool runsLayout)
    {
        using var layout = StateLayout.Use(runsLayout);
        using var spill = SpillSettings.UseStepSpill(64, 4096);
        var p = Compile(bulk: true);
        var twin = Compile(bulk: false);
        var fs = new InMemoryTableFileSystem();
        var live = new Dictionary<string, (object?[] Row, long Weight)>(StringComparer.Ordinal);
        var rng = new Random(3);
        CompiledProgram? restored = null;
        for (var tick = 0; tick < 10; tick++)
        {
            var delta = Tick(tick, live, rng);
            Push(p, delta);
            Push(twin, delta);
            if (restored is not null)
            {
                Push(restored, delta);
                Assert.Equal(Digest(twin), Digest(restored));
            }

            Assert.Equal(Digest(twin), Digest(p));
            if (tick is 0 or 2)
            {
                await Snapshot.WriteAsync(p.Circuit, fs, retainCount: 1);
                restored = Compile(bulk: true);
                await Snapshot.ReadAsync(restored.Circuit, fs);
                Assert.Equal(Digest(p), Digest(restored));
            }
        }

        Assert.Equal(Batch(live), Digest(restored!));
    }
}
