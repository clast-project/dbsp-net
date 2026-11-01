// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
// Lazy restore (docs/design-durable-state.md §1f, increment 4). An indexed trace restored from a
// run chain starts with an empty map and a cold source, and reads a key's group the first time it
// is asked for. Required:
//
//   1. a cold-backed IndexedZSet is indistinguishable from an eager one under any interleaving of
//      probes, merges, counts, enumeration and GC drops (property test against an oracle);
//   2. after restoring a program, its join traces stay cold — only the keys the new input touches
//      are read — and the outputs match an uninterrupted twin, through further ticks, checkpoints
//      and restores.
using System.Reflection;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Persistence;
using DbspNet.Persistence.IO;
using DbspNet.Persistence.Runs;
using DbspNet.Sql.Compiler;

namespace DbspNet.Tests.Persistence;

public class LazyRestoreTests
{
    // An in-memory cold source that counts reads.
    private sealed class FakeCold(IndexedZSet<int, int, Z64> groups) : IColdGroups<int, int, Z64>
    {
        public int Reads { get; private set; }

        public int GroupCount => groups.GroupCount;

        public ZSet<int, Z64> Read(int key)
        {
            Reads++;
            return groups.GroupFor(key);
        }

        public IEnumerable<KeyValuePair<int, ZSet<int, Z64>>> ReadAll() => groups;

        public IReadOnlyCollection<int> PreloadKeys { get; init; } = [];

        public void ReleasePreload()
        {
        }
    }

    private static IndexedZSet<int, int, Z64> Random(Random rng, int keys, int rows)
    {
        var b = new IndexedZSetBuilder<int, int, Z64>();
        for (var i = 0; i < rows; i++)
        {
            b.Add(rng.Next(keys), rng.Next(6), new Z64(rng.Next(1, 3) * (rng.Next(4) == 0 ? -1 : 1)));
        }

        return b.Build();
    }

    private static string Show(IndexedZSet<int, int, Z64> z) =>
        string.Join(";", z.OrderBy(g => g.Key).Select(g => g.Key + ":" + string.Join(",", g.Value.OrderBy(e => e.Key).Select(e => e.Key + "#" + e.Value.Value))));

    private static string Show(ZSet<int, Z64> z) => string.Join(",", z.OrderBy(e => e.Key).Select(e => e.Key + "#" + e.Value.Value));

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(5)]
    public void AColdBackedIndexedZSet_BehavesExactlyLikeAnEagerOne(int seed)
    {
        for (var trial = 0; trial < 40; trial++)
        {
            var rng = new Random(seed * 1000 + trial);
            var initial = Random(rng, 30, 80);
            var cold = new FakeCold(initial);
            var lazy = IndexedZSet<int, int, Z64>.ColdBacked(cold);
            var oracle = IndexedZSet<int, int, Z64>.NewMutable();
            oracle.MergeInPlace(initial);

            for (var op = 0; op < 60; op++)
            {
                var key = rng.Next(-3, 35);
                switch (rng.Next(9))
                {
                    case 0:
                    case 1:
                        Assert.Equal(Show(oracle.GroupFor(key)), Show(lazy.GroupFor(key)));
                        break;
                    case 2:
                        Assert.Equal(oracle.ContainsKey(key), lazy.ContainsKey(key));
                        break;
                    case 3:
                    case 4:
                    case 5:
                        var delta = Random(rng, 35, rng.Next(1, 8));
                        oracle.MergeInPlace(delta);
                        lazy.MergeInPlace(delta);
                        break;
                    case 6:
                        Assert.Equal(oracle.GroupCount, lazy.GroupCount);
                        Assert.Equal(oracle.IsEmpty, lazy.IsEmpty);
                        break;
                    case 7 when rng.Next(6) == 0:
                        Assert.Equal(Show(oracle), Show(lazy)); // promotes
                        Assert.False(lazy.IsCold);
                        break;
                    case 8 when rng.Next(6) == 0:
                        var threshold = rng.Next(0, 30);
                        Assert.Equal(
                            oracle.RemoveKeysBelow(threshold, k => k).Order(),
                            lazy.RemoveKeysBelow(threshold, k => k).Order());
                        break;
                }

                Assert.Equal(oracle.GroupCount, lazy.GroupCount);
            }

            Assert.Equal(Show(oracle), Show(lazy));
        }
    }

    [Fact]
    public void AProbe_ReadsAKeyFromColdOnce()
    {
        var initial = Random(new Random(7), 50, 200);
        var cold = new FakeCold(initial);
        var lazy = IndexedZSet<int, int, Z64>.ColdBacked(cold);
        for (var i = 0; i < 5; i++)
        {
            lazy.GroupFor(3);
            lazy.ContainsKey(3);
            lazy.GroupFor(-1); // absent everywhere: read once, remembered
        }

        Assert.Equal(2, cold.Reads);
        Assert.True(lazy.IsCold);
        Assert.Equal(initial.GroupCount, lazy.GroupCount);
    }

    private static readonly string[] Sql =
    [
        "CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)",
        "CREATE TABLE u (a INT NOT NULL, s VARCHAR NOT NULL)",
        "CREATE VIEW j AS SELECT t.a, t.b, u.s FROM t JOIN u ON t.a = u.a",
        "CREATE VIEW lj AS SELECT t.a, t.b, u.s FROM t LEFT JOIN u ON t.a = u.a",
    ];

    private static readonly HashSet<string> Views = new(["j", "lj"], StringComparer.Ordinal);

    private static void Load(CompiledProgram p)
    {
        for (var a = 0; a < 2000; a++)
        {
            p.Table("t").Insert(a, a % 17);
            p.Table("u").Insert(a, "s" + (a % 5));
        }

        p.Step();
    }

    private static void Small(CompiledProgram p, int tick)
    {
        p.Table("t").Insert(tick * 7 % 2000, tick);
        p.Table("u").Insert(tick * 11 % 2000, "x" + tick);
        p.Table("t").Delete(tick * 13 % 2000, tick * 13 % 2000 % 17);
        p.Step();
    }

    private static string Digest(CompiledProgram p) =>
        string.Join("\n", Views.Order(StringComparer.Ordinal).Select(v => v + ": " + string.Join("; ",
            p.Outputs[v].CurrentView.Select(e => string.Join("|", Enumerable.Range(0, p.Outputs[v].Schema.Count).Select(i => FormattableString.Invariant($"{e.Key[i]}"))) + "#" + e.Value.Value)
                .Order(StringComparer.Ordinal))));

    // Every indexed trace's state in the program's join operators (reflection: the operators and
    // their traces are internal implementation detail; this test is about their state).
    private static List<object> JoinTraceStates(CompiledProgram p)
    {
        var states = new List<object>();
        foreach (var op in p.Circuit.Operators)
        {
            if (!op.GetType().Name.StartsWith("Incremental", StringComparison.Ordinal) || !op.GetType().Name.Contains("Join", StringComparison.Ordinal))
            {
                continue;
            }

            foreach (var name in new[] { "_leftTrace", "_rightTrace" })
            {
                var trace = op.GetType().GetField(name, BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(op)!;
                states.Add(trace.GetType().GetProperty("Current")!.GetValue(trace)!);
            }
        }

        return states;
    }

    private static bool IsCold(object state) =>
        (bool)state.GetType().GetProperty("IsCold", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(state)!;

    private static int Resolved(object state) =>
        (int)state.GetType().GetProperty("ColdResolvedCount", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(state)!;

    [Fact]
    public async Task ARestoredJoin_StaysCold_ReadsOnlyTouchedKeys_AndMatchesItsTwin()
    {
        using var layout = StateLayout.Use(true);
        var fs = new InMemoryTableFileSystem();
        var producer = SqlProgram.Compile(Sql, Views, ArrowSqlSnapshotCodecs.Instance);
        var twin = SqlProgram.Compile(Sql, Views, ArrowSqlSnapshotCodecs.Instance);
        Load(producer);
        Load(twin);
        await Snapshot.WriteAsync(producer.Circuit, fs);

        var restored = SqlProgram.Compile(Sql, Views, ArrowSqlSnapshotCodecs.Instance);
        await Snapshot.ReadAsync(restored.Circuit, fs);
        var states = JoinTraceStates(restored);
        Assert.NotEmpty(states);
        Assert.All(states, s => Assert.True(IsCold(s), "a join trace was read whole at restore"));

        for (var tick = 1; tick <= 5; tick++)
        {
            Small(restored, tick);
            Small(twin, tick);
            Assert.Equal(Digest(twin), Digest(restored));
        }

        // Five small ticks touch a handful of the 2,000 keys per side.
        Assert.All(states, s =>
        {
            Assert.True(IsCold(s), "a join trace was promoted by small ticks");
            Assert.InRange(Resolved(s), 0, 40);
        });

        // A checkpoint of the lazily restored program extends the chains without reading them,
        // and restores (lazily again) to the same state.
        await Snapshot.WriteAsync(restored.Circuit, fs);
        var again = SqlProgram.Compile(Sql, Views, ArrowSqlSnapshotCodecs.Instance);
        await Snapshot.ReadAsync(again.Circuit, fs);
        for (var tick = 6; tick <= 8; tick++)
        {
            Small(again, tick);
            Small(twin, tick);
            Assert.Equal(Digest(twin), Digest(again));
        }
    }

    private static readonly string[] AggSql =
    [
        "CREATE TABLE e (k INT NOT NULL, v DOUBLE NOT NULL, s VARCHAR NOT NULL)",
        "CREATE VIEW g AS SELECT k, SUM(v) AS sv, AVG(v) AS av, STDDEV(v) AS sd, MIN(v) AS mn, MAX(v) AS mx, COUNT(*) AS n, COUNT(DISTINCT s) AS ds FROM e GROUP BY k",
    ];

    private static readonly HashSet<string> AggViews = new(["g"], StringComparer.Ordinal);

    private static string AggDigest(CompiledProgram p) =>
        string.Join("; ", p.Outputs["g"].CurrentView.Select(e => string.Join("|", Enumerable.Range(0, p.Outputs["g"].Schema.Count)
            .Select(i => FormattableString.Invariant($"{e.Key[i]:R}"))) + "#" + e.Value.Value).Order(StringComparer.Ordinal));

    // Values whose running double sum depends on the order they arrived in: a re-fold from the
    // trace would round differently from the live accumulator, so only the persisted state is exact.
    // The FloatAggregateRestoreTests construction: 1e16, then ten separate ticks of +1.0. Live,
    // each +1.0 is absorbed (ulp(1e16) is 2); the trace coalesces the ten rows to one of weight 10,
    // so a re-fold adds +10.0, which is not. Only the persisted state reproduces the live sum.
    private static void AggLoad(CompiledProgram p)
    {
        for (var k = 0; k < 500; k++)
        {
            p.Table("e").Insert(k, 1e16, "a");
        }

        p.Step();
        for (var tick = 0; tick < 10; tick++)
        {
            for (var k = 0; k < 500; k++)
            {
                p.Table("e").Insert(k, 1.0, "b");
            }

            p.Step();
        }
    }

    private static void AggTouch(CompiledProgram p, int from, int to, int tick)
    {
        for (var k = from; k < to; k++)
        {
            p.Table("e").Insert(k, 0.25 * tick, "t" + tick);
        }

        p.Step();
    }

    private static object AggregateTraceState(CompiledProgram p)
    {
        var op = p.Circuit.Operators.Single(o => o.GetType().Name.StartsWith("IncrementalAggregateOp", StringComparison.Ordinal));
        var trace = op.GetType().GetField("_trace", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(op)!;
        return trace.GetType().GetProperty("Current")!.GetValue(trace)!;
    }

    [Fact]
    public async Task ARestoredAggregate_RebuildsOnlyTouchedGroups_AndKeepsTheRestsPersistedState()
    {
        using var layout = StateLayout.Use(true);
        var fs = new InMemoryTableFileSystem();
        var producer = SqlProgram.Compile(AggSql, AggViews, ArrowSqlSnapshotCodecs.Instance);
        var twin = SqlProgram.Compile(AggSql, AggViews, ArrowSqlSnapshotCodecs.Instance);
        AggLoad(producer);
        AggLoad(twin);
        await Snapshot.WriteAsync(producer.Circuit, fs);

        var restored = SqlProgram.Compile(AggSql, AggViews, ArrowSqlSnapshotCodecs.Instance);
        await Snapshot.ReadAsync(restored.Circuit, fs);
        var state = AggregateTraceState(restored);
        Assert.True(IsCold(state), "the aggregate trace was read whole at restore");

        // Touch groups 0-9 only, then checkpoint: groups 10-499 were never rebuilt, so their
        // persisted states must be written back as they were.
        AggTouch(restored, 0, 10, 1);
        AggTouch(twin, 0, 10, 1);
        Assert.Equal(AggDigest(twin), AggDigest(restored));
        Assert.True(IsCold(state), "touching ten groups promoted the aggregate trace");
        Assert.InRange(Resolved(state), 1, 40);
        await Snapshot.WriteAsync(restored.Circuit, fs);

        // Restore again and touch the groups the first restore never did: their emitted values
        // (retract + insert) depend on the persisted running sums being exact.
        var again = SqlProgram.Compile(AggSql, AggViews, ArrowSqlSnapshotCodecs.Instance);
        await Snapshot.ReadAsync(again.Circuit, fs);
        AggTouch(again, 0, 500, 2);
        AggTouch(twin, 0, 500, 2);
        Assert.Equal(AggDigest(twin), AggDigest(again));
    }

    [Fact]
    public async Task AGiantGroup_IsReadAtRestore_TheRestOnDemand()
    {
        // One join key with far more values than a run block holds (its block holds that group
        // alone), many small keys: the giant key is resolved by the restore itself.
        using var layout = StateLayout.Use(true);
        string[] sql =
        [
            "CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)",
            "CREATE TABLE u (a INT NOT NULL, s VARCHAR NOT NULL)",
            "CREATE VIEW j AS SELECT t.a, t.b, u.s FROM t JOIN u ON t.a = u.a",
        ];
        var views = new HashSet<string>(["j"], StringComparer.Ordinal);
        void Load(CompiledProgram p)
        {
            for (var i = 0; i < RunColdGroups.GiantGroupEntries + 500; i++)
            {
                p.Table("t").Insert(0, i); // key 0: the giant group
            }

            for (var a = 1; a < 3000; a++)
            {
                p.Table("t").Insert(a, a);
                p.Table("u").Insert(a, "s");
            }

            p.Step();
        }

        var fs = new InMemoryTableFileSystem();
        var producer = SqlProgram.Compile(sql, views, ArrowSqlSnapshotCodecs.Instance);
        var twin = SqlProgram.Compile(sql, views, ArrowSqlSnapshotCodecs.Instance);
        Load(producer);
        Load(twin);
        await Snapshot.WriteAsync(producer.Circuit, fs);

        var restored = SqlProgram.Compile(sql, views, ArrowSqlSnapshotCodecs.Instance);
        await Snapshot.ReadAsync(restored.Circuit, fs);
        var states = JoinTraceStates(restored);
        Assert.True(states.Any(s => IsCold(s) && Resolved(s) == 1), // the giant key, preloaded
            "states: " + string.Join(", ", states.Select(s => $"cold={IsCold(s)} resolved={Resolved(s)} groups={s.GetType().GetProperty("GroupCount")!.GetValue(s)}")));
        Assert.All(states, s => Assert.True(IsCold(s)));

        foreach (var p in new[] { restored, twin })
        {
            p.Table("u").Insert(0, "giant");
            p.Table("t").Insert(5, 55);
            p.Step();
        }

        Assert.Equal(
            string.Join(";", twin.Outputs["j"].CurrentView.Select(e => e.Key.ToString() + "#" + e.Value.Value).Order(StringComparer.Ordinal)),
            string.Join(";", restored.Outputs["j"].CurrentView.Select(e => e.Key.ToString() + "#" + e.Value.Value).Order(StringComparer.Ordinal)));
    }
}
