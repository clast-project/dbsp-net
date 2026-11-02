// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.IO;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Core.Operators.Stateful.Blocks;
using DbspNet.Persistence;
using DbspNet.Persistence.Blocks;
using DbspNet.Persistence.IO;
using DbspNet.Persistence.Runs;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Optimizer;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;
using Xunit;

namespace DbspNet.Tests.Sql;

/// <summary>
/// DISTINCT's trace and stored output views held as blocks (<c>DBSPNET_TRACE_BLOCKS=view,distinct</c>,
/// docs/design-flat-trace-state.md §3.7): <see cref="BlockedZSetTrace"/> against the managed
/// <see cref="ZSetTrace{TKey,TWeight}"/> under random integrates, drops, idle freezes and reads;
/// SQL DISTINCT and a stored view tick by tick against managed twins; and checkpoints that a trace
/// of either kind restores, continuing exactly like an uninterrupted run.
/// </summary>
[Collection("BlockedCheckpointTests")]
public class BlockedZSetTraceTests
{
    private static readonly Schema RowSchema = new([
        new SchemaColumn("k", new SqlBigintType(false)),
        new SchemaColumn("s", new SqlVarcharType(null, true)),
        new SchemaColumn("d", new SqlDoubleType(true)),
    ]);

    private static readonly string?[] Strings = [null, "", "a", "é", "日本"];

    // −0.0 equals 0.0, and two NaN payloads are equal cells with different row hashes.
    private static readonly double[] Doubles = [0.0, -0.0, 0.5, double.NaN, BitConverter.Int64BitsToDouble(0x7ff8000000000001)];

    private static StructuralRow Row(Random rnd) => new(
        (long)rnd.Next(-3, 10),
        Strings[rnd.Next(Strings.Length)] is { } s ? Utf8String.Of(s) : null,
        rnd.Next(5) == 0 ? null : Doubles[rnd.Next(Doubles.Length)]);

    public static TheoryData<int> Seeds() => new(Enumerable.Range(1, 40));

    [Theory]
    [MemberData(nameof(Seeds))]
    public void BlockedZSetTrace_BehavesLikeTheManagedTrace(int seed)
    {
        var rnd = new Random(seed);
        var format = BlockSchema.TryCreate(RowSchema, Schema.Empty)!;
        BlockedZSetTrace blocked;
        using (TraceBlocksMode.Use(join: true, freezeEntries: rnd.Next(1, 40), blockEntries: rnd.Next(1, 7)))
        {
            blocked = new BlockedZSetTrace(format);
        }

        var oracle = new ZSetTrace<StructuralRow, Z64>();
        static long Monotone(StructuralRow row) => (long)row[0]!;
        var counting = seed % 2 == 0;
        if (counting)
        {
            Assert.Equal(0, blocked.Count); // from now on kept per delta
        }

        for (var step = 0; step < 150; step++)
        {
            switch (rnd.Next(10))
            {
                case < 6:
                {
                    var delta = new ZSetBuilder<StructuralRow, Z64>();
                    if (rnd.Next(3) == 0)
                    {
                        foreach (var (row, weight) in oracle.Current)
                        {
                            if (rnd.Next(2) == 0)
                            {
                                delta.Add(row, Z64.Negate(weight));
                            }
                        }
                    }

                    for (var n = rnd.Next(0, 20); n > 0; n--)
                    {
                        delta.Add(Row(rnd), new Z64(rnd.Next(2) == 0 ? rnd.Next(1, 3) : -rnd.Next(1, 3)));
                    }

                    var built = delta.Build();
                    blocked.Integrate(built);
                    oracle.Integrate(built);
                    break;
                }

                case 6:
                {
                    var threshold = (long)rnd.Next(-4, 8);
                    blocked.DropKeysBelow(threshold, Monotone);
                    oracle.DropKeysBelow(threshold, Monotone);
                    break;
                }

                case 7:
                    blocked.Idle();
                    break;

                default:
                    blocked.Trace.CheckRuns();
                    Assert.Equal(oracle.Current, blocked.Current);
                    Assert.Equal(oracle.Current, ZSet.FromEntries(blocked.EnumerateWhole()));
                    break;
            }

            Assert.Equal(oracle.Count, blocked.Count);
            for (var p = 0; p < 6; p++)
            {
                var row = p < 2 && oracle.Count > 0 ? oracle.Current.ElementAt(rnd.Next(oracle.Count)).Key : Row(rnd);
                Assert.Equal(oracle.WeightOf(row), blocked.WeightOf(row));
            }
        }

        blocked.Idle();
        blocked.Trace.CheckRuns();
        Assert.Equal(oracle.Current, blocked.Current);
    }

    private static readonly string[] Ddl = ["CREATE TABLE t (g INT NOT NULL, s VARCHAR, d DOUBLE)"];

    private static CompiledQuery CompileQuery(string sql, bool blocked, bool emitted, int freeze, int block)
    {
        using var mode = TraceBlocksMode.Use(join: false, freezeEntries: freeze, blockEntries: block, distinct: blocked);
        var resolver = new Resolver(new Catalog());
        foreach (var d in Ddl)
        {
            resolver.Resolve(Parser.ParseStatement(d));
        }

        var plan = PlanOptimizer.Optimize(((SelectPlan)resolver.Resolve(Parser.ParseStatement(sql))).Query);
        return emitted
            ? PlanToCircuit.Compile(plan, CompileOptions.Default, EmittedEqualityCodec.Instance)
            : PlanToCircuit.Compile(plan, options: CompileOptions.Default with { ShareArrangements = true });
    }

    private static int BlockedDistincts(CompiledQuery q) =>
        q.Circuit.Operators.OfType<DistinctOp<StructuralRow, Z64>>().Count(o => o.IsBlocked);

    public static TheoryData<string, int> DistinctCases()
    {
        var data = new TheoryData<string, int>();
        foreach (var sql in new[]
        {
            "SELECT DISTINCT g, s FROM t",
            "SELECT DISTINCT s, d FROM t WHERE g > 1",
            "SELECT g, s FROM t WHERE g < 3 UNION SELECT g, s FROM t WHERE g > 2",
        })
        {
            for (var seed = 1; seed <= 4; seed++)
            {
                data.Add(sql, seed);
            }
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(DistinctCases))]
    public void Distinct_MatchesTheManagedTwin_EveryTick(string sql, int seed)
    {
        var rnd = new Random(seed);
        var (freeze, block) = (1 + rnd.Next(20), 1 + rnd.Next(6));
        var blocked = CompileQuery(sql, blocked: true, emitted: seed % 2 == 0, freeze, block);
        var managed = CompileQuery(sql, blocked: false, emitted: seed % 2 == 0, freeze, block);
        Assert.True(BlockedDistincts(blocked) > 0);
        Assert.Equal(0, BlockedDistincts(managed));

        var live = new List<object?[]>();
        for (var tick = 0; tick < 60; tick++)
        {
            for (var n = rnd.Next(0, 12); n > 0; n--)
            {
                var deleting = live.Count > 0 && rnd.Next(3) == 0;
                object?[] row = deleting
                    ? live[rnd.Next(live.Count)]
                    : [rnd.Next(0, 6), Strings[rnd.Next(Strings.Length)], rnd.Next(5) == 0 ? null : Doubles[rnd.Next(Doubles.Length)]];
                foreach (var q in new[] { blocked, managed })
                {
                    if (deleting)
                    {
                        q.Table("t").Delete(row);
                    }
                    else
                    {
                        q.Table("t").Insert(row);
                    }
                }

                if (deleting)
                {
                    live.Remove(row);
                }
                else
                {
                    live.Add(row);
                }
            }

            blocked.Step();
            managed.Step();
            Assert.Equal(managed.Current, blocked.Current);
            if (tick % 7 == 6)
            {
                blocked.Step(); // idle: the blocked traces freeze
                managed.Step();
                Assert.Equal(managed.Current, blocked.Current);
            }
        }
    }

    private static readonly string[] ProgramSql =
    [
        "CREATE TABLE a (k INT NOT NULL, s VARCHAR, x BIGINT)",
        "CREATE VIEW v AS SELECT k, s, x FROM a WHERE x > 10",
        "CREATE VIEW d AS SELECT DISTINCT k, s FROM a",
    ];

    private static readonly HashSet<string> Views = new(["v", "d"], StringComparer.Ordinal);

    private static CompiledProgram CompileProgram(bool blocked)
    {
        using var mode = TraceBlocksMode.Use(join: false, freezeEntries: 16, blockEntries: 8, view: blocked, distinct: blocked);
        return SqlProgram.Compile(ProgramSql, Views, ArrowSqlSnapshotCodecs.Instance);
    }

    private static void Tick(CompiledProgram p, int tick)
    {
        var rng = new Random(tick);
        for (var i = 0; i < (tick < 2 ? 200 : 30); i++)
        {
            p.Table("a").Insert(rng.Next(0, 20), "s" + rng.Next(5), (long)rng.Next(40));
        }

        if (tick > 0)
        {
            var old = new Random(tick - 1);
            for (var i = 0; i < 10; i++)
            {
                p.Table("a").Delete(old.Next(0, 20), "s" + old.Next(5), (long)old.Next(40));
            }
        }

        p.Step();
    }

    private static string Digest(CompiledProgram p, string view) =>
        string.Join("; ", p.Outputs[view].CurrentView
            .Select(e => string.Join("|", Enumerable.Range(0, p.Outputs[view].Schema.Count).Select(i => FormattableString.Invariant($"{e.Key[i]}"))) + "#" + e.Value.Value)
            .Order(StringComparer.Ordinal));

    private static string Digest(CompiledProgram p) => Digest(p, "v") + " || " + Digest(p, "d");

    private static int BlockedViews(CompiledProgram p) =>
        p.Circuit.Operators.OfType<IntegrateOp<StructuralRow>>().Count(o => o.IsBlocked);

    [Fact]
    public void StoredViews_MatchTheManagedTwin_EveryTick_IncludingCountsAndWeights()
    {
        var blocked = CompileProgram(blocked: true);
        var managed = CompileProgram(blocked: false);
        Assert.Equal(2, BlockedViews(blocked));
        Assert.Equal(0, BlockedViews(managed));
        Assert.Contains(blocked.Circuit.Operators.OfType<DistinctOp<StructuralRow, Z64>>(), o => o.IsBlocked);

        for (var tick = 0; tick < 25; tick++)
        {
            Tick(blocked, tick);
            Tick(managed, tick);
            if (tick % 5 == 4)
            {
                blocked.Step(); // idle
                managed.Step();
            }

            Assert.Equal(Digest(managed), Digest(blocked));
            foreach (var view in Views)
            {
                Assert.Equal(managed.Outputs[view].View.Count, blocked.Outputs[view].View.Count);
                foreach (var (row, weight) in managed.Outputs[view].CurrentView.Take(10))
                {
                    Assert.Equal(weight, blocked.Outputs[view].View.WeightOf(row));
                }
            }
        }
    }

    [Theory]
    [InlineData(true, true, true)]
    [InlineData(true, false, true)]
    [InlineData(false, true, true)]
    [InlineData(true, true, false)]
    [InlineData(true, false, false)]
    [InlineData(false, true, false)]
    public async Task Checkpoint_RestoresExactly_InEitherMode(bool writeBlocked, bool readBlocked, bool runs)
    {
        RunCheckpoint.Verify = true;
        using var layout = StateLayout.Use(runs);
        var fs = new InMemoryTableFileSystem();
        var p = CompileProgram(writeBlocked);
        for (var tick = 0; tick < 8; tick++)
        {
            Tick(p, tick);
        }

        await Snapshot.WriteAsync(p.Circuit, fs);

        // A second checkpoint after a tick: unchanged runs are named again.
        Tick(p, 8);
        await Snapshot.WriteAsync(p.Circuit, fs);

        var restored = CompileProgram(readBlocked);
        await Snapshot.ReadAsync(restored.Circuit, fs);
        Assert.Equal(Digest(p), Digest(restored));

        var twin = CompileProgram(readBlocked);
        for (var tick = 0; tick < 9; tick++)
        {
            Tick(twin, tick);
        }

        for (var tick = 100; tick < 106; tick++)
        {
            Tick(twin, tick);
            Tick(restored, tick);
            Assert.Equal(Digest(twin), Digest(restored));
        }
    }

    [Fact]
    public async Task ABlockedRestore_AdoptsTheRunsLazily()
    {
        RunCheckpoint.Verify = true;
        using var layout = StateLayout.Use(true);
        var fs = new InMemoryTableFileSystem();
        var p = CompileProgram(blocked: true);
        for (var tick = 0; tick < 4; tick++)
        {
            Tick(p, tick);
        }

        await Snapshot.WriteAsync(p.Circuit, fs);
        var restored = CompileProgram(blocked: true);
        await Snapshot.ReadAsync(restored.Circuit, fs);
        var view = restored.Circuit.Operators.OfType<IntegrateOp<StructuralRow>>().First();
        var trace = (BlockedZSetTrace)typeof(IntegrateOp<StructuralRow>)
            .GetField("_view", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!.GetValue(view)!;
        Assert.NotEmpty(trace.Trace.Runs);
        Assert.All(trace.Trace.Runs.SelectMany(r => r.Blocks), b => Assert.IsType<LazyTraceBlock>(b));
        Assert.Equal(Digest(p), Digest(restored));
    }
}
