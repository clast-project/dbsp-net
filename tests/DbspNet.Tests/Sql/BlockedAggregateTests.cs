// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Core.Operators.Stateful.Blocks;
using DbspNet.Persistence;
using DbspNet.Persistence.IO;
using DbspNet.Persistence.Runs;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Optimizer;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;
using Xunit;

namespace DbspNet.Tests.Sql;

/// <summary>
/// Incremental aggregates with the input trace held as blocks (<c>DBSPNET_TRACE_BLOCKS=agg</c>,
/// docs/design-flat-trace-state.md §3.4): each tick's output must match a managed twin over deletes,
/// a giant group, idle ticks (which freeze) and both row codecs, for aggregates that read only the
/// delta (SUM, COUNT) and ones that walk the whole group (MIN/MAX on doubles and strings,
/// COUNT DISTINCT); and a checkpoint must restore lazily — caches rebuilt per group on first touch,
/// persisted states applied — and continue exactly like an uninterrupted run.
/// </summary>
public class BlockedAggregateTests
{
    private static readonly string[] Ddl = ["CREATE TABLE t (g INT NOT NULL, s VARCHAR, d DOUBLE, x BIGINT)"];

    private const string Query =
        "SELECT g, SUM(x) AS sx, COUNT(*) AS n, AVG(d) AS ad, MIN(d) AS lo, MAX(s) AS hs, COUNT(DISTINCT s) AS ds FROM t GROUP BY g";

    private static CompiledQuery Compile(bool blocked, bool emitted, int freeze, int block)
    {
        using var mode = TraceBlocksMode.Use(join: false, freezeEntries: freeze, blockEntries: block, aggregate: blocked);
        var resolver = new Resolver(new Catalog());
        foreach (var d in Ddl)
        {
            resolver.Resolve(Parser.ParseStatement(d));
        }

        var plan = PlanOptimizer.Optimize(((SelectPlan)resolver.Resolve(Parser.ParseStatement(Query))).Query);
        return emitted
            ? PlanToCircuit.Compile(plan, CompileOptions.Default, EmittedEqualityCodec.Instance)
            : PlanToCircuit.Compile(plan, options: CompileOptions.Default with { ShareArrangements = true });
    }

    private static int BlockedAggregates(CompiledQuery q) =>
        q.Circuit.Operators.OfType<IncrementalAggregateOp<StructuralRow, StructuralRow, StructuralRow>>().Count(o => o.IsBlocked);

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(5)]
    [InlineData(6)]
    public void Aggregate_MatchesTheManagedTwin_EveryTick(int seed)
    {
        var rnd = new Random(seed);
        var (freeze, block) = (1 + rnd.Next(20), 1 + rnd.Next(6));
        var blocked = Compile(blocked: true, emitted: seed % 2 == 0, freeze, block);
        var managed = Compile(blocked: false, emitted: seed % 2 == 0, freeze, block);
        Assert.Equal(1, BlockedAggregates(blocked));
        Assert.Equal(0, BlockedAggregates(managed));

        string?[] strings = [null, "", "a", "b", "é", "日本語"];
        var live = new List<object?[]>();
        for (var tick = 0; tick < 80; tick++)
        {
            for (var n = rnd.Next(0, 14); n > 0; n--)
            {
                var deleting = live.Count > 0 && rnd.Next(3) == 0;
                object?[] row = deleting
                    ? live[rnd.Next(live.Count)]
                    : [rnd.Next(4) == 0 ? 0 : rnd.Next(1, 6), strings[rnd.Next(strings.Length)], rnd.Next(5) == 0 ? null : rnd.Next(7) / 2.0, (long)rnd.Next(-50, 50)];
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
            if (tick % 9 == 8)
            {
                blocked.Step(); // idle: the blocked trace freezes
                managed.Step();
                Assert.Equal(managed.Current, blocked.Current);
            }
        }
    }

    private static readonly string[] ProgramSql =
    [
        "CREATE TABLE t (g INT NOT NULL, s VARCHAR, d DOUBLE, x BIGINT)",
        "CREATE VIEW a AS " + Query,
    ];

    private static CompiledProgram CompileProgram(bool blocked)
    {
        using var mode = TraceBlocksMode.Use(join: false, freezeEntries: 8, blockEntries: 4, aggregate: blocked);
        return SqlProgram.Compile(ProgramSql, new HashSet<string>(["a"], StringComparer.Ordinal), ArrowSqlSnapshotCodecs.Instance);
    }

    private static void Tick(CompiledProgram p, int tick)
    {
        var rng = new Random(tick);
        for (var i = 0; i < 25; i++)
        {
            p.Table("t").Insert(rng.Next(0, 8), "s" + rng.Next(4), rng.Next(9) / 4.0, (long)rng.Next(100));
        }

        if (tick > 0)
        {
            var old = new Random(tick - 1);
            for (var i = 0; i < 8; i++)
            {
                p.Table("t").Delete(old.Next(0, 8), "s" + old.Next(4), old.Next(9) / 4.0, (long)old.Next(100));
            }
        }

        p.Step();
    }

    private static string Digest(CompiledProgram p) =>
        string.Join("; ", p.Outputs["a"].CurrentView
            .Select(e => string.Join("|", Enumerable.Range(0, p.Outputs["a"].Schema.Count).Select(i => FormattableString.Invariant($"{e.Key[i]}"))) + "#" + e.Value.Value)
            .Order(StringComparer.Ordinal));

    [Theory]
    [InlineData(true, true)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    public async Task Checkpoint_RestoresLazily_AndContinuesExactly(bool writeBlocked, bool readBlocked)
    {
        RunCheckpoint.Verify = true;
        using var layout = StateLayout.Use(true);
        var fs = new InMemoryTableFileSystem();
        var p = CompileProgram(writeBlocked);
        for (var tick = 0; tick < 10; tick++)
        {
            Tick(p, tick);
        }

        await Snapshot.WriteAsync(p.Circuit, fs);
        var restored = CompileProgram(readBlocked);
        await Snapshot.ReadAsync(restored.Circuit, fs);
        Assert.Equal(Digest(p), Digest(restored));

        var twin = CompileProgram(readBlocked);
        for (var tick = 0; tick < 10; tick++)
        {
            Tick(twin, tick);
        }

        for (var tick = 100; tick < 108; tick++)
        {
            Tick(twin, tick);
            Tick(restored, tick);
            Assert.Equal(Digest(twin), Digest(restored));
        }
    }
}
