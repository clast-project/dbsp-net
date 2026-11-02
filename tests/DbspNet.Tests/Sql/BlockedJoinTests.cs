// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using CsCheck;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Core.Operators.Stateful.Blocks;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Optimizer;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;
using DbspNet.Tests.EndToEnd;
using Xunit;

namespace DbspNet.Tests.Sql;

/// <summary>
/// The inner join with block-held traces (<c>DBSPNET_TRACE_BLOCKS=join</c>,
/// docs/design-flat-trace-state.md §3) end to end. A blocked circuit, with tiny freeze thresholds
/// and blocks so every tick exercises runs and merges, must match the batch evaluator over random
/// queries and ±1 streams, and must match its managed twin tick by tick over a string-heavy join
/// with deletes. The compiler must also engage it only where it applies.
/// <para>A single query compiles typed when it can, and typed traces are out of scope, so these
/// tests force the structural path: with <c>ShareArrangements</c> (which turns the typed path off;
/// a join with no shared arrangement stays an ordinary structural join) or with the emitted-row
/// codec (which also exercises the non-default output row builder).</para>
/// </summary>
public class BlockedJoinTests
{
    private static CompiledQuery Structural(LogicalPlan optimized, bool emitted) => emitted
        ? PlanToCircuit.Compile(optimized, CompileOptions.Default, EmittedEqualityCodec.Instance)
        : PlanToCircuit.Compile(optimized, options: CompileOptions.Default with { ShareArrangements = true });

    private static CompiledQuery Compile(IEnumerable<string> ddl, string sql, bool emitted = false)
    {
        var resolver = new Resolver(new Catalog());
        foreach (var d in ddl)
        {
            resolver.Resolve(Parser.ParseStatement(d));
        }

        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(sql))).Query;
        return Structural(PlanOptimizer.Optimize(plan), emitted);
    }

    private static int Blocked(CompiledQuery q) => q.Circuit.Operators.OfType<BlockedJoinOp>().Count();

    [Fact]
    public void RandomQuery_BlockedCircuitEqualsBatch()
    {
        Gen.Select(RandomQuery.GenQuery, RandomQuery.GenTicks, Gen.Int[1, 6], Gen.Int[1, 4], Gen.Bool)
            .Sample((sql, ticks, freeze, block, emitted) => CheckOne(sql, ticks, freeze, block, emitted), iter: 1500, threads: 1);
        Assert.True(_engaged >= 100, $"only {_engaged} of 1500 random queries compiled a blocked join");
    }

    private static int _engaged;

    private static bool CheckOne(string sql, IReadOnlyList<IReadOnlyList<InputEvent>> ticks, int freeze, int block, bool emitted)
    {
        var resolver = new Resolver(new Catalog());
        foreach (var ddl in RandomQuery.FixedDdl)
        {
            resolver.Resolve(Parser.ParseStatement(ddl));
        }

        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(sql))).Query;
        ZSet<StructuralRow, Z64> accumulated;
        using (TraceBlocksMode.Use(join: true, freezeEntries: freeze, blockEntries: block))
        {
            var compiled = Structural(PlanOptimizer.Optimize(plan), emitted);
            _engaged += Blocked(compiled) > 0 ? 1 : 0;
            accumulated = IncrementalOracle.RunAndAccumulate(
                compiled,
                [.. ticks.Select(t => (IReadOnlyList<InputEvent>)[.. t.Where(e => compiled.Inputs.ContainsKey(e.Table))])]);
        }

        var tableStates = new Dictionary<string, ZSet<StructuralRow, Z64>>(StringComparer.Ordinal);
        foreach (var ddl in RandomQuery.FixedDdl)
        {
            var open = ddl.IndexOf('(', StringComparison.Ordinal);
            var name = ddl["CREATE TABLE ".Length..open].Trim();
            tableStates[name] = IncrementalOracle.NetTable(ticks.SelectMany(t => t), name);
        }

        var batch = BatchPlanEvaluator.Evaluate(plan, new BatchEvalContext(tableStates, new Dictionary<CteRef, ZSet<StructuralRow, Z64>>()));
        if (RandomQuery.OutsideNullableSumDomain(sql, ticks.SelectMany(t => t)) || accumulated.Equals(batch))
        {
            return true;
        }

        Console.Error.WriteLine($"SQL (blocked, freeze={freeze}, block={block}, emitted={emitted}): {sql}");
        Console.Error.WriteLine("accumulated: " + accumulated);
        Console.Error.WriteLine("batch:       " + batch);
        return false;
    }

    private static readonly string[] WideDdl =
    [
        "CREATE TABLE a (k INT NOT NULL, s VARCHAR, d DOUBLE, dt DATE, x BIGINT)",
        "CREATE TABLE b (k INT NOT NULL, name VARCHAR, amt DECIMAL(10, 2), flag BOOLEAN)",
    ];

    private const string WideJoin = "SELECT a.k, a.s, a.d, a.dt, a.x, b.name, b.amt, b.flag FROM a JOIN b ON a.k = b.k AND a.s = b.name";

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    public void WideStringJoin_MatchesTheManagedTwin_EveryTick(int seed)
    {
        var rnd = new Random(seed);
        CompiledQuery blocked;
        using (TraceBlocksMode.Use(join: true, freezeEntries: 1 + rnd.Next(20), blockEntries: 1 + rnd.Next(6)))
        {
            blocked = Compile(WideDdl, WideJoin, emitted: seed % 2 == 0);
        }

        CompiledQuery managed;
        using (TraceBlocksMode.Use(join: false))
        {
            managed = Compile(WideDdl, WideJoin, emitted: seed % 2 == 0);
        }

        Assert.Equal(1, Blocked(blocked));
        Assert.Equal(0, Blocked(managed));
        string?[] names = [null, "", "x", "é", "日本語", "long name value"];
        var liveA = new List<object?[]>();
        var liveB = new List<object?[]>();
        for (var tick = 0; tick < 60; tick++)
        {
            for (var n = rnd.Next(0, 12); n > 0; n--)
            {
                var deleting = liveA.Count > 0 && rnd.Next(4) == 0;
                var row = deleting
                    ? liveA[rnd.Next(liveA.Count)]
                    : [rnd.Next(6), names[rnd.Next(names.Length)], rnd.Next(4) == 0 ? null : rnd.Next(3) / 2.0, rnd.Next(3) == 0 ? null : new Date32(18_000 + rnd.Next(3)), (long)rnd.Next(100)];
                foreach (var q in new[] { blocked, managed })
                {
                    if (deleting)
                    {
                        q.Table("a").Delete(row);
                    }
                    else
                    {
                        q.Table("a").Insert(row);
                    }
                }

                if (deleting)
                {
                    liveA.Remove(row);
                }
                else
                {
                    liveA.Add(row);
                }
            }

            for (var n = rnd.Next(0, 8); n > 0; n--)
            {
                var deleting = liveB.Count > 0 && rnd.Next(4) == 0;
                var row = deleting
                    ? liveB[rnd.Next(liveB.Count)]
                    : [rnd.Next(6), names[rnd.Next(names.Length)], rnd.Next(3) == 0 ? null : (rnd.Next(1000) / 100.0).ToString("0.00", System.Globalization.CultureInfo.InvariantCulture), rnd.Next(3) == 0 ? null : rnd.Next(2) == 0];
                foreach (var q in new[] { blocked, managed })
                {
                    if (deleting)
                    {
                        q.Table("b").Delete(row);
                    }
                    else
                    {
                        q.Table("b").Insert(row);
                    }
                }

                if (deleting)
                {
                    liveB.Remove(row);
                }
                else
                {
                    liveB.Add(row);
                }
            }

            blocked.Step();
            managed.Step();
            Assert.Equal(managed.Current, blocked.Current);
        }

        var op = blocked.Circuit.Operators.OfType<BlockedJoinOp>().Single();
        Assert.True(op.LeftTrace.RunEntries.Count + op.RightTrace.RunEntries.Count > 0, "the traces never froze");
    }

    [Fact]
    public void Compiler_EngagesBlocks_OnlyForAResidualFreeStructuralInnerJoin()
    {
        string[] ddl = ["CREATE TABLE t (k INT NOT NULL, v INT NOT NULL)", "CREATE TABLE u (k INT NOT NULL, v INT NOT NULL)"];
        using (TraceBlocksMode.Use(join: true))
        {
            Assert.Equal(1, Blocked(Compile(ddl, "SELECT t.v, u.v FROM t JOIN u ON t.k = u.k")));
            Assert.Equal(0, Blocked(Compile(ddl, "SELECT t.v, u.v FROM t JOIN u ON t.k = u.k AND t.v < u.v")));
            Assert.Equal(0, Blocked(Compile(ddl, "SELECT t.v, u.v FROM t LEFT JOIN u ON t.k = u.k")));
        }

        using (TraceBlocksMode.Use(join: false))
        {
            Assert.Equal(0, Blocked(Compile(ddl, "SELECT t.v, u.v FROM t JOIN u ON t.k = u.k")));
        }
    }
}
