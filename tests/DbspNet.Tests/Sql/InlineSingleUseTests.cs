// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit.Operators;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;
using DbspNet.Tests.EndToEnd;
using Xunit;

namespace DbspNet.Tests.Sql;

/// <summary>
/// Single-use inlining (docs/design-state-representation.md §9.11,
/// <see cref="InlineSingleUseMode"/>): a CTE referenced once, and in a program a non-output
/// view referenced once, compiles in place at its reference so its trailing Filter/Project
/// chain fuses with the consumer's. Compiling a body in place instead of sharing its stream
/// changes no result, so inlined ≡ not inlined (and ≡ the batch oracle) holds for
/// <b>arbitrary signed</b> streams.
/// </summary>
public class InlineSingleUseTests
{
    private static readonly string[] Ddl =
    {
        "CREATE TABLE t (k INT NOT NULL, a INT NOT NULL, b INT)",
        "CREATE TABLE u (k INT NOT NULL, c INT NOT NULL)",
    };

    public static readonly TheoryData<string> CteQueries = new()
    {
        // A single-use CTE under a projection: the two chains fuse.
        "WITH s AS (SELECT k, a + 1 AS a1 FROM t WHERE b > 0) SELECT k, a1 * 2 AS a2 FROM s WHERE a1 <> 2",
        // Nested single-use CTEs.
        "WITH s AS (SELECT k, a FROM t WHERE a > 0), r AS (SELECT k, a + 1 AS a1 FROM s) SELECT k, a1 FROM r",
        // A single-use CTE as a join input and as an aggregate input.
        "WITH s AS (SELECT k, a FROM t WHERE b IS NOT NULL) SELECT s.a, u.c FROM s JOIN u ON s.k = u.k",
        "WITH s AS (SELECT k, a * 2 AS a2 FROM t) SELECT k, SUM(a2) AS total, COUNT(*) AS n FROM s GROUP BY k",
        // A CTE used twice is NOT inlined (it stays one shared stream).
        "WITH s AS (SELECT k, a FROM t WHERE a > 0) SELECT s1.a, s2.a FROM s s1 JOIN s s2 ON s1.k = s2.k",
        // A single-use CTE whose body is a join.
        "WITH j AS (SELECT t.k, t.a, u.c FROM t JOIN u ON t.k = u.k) SELECT k, a + c AS s FROM j WHERE c > 0",
    };

    [Theory]
    [MemberData(nameof(CteQueries))]
    public void InlinedCte_EqualsShared_AndBatch_OverArbitrarySignedStreams(string sql)
    {
        foreach (var structural in new[] { false, true })
        {
            for (var seed = 0; seed < 150; seed++)
            {
                var ticks = GenerateSigned(seed);
                var shared = RunQuery(sql, inline: false, structural, ticks);
                var inlined = RunQuery(sql, inline: true, structural, ticks);
                Assert.True(shared.Equals(inlined), $"seed {seed} structural={structural}: inlined diverged.\nshared:  {shared}\ninlined: {inlined}");
                var batch = Batch(sql, ticks);
                Assert.True(batch.Equals(inlined), $"seed {seed}: inlined != batch.\nbatch:   {batch}\ninlined: {inlined}");
            }
        }
    }

    [Fact]
    public void RecursiveCte_StillCompilesAndMatchesBatch()
    {
        // Reachability over u's acyclic edges (1 → 2 → 3). The self-reference must never
        // be inlined; the recursive CTE keeps its dedicated runtime.
        const string sql =
            "WITH RECURSIVE r AS (SELECT k, c FROM u " +
            "UNION ALL SELECT r.k, u.c FROM r JOIN u ON r.c = u.k) " +
            "SELECT k, c FROM r";
        var ticks = new List<IReadOnlyList<InputEvent>>
        {
            new List<InputEvent>
            {
                new("t", new object?[] { 1, 0, 0 }, 1),
                new("u", new object?[] { 1, 2 }, 1),
                new("u", new object?[] { 2, 3 }, 1),
            },
        };
        Assert.Equal(RunQuery(sql, inline: false, structural: true, ticks), RunQuery(sql, inline: true, structural: true, ticks));
    }

    [Fact]
    public void SingleUseCte_FusesWithItsConsumer()
    {
        const string sql = "WITH s AS (SELECT k, a + 1 AS a1 FROM t WHERE b > 0) SELECT k, a1 * 2 AS a2 FROM s WHERE a1 <> 2";
        Assert.True(
            ApplyOps(Compile(sql, inline: true, structural: true)) < ApplyOps(Compile(sql, inline: false, structural: true)),
            "inlining the CTE did not fuse its chain with the consumer's");
    }

    // ---- program views ----

    private static readonly string[] Program =
    [
        "CREATE TABLE t (k INT NOT NULL, a INT NOT NULL, b INT)",
        "CREATE TABLE u (k INT NOT NULL, c INT NOT NULL)",
        // Single-use, non-output: inlined into v2.
        "CREATE VIEW v1 AS SELECT k, a + 1 AS a1 FROM t WHERE b > 0",
        // Single-use, non-output, itself containing an inlined view: inlined into out1.
        "CREATE VIEW v2 AS SELECT k, a1 * 2 AS a2 FROM v1 WHERE a1 <> 2",
        // Used twice: stays its own stream.
        "CREATE VIEW shared AS SELECT k, c FROM u WHERE c > 0",
        "CREATE VIEW out1 AS SELECT v2.a2, shared.c FROM v2 JOIN shared ON v2.k = shared.k",
        "CREATE VIEW out2 AS SELECT k, COUNT(*) AS n FROM shared GROUP BY k",
        // An output view referenced by another view: never inlined (it needs its stream).
        "CREATE VIEW out3 AS SELECT k, a FROM t",
        "CREATE VIEW out4 AS SELECT k, a * 3 AS a3 FROM out3",
    ];

    private static readonly string[] ProgramOutputs = ["out1", "out2", "out3", "out4"];

    [Fact]
    public void InlinedViews_EqualShared_OverArbitrarySignedStreams()
    {
        for (var seed = 0; seed < 150; seed++)
        {
            var ticks = GenerateSigned(seed);
            var shared = RunProgram(inline: false, ticks);
            var inlined = RunProgram(inline: true, ticks);
            foreach (var view in ProgramOutputs)
            {
                Assert.True(
                    shared[view].Equals(inlined[view]),
                    $"seed {seed} view {view}: inlined diverged.\nshared:  {shared[view]}\ninlined: {inlined[view]}");
            }
        }
    }

    [Fact]
    public void Program_InliningRemovesOperators()
    {
        Assert.True(
            ApplyOps(CompileProgram(inline: true)) < ApplyOps(CompileProgram(inline: false)),
            "inlining v1/v2 did not remove any pointwise operator");
    }

    private static Dictionary<string, ZSet<StructuralRow, Z64>> RunProgram(bool inline, IReadOnlyList<IReadOnlyList<InputEvent>> ticks)
    {
        var prog = CompileProgram(inline);
        foreach (var tick in ticks)
        {
            foreach (var e in tick)
            {
                if (e.Weight > 0)
                {
                    prog.Table(e.Table).Insert(e.Row);
                }
                else
                {
                    prog.Table(e.Table).Delete(e.Row);
                }
            }

            prog.Step();
        }

        return ProgramOutputs.ToDictionary(v => v, v => prog.Outputs[v].View.Current);
    }

    private static CompiledProgram CompileProgram(bool inline) =>
        WithInline(inline, () => SqlProgram.Compile(Program, new HashSet<string>(ProgramOutputs, StringComparer.Ordinal)));

    // ---- helpers ----

    private static int ApplyOps(CompiledQuery q) => ApplyOps(q.Circuit.Operators);

    private static int ApplyOps(CompiledProgram p) => ApplyOps(p.Circuit.Operators);

    private static int ApplyOps(IEnumerable<object> ops) => ops.Count(o =>
        o.GetType().IsGenericType && o.GetType().GetGenericTypeDefinition() == typeof(ApplyOp<,>));

    private static T WithInline<T>(bool inline, Func<T> body)
    {
        var prev = InlineSingleUseMode.Enabled;
        InlineSingleUseMode.Enabled = inline;
        try
        {
            return body();
        }
        finally
        {
            InlineSingleUseMode.Enabled = prev;
        }
    }

    private static CompiledQuery Compile(string sql, bool inline, bool structural) =>
        WithInline(inline, () => PlanToCircuit.Compile(
            Parse(sql),
            // ShareArrangements is structural-only, so it forces that compile path.
            options: structural ? new CompileOptions { ShareArrangements = true } : null));

    private static ZSet<StructuralRow, Z64> RunQuery(
        string sql, bool inline, bool structural, IReadOnlyList<IReadOnlyList<InputEvent>> ticks)
    {
        var compiled = Compile(sql, inline, structural);
        var filtered = ticks
            .Select(t => (IReadOnlyList<InputEvent>)t.Where(e => compiled.Inputs.ContainsKey(e.Table)).ToList())
            .ToList();
        return IncrementalOracle.RunAndAccumulate(compiled, filtered);
    }

    private static ZSet<StructuralRow, Z64> Batch(string sql, IReadOnlyList<IReadOnlyList<InputEvent>> ticks)
    {
        var events = ticks.SelectMany(t => t).ToList();
        var tables = new Dictionary<string, ZSet<StructuralRow, Z64>>(StringComparer.Ordinal)
        {
            ["t"] = IncrementalOracle.NetTable(events, "t"),
            ["u"] = IncrementalOracle.NetTable(events, "u"),
        };
        return BatchPlanEvaluator.Evaluate(
            Parse(sql), new BatchEvalContext(tables, new Dictionary<CteRef, ZSet<StructuralRow, Z64>>()));
    }

    private static LogicalPlan Parse(string sql)
    {
        var resolver = new Resolver(new Catalog());
        foreach (var ddl in Ddl)
        {
            resolver.Resolve(Parser.ParseStatement(ddl));
        }

        return ((SelectPlan)resolver.Resolve(Parser.ParseStatement(sql))).Query;
    }

    private static List<IReadOnlyList<InputEvent>> GenerateSigned(int seed)
    {
        var rng = new Random(seed);
        var ticks = new List<IReadOnlyList<InputEvent>>();
        for (var t = 0; t < 3 + rng.Next(6); t++)
        {
            var tick = new List<InputEvent>();
            for (var i = rng.Next(10); i > 0; i--)
            {
                var table = rng.Next(2) == 0 ? "t" : "u";
                object?[] row = table == "t"
                    ? [rng.Next(0, 3), rng.Next(0, 3), rng.Next(4) == 0 ? null : rng.Next(0, 3)]
                    : [rng.Next(0, 3), rng.Next(0, 3)];
                tick.Add(new InputEvent(table, row, rng.Next(2) == 0 ? 1 : -1));
            }

            ticks.Add(tick);
        }

        return ticks;
    }
}
