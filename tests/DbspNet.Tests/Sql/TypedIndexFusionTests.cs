// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Circuit.Operators;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Optimizer;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;
using Xunit;

namespace DbspNet.Tests.Sql;

/// <summary>
/// Typed-path index fusion (docs/design-state-representation.md §9.13): a join's or
/// aggregate's input Filter/Project chain, and a once-scanned table's structural→typed
/// lift, run inside the pass that indexes the input (<c>StepGroupProject</c> at W=1,
/// <c>ExchangeIndexStepOp</c> at W&gt;1). Pure row functions and associative Z-set
/// addition make it exact for <b>arbitrary signed</b> streams, so fused ≡ unfused is
/// checked on that full surface, single-circuit and parallel.
/// </summary>
public class TypedIndexFusionTests
{
    private static readonly string[] Ddl =
    {
        "CREATE TABLE t (k INT NOT NULL, a INT NOT NULL, b INT)",
        "CREATE TABLE u (k INT NOT NULL, c INT NOT NULL, d INT NOT NULL)",
    };

    public static readonly TheoryData<string> Queries = new()
    {
        // The optimizer's join-input pruning inserts a projection on each side: lift + chain.
        "SELECT t.a, u.c FROM t JOIN u ON t.k = u.k",
        "SELECT x.a1, y.c FROM (SELECT k, a + 1 AS a1 FROM t WHERE b > 0) x " +
        "JOIN (SELECT k, c FROM u WHERE d < 2) y ON x.k = y.k",
        // A projection that collapses distinct input rows (weights must sum).
        "SELECT x.k2, u.c FROM (SELECT k AS k2 FROM t) x JOIN u ON x.k2 = u.k",
        // t scanned twice: its lift is shared, not deferred.
        "SELECT x.a, y.a FROM t x JOIN t y ON x.k = y.k",
        "SELECT k, COUNT(*) AS n, SUM(a) AS s FROM t WHERE b > 0 GROUP BY k",
        "SELECT a + 1 AS g, COUNT(*) AS n FROM t GROUP BY a + 1",
        "SELECT k, MIN(a) AS lo, MAX(a) AS hi FROM t GROUP BY k",
        "SELECT t.a, SUM(u.c) AS s FROM t JOIN u ON t.k = u.k GROUP BY t.a",
    };

    [Theory]
    [MemberData(nameof(Queries))]
    public void Fused_EqualsUnfused_SingleAndParallel_OverArbitrarySignedStreams(string sql)
    {
        var plan = Plan(sql);
        for (var seed = 0; seed < 60; seed++)
        {
            var ticks = GenerateSigned(seed);
            var unfused = RunSingle(plan, fused: false, ticks);
            var fused = RunSingle(plan, fused: true, ticks);
            Assert.True(Same(unfused, fused), $"seed {seed} W=1: fused {Show(fused)}\nunfused {Show(unfused)}");

            var parallel = RunParallel(plan, ticks);
            Assert.True(Same(unfused, parallel), $"seed {seed} W=3: fused {Show(parallel)}\nunfused {Show(unfused)}");
        }
    }

    [Fact]
    public void Fusion_RemovesTheLiftAndProjectionOperators()
    {
        var plan = Plan("SELECT t.a, u.c FROM t JOIN u ON t.k = u.k");
        var fused = ApplyOps(WithFusion(true, () => PlanToCircuit.Compile(plan)));
        var unfused = ApplyOps(WithFusion(false, () => PlanToCircuit.Compile(plan)));
        Assert.True(fused < unfused, $"fused {fused} ApplyOps, unfused {unfused}");
    }

    private static int ApplyOps(CompiledQuery q) => q.Circuit.Operators.Count(o =>
        o.GetType().IsGenericType && o.GetType().GetGenericTypeDefinition() == typeof(ApplyOp<,>));

    private static Dictionary<string, long> RunSingle(LogicalPlan plan, bool fused, List<List<(string, object?[], int)>> ticks)
    {
        var q = WithFusion(fused, () => PlanToCircuit.Compile(plan));
        var acc = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach (var tick in ticks)
        {
            foreach (var (table, row, w) in tick.Where(e => q.Inputs.ContainsKey(e.Item1)))
            {
                if (w > 0) q.Table(table).Insert(row);
                else q.Table(table).Delete(row);
            }

            q.Step();
            foreach (var (k, v) in q.Current) Add(acc, Key(k.ToArray()), v.Value);
        }

        return acc;
    }

    private static Dictionary<string, long> RunParallel(LogicalPlan plan, List<List<(string, object?[], int)>> ticks)
    {
        var p = WithFusion(true, () =>
        {
            Assert.True(TypedPlanCompiler.TryCompileParallel(plan, 3, out var compiled), "parallel compile failed");
            return compiled!;
        });
        var acc = new Dictionary<string, long>(StringComparer.Ordinal);
        using (p)
        {
            foreach (var tick in ticks)
            {
                foreach (var (table, row, w) in tick.Where(e => p.Inputs.ContainsKey(e.Item1)))
                {
                    if (w > 0) p.Table(table).Insert(row);
                    else p.Table(table).Delete(row);
                }

                p.Step();
                foreach (var (values, weight) in p.Current) Add(acc, Key(values), weight);
            }
        }

        return acc;
    }

    private static T WithFusion<T>(bool fused, Func<T> body)
    {
        var prev = IndexFusionMode.Enabled;
        IndexFusionMode.Enabled = fused;
        try
        {
            return body();
        }
        finally
        {
            IndexFusionMode.Enabled = prev;
        }
    }

    private static string Key(object?[] values) => string.Join("|", values.Select(v => v?.ToString() ?? "<null>"));

    private static void Add(Dictionary<string, long> map, string key, long w)
    {
        var v = map.GetValueOrDefault(key) + w;
        if (v == 0) map.Remove(key);
        else map[key] = v;
    }

    private static bool Same(Dictionary<string, long> a, Dictionary<string, long> b) =>
        a.Count == b.Count && a.All(kv => b.TryGetValue(kv.Key, out var v) && v == kv.Value);

    private static string Show(Dictionary<string, long> m) =>
        string.Join(", ", m.OrderBy(e => e.Key, StringComparer.Ordinal).Select(e => $"{e.Key}:{e.Value}"));

    private static LogicalPlan Plan(string sql)
    {
        var resolver = new Resolver(new Catalog());
        foreach (var s in Ddl) resolver.Resolve(Parser.ParseStatement(s));
        return PlanOptimizer.Optimize(((SelectPlan)resolver.Resolve(Parser.ParseStatement(sql))).Query);
    }

    // Arbitrary ±1 events (deletes of rows never inserted included); small ranges so keys
    // match often and projections collide.
    private static List<List<(string, object?[], int)>> GenerateSigned(int seed)
    {
        var rng = new Random(seed);
        var ticks = new List<List<(string, object?[], int)>>();
        for (var t = 0; t < 3 + rng.Next(5); t++)
        {
            var tick = new List<(string, object?[], int)>();
            for (var i = rng.Next(10); i > 0; i--)
            {
                var table = rng.Next(2) == 0 ? "t" : "u";
                object?[] row = table == "t"
                    ? [rng.Next(0, 3), rng.Next(0, 3), rng.Next(4) == 0 ? null : rng.Next(0, 3)]
                    : [rng.Next(0, 3), rng.Next(0, 3), rng.Next(0, 3)];
                tick.Add((table, row, rng.Next(2) == 0 ? 1 : -1));
            }

            ticks.Add(tick);
        }

        return ticks;
    }
}
