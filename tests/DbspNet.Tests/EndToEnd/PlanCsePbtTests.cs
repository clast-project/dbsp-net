// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using CsCheck;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Optimizer;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

namespace DbspNet.Tests.EndToEnd;

/// <summary>
/// Property-based hardening for <see cref="PlanCse"/>: the general random-query PBT
/// (<see cref="RandomQueryPbtTests"/>) rarely *generates* a query that spells the
/// same subquery twice, so it under-exercises the exact shape CSE targets. This
/// deliberately injects a duplicated subquery — the two copies are separate parses,
/// structurally identical, and so candidates for interning — in the three shapes
/// that matter: both copies under one parent (<c>UNION ALL</c>), feeding two join
/// inputs (self-join), and feeding two <em>different</em> parents (q5's join-to-MAX).
/// For every generated (SQL, tick stream) the CSE-optimized circuit must agree with
/// the batch oracle over the un-optimized plan (via
/// <see cref="RandomQueryPbtTests.CheckOne"/>).
/// </summary>
public class PlanCsePbtTests
{
    private static readonly Gen<string> Tbl = Gen.OneOfConst("t", "u");
    private static readonly Gen<string> Cmp = Gen.OneOfConst("=", "<>", "<", "<=", ">", ">=");
    private static readonly Gen<int> Lit = Gen.Int[-5, 15];

    // Curated inner subqueries, each guaranteed to expose an INT column `k` (so the
    // wrappers below can always project/join on it) and to root at an intern-eligible
    // node (Project / Filter / Aggregate). The full random-query generator is not
    // reused here because most of its shapes don't expose a stable join key.
    private static readonly Gen<string> Inner = Gen.OneOf(
        Tbl.Select(t => $"SELECT k, v FROM {t}"),
        Gen.Select(Tbl, Cmp, Lit).Select(p => $"SELECT k, v FROM {p.Item1} WHERE v {p.Item2} {p.Item3}"),
        Gen.Select(Tbl, Lit).Select(p => $"SELECT k, v + {p.Item2} AS w FROM {p.Item1}"),
        Tbl.Select(t => $"SELECT k, SUM(v) AS s FROM {t} GROUP BY k"),
        Tbl.Select(t => $"SELECT k, COUNT(*) AS c FROM {t} GROUP BY k"));

    // Wrap one inner query so it appears twice. The `{q}` text is substituted into
    // both slots, so the two parses are structurally identical (CSE's precondition).
    private static readonly Gen<string> Duplicated = Inner.SelectMany(q => Gen.OneOfConst(
        // Both copies under one parent (UNION ALL → 2× the rows).
        $"SELECT a.k FROM ({q}) a UNION ALL SELECT b.k FROM ({q}) b",
        // Copies as the two inputs of a self-join.
        $"SELECT a.k FROM ({q}) a JOIN ({q}) b ON a.k = b.k",
        // The q5 shape: one copy feeds the output side, the other feeds a MAX the
        // output is joined against. (The optimizer may narrow the MAX-side copy, in
        // which case CSE correctly declines to share — this asserts correctness
        // either way, not that sharing fires.)
        $"SELECT a.k FROM ({q}) a JOIN (SELECT MAX(k) AS mk FROM ({q}) c) m ON a.k = m.mk"));

    [Fact]
    public void DuplicatedSubplan_CseOptimizedEqualsBatch()
    {
        Gen.Select(Duplicated, RandomQuery.GenTicks)
            .Sample((sql, ticks) => RandomQueryPbtTests.CheckOne(sql, ticks, optimize: true), iter: 2000);
    }

    /// <summary>
    /// Non-vacuity guard: the symmetric injection shapes (UNION ALL, self-join) place
    /// the two copies in positions the optimizer rewrites identically, so CSE must
    /// share them and the compiler must serve the second reference from its per-plan
    /// cache (<see cref="PlanToCircuit.MemoHits"/> &gt; 0). If this regressed to 0,
    /// the PBT above would be silently testing nothing.
    /// </summary>
    [Fact]
    public void InjectionShapes_DoTriggerSharing()
    {
        const string q = "SELECT k, SUM(v) AS s FROM t GROUP BY k";
        var shapes = new[]
        {
            $"SELECT a.k FROM ({q}) a UNION ALL SELECT b.k FROM ({q}) b",
            $"SELECT a.k FROM ({q}) a JOIN ({q}) b ON a.k = b.k",
        };

        foreach (var sql in shapes)
        {
            PlanToCircuit.MemoHits = 0;
            var catalog = new Catalog();
            var resolver = new Resolver(catalog);
            foreach (var ddl in RandomQuery.FixedDdl)
            {
                resolver.Resolve(Parser.ParseStatement(ddl));
            }

            var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(sql))).Query;
            PlanToCircuit.Compile(PlanOptimizer.Optimize(plan));
            Assert.True(PlanToCircuit.MemoHits > 0, $"expected CSE sharing for: {sql}");
        }
    }
}
