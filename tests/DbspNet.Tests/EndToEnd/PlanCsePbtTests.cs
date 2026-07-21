// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using CsCheck;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
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
// Shares the "PlanCseCompileCounter" collection with PlanCseTests so their tests
// that reset/read the process-wide PlanToCircuit.MemoHits counter never run
// concurrently (xUnit parallelizes across collections by default).
[Collection("PlanCseCompileCounter")]
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

    // Ranked (top-k) inners — the PartitionedTopK node CSE learned to intern in the
    // ranking-family expansion. The current SQL surface reaches this family only via
    // the incremental TOP-K pattern (ROW_NUMBER/RANK/DENSE_RANK OVER (…) with an
    // enclosing `rn <= k` filter). Each exposes `k` so the wrappers still apply.
    // PartitionedTopK has no batch oracle, so these are validated circuit-vs-circuit.
    private static readonly Gen<string> RankFn = Gen.OneOfConst("ROW_NUMBER()", "RANK()", "DENSE_RANK()");

    private static readonly Gen<string> WindowedInner = Gen.Select(Tbl, RankFn, Gen.Int[1, 3])
        .Select(p =>
            $"SELECT k, v FROM (SELECT k, v, {p.Item2} OVER (PARTITION BY k ORDER BY v) AS rn FROM {p.Item1}) z "
            + $"WHERE rn <= {p.Item3}");

    private static readonly Gen<string> WindowedDuplicated = WindowedInner.SelectMany(q => Gen.OneOfConst(
        $"SELECT a.k FROM ({q}) a UNION ALL SELECT b.k FROM ({q}) b",
        $"SELECT a.k FROM ({q}) a JOIN ({q}) b ON a.k = b.k",
        $"SELECT a.k FROM ({q}) a JOIN (SELECT MAX(k) AS mk FROM ({q}) c) m ON a.k = m.mk"));

    /// <summary>
    /// Duplicated windowed / ranked / top-k subqueries: the CSE-optimized circuit
    /// must produce the same incremental output as the un-optimized circuit over the
    /// same tick stream. Circuit-vs-circuit (not batch oracle) because top-k has no
    /// batch evaluator — and it isolates the optimizer against the well-tested
    /// unoptimized baseline, which is exactly the CSE-preserves-semantics property.
    /// </summary>
    [Fact]
    public void DuplicatedWindowedSubplan_CseEqualsUnoptimizedCircuit()
    {
        Gen.Select(WindowedDuplicated, RandomQuery.GenTicks)
            .Sample(CircuitEquiv, iter: 1500);
    }

    private static bool CircuitEquiv(string sql, IReadOnlyList<IReadOnlyList<InputEvent>> ticks)
    {
        var optimized = Accumulate(sql, ticks, optimize: true);
        var raw = Accumulate(sql, ticks, optimize: false);
        return optimized.Equals(raw);
    }

    private static ZSet<StructuralRow, Z64> Accumulate(
        string sql, IReadOnlyList<IReadOnlyList<InputEvent>> ticks, bool optimize)
    {
        var plan = Resolve(sql);
        var compiled = PlanToCircuit.Compile(optimize ? PlanOptimizer.Optimize(plan) : plan);
        var filtered = ticks
            .Select(tick => (IReadOnlyList<InputEvent>)tick
                .Where(e => compiled.Inputs.ContainsKey(e.Table)).ToList())
            .ToList();
        return IncrementalOracle.RunAndAccumulate(compiled, filtered);
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
        // Symmetric shapes (UNION ALL, self-join) over an aggregate AND over a
        // ranked (top-k) subquery — the optimizer rewrites both copies identically, so
        // CSE must share them (proving both the base and ranking-family interning fire,
        // and that the PBTs above aren't vacuous).
        const string topk =
            "SELECT k, v FROM (SELECT k, v, ROW_NUMBER() OVER (PARTITION BY k ORDER BY v) AS rn FROM t) z WHERE rn <= 2";
        var shapes = new[]
        {
            "SELECT a.k FROM (SELECT k, SUM(v) AS s FROM t GROUP BY k) a "
                + "UNION ALL SELECT b.k FROM (SELECT k, SUM(v) AS s FROM t GROUP BY k) b",
            "SELECT a.k FROM (SELECT k, SUM(v) AS s FROM t GROUP BY k) a "
                + "JOIN (SELECT k, SUM(v) AS s FROM t GROUP BY k) b ON a.k = b.k",
            $"SELECT a.k FROM ({topk}) a JOIN ({topk}) b ON a.k = b.k",
        };

        foreach (var sql in shapes)
        {
            PlanToCircuit.MemoHits = 0;
            PlanToCircuit.Compile(PlanOptimizer.Optimize(Resolve(sql)));
            Assert.True(PlanToCircuit.MemoHits > 0, $"expected CSE sharing for: {sql}");
        }
    }

    private static LogicalPlan Resolve(string sql)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var ddl in RandomQuery.FixedDdl)
        {
            resolver.Resolve(Parser.ParseStatement(ddl));
        }

        return ((SelectPlan)resolver.Resolve(Parser.ParseStatement(sql))).Query;
    }
}
