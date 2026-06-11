// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using CsCheck;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

namespace DbspNet.Tests.EndToEnd;

/// <summary>
/// Correctness gate for cross-tick delta-builder pooling
/// (docs/design-row-representation.md §20, <see cref="DeltaPoolMode"/>): the
/// stateful operators reuse one output builder across ticks instead of allocating
/// a fresh dictionary each <c>Step</c>. Pooling is a pure <b>memory-reuse</b>
/// change — it must not alter any result — and is sound on a dead-after-tick edge
/// (no <c>z⁻¹</c>/terminal retains the output across ticks). The random-query
/// generator emits only <b>flat</b> SQL (joins / aggregates / filters / TOP-K — no
/// recursive CTE, so no <c>z⁻¹</c> on an operator-output delta), exactly the safe
/// envelope.
///
/// <para>So the strongest possible check is to reuse the established random-query
/// oracle with pooling ON: the pooled circuit's accumulated output must still equal
/// the batch re-computation over the original plan, across the full 3000-iteration
/// surface including retractions (<c>±1</c> weights). A failure would expose an
/// aliasing bug (an output Z-set retained across ticks and corrupted by reuse) — the
/// whole point of running it. The complementary guard is the full suite staying
/// green with the seam OFF (default), proving byte-identical-when-disabled.</para>
/// </summary>
public class DeltaPoolingPbtTests
{
    [Fact]
    public void RandomQuery_PooledCircuitEqualsBatch()
    {
        Gen.Select(RandomQuery.GenQuery, RandomQuery.GenTicks)
            .Sample((sql, ticks) => CheckOnePooled(sql, ticks), iter: 3000);
    }

    private static bool CheckOnePooled(string sql, IReadOnlyList<IReadOnlyList<InputEvent>> ticks)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var ddl in RandomQuery.FixedDdl)
        {
            resolver.Resolve(Parser.ParseStatement(ddl));
        }

        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(sql))).Query;

        // Pooling is read at operator construction, so enable it across Compile and
        // keep it for the circuit's lifetime, then restore. Thread-static: this test
        // body runs synchronously on one thread.
        var prev = DeltaPoolMode.Enabled;
        DeltaPoolMode.Enabled = true;
        ZSet<StructuralRow, Z64> accumulated;
        try
        {
            var compiled = PlanToCircuit.Compile(plan);
            var filteredTicks = ticks
                .Select(tick => (IReadOnlyList<InputEvent>)tick
                    .Where(e => compiled.Inputs.ContainsKey(e.Table))
                    .ToList())
                .ToList();
            accumulated = IncrementalOracle.RunAndAccumulate(compiled, filteredTicks);
        }
        finally
        {
            DeltaPoolMode.Enabled = prev;
        }

        var allFiltered = ticks
            .Select(tick => (IReadOnlyList<InputEvent>)tick.ToList())
            .ToList();
        var tableStates = new Dictionary<string, ZSet<StructuralRow, Z64>>(StringComparer.Ordinal);
        foreach (var ddl in RandomQuery.FixedDdl)
        {
            var open = ddl.IndexOf('(', StringComparison.Ordinal);
            var name = ddl.Substring("CREATE TABLE ".Length, open - "CREATE TABLE ".Length).Trim();
            tableStates[name] = IncrementalOracle.NetTable(allFiltered.SelectMany(t => t), name);
        }

        var ctx = new BatchEvalContext(tableStates, new Dictionary<CteRef, ZSet<StructuralRow, Z64>>());
        var batch = BatchPlanEvaluator.Evaluate(plan, ctx);

        if (!accumulated.Equals(batch))
        {
            Console.Error.WriteLine($"SQL (pooled): {sql}");
            Console.Error.WriteLine("accumulated (pooled circuit): " + accumulated);
            Console.Error.WriteLine("batch       (oracle):         " + batch);
            return false;
        }

        return true;
    }
}
