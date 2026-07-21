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
/// Random-query PBT. For every generated (SQL, tick plan) pair:
/// <list type="number">
/// <item>parse + resolve + compile into a circuit,</item>
/// <item>push each tick's events, <c>Step</c>, accumulate per-tick output
/// deltas into a single Z-set (the "incremental" answer),</item>
/// <item>independently build the accumulated table states and invoke
/// <see cref="BatchPlanEvaluator"/> over them (the "batch" answer),</item>
/// <item>assert the two Z-sets are equal.</item>
/// </list>
/// A counterexample is either a bug in the circuit (most likely, it's the
/// larger, state-heavy code path) or a bug shared by both paths (e.g. in
/// the resolver or in a semantic helper). CsCheck shrinks to minimal
/// reproducer; set <c>CsCheck_Seed</c> to replay.
/// </summary>
public class RandomQueryPbtTests
{
    [Fact]
    public void RandomQuery_IncrementalEqualsBatch()
    {
        Gen.Select(RandomQuery.GenQuery, RandomQuery.GenTicks)
            .Sample((sql, ticks) => CheckOne(sql, ticks, optimize: false), iter: 3000);
    }

    /// <summary>
    /// The same PBT, but with <see cref="PlanOptimizer"/> applied before
    /// compilation. The batch oracle still uses the <b>original</b>
    /// unoptimized plan, so any divergence here is an optimizer bug
    /// (either the rewrite changed semantics, or the optimized plan
    /// compiles to a circuit that disagrees with the pre-optimization
    /// batch answer).
    /// </summary>
    [Fact]
    public void RandomQuery_OptimizedCircuitEqualsBatchOverOriginal()
    {
        Gen.Select(RandomQuery.GenQuery, RandomQuery.GenTicks)
            .Sample((sql, ticks) => CheckOne(sql, ticks, optimize: true), iter: 3000);
    }

    /// <summary>
    /// The same PBT, but compiling stateful operators onto the spine
    /// (LSM-style) trace family instead of the flat dictionary traces.
    /// The batch oracle is unchanged, so any divergence is a spine bug:
    /// the spine operators claim observational equivalence to their flat
    /// counterparts, and this is the sweep that proves it across the
    /// random query surface (including the StructuralRowComparer ordering
    /// the spine relies on).
    /// </summary>
    [Fact]
    public void RandomQuery_SpineCircuitEqualsBatch()
    {
        Gen.Select(RandomQuery.GenQuery, RandomQuery.GenTicks)
            .Sample(
                (sql, ticks) => CheckOne(
                    sql, ticks, optimize: false,
                    compileOptions: new CompileOptions { TraceFamily = TraceFamily.Spine }),
                iter: 3000);
    }

    internal static bool CheckOne(
        string sql,
        IReadOnlyList<IReadOnlyList<InputEvent>> ticks,
        bool optimize,
        CompileOptions? compileOptions = null)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var ddl in RandomQuery.FixedDdl)
        {
            resolver.Resolve(Parser.ParseStatement(ddl));
        }

        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(sql))).Query;

        var planForCircuit = optimize ? PlanOptimizer.Optimize(plan) : plan;
        var compiled = PlanToCircuit.Compile(planForCircuit, snapshotCodecs: null, compileOptions);

        // The compiler only declares inputs for tables the query references
        // — filter out events targeting unreferenced tables.
        var filteredTicks = ticks
            .Select(tick => (IReadOnlyList<InputEvent>)tick
                .Where(e => compiled.Inputs.ContainsKey(e.Table))
                .ToList())
            .ToList();
        var accumulated = IncrementalOracle.RunAndAccumulate(compiled, filteredTicks);

        // Batch oracle always uses the ORIGINAL (unoptimized) plan. If the
        // optimizer preserves semantics, the optimized circuit's output must
        // match this — any divergence is an optimizer bug.
        var tableStates = new Dictionary<string, ZSet<StructuralRow, Z64>>(StringComparer.Ordinal);
        foreach (var ddl in RandomQuery.FixedDdl)
        {
            var open = ddl.IndexOf('(', StringComparison.Ordinal);
            var name = ddl.Substring("CREATE TABLE ".Length, open - "CREATE TABLE ".Length).Trim();
            tableStates[name] = IncrementalOracle.NetTable(filteredTicks.SelectMany(t => t), name);
        }

        var ctx = new BatchEvalContext(tableStates, new Dictionary<CteRef, ZSet<StructuralRow, Z64>>());
        var batch = BatchPlanEvaluator.Evaluate(plan, ctx);

        if (!accumulated.Equals(batch))
        {
            var family = compileOptions?.TraceFamily ?? TraceFamily.Flat;
            Console.Error.WriteLine($"SQL (optimize={optimize}, trace={family}): {sql}");
            Console.Error.WriteLine("ticks: " + DescribeTicks(ticks));
            Console.Error.WriteLine("accumulated (circuit): " + accumulated);
            Console.Error.WriteLine("batch       (oracle):  " + batch);
            return false;
        }

        return true;
    }

    private static string DescribeTicks(IReadOnlyList<IReadOnlyList<InputEvent>> ticks)
    {
        var parts = ticks.Select(t =>
            t.Count == 0 ? "[]" : "[" + string.Join(", ", t.Select(e => e.ToString())) + "]");
        return "[" + string.Join(", ", parts) + "]";
    }
}
