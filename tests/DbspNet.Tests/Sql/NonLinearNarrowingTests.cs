// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Optimizer;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;
using DbspNet.Tests.EndToEnd;
using Xunit;

namespace DbspNet.Tests.Sql;

/// <summary>
/// Correctness gate for the term-2 / whole-row-hash lever
/// (docs/design-row-representation.md §18): narrowing the input of a
/// <b>non-linear</b> aggregate (MIN/MAX) to <c>{group keys, argument columns}</c>,
/// which the default <c>NarrowAggregateInput</c> rule conservatively skips.
///
/// <para>The narrowing is sound only when the per-group consolidated multiset is
/// <b>non-negative</b> — i.e. for <i>well-formed</i> insert/delete streams (a row is
/// only deleted if previously inserted). These tests therefore drive
/// <b>well-formed</b> sequences (every <c>-1</c> retracts a currently-live row) that
/// deliberately create rows sharing <c>{k, val}</c> but differing in the dropped
/// columns <c>x, y</c> — exactly the "collapse two rows" case the default guard
/// protects against — and assert the narrowed circuit's output is byte-identical to
/// the full-row circuit's across every tick. The complementary regression guard is
/// the full suite staying green with the seam <b>off</b> (default), proving the
/// change is byte-identical when disabled.</para>
///
/// <para>(The random-query PBT is deliberately NOT reused here: its generator emits
/// arbitrary <c>±1</c> weights including deletes of never-inserted rows — net-negative
/// multisets outside this envelope — where narrowing is genuinely unsound. That is
/// the precondition this prototype documents, not a bug.)</para>
/// </summary>
public class NonLinearNarrowingTests
{
    private static readonly string[] Ddl =
    {
        "CREATE TABLE w (k INT NOT NULL, val INT NOT NULL, x INT NOT NULL, y INT NOT NULL)",
    };

    // MAX and MIN over `val`, grouped by `k`. The aggregate references only
    // {k, val}; x and y are droppable — narrowing collapses rows that share
    // {k, val} but differ in x/y.
    private const string Sql =
        "SELECT k, MAX(val) AS mx, MIN(val) AS mn FROM w GROUP BY k";

    [Fact]
    public void NarrowedMinMax_EqualsFullRow_OverWellFormedStreams()
    {
        // Many seeded well-formed sequences. Each must agree tick-for-tick
        // between the narrowed and full-row circuits.
        for (var seed = 0; seed < 300; seed++)
        {
            var ticks = GenerateWellFormed(seed);

            var full = RunCompiled(optimizeNarrowNonLinear: false, ticks);
            var narrowed = RunCompiled(optimizeNarrowNonLinear: true, ticks);

            Assert.True(
                full.Equals(narrowed),
                $"seed {seed}: narrowed MIN/MAX diverged from full-row.\n" +
                $"full:     {full}\nnarrowed: {narrowed}");
        }
    }

    [Fact]
    public void NarrowingActuallyFires_ForMinMax_WhenEnabled()
    {
        // Guard against a vacuous pass: confirm the seam genuinely changes the
        // compiled plan's aggregate input arity for MIN/MAX (drops x, y), and
        // does NOT when disabled.
        var plan = ParsePlan(Sql);

        var off = PlanOptimizer.Optimize(plan);
        var on = WithNarrowing(() => PlanOptimizer.Optimize(plan));

        Assert.Equal(4, AggregateInputArity(off)); // k, val, x, y (unchanged)
        Assert.Equal(2, AggregateInputArity(on));  // k, val (narrowed)
    }

    // ---- helpers ----

    private static ZSet<StructuralRow, Z64> RunCompiled(
        bool optimizeNarrowNonLinear, IReadOnlyList<IReadOnlyList<InputEvent>> ticks)
    {
        var plan = ParsePlan(Sql);
        var optimized = optimizeNarrowNonLinear
            ? WithNarrowing(() => PlanOptimizer.Optimize(plan))
            : PlanOptimizer.Optimize(plan);
        var compiled = PlanToCircuit.Compile(optimized);
        return IncrementalOracle.RunAndAccumulate(compiled, ticks);
    }

    private static T WithNarrowing<T>(Func<T> body)
    {
        var prev = NonLinearNarrowingMode.Enabled;
        NonLinearNarrowingMode.Enabled = true;
        try
        {
            return body();
        }
        finally
        {
            NonLinearNarrowingMode.Enabled = prev;
        }
    }

    private static LogicalPlan ParsePlan(string sql)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var ddl in Ddl)
        {
            resolver.Resolve(Parser.ParseStatement(ddl));
        }

        return ((SelectPlan)resolver.Resolve(Parser.ParseStatement(sql))).Query;
    }

    private static int AggregateInputArity(LogicalPlan plan) =>
        FindAggregate(plan).Input.Schema.Count;

    private static AggregatePlan FindAggregate(LogicalPlan plan) => plan switch
    {
        AggregatePlan a => a,
        ProjectPlan p => FindAggregate(p.Input),
        FilterPlan f => FindAggregate(f.Input),
        _ => throw new InvalidOperationException("no AggregatePlan found"),
    };

    /// <summary>
    /// A well-formed insert/delete sequence: maintains a list of currently-live
    /// rows; each tick inserts a few new rows and deletes a few currently-live
    /// ones, so every <c>-1</c> retracts a row that was previously inserted (the
    /// per-group integral stays a valid non-negative bag). Values are drawn from
    /// small ranges so rows frequently share <c>{k, val}</c> while differing in
    /// <c>x, y</c> — exercising the collapse case.
    /// </summary>
    private static List<IReadOnlyList<InputEvent>> GenerateWellFormed(int seed)
    {
        var rng = new Random(seed);
        var live = new List<object?[]>();
        var ticks = new List<IReadOnlyList<InputEvent>>();
        var tickCount = 4 + rng.Next(6); // 4..9 ticks

        for (var t = 0; t < tickCount; t++)
        {
            var tick = new List<InputEvent>();

            // Inserts: 0..4 new rows.
            var inserts = rng.Next(5);
            for (var i = 0; i < inserts; i++)
            {
                var row = new object?[]
                {
                    rng.Next(0, 3), // k
                    rng.Next(0, 6), // val
                    rng.Next(0, 3), // x (droppable)
                    rng.Next(0, 3), // y (droppable)
                };
                live.Add(row);
                tick.Add(new InputEvent("w", row, 1));
            }

            // Deletes: 0..2 currently-live rows (well-formed retractions).
            var deletes = Math.Min(live.Count, rng.Next(3));
            for (var d = 0; d < deletes; d++)
            {
                var idx = rng.Next(live.Count);
                var row = live[idx];
                live.RemoveAt(idx);
                tick.Add(new InputEvent("w", row, -1));
            }

            ticks.Add(tick);
        }

        return ticks;
    }
}
