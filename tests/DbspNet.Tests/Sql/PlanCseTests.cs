// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections;
using System.Reflection;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Optimizer;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

namespace DbspNet.Tests.Sql;

/// <summary>
/// Common-subexpression elimination (<see cref="PlanCse"/>): a subquery spelled
/// twice must collapse to a single shared subplan instance (so the plan→circuit
/// compiler builds it once) WITHOUT changing results. The motivating case is
/// Nexmark q5, which writes the same windowed per-auction bid count twice — once
/// for the per-auction output, once inside the per-window <c>MAX</c> — and so ran
/// the 5× HOP fan-out + count aggregate twice before CSE.
/// </summary>
public class PlanCseTests
{
    // A q5-shaped query WITHOUT the HOP window: the identical
    // `(SELECT k, COUNT(*) AS c FROM t GROUP BY k)` subquery appears twice — once
    // directly, once inside a per-nothing MAX — joined on the "count >= max" band.
    // CSE must share the two count aggregates.
    private const string DupQuery =
        @"SELECT ab.k, ab.c
          FROM (SELECT k, COUNT(*) AS c FROM t GROUP BY k) AS ab
          JOIN (
            SELECT MAX(cb.c) AS maxc
            FROM (SELECT k, COUNT(*) AS c FROM t GROUP BY k) AS cb
          ) AS mb
          ON ab.c >= mb.maxc";

    private static readonly string[] DupDdl = { "CREATE TABLE t (k BIGINT, v BIGINT)" };

    // The verbatim Nexmark q5 shape (HOP self-join), kept in sync with the benchmark
    // — the specific regression this pass fixes.
    private static readonly string[] BidDdl =
        { "CREATE TABLE bid (auction BIGINT, price BIGINT, date_time TIMESTAMP)" };

    private const string Q5 =
        @"SELECT AuctionBids.auction, AuctionBids.num
          FROM (
            SELECT B1.auction, COUNT(*) AS num,
                   window_start AS starttime, window_end AS endtime
            FROM TABLE(HOP(TABLE bid, DESCRIPTOR(date_time), INTERVAL '2' SECOND, INTERVAL '10' SECOND)) AS B1
            GROUP BY B1.auction, window_start, window_end
          ) AS AuctionBids
          JOIN (
            SELECT MAX(CountBids.num) AS maxn, CountBids.starttime, CountBids.endtime
            FROM (
              SELECT COUNT(*) AS num, window_start AS starttime, window_end AS endtime
              FROM TABLE(HOP(TABLE bid, DESCRIPTOR(date_time), INTERVAL '2' SECOND, INTERVAL '10' SECOND)) AS B2
              GROUP BY B2.auction, window_start, window_end
            ) AS CountBids
            GROUP BY CountBids.starttime, CountBids.endtime
          ) AS MaxBids
          ON AuctionBids.starttime = MaxBids.starttime
            AND AuctionBids.endtime = MaxBids.endtime
            AND AuctionBids.num >= MaxBids.maxn";

    [Fact]
    public void Cse_SharesDuplicatedAggregateSubquery()
    {
        var optimized = OptimizedPlan(DupDdl, DupQuery);

        // Before CSE: two COUNT aggregates + one MAX aggregate = 3 distinct instances.
        // After CSE: the two counts share, so 2 distinct AggregatePlan instances.
        var aggregates = DistinctOfType<AggregatePlan>(optimized);
        Assert.Equal(2, aggregates.Count);
    }

    [Fact]
    public void Cse_SharesQ5HopWindowedCount()
    {
        var optimized = OptimizedPlan(BidDdl, Q5);

        // q5 spells HOP(bid)+GROUP BY twice. Each HOP lowers to a 5-branch UNION ALL.
        // CSE must collapse the two identical fan-outs + count aggregates to one each.
        Assert.Single(DistinctOfType<UnionAllPlan>(optimized));

        // Distinct aggregates: the shared per-(auction,window) COUNT + the per-window
        // MAX = 2 (was 3 before CSE).
        Assert.Equal(2, DistinctOfType<AggregatePlan>(optimized).Count);

        // The single base-table scan is shared across all five branches.
        Assert.Single(DistinctOfType<ScanPlan>(optimized));
    }

    [Fact]
    public void Cse_SharedSubplanCompilesOnce()
    {
        // Compiler-level proof (not just plan-level): the interned shared subplan is
        // served from the per-reference compile cache instead of being re-emitted, on
        // both the typed fast path and the structural fallback.
        PlanToCircuit.MemoHits = 0;
        PlanToCircuit.MemoMisses = 0;
        PlanToCircuit.Compile(OptimizedPlan(DupDdl, DupQuery));
        Assert.True(PlanToCircuit.MemoHits > 0, "expected ≥1 shared-subplan compile-cache hit");
    }

    [Fact]
    public void Cse_PreservesResults_AgainstUnoptimized()
    {
        // Same input stream through the CSE'd plan and the un-optimized plan; the
        // output change-sets must be identical after every step. This is the
        // semantic-equivalence guard specific to the CSE rewrite.
        var rng = new Random(20260721);
        var rows = new List<(long k, long v)>();
        for (var i = 0; i < 200; i++)
        {
            rows.Add((rng.Next(0, 12), rng.Next(0, 100)));
        }

        var withCse = PlanToCircuit.Compile(OptimizedPlan(DupDdl, DupQuery));
        var noCse = PlanToCircuit.Compile(RawPlan(DupDdl, DupQuery));

        // Feed in 5 micro-batches, comparing the materialized output after each.
        for (var batch = 0; batch < 5; batch++)
        {
            foreach (var (k, v) in rows.Skip(batch * 40).Take(40))
            {
                withCse.Table("t").Insert(k, v);
                noCse.Table("t").Insert(k, v);
            }

            withCse.Step();
            noCse.Step();
            AssertSameOutput(noCse.Current, withCse.Current);
        }
    }

    // ---- helpers --------------------------------------------------------------

    private static LogicalPlan RawPlan(string[] ddl, string sql)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in ddl)
        {
            resolver.Resolve(Parser.ParseStatement(s));
        }

        return ((SelectPlan)resolver.Resolve(Parser.ParseStatement(sql))).Query;
    }

    private static LogicalPlan OptimizedPlan(string[] ddl, string sql) =>
        PlanOptimizer.Optimize(RawPlan(ddl, sql));

    private static void AssertSameOutput(ZSet<StructuralRow, Z64> expected, ZSet<StructuralRow, Z64> actual)
    {
        foreach (var (row, w) in expected)
        {
            Assert.Equal(w.Value, actual.WeightOf(row).Value);
        }

        foreach (var (row, w) in actual)
        {
            Assert.Equal(w.Value, expected.WeightOf(row).Value);
        }
    }

    // Collect every distinct-by-reference plan node of a given kind, walking child
    // plans reflectively (LogicalPlan-typed or IEnumerable<LogicalPlan> properties).
    private static List<T> DistinctOfType<T>(LogicalPlan root) where T : LogicalPlan
    {
        var seen = new HashSet<LogicalPlan>(ReferenceEqualityComparer.Instance);
        var hits = new List<T>();
        var stack = new Stack<LogicalPlan>();
        stack.Push(root);
        while (stack.Count > 0)
        {
            var node = stack.Pop();
            if (!seen.Add(node))
            {
                continue;
            }

            if (node is T t)
            {
                hits.Add(t);
            }

            foreach (var child in Children(node))
            {
                stack.Push(child);
            }
        }

        return hits;
    }

    private static IEnumerable<LogicalPlan> Children(LogicalPlan plan)
    {
        foreach (var p in plan.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            if (typeof(LogicalPlan).IsAssignableFrom(p.PropertyType))
            {
                if (p.GetValue(plan) is LogicalPlan child)
                {
                    yield return child;
                }
            }
            else if (p.PropertyType != typeof(string) && typeof(IEnumerable).IsAssignableFrom(p.PropertyType))
            {
                if (p.GetValue(plan) is IEnumerable seq)
                {
                    foreach (var item in seq)
                    {
                        if (item is LogicalPlan child)
                        {
                            yield return child;
                        }
                    }
                }
            }
        }
    }
}
