// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

namespace DbspNet.Tests.Sql;

/// <summary>
/// The compact aggregator sub-state of docs/design-state-representation.md §4:
/// unboxed, FILTER-shared COUNT(DISTINCT) tables and the (best, count) MIN/MAX
/// that rescans the trace. Differential: the typed path with the compact state
/// on, the typed path with it off (the boxed / sorted-set fallback) and the
/// structural path must integrate to the same view after every tick of a random
/// insert/delete stream with duplicated values, NULLs and retractions of the
/// current extreme.
/// </summary>
public class CompactAggregatorStateTests
{
    private static readonly string[] Ddl =
        ["CREATE TABLE t (g INT NOT NULL, id INT NOT NULL, v INT, p INT NOT NULL, s VARCHAR)"];

    private const string Query =
        @"SELECT g,
                 COUNT(*) AS n,
                 COUNT(DISTINCT v) AS dv,
                 COUNT(DISTINCT v) FILTER (WHERE p < 3) AS dv_lo,
                 COUNT(DISTINCT v) FILTER (WHERE p >= 3 AND p < 6) AS dv_mid,
                 COUNT(*) FILTER (WHERE p >= 6) AS n_hi,
                 COUNT(DISTINCT v) FILTER (WHERE p >= 6) AS dv_hi,
                 COUNT(DISTINCT p) AS dp,
                 COUNT(DISTINCT p) FILTER (WHERE v IS NULL) AS dp_nullv,
                 COUNT(DISTINCT s) AS ds,
                 COUNT(DISTINCT v + p) AS dvp,
                 MIN(v) AS minv, MAX(v) AS maxv,
                 MIN(p) AS minp, MAX(p) FILTER (WHERE v > 10) AS maxp_v
          FROM t
          GROUP BY g";

    private static CompiledQuery Compile(bool structural)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in Ddl)
        {
            resolver.Resolve(Parser.ParseStatement(s));
        }

        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(Query))).Query;
        return structural
            ? PlanToCircuit.Compile(plan, EmittedEqualityCodec.Instance)
            : PlanToCircuit.Compile(plan);
    }

    private static CompiledQuery CompileTyped(bool compact)
    {
        var (d, m) = (CompactAggregatorStateMode.CompactDistinct, CompactAggregatorStateMode.CompactMinMax);
        CompactAggregatorStateMode.CompactDistinct = compact;
        CompactAggregatorStateMode.CompactMinMax = compact;
        try
        {
            return Compile(structural: false);
        }
        finally
        {
            CompactAggregatorStateMode.CompactDistinct = d;
            CompactAggregatorStateMode.CompactMinMax = m;
        }
    }

    private static void Accumulate(Dictionary<string, long> view, CompiledQuery q)
    {
        foreach (var (row, w) in q.Current)
        {
            var key = string.Join("|", Enumerable.Range(0, row.Count).Select(i => row[i]?.ToString() ?? "NULL"));
            var n = view.GetValueOrDefault(key) + w.Value;
            if (n == 0) view.Remove(key);
            else view[key] = n;
        }
    }

    [Theory]
    [InlineData(20261028, 4, 12)]
    [InlineData(7, 2, 40)]
    [InlineData(99, 9, 3)]
    public void Compact_Fallback_And_Structural_IntegrateToTheSameView(int seed, int groups, int valueRange)
    {
        var rng = new Random(seed);
        var compact = CompileTyped(compact: true);
        var fallback = CompileTyped(compact: false);
        var structural = Compile(structural: true);
        var queries = new[] { compact, fallback, structural };
        var views = queries.Select(_ => new Dictionary<string, long>()).ToArray();

        var live = new List<object?[]>();
        var nextId = 0;
        for (var tick = 0; tick < 60; tick++)
        {
            var ops = rng.Next(1, 30);
            for (var op = 0; op < ops; op++)
            {
                // Bias deletes toward the extremes so MIN/MAX lose their best often.
                if (live.Count > 0 && rng.Next(3) == 0)
                {
                    var idx = rng.Next(2) == 0
                        ? live.IndexOf(live.OrderBy(r => (int?)r[2] ?? int.MaxValue).First())
                        : rng.Next(live.Count);
                    var row = live[idx];
                    live.RemoveAt(idx);
                    foreach (var q in queries) q.Table("t").Delete(row);
                }
                else
                {
                    int? v = rng.Next(8) == 0 ? null : rng.Next(valueRange);
                    var s = rng.Next(6) == 0 ? null : "s" + rng.Next(valueRange);
                    var row = new object?[] { rng.Next(groups), nextId++, v, rng.Next(9), s };
                    live.Add(row);
                    foreach (var q in queries) q.Table("t").Insert(row);
                }
            }

            for (var i = 0; i < queries.Length; i++)
            {
                queries[i].Step();
                Accumulate(views[i], queries[i]);
            }

            Assert.Equal(views[2].OrderBy(kv => kv.Key), views[1].OrderBy(kv => kv.Key));
            Assert.Equal(views[2].OrderBy(kv => kv.Key), views[0].OrderBy(kv => kv.Key));
        }

        // And a from-scratch batch over the survivors agrees with the incremental view.
        var batch = CompileTyped(compact: true);
        foreach (var row in live) batch.Table("t").Insert(row);
        batch.Step();
        var batchView = new Dictionary<string, long>();
        Accumulate(batchView, batch);
        Assert.Equal(batchView.OrderBy(kv => kv.Key), views[0].OrderBy(kv => kv.Key));
    }

    [Fact]
    public void DistinctValueTable_MatchesDictionaryUnderChurn()
    {
        // Heavy insert/remove churn over a small key space exercises probe chains,
        // growth and backward-shift deletion; the per-member distinct counts must
        // match a Dictionary oracle after every operation.
        var rng = new Random(42);
        const int k = 3;
        var table = new DistinctValueTable<long>(k);
        var oracle = new Dictionary<long, int[]>();
        for (var step = 0; step < 200_000; step++)
        {
            var key = (long)rng.Next(5_000) * (rng.Next(2) == 0 ? 1 : 1L << 32);
            var mask = (ulong)rng.Next(1, 1 << k);
            var counts = oracle.GetValueOrDefault(key);
            var canRemove = counts is not null
                && Enumerable.Range(0, k).All(j => (mask & (1UL << j)) == 0 || counts[j] > 0);
            if (canRemove && rng.Next(2) == 0)
            {
                table.Remove(key, mask);
                for (var j = 0; j < k; j++) if ((mask & (1UL << j)) != 0) counts![j]--;
                if (counts!.All(c => c == 0)) oracle.Remove(key);
            }
            else
            {
                table.Insert(key, mask);
                counts ??= oracle[key] = new int[k];
                for (var j = 0; j < k; j++) if ((mask & (1UL << j)) != 0) counts[j]++;
            }

            if (step % 997 == 0 || step > 199_000)
            {
                Assert.Equal(oracle.Count, table.Count);
                for (var j = 0; j < k; j++)
                {
                    Assert.Equal(oracle.Values.Count(c => c[j] > 0), table.Distinct[j]);
                }
            }
        }

        // Drain everything; the table must end empty.
        foreach (var (key, counts) in oracle.ToList())
        {
            for (var j = 0; j < k; j++)
            {
                while (counts[j]-- > 0) table.Remove(key, 1UL << j);
            }
        }

        Assert.Equal(0, table.Count);
        Assert.All(table.Distinct, d => Assert.Equal(0, d));
    }
}
