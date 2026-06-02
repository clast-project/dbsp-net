// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

namespace DbspNet.Tests.Sql;

/// <summary>
/// <c>APPROX_COUNT_DISTINCT</c> (HyperLogLog) on both compile paths and both
/// trace families. The sketch is a deterministic function of the present
/// value set, so for fixed inputs the estimate is fixed (these tests are not
/// flaky) and the incremental result equals a from-scratch batch recompute
/// exactly. Small distinct sets land in the linear-counting regime and are
/// exact; large sets are checked within the estimator's error bound.
/// </summary>
public class ApproxCountDistinctTests
{
    private static CompiledQuery Compile(string[] ddl, string query, CompileMode mode = CompileMode.Typed)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in ddl)
        {
            resolver.Resolve(Parser.ParseStatement(s));
        }

        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(query))).Query;
        return mode switch
        {
            // A non-default codec disables the typed fast path, forcing the
            // structural compile (and its structural aggregator).
            CompileMode.Structural => PlanToCircuit.Compile(plan, EmittedEqualityCodec.Instance),
            CompileMode.Spine => PlanToCircuit.Compile(plan, null, new CompileOptions { TraceFamily = TraceFamily.Spine }),
            _ => PlanToCircuit.Compile(plan),
        };
    }

    public enum CompileMode { Typed, Structural, Spine }

    /// <summary>The value of the single positive-weight output row's column 0.</summary>
    private static long ScalarEstimate(CompiledQuery q)
    {
        long? found = null;
        foreach (var (row, weight) in q.Current)
        {
            if (weight.Value <= 0)
            {
                continue;
            }

            Assert.Null(found);
            found = (long)row[0]!;
        }

        Assert.NotNull(found);
        return found!.Value;
    }

    private static long WeightOf(ZSet<StructuralRow, Z64> z, params object?[] row) =>
        z.WeightOf(new StructuralRow(SqlTestHelpers.EncodeStrings(row))).Value;

    [Theory]
    [InlineData(CompileMode.Typed)]
    [InlineData(CompileMode.Structural)]
    [InlineData(CompileMode.Spine)]
    public void CountsDistinctValues_IgnoringDuplicates(CompileMode mode)
    {
        var q = Compile(
            ["CREATE TABLE t (v INT NOT NULL)"],
            "SELECT APPROX_COUNT_DISTINCT(v) AS c FROM t",
            mode);

        foreach (var v in new[] { 1, 1, 2, 2, 3, 3, 4, 5 })
        {
            q.Table("t").Insert(v);
        }

        q.Step();
        Assert.Equal(5, ScalarEstimate(q));
    }

    [Theory]
    [InlineData(CompileMode.Typed)]
    [InlineData(CompileMode.Structural)]
    [InlineData(CompileMode.Spine)]
    public void GroupBy_PerGroupDistinctCounts(CompileMode mode)
    {
        var q = Compile(
            ["CREATE TABLE t (g INT NOT NULL, v INT NOT NULL)"],
            "SELECT g, APPROX_COUNT_DISTINCT(v) AS c FROM t GROUP BY g",
            mode);

        // group 1: {10, 20, 20, 30} -> 3 distinct; group 2: {40, 40} -> 1.
        q.Table("t").Insert(1, 10);
        q.Table("t").Insert(1, 20);
        q.Table("t").Insert(1, 20);
        q.Table("t").Insert(1, 30);
        q.Table("t").Insert(2, 40);
        q.Table("t").Insert(2, 40);
        q.Step();

        Assert.Equal(2, q.Current.Count);
        Assert.Equal(1, WeightOf(q.Current, 1, 3L));
        Assert.Equal(1, WeightOf(q.Current, 2, 1L));
    }

    [Fact]
    public void IgnoresNullArguments()
    {
        var q = Compile(
            ["CREATE TABLE t (v INT)"],
            "SELECT APPROX_COUNT_DISTINCT(v) AS c FROM t");

        q.Table("t").Insert(1);
        q.Table("t").Insert(2);
        q.Table("t").Insert((object?)null);
        q.Table("t").Insert(2);
        q.Table("t").Insert((object?)null);
        q.Step();

        Assert.Equal(2, ScalarEstimate(q));
    }

    [Fact]
    public void AllNullGroup_ReturnsZero()
    {
        var q = Compile(
            ["CREATE TABLE t (g INT NOT NULL, v INT)"],
            "SELECT g, APPROX_COUNT_DISTINCT(v) AS c FROM t GROUP BY g");

        q.Table("t").Insert(1, null);
        q.Table("t").Insert(1, null);
        q.Step();

        // The group is present (positive weight) but has no non-NULL value.
        Assert.Equal(1, WeightOf(q.Current, 1, 0L));
    }

    [Theory]
    [InlineData(CompileMode.Typed)]
    [InlineData(CompileMode.Structural)]
    [InlineData(CompileMode.Spine)]
    public void Delete_RemovingLastOccurrence_DropsTheValue(CompileMode mode)
    {
        var q = Compile(
            ["CREATE TABLE t (v INT NOT NULL)"],
            "SELECT APPROX_COUNT_DISTINCT(v) AS c FROM t",
            mode);

        q.Table("t").Insert(1);
        q.Table("t").Insert(2);
        q.Table("t").Insert(3);
        q.Step();
        Assert.Equal(3, ScalarEstimate(q));

        // Remove the only row carrying value 3 — the distinct count must drop
        // (exercises the retraction-rebuild path).
        q.Table("t").Delete(3);
        q.Step();
        Assert.Equal(2, ScalarEstimate(q));
    }

    [Theory]
    [InlineData(CompileMode.Typed)]
    [InlineData(CompileMode.Structural)]
    [InlineData(CompileMode.Spine)]
    public void Delete_RemovingDuplicate_KeepsTheValue(CompileMode mode)
    {
        var q = Compile(
            ["CREATE TABLE t (v INT NOT NULL)"],
            "SELECT APPROX_COUNT_DISTINCT(v) AS c FROM t",
            mode);

        // Value 1 appears twice; value 2 once.
        q.Table("t").Insert(1);
        q.Table("t").Insert(1);
        q.Table("t").Insert(2);
        q.Step();
        Assert.Equal(2, ScalarEstimate(q));

        // Remove one copy of 1 — it is still present, so the count is unchanged
        // and no delta should be emitted.
        q.Table("t").Delete(1);
        q.Step();
        Assert.Equal(0, q.Current.Count);
    }

    [Fact]
    public void Varchar_DistinctValues()
    {
        var q = Compile(
            ["CREATE TABLE t (s VARCHAR NOT NULL)"],
            "SELECT APPROX_COUNT_DISTINCT(s) AS c FROM t");

        foreach (var s in new[] { "apple", "banana", "banana", "cherry" })
        {
            q.Table("t").Insert(s);
        }

        q.Step();
        Assert.Equal(3, ScalarEstimate(q));
    }

    [Theory]
    [InlineData(CompileMode.Typed)]
    [InlineData(CompileMode.Structural)]
    public void LargeCardinality_WithinErrorBound(CompileMode mode)
    {
        var q = Compile(
            ["CREATE TABLE t (v INT NOT NULL)"],
            "SELECT APPROX_COUNT_DISTINCT(v) AS c FROM t",
            mode);

        const int distinct = 5000;
        for (var v = 0; v < distinct; v++)
        {
            // Each value twice — duplicates must not inflate the estimate.
            q.Table("t").Insert(v);
            q.Table("t").Insert(v);
        }

        q.Step();
        var estimate = ScalarEstimate(q);
        var error = Math.Abs(estimate - distinct) / (double)distinct;
        Assert.True(error <= 0.05, $"estimate {estimate} for {distinct} distinct, error {error:P2}");
    }

    [Fact]
    public void Resolver_ApproxCountDistinct_IsBigintNonNull()
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        resolver.Resolve(Parser.ParseStatement("CREATE TABLE t (v INT NOT NULL)"));
        var plan = ((SelectPlan)resolver.Resolve(
            Parser.ParseStatement("SELECT APPROX_COUNT_DISTINCT(v) AS c FROM t"))).Query;

        Assert.Equal("BIGINT", plan.Schema[0].Type.Name);
        Assert.False(plan.Schema[0].Type.Nullable);
    }

    [Fact]
    public void Resolver_ApproxCountDistinct_RequiresArgument()
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        resolver.Resolve(Parser.ParseStatement("CREATE TABLE t (v INT NOT NULL)"));

        var ex = Assert.Throws<ResolveException>(() => resolver.Resolve(
            Parser.ParseStatement("SELECT APPROX_COUNT_DISTINCT() AS c FROM t")));
        Assert.Contains("approx_count_distinct", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void IncrementalEqualsBatch_OverRandomInsertsAndDeletes()
    {
        // A deterministic op stream (fixed seed): apply it tick-by-tick with
        // interleaved deletes, then feed the net multiset to a fresh query in a
        // single batch tick. The HLL sketch is a deterministic function of the
        // present value set, so the two estimates must match exactly.
        var rng = new Random(20260602);
        var live = new Dictionary<int, int>(); // value -> current multiplicity

        var incremental = Compile(
            ["CREATE TABLE t (v INT NOT NULL)"],
            "SELECT APPROX_COUNT_DISTINCT(v) AS c FROM t");

        // Accumulate every per-tick output delta into a net Z-set so we can read
        // the current estimate after a stream of retract/insert deltas.
        var net = new Dictionary<StructuralRow, long>();
        for (var tick = 0; tick < 40; tick++)
        {
            for (var op = 0; op < 25; op++)
            {
                var v = rng.Next(0, 300);
                var present = live.GetValueOrDefault(v, 0);
                if (present > 0 && rng.Next(2) == 0)
                {
                    incremental.Table("t").Delete(v);
                    live[v] = present - 1;
                }
                else
                {
                    incremental.Table("t").Insert(v);
                    live[v] = present + 1;
                }
            }

            incremental.Step();
            foreach (var (row, weight) in incremental.Current)
            {
                net[row] = net.GetValueOrDefault(row, 0) + weight.Value;
            }
        }

        var survivors = net.Where(kv => kv.Value > 0).ToList();
        Assert.Single(survivors);
        var netIncremental = (long)survivors[0].Key[0]!;

        // Batch: one tick with the surviving multiset.
        var batch = Compile(
            ["CREATE TABLE t (v INT NOT NULL)"],
            "SELECT APPROX_COUNT_DISTINCT(v) AS c FROM t");
        foreach (var (v, mult) in live)
        {
            for (var i = 0; i < mult; i++)
            {
                batch.Table("t").Insert(v);
            }
        }

        batch.Step();
        var batchEstimate = ScalarEstimate(batch);

        Assert.Equal(batchEstimate, netIncremental);

        // And it should be a sane estimate of the true distinct count.
        var trueDistinct = live.Count(kv => kv.Value > 0);
        var error = Math.Abs(netIncremental - trueDistinct) / (double)Math.Max(1, trueDistinct);
        Assert.True(error <= 0.05, $"estimate {netIncremental} vs true {trueDistinct}, error {error:P2}");
    }
}
