// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

namespace DbspNet.Tests.Sql;

/// <summary>
/// Exact <c>COUNT(DISTINCT x)</c> on both compile paths and both trace families.
/// The aggregate tracks an invertible per-value count, so the incremental result
/// equals a from-scratch batch recompute exactly — every assertion here is exact
/// (no estimator tolerance, unlike <see cref="ApproxCountDistinctTests"/>).
/// </summary>
public class CountDistinctTests
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
    private static long Scalar(CompiledQuery q)
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
            "SELECT COUNT(DISTINCT v) AS c FROM t",
            mode);

        foreach (var v in new[] { 1, 1, 2, 2, 3, 3, 4, 5 })
        {
            q.Table("t").Insert(v);
        }

        q.Step();
        Assert.Equal(5, Scalar(q));
    }

    [Theory]
    [InlineData(CompileMode.Typed)]
    [InlineData(CompileMode.Structural)]
    [InlineData(CompileMode.Spine)]
    public void GroupBy_PerGroupDistinctCounts(CompileMode mode)
    {
        var q = Compile(
            ["CREATE TABLE t (g INT NOT NULL, v INT NOT NULL)"],
            "SELECT g, COUNT(DISTINCT v) AS c FROM t GROUP BY g",
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
            "SELECT COUNT(DISTINCT v) AS c FROM t");

        q.Table("t").Insert(1);
        q.Table("t").Insert(2);
        q.Table("t").Insert((object?)null);
        q.Table("t").Insert(2);
        q.Table("t").Insert((object?)null);
        q.Step();

        Assert.Equal(2, Scalar(q));
    }

    [Theory]
    [InlineData(CompileMode.Typed)]
    [InlineData(CompileMode.Structural)]
    [InlineData(CompileMode.Spine)]
    public void AllNullGroup_ReturnsZero(CompileMode mode)
    {
        var q = Compile(
            ["CREATE TABLE t (g INT NOT NULL, v INT)"],
            "SELECT g, COUNT(DISTINCT v) AS c FROM t GROUP BY g",
            mode);

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
            "SELECT COUNT(DISTINCT v) AS c FROM t",
            mode);

        q.Table("t").Insert(1);
        q.Table("t").Insert(2);
        q.Table("t").Insert(3);
        q.Step();
        Assert.Equal(3, Scalar(q));

        // Remove the only row carrying value 3 — the distinct count must drop.
        q.Table("t").Delete(3);
        q.Step();
        Assert.Equal(2, Scalar(q));
    }

    [Theory]
    [InlineData(CompileMode.Typed)]
    [InlineData(CompileMode.Structural)]
    [InlineData(CompileMode.Spine)]
    public void Delete_RemovingDuplicate_KeepsTheValue(CompileMode mode)
    {
        var q = Compile(
            ["CREATE TABLE t (v INT NOT NULL)"],
            "SELECT COUNT(DISTINCT v) AS c FROM t",
            mode);

        // Value 1 appears twice; value 2 once.
        q.Table("t").Insert(1);
        q.Table("t").Insert(1);
        q.Table("t").Insert(2);
        q.Step();
        Assert.Equal(2, Scalar(q));

        // Remove one copy of 1 — it is still present, so the count is unchanged
        // and no delta should be emitted.
        q.Table("t").Delete(1);
        q.Step();
        Assert.Equal(0, q.Current.Count);
    }

    [Theory]
    [InlineData(CompileMode.Typed)]
    [InlineData(CompileMode.Structural)]
    [InlineData(CompileMode.Spine)]
    public void Varchar_DistinctValues(CompileMode mode)
    {
        var q = Compile(
            ["CREATE TABLE t (s VARCHAR NOT NULL)"],
            "SELECT COUNT(DISTINCT s) AS c FROM t",
            mode);

        foreach (var s in new[] { "apple", "banana", "banana", "cherry" })
        {
            q.Table("t").Insert(s);
        }

        q.Step();
        Assert.Equal(3, Scalar(q));
    }

    [Theory]
    [InlineData(CompileMode.Typed)]
    [InlineData(CompileMode.Structural)]
    public void LargeCardinality_Exact(CompileMode mode)
    {
        var q = Compile(
            ["CREATE TABLE t (v INT NOT NULL)"],
            "SELECT COUNT(DISTINCT v) AS c FROM t",
            mode);

        const int distinct = 5000;
        for (var v = 0; v < distinct; v++)
        {
            // Each value twice — duplicates must not inflate the count.
            q.Table("t").Insert(v);
            q.Table("t").Insert(v);
        }

        q.Step();
        Assert.Equal(distinct, Scalar(q));
    }

    [Fact]
    public void CountDistinct_AlongsideCountAndSum()
    {
        // COUNT(DISTINCT v) must coexist with the linear COUNT(*)/SUM in one
        // composite without disturbing them.
        var q = Compile(
            ["CREATE TABLE t (v INT NOT NULL)"],
            "SELECT COUNT(*) AS n, COUNT(DISTINCT v) AS d, SUM(v) AS s FROM t");

        foreach (var v in new[] { 5, 5, 7 })
        {
            q.Table("t").Insert(v);
        }

        q.Step();
        long? n = null, d = null, s = null;
        foreach (var (row, weight) in q.Current)
        {
            if (weight.Value <= 0) continue;
            n = (long)row[0]!;
            d = (long)row[1]!;
            s = (long)row[2]!;
        }

        Assert.Equal(3, n);
        Assert.Equal(2, d);
        Assert.Equal(17, s);
    }

    [Fact]
    public void Resolver_CountDistinct_IsBigintNonNull()
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        resolver.Resolve(Parser.ParseStatement("CREATE TABLE t (v INT NOT NULL)"));
        var plan = ((SelectPlan)resolver.Resolve(
            Parser.ParseStatement("SELECT COUNT(DISTINCT v) AS c FROM t"))).Query;

        Assert.Equal("BIGINT", plan.Schema[0].Type.Name);
        Assert.False(plan.Schema[0].Type.Nullable);
    }

    [Fact]
    public void Resolver_DistinctOnNonCount_IsRejected()
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        resolver.Resolve(Parser.ParseStatement("CREATE TABLE t (v INT NOT NULL)"));

        var ex = Assert.Throws<ResolveException>(() => resolver.Resolve(
            Parser.ParseStatement("SELECT SUM(DISTINCT v) AS c FROM t")));
        Assert.Contains("DISTINCT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData(CompileMode.Typed)]
    [InlineData(CompileMode.Structural)]
    [InlineData(CompileMode.Spine)]
    public void IncrementalEqualsBatch_OverRandomInsertsAndDeletes(CompileMode mode)
    {
        // A deterministic op stream (fixed seed): apply it tick-by-tick with
        // interleaved deletes, then feed the net multiset to a fresh query in a
        // single batch tick. The count is a deterministic function of the
        // present value set, so the two results must match exactly.
        var rng = new Random(20260609);
        var live = new Dictionary<int, int>(); // value -> current multiplicity

        var incremental = Compile(
            ["CREATE TABLE t (v INT NOT NULL)"],
            "SELECT COUNT(DISTINCT v) AS c FROM t",
            mode);

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
            "SELECT COUNT(DISTINCT v) AS c FROM t",
            mode);
        foreach (var (v, mult) in live)
        {
            for (var i = 0; i < mult; i++)
            {
                batch.Table("t").Insert(v);
            }
        }

        batch.Step();
        var batchCount = Scalar(batch);

        Assert.Equal(batchCount, netIncremental);

        // And it must equal the true distinct count exactly.
        var trueDistinct = live.Count(kv => kv.Value > 0);
        Assert.Equal(trueDistinct, netIncremental);
    }
}
