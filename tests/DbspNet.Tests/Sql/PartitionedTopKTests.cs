// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Tests.Sql;

/// <summary>
/// Coverage for partitioned / windowed TOP-K — <c>ROW_NUMBER</c> / <c>RANK</c> /
/// <c>DENSE_RANK</c> in the incremental filter pattern
/// <c>… OVER (PARTITION BY p ORDER BY o) &lt;= k</c>. Behavioural tests run on
/// both compiler paths (<c>typed: true</c> = typed-row fast path, <c>false</c> =
/// structural fallback via a non-default codec). The rank value is never part of
/// the output, so assertions check per-partition <em>membership</em> and
/// multiplicity; row order is unobservable in a Z-set.
/// </summary>
public class PartitionedTopKTests
{
    private static CompiledQuery Compile(string ddl, string query, bool typed)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        resolver.Resolve(Parser.ParseStatement(ddl));
        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(query))).Query;
        return typed
            ? PlanToCircuit.Compile(plan)
            : PlanToCircuit.Compile(plan, EmittedEqualityCodec.Instance);
    }

    private static void Resolve(string ddl, string query)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        resolver.Resolve(Parser.ParseStatement(ddl));
        resolver.Resolve(Parser.ParseStatement(query));
    }

    private static long WeightOf(ZSet<StructuralRow, Z64> z, params object?[] row) =>
        z.WeightOf(new StructuralRow(SqlTestHelpers.EncodeStrings(row))).Value;

    private const string Emp = "CREATE TABLE emp (dept INT NOT NULL, sal INT NOT NULL)";

    private static string RowNumber(string order = "sal DESC", int k = 2) =>
        $"SELECT dept, sal FROM (SELECT dept, sal, ROW_NUMBER() OVER " +
        $"(PARTITION BY dept ORDER BY {order}) AS rn FROM emp) s WHERE rn <= {k}";

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void RowNumber_TopNPerPartition(bool typed)
    {
        var q = Compile(Emp, RowNumber(), typed);
        q.Table("emp").Insert(1, 100);
        q.Table("emp").Insert(1, 90);
        q.Table("emp").Insert(1, 80);
        q.Table("emp").Insert(1, 70);
        q.Table("emp").Insert(2, 50);
        q.Table("emp").Insert(2, 40);
        q.Step();

        // dept 1 keeps its top two salaries; dept 2 has only two rows.
        Assert.Equal(4, q.Current.Count);
        Assert.Equal(1, WeightOf(q.Current, 1, 100));
        Assert.Equal(1, WeightOf(q.Current, 1, 90));
        Assert.Equal(0, WeightOf(q.Current, 1, 80));
        Assert.Equal(1, WeightOf(q.Current, 2, 50));
        Assert.Equal(1, WeightOf(q.Current, 2, 40));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void RowNumber_RetractionPromotesWithinPartition(bool typed)
    {
        var q = Compile(Emp, RowNumber(), typed);
        q.Table("emp").Insert(1, 100);
        q.Table("emp").Insert(1, 90);
        q.Table("emp").Insert(1, 80);
        q.Table("emp").Insert(2, 50);
        q.Step();

        Assert.Equal(3, q.Current.Count);

        // Retract dept-1's top salary: 80 is promoted into dept 1's window;
        // dept 2 is untouched.
        q.Table("emp").Delete(1, 100);
        q.Step();

        Assert.Equal(-1, WeightOf(q.Current, 1, 100));
        Assert.Equal(1, WeightOf(q.Current, 1, 80));
        Assert.Equal(0, WeightOf(q.Current, 1, 90)); // already in window
        Assert.Equal(0, WeightOf(q.Current, 2, 50)); // other partition
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void RowNumber_StrictLessThan(bool typed)
    {
        // rn < 3  ⇒  limit 2.
        var q = Compile(Emp,
            "SELECT dept, sal FROM (SELECT dept, sal, ROW_NUMBER() OVER " +
            "(PARTITION BY dept ORDER BY sal DESC) AS rn FROM emp) s WHERE rn < 3", typed);
        q.Table("emp").Insert(1, 100);
        q.Table("emp").Insert(1, 90);
        q.Table("emp").Insert(1, 80);
        q.Step();

        Assert.Equal(2, q.Current.Count);
        Assert.Equal(1, WeightOf(q.Current, 1, 100));
        Assert.Equal(1, WeightOf(q.Current, 1, 90));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void RowNumber_SelectStar_ExcludesRankColumn(bool typed)
    {
        // SELECT * over the windowed derived table yields only the data columns —
        // the rank alias is not part of the output (a 3-column row would not match).
        var q = Compile(Emp,
            "SELECT * FROM (SELECT dept, sal, ROW_NUMBER() OVER " +
            "(PARTITION BY dept ORDER BY sal DESC) AS rn FROM emp) s WHERE rn <= 1", typed);
        q.Table("emp").Insert(1, 100);
        q.Table("emp").Insert(1, 90);
        q.Table("emp").Insert(2, 40);
        q.Step();

        Assert.Equal(2, q.Current.Count);
        Assert.Equal(1, WeightOf(q.Current, 1, 100));
        Assert.Equal(1, WeightOf(q.Current, 2, 40));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void GlobalPartition_NoPartitionBy_IsPlainTopK(bool typed)
    {
        var q = Compile(Emp,
            "SELECT dept, sal FROM (SELECT dept, sal, ROW_NUMBER() OVER " +
            "(ORDER BY sal DESC) AS rn FROM emp) s WHERE rn <= 2", typed);
        q.Table("emp").Insert(1, 100);
        q.Table("emp").Insert(2, 90);
        q.Table("emp").Insert(1, 80);
        q.Step();

        // A single global partition: the two largest salaries overall.
        Assert.Equal(2, q.Current.Count);
        Assert.Equal(1, WeightOf(q.Current, 1, 100));
        Assert.Equal(1, WeightOf(q.Current, 2, 90));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void PartitionAndOrderOverNonSelectedColumns(bool typed)
    {
        // Neither the PARTITION BY column (dept) nor the ORDER BY column (sal) is
        // in the inner select list — both are carried as hidden columns.
        var ddl = "CREATE TABLE emp (dept INT NOT NULL, sal INT NOT NULL, id INT NOT NULL)";
        var q = Compile(ddl,
            "SELECT id FROM (SELECT id, ROW_NUMBER() OVER " +
            "(PARTITION BY dept ORDER BY sal DESC) AS rn FROM emp) s WHERE rn <= 1", typed);
        q.Table("emp").Insert(1, 100, 11);
        q.Table("emp").Insert(1, 90, 12);
        q.Table("emp").Insert(2, 50, 21);
        q.Step();

        // Per dept, the id of the highest-paid row.
        Assert.Equal(2, q.Current.Count);
        Assert.Equal(1, WeightOf(q.Current, 11));
        Assert.Equal(1, WeightOf(q.Current, 21));
    }

    // ---- RANK / DENSE_RANK tie semantics -------------------------------------

    private const string Tied = "CREATE TABLE t (g INT NOT NULL, v INT NOT NULL, id INT NOT NULL)";

    private static string Ranked(string fn, int k = 2) =>
        $"SELECT g, v, id FROM (SELECT g, v, id, {fn}() OVER " +
        $"(PARTITION BY g ORDER BY v) AS rn FROM t) s WHERE rn <= {k}";

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Rank_KeepsWholeTieGroup_RowNumberDoesNot(bool typed)
    {
        // Three distinct rows sharing one ORDER BY key (v = 10).
        void Load(CompiledQuery q)
        {
            q.Table("t").Insert(1, 10, 1);
            q.Table("t").Insert(1, 10, 2);
            q.Table("t").Insert(1, 10, 3);
            q.Step();
        }

        var rn = Compile(Tied, Ranked("ROW_NUMBER"), typed);
        Load(rn);
        Assert.Equal(2, rn.Current.Count); // ROW_NUMBER cuts the tie at the limit.

        var rank = Compile(Tied, Ranked("RANK"), typed);
        Load(rank);
        Assert.Equal(3, rank.Current.Count); // RANK keeps all rank-1 rows.
        Assert.Equal(1, WeightOf(rank.Current, 1, 10, 1));
        Assert.Equal(1, WeightOf(rank.Current, 1, 10, 2));
        Assert.Equal(1, WeightOf(rank.Current, 1, 10, 3));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Rank_SkipsAfterTie_DenseRankDoesNot(bool typed)
    {
        // Keys: 10, 10, 20, 30. RANK(20) = 3 (skips), DENSE_RANK(20) = 2.
        void Load(CompiledQuery q)
        {
            q.Table("t").Insert(1, 10, 1);
            q.Table("t").Insert(1, 10, 2);
            q.Table("t").Insert(1, 20, 3);
            q.Table("t").Insert(1, 30, 4);
            q.Step();
        }

        var rank = Compile(Tied, Ranked("RANK"), typed);
        Load(rank);
        // rank <= 2 keeps only the two v=10 rows (v=20 is rank 3).
        Assert.Equal(2, rank.Current.Count);
        Assert.Equal(1, WeightOf(rank.Current, 1, 10, 1));
        Assert.Equal(1, WeightOf(rank.Current, 1, 10, 2));
        Assert.Equal(0, WeightOf(rank.Current, 1, 20, 3));

        var dense = Compile(Tied, Ranked("DENSE_RANK"), typed);
        Load(dense);
        // dense_rank <= 2 keeps v=10 (dense 1) and v=20 (dense 2).
        Assert.Equal(3, dense.Current.Count);
        Assert.Equal(1, WeightOf(dense.Current, 1, 10, 1));
        Assert.Equal(1, WeightOf(dense.Current, 1, 10, 2));
        Assert.Equal(1, WeightOf(dense.Current, 1, 20, 3));
        Assert.Equal(0, WeightOf(dense.Current, 1, 30, 4));
    }

    // ---- Reject everything outside the supported TOP-K pattern ---------------

    [Fact]
    public void Rejects_SelectingTheRankColumn() =>
        Assert.Throws<ResolveException>(() => Resolve(Emp,
            "SELECT dept, rn FROM (SELECT dept, sal, ROW_NUMBER() OVER " +
            "(PARTITION BY dept ORDER BY sal) AS rn FROM emp) s WHERE rn <= 2"));

    [Fact]
    public void Rejects_UnsupportedWindowFunction() =>
        Assert.Throws<ResolveException>(() => Resolve(Emp,
            "SELECT dept, total FROM (SELECT dept, SUM(sal) OVER " +
            "(PARTITION BY dept ORDER BY sal) AS total FROM emp) s WHERE total <= 2"));

    [Fact]
    public void Rejects_NoQualifyingFilter() =>
        Assert.Throws<ResolveException>(() => Resolve(Emp,
            "SELECT dept, sal FROM (SELECT dept, sal, ROW_NUMBER() OVER " +
            "(PARTITION BY dept ORDER BY sal) AS rn FROM emp) s"));

    [Fact]
    public void Rejects_WindowWithoutAlias() =>
        Assert.Throws<ResolveException>(() => Resolve(Emp,
            "SELECT dept, sal FROM (SELECT dept, sal, ROW_NUMBER() OVER " +
            "(PARTITION BY dept ORDER BY sal) FROM emp) s WHERE rn <= 2"));

    [Fact]
    public void Rejects_WindowWithoutOrderBy() =>
        Assert.Throws<ResolveException>(() => Resolve(Emp,
            "SELECT dept, sal FROM (SELECT dept, sal, ROW_NUMBER() OVER " +
            "(PARTITION BY dept) AS rn FROM emp) s WHERE rn <= 2"));

    [Fact]
    public void Rejects_EqualityOnRank() =>
        Assert.Throws<ResolveException>(() => Resolve(Emp,
            "SELECT dept, sal FROM (SELECT dept, sal, ROW_NUMBER() OVER " +
            "(PARTITION BY dept ORDER BY sal) AS rn FROM emp) s WHERE rn = 2"));
}
