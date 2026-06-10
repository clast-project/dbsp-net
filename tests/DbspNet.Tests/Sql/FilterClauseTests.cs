// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

namespace DbspNet.Tests.Sql;

/// <summary>
/// The aggregate <c>FILTER (WHERE …)</c> clause — pure parser sugar that lowers
/// <c>agg(x) FILTER (WHERE p)</c> to <c>agg(CASE WHEN p THEN x END)</c> (and
/// <c>COUNT(*) FILTER (WHERE p)</c> to <c>COUNT(CASE WHEN p THEN 1 END)</c>), so
/// it flows through the existing CASE + aggregate machinery on every compile path
/// and trace family. These pin the semantics and the equivalence to the hand-
/// written CASE form.
/// </summary>
public class FilterClauseTests
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
            CompileMode.Structural => PlanToCircuit.Compile(plan, EmittedEqualityCodec.Instance),
            CompileMode.Spine => PlanToCircuit.Compile(plan, null, new CompileOptions { TraceFamily = TraceFamily.Spine }),
            _ => PlanToCircuit.Compile(plan),
        };
    }

    public enum CompileMode { Typed, Structural, Spine }

    private static object? Scalar(CompiledQuery q, int col = 0)
    {
        object? found = null;
        var seen = false;
        foreach (var (row, weight) in q.Current)
        {
            if (weight.Value <= 0)
            {
                continue;
            }

            Assert.False(seen);
            found = row[col];
            seen = true;
        }

        Assert.True(seen);
        return found;
    }

    [Theory]
    [InlineData(CompileMode.Typed)]
    [InlineData(CompileMode.Structural)]
    [InlineData(CompileMode.Spine)]
    public void CountStar_Filter_CountsMatchingRows(CompileMode mode)
    {
        var q = Compile(
            ["CREATE TABLE t (v INT NOT NULL)"],
            "SELECT COUNT(*) FILTER (WHERE v < 10) AS c FROM t",
            mode);

        foreach (var v in new[] { 5, 8, 50, 100 })
        {
            q.Table("t").Insert(v);
        }

        q.Step();
        Assert.Equal(2L, Scalar(q)); // 5, 8
    }

    [Theory]
    [InlineData(CompileMode.Typed)]
    [InlineData(CompileMode.Structural)]
    [InlineData(CompileMode.Spine)]
    public void Sum_Filter_SumsMatchingRows(CompileMode mode)
    {
        var q = Compile(
            ["CREATE TABLE t (v INT NOT NULL)"],
            "SELECT SUM(v) FILTER (WHERE v >= 10) AS s FROM t",
            mode);

        foreach (var v in new[] { 5, 8, 50, 100 })
        {
            q.Table("t").Insert(v);
        }

        q.Step();
        Assert.Equal(150L, Scalar(q)); // 50 + 100
    }

    [Theory]
    [InlineData(CompileMode.Typed)]
    [InlineData(CompileMode.Structural)]
    [InlineData(CompileMode.Spine)]
    public void CountDistinct_Filter_CountsDistinctMatching(CompileMode mode)
    {
        var q = Compile(
            ["CREATE TABLE t (v INT NOT NULL)"],
            "SELECT COUNT(DISTINCT v) FILTER (WHERE v < 100) AS c FROM t",
            mode);

        foreach (var v in new[] { 5, 5, 8, 50, 100 })
        {
            q.Table("t").Insert(v);
        }

        q.Step();
        Assert.Equal(3L, Scalar(q)); // distinct {5, 8, 50}
    }

    [Fact]
    public void Sum_AllFilteredOut_IsNull()
    {
        var q = Compile(
            ["CREATE TABLE t (v INT NOT NULL)"],
            "SELECT SUM(v) FILTER (WHERE v > 1000) AS s FROM t");

        q.Table("t").Insert(5);
        q.Table("t").Insert(8);
        q.Step();

        // SUM over no matching rows is NULL, like SUM of an empty group.
        Assert.Null(Scalar(q));
    }

    [Fact]
    public void GroupBy_PerGroupFilteredCounts()
    {
        // Mirrors the Nexmark q15/q16 shape: per-group conditional counts.
        var q = Compile(
            ["CREATE TABLE t (g INT NOT NULL, v INT NOT NULL)"],
            "SELECT g, COUNT(*) FILTER (WHERE v < 10) AS lo, SUM(v) FILTER (WHERE v >= 10) AS hi " +
            "FROM t GROUP BY g");

        q.Table("t").Insert(1, 5);
        q.Table("t").Insert(1, 50);
        q.Table("t").Insert(1, 7);
        q.Table("t").Insert(2, 100);
        q.Step();

        long? g1Lo = null, g1Hi = null, g2Lo = null, g2Hi = null;
        foreach (var (row, w) in q.Current)
        {
            if (w.Value <= 0) continue;
            if ((int)row[0]! == 1) { g1Lo = (long)row[1]!; g1Hi = (long?)row[2]; }
            else { g2Lo = (long)row[1]!; g2Hi = (long?)row[2]; }
        }

        Assert.Equal(2L, g1Lo);   // 5, 7
        Assert.Equal(50L, g1Hi);  // 50
        Assert.Equal(0L, g2Lo);   // none < 10
        Assert.Equal(100L, g2Hi); // 100
    }

    [Fact]
    public void Filter_EquivalentToHandWrittenCase()
    {
        // The desugar must produce exactly the CASE form's result.
        var filtered = Compile(
            ["CREATE TABLE t (v INT NOT NULL)"],
            "SELECT SUM(v) FILTER (WHERE v >= 10) AS s FROM t");
        var cased = Compile(
            ["CREATE TABLE t (v INT NOT NULL)"],
            "SELECT SUM(CASE WHEN v >= 10 THEN v END) AS s FROM t");

        foreach (var v in new[] { 5, 12, 50, 9, 100 })
        {
            filtered.Table("t").Insert(v);
            cased.Table("t").Insert(v);
        }

        filtered.Step();
        cased.Step();
        Assert.Equal(Scalar(cased), Scalar(filtered));
    }

    [Fact]
    public void Filter_OnWindowFunction_IsRejected()
    {
        var ex = Record.Exception(() =>
            Parser.ParseStatement("SELECT COUNT(*) FILTER (WHERE v < 10) OVER () AS c FROM t"));
        Assert.IsType<ParseException>(ex);
        Assert.Contains("FILTER", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void FilterIdentifier_StillUsableAsColumnName()
    {
        // `filter` is contextual — a column literally named "filter" still works.
        var q = Compile(
            ["CREATE TABLE t (filter INT NOT NULL)"],
            "SELECT filter FROM t");
        q.Table("t").Insert(42);
        q.Step();
        Assert.Equal(42, Scalar(q));
    }
}
