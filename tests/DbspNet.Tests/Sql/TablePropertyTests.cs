// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Sql.Parser;
using DbspNet.Sql.Parser.Ast;
using DbspNet.Sql.Plan;
using Xunit;

namespace DbspNet.Tests.Sql;

/// <summary>
/// The <c>CREATE TABLE … WITH ('key' = 'value')</c> table-property surface (spelled
/// to match Feldera). v1 understands one key, <c>append_only</c> — the declaration
/// that a table's rows are only ever inserted, which is what lets the optimizer apply
/// rewrites that are sound over a bag but not over a signed Z-set
/// (docs/design-row-representation.md §18.6).
/// </summary>
public class TablePropertyTests
{
    [Fact]
    public void Parser_ReadsPropertiesVerbatim_WithoutInterpretingThem()
    {
        var stmt = (CreateTableStatement)Parser.ParseStatement(
            "CREATE TABLE t (a INT) WITH ('append_only' = 'true', 'other' = 'x')");

        Assert.Equal(
            new[] { new TableProperty("append_only", "true"), new TableProperty("other", "x") },
            stmt.Properties);
    }

    [Fact]
    public void NoWithClause_LeavesPropertiesAbsent()
    {
        var stmt = (CreateTableStatement)Parser.ParseStatement("CREATE TABLE t (a INT)");
        Assert.Null(stmt.Properties);
    }

    [Theory]
    [InlineData("'append_only' = 'true'", true)]
    [InlineData("'APPEND_ONLY' = 'TRUE'", true)]   // key and value are case-insensitive
    [InlineData("'append_only' = 'false'", false)]
    public void AppendOnly_ReachesTheScan(string property, bool expected)
    {
        Assert.Equal(expected, ResolveScan($"CREATE TABLE t (a INT NOT NULL) WITH ({property})").AppendOnly);
    }

    [Fact]
    public void UndeclaredTable_IsNotAppendOnly()
    {
        Assert.False(ResolveScan("CREATE TABLE t (a INT NOT NULL)").AppendOnly);
    }

    [Fact]
    public void UnknownProperty_IsAnError_NotASilentNoOp()
    {
        // A misspelled property that quietly does nothing is worse than a compile
        // failure: the property changes which rewrites are legal.
        var ex = Assert.Throws<ResolveException>(() =>
            Resolve("CREATE TABLE t (a INT NOT NULL) WITH ('append-only' = 'true')"));
        Assert.Contains("unknown table property", ex.Message);
    }

    [Fact]
    public void NonBooleanAppendOnlyValue_IsAnError()
    {
        var ex = Assert.Throws<ResolveException>(() =>
            Resolve("CREATE TABLE t (a INT NOT NULL) WITH ('append_only' = 'yes')"));
        Assert.Contains("must be 'true' or 'false'", ex.Message);
    }

    [Fact]
    public void UnquotedPropertyName_IsAParseError()
    {
        Assert.Throws<ParseException>(() =>
            Parser.ParseStatement("CREATE TABLE t (a INT) WITH (append_only = 'true')"));
    }

    /// <summary>Resolve the DDL, then dig the scan out of <c>SELECT a FROM t</c>.</summary>
    private static ScanPlan ResolveScan(string ddl)
    {
        var query = Resolve(ddl);
        return query switch
        {
            ScanPlan s => s,
            ProjectPlan p when p.Input is ScanPlan s => s,
            _ => throw new InvalidOperationException($"unexpected plan shape {query.GetType().Name}"),
        };
    }

    private static LogicalPlan Resolve(string ddl)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        resolver.Resolve(Parser.ParseStatement(ddl));
        return ((SelectPlan)resolver.Resolve(Parser.ParseStatement("SELECT a FROM t"))).Query;
    }
}
