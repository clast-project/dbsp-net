// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Expressions;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Parser.Ast;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Tests.Sql;

/// <summary>
/// Coverage for <c>probe [NOT] IN (v1, ..., vN)</c> — the literal-list form.
/// Modeled as a flat <see cref="InListExpression"/> AST node (not desugared
/// at parse time) so large lists don't blow the C# stack in the recursive
/// expression walkers. Three-valued NULL semantics verified at compile time
/// via <see cref="InListRuntime.Evaluate"/>.
/// </summary>
public class InListTests
{
    // ---------- Parser ----------

    [Fact]
    public void Parser_InList_ProducesFlatInListExpression()
    {
        var expr = new Parser(Lexer.Tokenize("x IN (1, 2, 3)")).ParseExpression();
        var il = Assert.IsType<InListExpression>(expr);
        Assert.False(il.IsNegated);
        Assert.IsType<ColumnReference>(il.Probe);
        Assert.Equal(3, il.Values.Count);
    }

    [Fact]
    public void Parser_NotInList_ProducesNegatedInListExpression()
    {
        var expr = new Parser(Lexer.Tokenize("x NOT IN (1, 2)")).ParseExpression();
        var il = Assert.IsType<InListExpression>(expr);
        Assert.True(il.IsNegated);
        Assert.Equal(2, il.Values.Count);
    }

    [Fact]
    public void Parser_InsideAndOr_Composes()
    {
        // IN binds at the comparison level, so AND/OR see it as a single term.
        var expr = new Parser(Lexer.Tokenize("x IN (1, 2) AND y = 5")).ParseExpression();
        var and = Assert.IsType<BinaryExpression>(expr);
        Assert.Equal(BinaryOperator.And, and.Operator);
        Assert.IsType<InListExpression>(and.Left);
        Assert.IsType<BinaryExpression>(and.Right);
    }

    [Fact]
    public void Parser_LargeList_DoesNotStackOverflow()
    {
        // Recursion-depth guard for the parser AND for the resolver / compiler
        // that walk the resulting AST. 10000 values is well past .NET's default
        // ~100-level practical recursion limit for a left-leaning OR chain.
        var values = string.Join(", ", Enumerable.Range(0, 10_000));
        var expr = new Parser(Lexer.Tokenize($"x IN ({values})")).ParseExpression();
        var il = Assert.IsType<InListExpression>(expr);
        Assert.Equal(10_000, il.Values.Count);
    }

    [Fact]
    public void Parser_InSubquery_RejectedUntilPhase2()
    {
        // Phase 1 only handles the literal-list form. Subquery form raises
        // a clear error rather than silently mis-parsing.
        var ex = Assert.Throws<ParseException>(() =>
            new Parser(Lexer.Tokenize("x IN (SELECT id FROM t)")).ParseExpression());
        Assert.Contains("subquery", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Parser_EmptyList_Rejected()
    {
        Assert.Throws<ParseException>(() =>
            new Parser(Lexer.Tokenize("x IN ()")).ParseExpression());
    }

    // ---------- Compiler / Evaluator (NULL-semantics unit tests) ----------

    private static Func<object?[], object?> CompileExpr(
        string exprText, params (string Name, SqlType Type)[] cols)
    {
        var schema = new Schema(cols.Select(c => new SchemaColumn(c.Name, c.Type, "t")).ToList());
        var ast = new Parser(Lexer.Tokenize(exprText)).ParseExpression();
        var resolved = ResolveViaPublicApi(ast, schema, cols);
        var fn = ExpressionCompiler.CompileScalar(resolved);
        return arr => fn(arr);
    }

    private static ResolvedExpression ResolveViaPublicApi(
        Expression ast, Schema schema, (string Name, SqlType Type)[] cols)
    {
        var cat = new Catalog();
        var colDefs = new List<ColumnDefinition>();
        foreach (var c in cols)
        {
            colDefs.Add(new ColumnDefinition(c.Name, SqlTypeSpecOf(c.Type), !c.Type.Nullable, PrimaryKey: false));
        }

        var create = new CreateTableStatement("t", colDefs);
        var resolver = new Resolver(cat);
        resolver.Resolve(create);

        var select = new SelectStatement(
            Items: [new ExpressionSelectItem(ast, Alias: null)],
            From: new TableReference("t", Alias: null),
            Where: null,
            GroupBy: Array.Empty<Expression>(),
            Having: null,
            Ctes: Array.Empty<CteDefinition>());
        var plan = ((SelectPlan)resolver.Resolve(select)).Query;
        var proj = (ProjectPlan)plan;
        return proj.Projections[0].Expression;
    }

    private static SqlTypeSpec SqlTypeSpecOf(SqlType t) => t switch
    {
        SqlIntegerType => new SqlTypeSpec("INTEGER"),
        SqlBigintType => new SqlTypeSpec("BIGINT"),
        SqlVarcharType v => new SqlTypeSpec("VARCHAR", v.MaxLength),
        SqlBooleanType => new SqlTypeSpec("BOOLEAN"),
        _ => throw new NotSupportedException(),
    };

    [Fact]
    public void Eval_InList_MatchedValue_True()
    {
        var f = CompileExpr("x IN (1, 2, 3)", ("x", new SqlIntegerType(false)));
        Assert.Equal(true, f([2]));
    }

    [Fact]
    public void Eval_InList_UnmatchedValue_False()
    {
        var f = CompileExpr("x IN (1, 2, 3)", ("x", new SqlIntegerType(false)));
        Assert.Equal(false, f([42]));
    }

    [Fact]
    public void Eval_NotInList_MatchedValue_False()
    {
        var f = CompileExpr("x NOT IN (1, 2, 3)", ("x", new SqlIntegerType(false)));
        Assert.Equal(false, f([2]));
    }

    [Fact]
    public void Eval_NotInList_UnmatchedValue_True()
    {
        var f = CompileExpr("x NOT IN (1, 2, 3)", ("x", new SqlIntegerType(false)));
        Assert.Equal(true, f([42]));
    }

    // ---- SQL three-valued NULL semantics ----

    [Fact]
    public void Eval_NullProbe_YieldsNull()
    {
        var f = CompileExpr("x IN (1, 2, 3)", ("x", new SqlIntegerType(true)));
        Assert.Null(f([null]));
    }

    [Fact]
    public void Eval_NullProbe_NotIn_StillYieldsNull()
    {
        var f = CompileExpr("x NOT IN (1, 2, 3)", ("x", new SqlIntegerType(true)));
        Assert.Null(f([null]));
    }

    [Fact]
    public void Eval_NonMatchWithNullValueInList_YieldsNull()
    {
        // 5 IN (1, NULL, 3) — no non-null value matches, but NULL is present,
        // so the result is unknown. Per SQL standard.
        var f = CompileExpr("x IN (1, NULL, 3)", ("x", new SqlIntegerType(false)));
        Assert.Null(f([5]));
    }

    [Fact]
    public void Eval_MatchPresent_NullValueIgnored_YieldsTrue()
    {
        // 1 IN (1, NULL): match found before NULL is observed.
        var f = CompileExpr("x IN (1, NULL)", ("x", new SqlIntegerType(false)));
        Assert.Equal(true, f([1]));
    }

    [Fact]
    public void Eval_NotInWithNullValue_NoMatch_YieldsNull()
    {
        // 5 NOT IN (1, NULL) — would need to assert "5 ≠ NULL" which is NULL.
        var f = CompileExpr("x NOT IN (1, NULL)", ("x", new SqlIntegerType(false)));
        Assert.Null(f([5]));
    }

    // ---------- End-to-end via WHERE ----------

    private static CompiledQuery CompileView(string[] ddl, string query)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in ddl)
        {
            resolver.Resolve(Parser.ParseStatement(s));
        }

        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(query))).Query;
        return PlanToCircuit.Compile(plan);
    }

    private static long WeightOf(ZSet<StructuralRow, Z64> z, params object?[] row) =>
        z.WeightOf(new StructuralRow(SqlTestHelpers.EncodeStrings(row))).Value;

    [Fact]
    public void EndToEnd_WhereInList_FiltersRows()
    {
        var q = CompileView(
            ["CREATE TABLE t (id INT NOT NULL, name VARCHAR NOT NULL)"],
            "SELECT name FROM t WHERE id IN (1, 3, 5)");

        q.Table("t").Insert(1, Utf8String.Of("a"));
        q.Table("t").Insert(2, Utf8String.Of("b"));
        q.Table("t").Insert(3, Utf8String.Of("c"));
        q.Table("t").Insert(4, Utf8String.Of("d"));
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, "a"));
        Assert.Equal(1, WeightOf(q.Current, "c"));
        Assert.Equal(2, q.Current.Count);
    }

    [Fact]
    public void EndToEnd_WhereNotInList_FiltersComplement()
    {
        var q = CompileView(
            ["CREATE TABLE t (id INT NOT NULL, name VARCHAR NOT NULL)"],
            "SELECT name FROM t WHERE id NOT IN (1, 3)");

        q.Table("t").Insert(1, Utf8String.Of("a"));
        q.Table("t").Insert(2, Utf8String.Of("b"));
        q.Table("t").Insert(3, Utf8String.Of("c"));
        q.Table("t").Insert(4, Utf8String.Of("d"));
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, "b"));
        Assert.Equal(1, WeightOf(q.Current, "d"));
        Assert.Equal(2, q.Current.Count);
    }

    [Fact]
    public void EndToEnd_WhereInList_RowWithNullProbe_FiltersOut()
    {
        // 3VL: NULL IN (...) → NULL → WHERE drops the row.
        var q = CompileView(
            ["CREATE TABLE t (id INT, name VARCHAR NOT NULL)"],
            "SELECT name FROM t WHERE id IN (1, 2)");

        q.Table("t").Insert(1, Utf8String.Of("a"));
        q.Table("t").Insert(null, Utf8String.Of("null-id"));
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, "a"));
        Assert.Equal(1, q.Current.Count);
    }
}
