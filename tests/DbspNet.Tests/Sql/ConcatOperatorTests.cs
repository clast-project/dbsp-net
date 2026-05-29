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
/// Coverage for the <c>||</c> string-concatenation operator. Unlike this
/// engine's PG-style <c>CONCAT</c> (which skips NULLs), <c>||</c> follows the
/// SQL standard and PROPAGATES NULL. A run of <c>||</c> parses to a single
/// flat <c>FunctionCallExpression("||", …)</c>, so walkers stay shallow and
/// each operand is compiled exactly once.
/// </summary>
public class ConcatOperatorTests
{
    private static readonly SqlVarcharType Str = new(null, Nullable: false);
    private static readonly SqlVarcharType StrN = new(null, Nullable: true);

    // ---------- Parser ----------

    [Fact]
    public void Parser_Concat_ProducesFlatCall()
    {
        var expr = new Parser(Lexer.Tokenize("a || b")).ParseExpression();
        var fn = Assert.IsType<FunctionCallExpression>(expr);
        Assert.Equal("||", fn.FunctionName);
        Assert.Equal(2, fn.Arguments.Count);
    }

    [Fact]
    public void Parser_ConcatChain_FlattensNotNests()
    {
        // a || b || c → one flat call with three args (not nested binaries).
        var expr = new Parser(Lexer.Tokenize("a || b || c || d")).ParseExpression();
        var fn = Assert.IsType<FunctionCallExpression>(expr);
        Assert.Equal("||", fn.FunctionName);
        Assert.Equal(4, fn.Arguments.Count);
        Assert.All(fn.Arguments, a => Assert.IsType<ColumnReference>(a));
    }

    [Fact]
    public void Parser_Concat_BindsTighterThanComparison()
    {
        // a = b || c → a = (b || c): top is the equality, RHS is the concat.
        var expr = new Parser(Lexer.Tokenize("a = b || c")).ParseExpression();
        var eq = Assert.IsType<BinaryExpression>(expr);
        Assert.Equal(BinaryOperator.Equal, eq.Operator);
        var fn = Assert.IsType<FunctionCallExpression>(eq.Right);
        Assert.Equal("||", fn.FunctionName);
    }

    [Fact]
    public void Lexer_SingleBar_IsRejected()
    {
        Assert.ThrowsAny<Exception>(() => Lexer.Tokenize("a | b"));
    }

    // ---------- Eval (structural) ----------

    [Fact]
    public void Eval_Concat_JoinsStrings()
    {
        var f = CompileExpr("a || b", ("a", Str), ("b", Str));
        Assert.Equal(Utf8String.Of("xy"), f([Utf8String.Of("x"), Utf8String.Of("y")]));
    }

    [Fact]
    public void Eval_Concat_Chain()
    {
        var f = CompileExpr("a || b || c", ("a", Str), ("b", Str), ("c", Str));
        Assert.Equal(
            Utf8String.Of("foobarbaz"),
            f([Utf8String.Of("foo"), Utf8String.Of("bar"), Utf8String.Of("baz")]));
    }

    [Fact]
    public void Eval_Concat_PropagatesNull()
    {
        // The defining difference from CONCAT: any NULL operand → NULL result.
        var f = CompileExpr("a || b", ("a", StrN), ("b", StrN));
        Assert.Null(f([null, Utf8String.Of("y")]));
        Assert.Null(f([Utf8String.Of("x"), null]));
        Assert.Equal(Utf8String.Of("xy"), f([Utf8String.Of("x"), Utf8String.Of("y")]));
    }

    [Fact]
    public void Eval_Concat_DiffersFromConcatFunctionOnNull()
    {
        // CONCAT skips NULLs; || propagates. Same inputs, different results.
        var concatFn = CompileExpr("CONCAT(a, b)", ("a", StrN), ("b", StrN));
        var barFn = CompileExpr("a || b", ("a", StrN), ("b", StrN));

        Assert.Equal(Utf8String.Of("x"), concatFn([Utf8String.Of("x"), null]));
        Assert.Null(barFn([Utf8String.Of("x"), null]));
    }

    // ---------- Typed fast path ----------

    [Fact]
    public void Typed_Concat_NonNullable_CompilesToUtf8()
    {
        var (rowType, factory) = RowFor(("a", Str), ("b", Str));
        var expr = Resolve("a || b", ("a", Str), ("b", Str));

        var del = TypedExpressionCompiler.TryCompile(expr, rowType);
        Assert.NotNull(del);
        Assert.Equal(typeof(Utf8String), del!.Method.ReturnType);
        Assert.Equal(
            Utf8String.Of("xy"),
            del.DynamicInvoke(factory(new object?[] { Utf8String.Of("x"), Utf8String.Of("y") })));
    }

    [Fact]
    public void Typed_Concat_Nullable_PropagatesNull()
    {
        var (rowType, factory) = RowFor(("a", StrN), ("b", StrN));
        var expr = Resolve("a || b", ("a", StrN), ("b", StrN));

        var del = TypedExpressionCompiler.TryCompile(expr, rowType);
        Assert.NotNull(del);
        Assert.Equal(typeof(Utf8String?), del!.Method.ReturnType);
        Assert.Equal(
            Utf8String.Of("xy"),
            del.DynamicInvoke(factory(new object?[] { Utf8String.Of("x"), Utf8String.Of("y") })));
        Assert.Null(del.DynamicInvoke(factory(new object?[] { null, Utf8String.Of("y") })));
    }

    // ---------- End-to-end ----------

    [Fact]
    public void EndToEnd_Concat_InProjection()
    {
        var q = CompileView(
            ["CREATE TABLE t (a VARCHAR NOT NULL, b VARCHAR NOT NULL)"],
            "SELECT a || '-' || b AS j FROM t");

        q.Table("t").Insert(Utf8String.Of("foo"), Utf8String.Of("bar"));
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, Utf8String.Of("foo-bar")));
        Assert.Equal(1, q.Current.Count);
    }

    [Fact]
    public void EndToEnd_Concat_NullableColumn_PropagatesNull()
    {
        var q = CompileView(
            ["CREATE TABLE t (id INT NOT NULL, a VARCHAR, b VARCHAR)"],
            "SELECT id, a || b AS j FROM t");

        q.Table("t").Insert(1, Utf8String.Of("x"), Utf8String.Of("y"));
        q.Table("t").Insert(2, (object?)null, Utf8String.Of("y"));
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 1, Utf8String.Of("xy")));
        Assert.Equal(1, WeightOf(q.Current, 2, null));
        Assert.Equal(2, q.Current.Count);
    }

    // ---------- Helpers ----------

    private static Func<object?[], object?> CompileExpr(string exprText, params (string Name, SqlType Type)[] cols)
    {
        var fn = ExpressionCompiler.CompileScalar(Resolve(exprText, cols));
        return arr => fn(arr);
    }

    private static ResolvedExpression Resolve(string exprText, params (string Name, SqlType Type)[] cols)
    {
        var ast = new Parser(Lexer.Tokenize(exprText)).ParseExpression();
        var cat = new Catalog();
        var colDefs = new List<ColumnDefinition>();
        foreach (var c in cols)
        {
            colDefs.Add(new ColumnDefinition(c.Name, SqlTypeSpecOf(c.Type), !c.Type.Nullable, PrimaryKey: false));
        }

        var resolver = new Resolver(cat);
        resolver.Resolve(new CreateTableStatement("t", colDefs));

        var select = new SelectStatement(
            Items: [new ExpressionSelectItem(ast, Alias: null)],
            From: new TableReference("t", Alias: null),
            Where: null,
            GroupBy: Array.Empty<Expression>(),
            Having: null,
            Ctes: Array.Empty<CteDefinition>());
        var proj = (ProjectPlan)((SelectPlan)resolver.Resolve(select)).Query;
        return proj.Projections[0].Expression;
    }

    private static (Type RowType, Func<object?[], object> Factory) RowFor(params (string Name, SqlType Type)[] cols)
    {
        var schema = new Schema(cols.Select(c => new SchemaColumn(c.Name, c.Type)).ToList());
        var rowType = TypedRowEmitter.EmitRowType(schema)!;
        var factory = TypedRowEmitter.BuildBoxedFactory(schema)!;
        return (rowType, factory);
    }

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

    private static SqlTypeSpec SqlTypeSpecOf(SqlType t) => t switch
    {
        SqlIntegerType => new SqlTypeSpec("INTEGER"),
        SqlVarcharType v => new SqlTypeSpec("VARCHAR", v.MaxLength),
        _ => throw new NotSupportedException(),
    };
}
