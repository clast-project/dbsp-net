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
/// Coverage for <c>CASE WHEN … THEN … [ELSE …] END</c>. The searched form is
/// modeled as a flat <see cref="CaseExpression"/> (so the recursive walkers
/// stay shallow regardless of arm count); the simple form
/// (<c>CASE x WHEN v THEN …</c>) desugars to the searched shape at parse time
/// by rewriting each arm to <c>x = v</c>. Branch evaluation is lazy and an arm
/// is taken iff its condition is a definite TRUE (SQL three-valued semantics).
/// </summary>
public class CaseWhenTests
{
    // ---------- Parser ----------

    [Fact]
    public void Parser_SearchedCase_ProducesFlatCaseExpression()
    {
        var expr = new Parser(Lexer.Tokenize(
            "CASE WHEN x > 1 THEN 'a' WHEN x > 2 THEN 'b' ELSE 'c' END")).ParseExpression();
        var ce = Assert.IsType<CaseExpression>(expr);
        Assert.Equal(2, ce.Whens.Count);
        Assert.NotNull(ce.ElseResult);
        // Searched form: condition is whatever the user wrote, untouched.
        Assert.IsType<BinaryExpression>(ce.Whens[0].Condition);
    }

    [Fact]
    public void Parser_SearchedCase_NoElse_LeavesElseNull()
    {
        var expr = new Parser(Lexer.Tokenize("CASE WHEN x > 1 THEN 'a' END")).ParseExpression();
        var ce = Assert.IsType<CaseExpression>(expr);
        Assert.Single(ce.Whens);
        Assert.Null(ce.ElseResult);
    }

    [Fact]
    public void Parser_SimpleCase_DesugarsToEqualityConditions()
    {
        // CASE x WHEN 1 THEN 'a' WHEN 2 THEN 'b' END
        //   → WHEN (x = 1) THEN 'a' WHEN (x = 2) THEN 'b'
        var expr = new Parser(Lexer.Tokenize(
            "CASE x WHEN 1 THEN 'a' WHEN 2 THEN 'b' END")).ParseExpression();
        var ce = Assert.IsType<CaseExpression>(expr);
        Assert.Equal(2, ce.Whens.Count);

        var c0 = Assert.IsType<BinaryExpression>(ce.Whens[0].Condition);
        Assert.Equal(BinaryOperator.Equal, c0.Operator);
        Assert.IsType<ColumnReference>(c0.Left);
        var lit = Assert.IsType<LiteralExpression>(c0.Right);
        Assert.Equal(1L, lit.Value);
    }

    [Fact]
    public void Parser_MissingWhen_Throws()
    {
        Assert.ThrowsAny<Exception>(() =>
            new Parser(Lexer.Tokenize("CASE ELSE 1 END")).ParseExpression());
    }

    [Fact]
    public void Parser_MissingEnd_Throws()
    {
        Assert.ThrowsAny<Exception>(() =>
            new Parser(Lexer.Tokenize("CASE WHEN x > 1 THEN 'a'")).ParseExpression());
    }

    [Fact]
    public void Parser_SubqueryOperandInSimpleCase_Throws()
    {
        Assert.ThrowsAny<Exception>(() =>
            new Parser(Lexer.Tokenize(
                "CASE (SELECT 1) WHEN 1 THEN 'a' END")).ParseExpression());
    }

    [Fact]
    public void Parser_LargeCase_DoesNotStackOverflow()
    {
        // Flat-list depth guard: 5000 arms is well past .NET's ~100-level
        // practical recursion limit for any nested-AST encoding.
        var arms = string.Join(" ", Enumerable.Range(0, 5000).Select(i => $"WHEN x = {i} THEN {i}"));
        var expr = new Parser(Lexer.Tokenize($"CASE {arms} ELSE -1 END")).ParseExpression();
        var ce = Assert.IsType<CaseExpression>(expr);
        Assert.Equal(5000, ce.Whens.Count);
    }

    // ---------- Resolver ----------

    [Fact]
    public void Resolver_UnifiesBranchTypes_IntAndDecimal_ToDecimal()
    {
        var resolved = (ResolvedCaseWhen)Resolve(
            "CASE WHEN a > 0 THEN 1 ELSE 2.5 END", ("a", new SqlIntegerType(false)));
        Assert.IsType<SqlDecimalType>(resolved.Type);
    }

    [Fact]
    public void Resolver_NoElse_ResultIsNullable_EvenWithNonNullBranches()
    {
        var resolved = (ResolvedCaseWhen)Resolve(
            "CASE WHEN a > 0 THEN 1 END", ("a", new SqlIntegerType(false)));
        Assert.True(resolved.Type.Nullable);
    }

    [Fact]
    public void Resolver_ElsePresent_NonNullBranches_ResultIsNonNullable()
    {
        var resolved = (ResolvedCaseWhen)Resolve(
            "CASE WHEN a > 0 THEN 1 ELSE 2 END", ("a", new SqlIntegerType(false)));
        Assert.False(resolved.Type.Nullable);
    }

    [Fact]
    public void Resolver_NonBooleanCondition_Throws()
    {
        var ex = Assert.Throws<ResolveException>(() =>
            Resolve("CASE WHEN a THEN 1 ELSE 2 END", ("a", new SqlIntegerType(false))));
        Assert.Contains("BOOLEAN", ex.Message);
    }

    [Fact]
    public void Resolver_IncompatibleBranchTypes_Throws()
    {
        Assert.Throws<ResolveException>(() =>
            Resolve("CASE WHEN a > 0 THEN 1 ELSE 'x' END", ("a", new SqlIntegerType(false))));
    }

    // ---------- Structural compiler eval ----------

    [Fact]
    public void Eval_FirstMatchingArmWins()
    {
        var f = CompileExpr(
            "CASE WHEN a > 10 THEN 1 WHEN a > 5 THEN 2 ELSE 3 END",
            ("a", new SqlIntegerType(false)));
        Assert.Equal(1, f([20]));
        Assert.Equal(2, f([7]));
        Assert.Equal(3, f([1]));
    }

    [Fact]
    public void Eval_NoMatch_NoElse_YieldsNull()
    {
        var f = CompileExpr("CASE WHEN a > 10 THEN 1 END", ("a", new SqlIntegerType(false)));
        Assert.Null(f([5]));
    }

    [Fact]
    public void Eval_NullCondition_FallsThrough()
    {
        // a > 5 is NULL when a is NULL (3VL): the arm is NOT taken, ELSE wins.
        var f = CompileExpr(
            "CASE WHEN a > 5 THEN 1 ELSE 0 END", ("a", new SqlIntegerType(true)));
        Assert.Equal(0, f([null]));
    }

    [Fact]
    public void Eval_NonTakenBranch_IsNotEvaluated()
    {
        // If the THEN of a non-taken arm were evaluated, 100 / 0 would throw.
        var f = CompileExpr(
            "CASE WHEN a <> 0 THEN 100 / a ELSE -1 END", ("a", new SqlIntegerType(false)));
        Assert.Equal(-1, f([0]));
        Assert.Equal(50, f([2]));
    }

    [Fact]
    public void Eval_SimpleCase_MatchesByEquality()
    {
        var f = CompileExpr(
            "CASE a WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END",
            ("a", new SqlIntegerType(false)));
        Assert.Equal(Utf8String.Of("one"), f([1]));
        Assert.Equal(Utf8String.Of("two"), f([2]));
        Assert.Equal(Utf8String.Of("other"), f([9]));
    }

    [Fact]
    public void Eval_SimpleCase_NullOperand_NeverMatches()
    {
        // NULL operand: NULL = v is UNKNOWN for every arm → ELSE.
        var f = CompileExpr(
            "CASE a WHEN 1 THEN 'one' ELSE 'other' END", ("a", new SqlIntegerType(true)));
        Assert.Equal(Utf8String.Of("other"), f([null]));
    }

    [Fact]
    public void Eval_LargeCase_CompilesAndEvaluates()
    {
        var arms = string.Join(" ", Enumerable.Range(0, 2000).Select(i => $"WHEN a = {i} THEN {i * 2}"));
        var f = CompileExpr($"CASE {arms} ELSE -1 END", ("a", new SqlIntegerType(false)));
        Assert.Equal(1000, f([500]));
        Assert.Equal(-1, f([99999]));
    }

    // ---------- Typed fast-path compiler ----------

    [Fact]
    public void Typed_AllNonNullBranches_CompilesToNonNullableInt()
    {
        var (rowType, factory) = RowFor(("a", new SqlIntegerType(false)));
        var expr = Resolve("CASE WHEN a > 5 THEN 1 ELSE 0 END", ("a", new SqlIntegerType(false)));

        var del = TypedExpressionCompiler.TryCompile(expr, rowType);
        Assert.NotNull(del);
        Assert.Equal(typeof(int), del!.Method.ReturnType);
        Assert.Equal(1, del.DynamicInvoke(factory(new object?[] { 9 })));
        Assert.Equal(0, del.DynamicInvoke(factory(new object?[] { 1 })));
    }

    [Fact]
    public void Typed_NoElse_CompilesToNullableInt()
    {
        var (rowType, factory) = RowFor(("a", new SqlIntegerType(false)));
        var expr = Resolve("CASE WHEN a > 5 THEN 1 END", ("a", new SqlIntegerType(false)));

        var del = TypedExpressionCompiler.TryCompile(expr, rowType);
        Assert.NotNull(del);
        Assert.Equal(typeof(int?), del!.Method.ReturnType);
        Assert.Equal(1, del.DynamicInvoke(factory(new object?[] { 9 })));
        Assert.Null(del.DynamicInvoke(factory(new object?[] { 1 })));
    }

    [Fact]
    public void Typed_NonTakenBranch_IsNotEvaluated()
    {
        var (rowType, factory) = RowFor(("a", new SqlIntegerType(false)));
        var expr = Resolve(
            "CASE WHEN a <> 0 THEN 100 / a ELSE -1 END", ("a", new SqlIntegerType(false)));

        var del = TypedExpressionCompiler.TryCompile(expr, rowType);
        Assert.NotNull(del);
        Assert.Equal(-1, del!.DynamicInvoke(factory(new object?[] { 0 })));
        Assert.Equal(50, del.DynamicInvoke(factory(new object?[] { 2 })));
    }

    // ---------- End-to-end ----------

    [Fact]
    public void EndToEnd_CaseInProjection_BucketsRows()
    {
        var q = CompileView(
            ["CREATE TABLE t (id INT NOT NULL, score INT NOT NULL)"],
            "SELECT CASE WHEN score >= 90 THEN 'A' WHEN score >= 80 THEN 'B' ELSE 'F' END FROM t");

        q.Table("t").Insert(1, 95);
        q.Table("t").Insert(2, 85);
        q.Table("t").Insert(3, 50);
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, Utf8String.Of("A")));
        Assert.Equal(1, WeightOf(q.Current, Utf8String.Of("B")));
        Assert.Equal(1, WeightOf(q.Current, Utf8String.Of("F")));
        Assert.Equal(3, q.Current.Count);
    }

    [Fact]
    public void EndToEnd_CaseInProjection_RetractsCleanly()
    {
        var q = CompileView(
            ["CREATE TABLE t (id INT NOT NULL, score INT NOT NULL)"],
            "SELECT CASE WHEN score >= 90 THEN 'A' ELSE 'F' END FROM t");

        q.Table("t").Insert(1, 95);
        q.Step();
        Assert.Equal(1, WeightOf(q.Current, Utf8String.Of("A")));

        q.Table("t").Delete(1, 95);
        q.Step();
        // Current holds the per-step delta: a single negative-weight retraction.
        Assert.Equal(-1, WeightOf(q.Current, Utf8String.Of("A")));
    }

    [Fact]
    public void EndToEnd_AggregateInsideCase_ConditionalSum()
    {
        // SUM over a CASE — the classic conditional-aggregation pattern.
        var q = CompileView(
            ["CREATE TABLE t (cat VARCHAR NOT NULL, amt INT NOT NULL)"],
            "SELECT cat, SUM(CASE WHEN amt > 0 THEN amt ELSE 0 END) FROM t GROUP BY cat");

        q.Table("t").Insert(Utf8String.Of("x"), 10);
        q.Table("t").Insert(Utf8String.Of("x"), -5);
        q.Table("t").Insert(Utf8String.Of("x"), 3);
        q.Table("t").Insert(Utf8String.Of("y"), 7);
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, Utf8String.Of("x"), 13L));
        Assert.Equal(1, WeightOf(q.Current, Utf8String.Of("y"), 7L));
    }

    // ---------- Helpers ----------

    private static Func<object?[], object?> CompileExpr(string exprText, params (string Name, SqlType Type)[] cols)
    {
        var schema = new Schema(cols.Select(c => new SchemaColumn(c.Name, c.Type, "t")).ToList());
        var ast = new Parser(Lexer.Tokenize(exprText)).ParseExpression();
        var resolved = ResolveViaPublicApi(ast, cols);
        var fn = ExpressionCompiler.CompileScalar(resolved);
        return arr => fn(arr);
    }

    private static ResolvedExpression Resolve(string exprText, params (string Name, SqlType Type)[] cols)
    {
        var ast = new Parser(Lexer.Tokenize(exprText)).ParseExpression();
        return ResolveViaPublicApi(ast, cols);
    }

    private static ResolvedExpression ResolveViaPublicApi(Expression ast, (string Name, SqlType Type)[] cols)
    {
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
        SqlBigintType => new SqlTypeSpec("BIGINT"),
        SqlDecimalType d => new SqlTypeSpec("DECIMAL", d.Precision, d.Scale),
        SqlVarcharType v => new SqlTypeSpec("VARCHAR", v.MaxLength),
        SqlBooleanType => new SqlTypeSpec("BOOLEAN"),
        _ => throw new NotSupportedException(),
    };
}
