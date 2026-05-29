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
/// Coverage for <c>a IS [NOT] DISTINCT FROM b</c> — NULL-safe (in)equality.
/// Always a definite boolean (never NULL): two NULLs are "not distinct"
/// (equal), and a one-sided NULL is "distinct". Implemented as a pure
/// parse-time desugar to guarded AND/OR/IS NULL/= nodes, so it rides the
/// existing resolver and both compilers.
/// </summary>
public class DistinctFromTests
{
    private static readonly SqlIntegerType IntN = new(Nullable: true);

    // ---------- Parser ----------

    [Fact]
    public void Parser_IsDistinctFrom_NegatesNullSafeEqual()
    {
        // IS DISTINCT FROM → NOT (null-safe-equal).
        var expr = new Parser(Lexer.Tokenize("a IS DISTINCT FROM b")).ParseExpression();
        var not = Assert.IsType<UnaryExpression>(expr);
        Assert.Equal(UnaryOperator.Not, not.Operator);
        Assert.Equal(BinaryOperator.Or, Assert.IsType<BinaryExpression>(not.Operand).Operator);
    }

    [Fact]
    public void Parser_IsNotDistinctFrom_IsNullSafeEqual()
    {
        // IS NOT DISTINCT FROM → the null-safe-equal disjunction (no outer NOT).
        var expr = new Parser(Lexer.Tokenize("a IS NOT DISTINCT FROM b")).ParseExpression();
        var or = Assert.IsType<BinaryExpression>(expr);
        Assert.Equal(BinaryOperator.Or, or.Operator);
    }

    [Fact]
    public void Parser_IsDistinct_WithoutFrom_Throws()
    {
        Assert.ThrowsAny<Exception>(() =>
            new Parser(Lexer.Tokenize("a IS DISTINCT b")).ParseExpression());
    }

    [Fact]
    public void Parser_SubqueryOperand_Throws()
    {
        Assert.ThrowsAny<Exception>(() =>
            new Parser(Lexer.Tokenize("a IS DISTINCT FROM (SELECT 1)")).ParseExpression());
    }

    [Fact]
    public void Parser_StillSupportsIsNotNull()
    {
        // The DISTINCT branch must not break the plain IS [NOT] NULL path.
        var expr = new Parser(Lexer.Tokenize("a IS NOT NULL")).ParseExpression();
        var isn = Assert.IsType<IsNullExpression>(expr);
        Assert.True(isn.Negated);
    }

    // ---------- Eval: full truth table ----------

    [Fact]
    public void Eval_IsDistinctFrom_TruthTable()
    {
        var f = CompileExpr("a IS DISTINCT FROM b", ("a", IntN), ("b", IntN));
        Assert.Equal(false, f([1, 1]));        // equal, both present
        Assert.Equal(true, f([1, 2]));         // differ
        Assert.Equal(true, f([null, 1]));      // one NULL → distinct
        Assert.Equal(true, f([1, null]));      // one NULL → distinct
        Assert.Equal(false, f([null, null]));  // both NULL → not distinct
    }

    [Fact]
    public void Eval_IsNotDistinctFrom_TruthTable()
    {
        var f = CompileExpr("a IS NOT DISTINCT FROM b", ("a", IntN), ("b", IntN));
        Assert.Equal(true, f([1, 1]));
        Assert.Equal(false, f([1, 2]));
        Assert.Equal(false, f([null, 1]));
        Assert.Equal(false, f([1, null]));
        Assert.Equal(true, f([null, null]));   // NULL-safe equal
    }

    [Fact]
    public void Eval_NeverYieldsNull()
    {
        // The defining property: the result is always a definite boolean,
        // even though the operands (and the inner `a = b`) are nullable.
        var f = CompileExpr("a IS DISTINCT FROM b", ("a", IntN), ("b", IntN));
        Assert.NotNull(f([null, 5]));
        Assert.NotNull(f([null, null]));
    }

    // ---------- Typed fast path ----------

    [Fact]
    public void Typed_IsNotDistinctFrom_Compiles()
    {
        var (rowType, factory) = RowFor(("a", IntN), ("b", IntN));
        var expr = Resolve("a IS NOT DISTINCT FROM b", ("a", IntN), ("b", IntN));

        var del = TypedExpressionCompiler.TryCompile(expr, rowType);
        Assert.NotNull(del);
        Assert.Equal(true, del!.DynamicInvoke(factory(new object?[] { null, null })));
        Assert.Equal(false, del.DynamicInvoke(factory(new object?[] { null, 1 })));
        Assert.Equal(true, del.DynamicInvoke(factory(new object?[] { 3, 3 })));
    }

    // ---------- End-to-end ----------

    [Fact]
    public void EndToEnd_WhereIsNotDistinctFrom_KeepsBothNullRows()
    {
        // NULL-safe equality: rows where a and b are both NULL pass the filter
        // (a = b would drop them, since NULL = NULL is UNKNOWN).
        var q = CompileView(
            ["CREATE TABLE t (id INT NOT NULL, a INT, b INT)"],
            "SELECT id FROM t WHERE a IS NOT DISTINCT FROM b");

        q.Table("t").Insert(1, 5, 5);              // equal → keep
        q.Table("t").Insert(2, 5, 6);              // differ → drop
        q.Table("t").Insert(3, (object?)null, 7);  // one NULL → drop
        q.Table("t").Insert(4, (object?)null, (object?)null);  // both NULL → keep
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 1));
        Assert.Equal(1, WeightOf(q.Current, 4));
        Assert.Equal(2, q.Current.Count);
    }

    [Fact]
    public void EndToEnd_IsDistinctFrom_InProjection()
    {
        var q = CompileView(
            ["CREATE TABLE t (id INT NOT NULL, a INT, b INT)"],
            "SELECT id, a IS DISTINCT FROM b AS d FROM t");

        q.Table("t").Insert(1, 5, 5);
        q.Table("t").Insert(2, (object?)null, (object?)null);
        q.Table("t").Insert(3, (object?)null, 9);
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 1, false));
        Assert.Equal(1, WeightOf(q.Current, 2, false));
        Assert.Equal(1, WeightOf(q.Current, 3, true));
        Assert.Equal(3, q.Current.Count);
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
