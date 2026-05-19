using DbspNet.Sql.Compiler;
using DbspNet.Sql.Expressions;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Tests.Sql;

/// <summary>
/// Direct tests for <see cref="TypedExpressionCompiler"/>. Lowers
/// resolver-built expressions against a typed row struct and verifies
/// the compiled delegate returns the expected value (or that
/// compilation is refused for unsupported shapes).
/// </summary>
public class TypedExpressionCompilerTests
{
    private static ResolvedExpression ResolveSelectExpression(string[] ddl, string selectExpr)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in ddl)
        {
            resolver.Resolve(Parser.ParseStatement(s));
        }

        var plan = (SelectPlan)resolver.Resolve(
            Parser.ParseStatement($"SELECT {selectExpr} FROM t"));
        var project = (ProjectPlan)plan.Query;
        return project.Projections[0].Expression;
    }

    private static (Type RowType, Func<object?[], object> Factory) RowFor(params (string Name, SqlType Type)[] cols)
    {
        var schema = new Schema(cols.Select(c => new SchemaColumn(c.Name, c.Type)).ToList());
        var rowType = TypedRowEmitter.EmitRowType(schema)!;
        var factory = TypedRowEmitter.BuildBoxedFactory(schema)!;
        return (rowType, factory);
    }

    private static object Invoke(Delegate del, object row)
    {
        // Delegate.DynamicInvoke unwraps the single TArg → TResult call.
        return del.DynamicInvoke(row)!;
    }

    [Fact]
    public void ColumnRead_Int()
    {
        var (rowType, factory) = RowFor(
            ("a", new SqlIntegerType(false)),
            ("b", new SqlIntegerType(false)));
        var expr = ResolveSelectExpression(
            ["CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)"], "a");

        var del = TypedExpressionCompiler.TryCompile(expr, rowType);
        Assert.NotNull(del);
        Assert.Equal(7, Invoke(del!, factory(new object?[] { 7, 99 })));
    }

    [Fact]
    public void Literal_Int()
    {
        var (rowType, factory) = RowFor(("a", new SqlIntegerType(false)));
        var expr = ResolveSelectExpression(
            ["CREATE TABLE t (a INT NOT NULL)"], "42");

        var del = TypedExpressionCompiler.TryCompile(expr, rowType);
        Assert.NotNull(del);
        Assert.Equal(42, Invoke(del!, factory(new object?[] { 0 })));
    }

    [Fact]
    public void Arithmetic_AddSubMulDivMod()
    {
        var (rowType, factory) = RowFor(
            ("a", new SqlIntegerType(false)),
            ("b", new SqlIntegerType(false)));
        var ddl = new[] { "CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)" };

        var row = factory(new object?[] { 17, 5 });
        Assert.Equal(22, Invoke(TypedExpressionCompiler.TryCompile(ResolveSelectExpression(ddl, "a + b"), rowType)!, row));
        Assert.Equal(12, Invoke(TypedExpressionCompiler.TryCompile(ResolveSelectExpression(ddl, "a - b"), rowType)!, row));
        Assert.Equal(85, Invoke(TypedExpressionCompiler.TryCompile(ResolveSelectExpression(ddl, "a * b"), rowType)!, row));
        Assert.Equal(3, Invoke(TypedExpressionCompiler.TryCompile(ResolveSelectExpression(ddl, "a / b"), rowType)!, row));
        Assert.Equal(2, Invoke(TypedExpressionCompiler.TryCompile(ResolveSelectExpression(ddl, "a % b"), rowType)!, row));
    }

    [Fact]
    public void Comparison_AllSixOps()
    {
        var (rowType, factory) = RowFor(
            ("a", new SqlIntegerType(false)),
            ("b", new SqlIntegerType(false)));
        var ddl = new[] { "CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)" };

        var row = factory(new object?[] { 3, 5 });
        Assert.Equal(false, Invoke(TypedExpressionCompiler.TryCompile(ResolveSelectExpression(ddl, "a = b"), rowType)!, row));
        Assert.Equal(true, Invoke(TypedExpressionCompiler.TryCompile(ResolveSelectExpression(ddl, "a <> b"), rowType)!, row));
        Assert.Equal(true, Invoke(TypedExpressionCompiler.TryCompile(ResolveSelectExpression(ddl, "a < b"), rowType)!, row));
        Assert.Equal(true, Invoke(TypedExpressionCompiler.TryCompile(ResolveSelectExpression(ddl, "a <= b"), rowType)!, row));
        Assert.Equal(false, Invoke(TypedExpressionCompiler.TryCompile(ResolveSelectExpression(ddl, "a > b"), rowType)!, row));
        Assert.Equal(false, Invoke(TypedExpressionCompiler.TryCompile(ResolveSelectExpression(ddl, "a >= b"), rowType)!, row));
    }

    [Fact]
    public void Comparison_OnVarchar()
    {
        var (rowType, factory) = RowFor(("name", new SqlVarcharType(null, false)));
        var ddl = new[] { "CREATE TABLE t (name VARCHAR NOT NULL)" };

        var aliceRow = factory(new object?[] { Utf8String.Of("alice") });
        var bobRow = factory(new object?[] { Utf8String.Of("bob") });

        var eqExpr = ResolveSelectExpression(ddl, "name = 'alice'");
        var del = TypedExpressionCompiler.TryCompile(eqExpr, rowType);
        Assert.NotNull(del);
        Assert.Equal(true, Invoke(del!, aliceRow));
        Assert.Equal(false, Invoke(del!, bobRow));

        var ltExpr = ResolveSelectExpression(ddl, "name < 'b'");
        var ltDel = TypedExpressionCompiler.TryCompile(ltExpr, rowType);
        Assert.NotNull(ltDel);
        Assert.Equal(true, Invoke(ltDel!, aliceRow));
        Assert.Equal(false, Invoke(ltDel!, bobRow));
    }

    [Fact]
    public void Logical_AndOrNot()
    {
        var (rowType, factory) = RowFor(("a", new SqlIntegerType(false)));
        var ddl = new[] { "CREATE TABLE t (a INT NOT NULL)" };

        Assert.Equal(true, Invoke(
            TypedExpressionCompiler.TryCompile(ResolveSelectExpression(ddl, "a > 0 AND a < 10"), rowType)!,
            factory(new object?[] { 5 })));
        Assert.Equal(false, Invoke(
            TypedExpressionCompiler.TryCompile(ResolveSelectExpression(ddl, "a > 0 AND a < 10"), rowType)!,
            factory(new object?[] { 15 })));
        Assert.Equal(true, Invoke(
            TypedExpressionCompiler.TryCompile(ResolveSelectExpression(ddl, "a < 0 OR a > 10"), rowType)!,
            factory(new object?[] { 15 })));
        Assert.Equal(true, Invoke(
            TypedExpressionCompiler.TryCompile(ResolveSelectExpression(ddl, "NOT (a = 0)"), rowType)!,
            factory(new object?[] { 1 })));
    }

    [Fact]
    public void Unary_Negate()
    {
        var (rowType, factory) = RowFor(("a", new SqlIntegerType(false)));
        var expr = ResolveSelectExpression(
            ["CREATE TABLE t (a INT NOT NULL)"], "-a");

        var del = TypedExpressionCompiler.TryCompile(expr, rowType);
        Assert.NotNull(del);
        Assert.Equal(-5, Invoke(del!, factory(new object?[] { 5 })));
    }

    [Fact]
    public void Cast_IntToBigint()
    {
        var (rowType, factory) = RowFor(("a", new SqlIntegerType(false)));
        var expr = ResolveSelectExpression(
            ["CREATE TABLE t (a INT NOT NULL)"], "CAST(a AS BIGINT)");

        var del = TypedExpressionCompiler.TryCompile(expr, rowType);
        Assert.NotNull(del);
        Assert.Equal(42L, Invoke(del!, factory(new object?[] { 42 })));
    }

    [Fact]
    public void IsNull_OnNonNullExpr_CollapsesToFalse()
    {
        var (rowType, factory) = RowFor(("a", new SqlIntegerType(false)));
        var expr = ResolveSelectExpression(
            ["CREATE TABLE t (a INT NOT NULL)"], "a IS NULL");

        var del = TypedExpressionCompiler.TryCompile(expr, rowType);
        Assert.NotNull(del);
        Assert.Equal(false, Invoke(del!, factory(new object?[] { 0 })));
        Assert.Equal(false, Invoke(del!, factory(new object?[] { 999 })));
    }

    [Fact]
    public void IsNotNull_OnNonNullExpr_CollapsesToTrue()
    {
        var (rowType, factory) = RowFor(("a", new SqlIntegerType(false)));
        var expr = ResolveSelectExpression(
            ["CREATE TABLE t (a INT NOT NULL)"], "a IS NOT NULL");

        var del = TypedExpressionCompiler.TryCompile(expr, rowType);
        Assert.NotNull(del);
        Assert.Equal(true, Invoke(del!, factory(new object?[] { 0 })));
    }

    // ----- Rejection cases -----

    [Fact]
    public void Rejects_FunctionCalls()
    {
        var (rowType, _) = RowFor(("a", new SqlIntegerType(false)));
        var expr = ResolveSelectExpression(
            ["CREATE TABLE t (a INT NOT NULL)"], "ABS(a)");

        Assert.Null(TypedExpressionCompiler.TryCompile(expr, rowType));
    }

    [Fact]
    public void Rejects_DecimalResultExpression()
    {
        // Decimal-typed expression rejected at the type gate even when
        // the row type itself is fine.
        var (rowType, _) = RowFor(("a", new SqlIntegerType(false)));
        var expr = ResolveSelectExpression(
            ["CREATE TABLE t (a INT NOT NULL)"], "CAST(a AS DECIMAL(10, 2))");

        Assert.Null(TypedExpressionCompiler.TryCompile(expr, rowType));
    }

    [Fact]
    public void Rejects_CastToVarchar()
    {
        var (rowType, _) = RowFor(("a", new SqlIntegerType(false)));
        var expr = ResolveSelectExpression(
            ["CREATE TABLE t (a INT NOT NULL)"], "CAST(a AS VARCHAR)");

        Assert.Null(TypedExpressionCompiler.TryCompile(expr, rowType));
    }
}
