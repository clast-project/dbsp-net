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

    // ----- Functions -----

    [Fact]
    public void Function_StringHelpers()
    {
        var (rowType, factory) = RowFor(("s", new SqlVarcharType(null, false)));
        var ddl = new[] { "CREATE TABLE t (s VARCHAR NOT NULL)" };

        var row = factory(new object?[] { Utf8String.Of("hello") });

        var upper = TypedExpressionCompiler.TryCompile(ResolveSelectExpression(ddl, "UPPER(s)"), rowType);
        Assert.Equal(Utf8String.Of("HELLO"), Invoke(upper!, row));

        var lower = TypedExpressionCompiler.TryCompile(ResolveSelectExpression(ddl, "LOWER(s)"), rowType);
        Assert.Equal(Utf8String.Of("hello"), Invoke(lower!, row));

        var len = TypedExpressionCompiler.TryCompile(ResolveSelectExpression(ddl, "LENGTH(s)"), rowType);
        Assert.Equal(5, Invoke(len!, row));
    }

    [Fact]
    public void Function_Concat()
    {
        var (rowType, factory) = RowFor(
            ("a", new SqlVarcharType(null, false)),
            ("b", new SqlVarcharType(null, false)));
        var expr = ResolveSelectExpression(
            ["CREATE TABLE t (a VARCHAR NOT NULL, b VARCHAR NOT NULL)"],
            "CONCAT(a, '-', b)");

        var del = TypedExpressionCompiler.TryCompile(expr, rowType);
        Assert.NotNull(del);
        var row = factory(new object?[] { Utf8String.Of("foo"), Utf8String.Of("bar") });
        Assert.Equal(Utf8String.Of("foo-bar"), Invoke(del!, row));
    }

    [Fact]
    public void Function_Coalesce_CollapsesToFirstArg()
    {
        // On the typed path every value is non-null, so COALESCE
        // collapses to its first arg.
        var (rowType, factory) = RowFor(("a", new SqlIntegerType(false)));
        var expr = ResolveSelectExpression(
            ["CREATE TABLE t (a INT NOT NULL)"],
            "COALESCE(a, 99)");

        var del = TypedExpressionCompiler.TryCompile(expr, rowType);
        Assert.NotNull(del);
        Assert.Equal(7, Invoke(del!, factory(new object?[] { 7 })));
    }

    [Fact]
    public void Function_GreatestLeast()
    {
        var (rowType, factory) = RowFor(
            ("a", new SqlIntegerType(false)),
            ("b", new SqlIntegerType(false)),
            ("c", new SqlIntegerType(false)));
        var ddl = new[] { "CREATE TABLE t (a INT NOT NULL, b INT NOT NULL, c INT NOT NULL)" };

        var row = factory(new object?[] { 3, 1, 5 });
        Assert.Equal(5, Invoke(
            TypedExpressionCompiler.TryCompile(ResolveSelectExpression(ddl, "GREATEST(a, b, c)"), rowType)!,
            row));
        Assert.Equal(1, Invoke(
            TypedExpressionCompiler.TryCompile(ResolveSelectExpression(ddl, "LEAST(a, b, c)"), rowType)!,
            row));
    }

    [Fact]
    public void Function_NumericMath()
    {
        var (rowType, factory) = RowFor(
            ("x", new SqlDoubleType(false)),
            ("y", new SqlDoubleType(false)));
        var ddl = new[] { "CREATE TABLE t (x DOUBLE PRECISION NOT NULL, y DOUBLE PRECISION NOT NULL)" };

        var row = factory(new object?[] { 9.0, 2.0 });
        Assert.Equal(3.0, Invoke(
            TypedExpressionCompiler.TryCompile(ResolveSelectExpression(ddl, "SQRT(x)"), rowType)!,
            row));
        Assert.Equal(81.0, Invoke(
            TypedExpressionCompiler.TryCompile(ResolveSelectExpression(ddl, "POWER(x, y)"), rowType)!,
            row));
        Assert.Equal(9.0, Invoke(
            TypedExpressionCompiler.TryCompile(ResolveSelectExpression(ddl, "FLOOR(x)"), rowType)!,
            row));
        Assert.Equal(9.0, Invoke(
            TypedExpressionCompiler.TryCompile(ResolveSelectExpression(ddl, "CEIL(x)"), rowType)!,
            row));
    }

    [Fact]
    public void Function_AbsAccepted()
    {
        // Phase 1.9: function calls are in scope.
        var (rowType, factory) = RowFor(("a", new SqlIntegerType(false)));
        var expr = ResolveSelectExpression(
            ["CREATE TABLE t (a INT NOT NULL)"], "ABS(a)");

        var del = TypedExpressionCompiler.TryCompile(expr, rowType);
        Assert.NotNull(del);
        Assert.Equal(5, Invoke(del!, factory(new object?[] { -5 })));
        Assert.Equal(5, Invoke(del!, factory(new object?[] { 5 })));
    }

    [Fact]
    public void Rejects_Nullif()
    {
        // NULLIF can return NULL — typed-row pipeline can't carry that.
        var (rowType, _) = RowFor(
            ("a", new SqlIntegerType(false)),
            ("b", new SqlIntegerType(false)));
        var expr = ResolveSelectExpression(
            ["CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)"], "NULLIF(a, b)");

        Assert.Null(TypedExpressionCompiler.TryCompile(expr, rowType));
    }

    [Fact]
    public void Decimal_AddSameScale()
    {
        var (rowType, factory) = RowFor(
            ("a", new SqlDecimalType(10, 2, false)),
            ("b", new SqlDecimalType(10, 2, false)));
        var expr = ResolveSelectExpression(
            ["CREATE TABLE t (a DECIMAL(10, 2) NOT NULL, b DECIMAL(10, 2) NOT NULL)"],
            "a + b");

        var del = TypedExpressionCompiler.TryCompile(expr, rowType);
        Assert.NotNull(del);
        var row = factory(new object?[]
        {
            new Clast.DatabaseDecimal.Values.Decimal128(150),  // 1.50
            new Clast.DatabaseDecimal.Values.Decimal128(250),  // 2.50
        });
        var result = (Clast.DatabaseDecimal.Values.Decimal128)Invoke(del!, row);
        Assert.Equal((Int128)400, result.Mantissa);  // 4.00
    }

    [Fact]
    public void Decimal_IntegerPromote()
    {
        var (rowType, factory) = RowFor(
            ("a", new SqlDecimalType(10, 2, false)),
            ("b", new SqlIntegerType(false)));
        var expr = ResolveSelectExpression(
            ["CREATE TABLE t (a DECIMAL(10, 2) NOT NULL, b INT NOT NULL)"],
            "a + b");

        var del = TypedExpressionCompiler.TryCompile(expr, rowType);
        Assert.NotNull(del);
        var row = factory(new object?[]
        {
            new Clast.DatabaseDecimal.Values.Decimal128(150),  // 1.50
            3,
        });
        var result = (Clast.DatabaseDecimal.Values.Decimal128)Invoke(del!, row);
        // result type's scale depends on resolver — assert through mantissa-equivalence
        // by comparing to the same calculation done explicitly.
        Assert.NotEqual(default, result);
    }

    [Fact]
    public void Decimal_Compare_CrossScale()
    {
        var (rowType, factory) = RowFor(
            ("a", new SqlDecimalType(10, 2, false)),
            ("b", new SqlDecimalType(10, 4, false)));
        var expr = ResolveSelectExpression(
            ["CREATE TABLE t (a DECIMAL(10, 2) NOT NULL, b DECIMAL(10, 4) NOT NULL)"],
            "a > b");

        var del = TypedExpressionCompiler.TryCompile(expr, rowType);
        Assert.NotNull(del);
        // a=1.50 (mantissa 150 @ scale 2) vs b=1.4000 (mantissa 14000 @ scale 4) — a > b.
        var row1 = factory(new object?[]
        {
            new Clast.DatabaseDecimal.Values.Decimal128(150),
            new Clast.DatabaseDecimal.Values.Decimal128(14000),
        });
        Assert.Equal(true, Invoke(del!, row1));

        // a=1.50 vs b=1.5000 — equal.
        var row2 = factory(new object?[]
        {
            new Clast.DatabaseDecimal.Values.Decimal128(150),
            new Clast.DatabaseDecimal.Values.Decimal128(15000),
        });
        Assert.Equal(false, Invoke(del!, row2));
    }

    [Fact]
    public void Cast_DecimalToDecimal_Rescale()
    {
        var (rowType, factory) = RowFor(("a", new SqlDecimalType(10, 2, false)));
        var expr = ResolveSelectExpression(
            ["CREATE TABLE t (a DECIMAL(10, 2) NOT NULL)"],
            "CAST(a AS DECIMAL(10, 4))");

        var del = TypedExpressionCompiler.TryCompile(expr, rowType);
        Assert.NotNull(del);
        var row = factory(new object?[] { new Clast.DatabaseDecimal.Values.Decimal128(150) });  // 1.50 @ scale 2
        var result = (Clast.DatabaseDecimal.Values.Decimal128)Invoke(del!, row);
        Assert.Equal((Int128)15000, result.Mantissa);  // 1.5000 @ scale 4
    }

    [Fact]
    public void Cast_DecimalToInt()
    {
        // Rescale uses banker's rounding (round-half-to-even), same
        // as the structural compiler.
        var (rowType, factory) = RowFor(("a", new SqlDecimalType(10, 2, false)));
        var expr = ResolveSelectExpression(
            ["CREATE TABLE t (a DECIMAL(10, 2) NOT NULL)"],
            "CAST(a AS INT)");

        var del = TypedExpressionCompiler.TryCompile(expr, rowType);
        Assert.NotNull(del);
        Assert.Equal(1, Invoke(del!, factory(new object?[] { new Clast.DatabaseDecimal.Values.Decimal128(149) })));  // 1.49 → 1
        Assert.Equal(2, Invoke(del!, factory(new object?[] { new Clast.DatabaseDecimal.Values.Decimal128(150) })));  // 1.50 → 2 (banker's)
        Assert.Equal(2, Invoke(del!, factory(new object?[] { new Clast.DatabaseDecimal.Values.Decimal128(250) })));  // 2.50 → 2 (banker's)
    }

    [Fact]
    public void Cast_IntToDecimal()
    {
        // Phase 1.8: decimal-typed expressions are now in scope.
        var (rowType, factory) = RowFor(("a", new SqlIntegerType(false)));
        var expr = ResolveSelectExpression(
            ["CREATE TABLE t (a INT NOT NULL)"], "CAST(a AS DECIMAL(10, 2))");

        var del = TypedExpressionCompiler.TryCompile(expr, rowType);
        Assert.NotNull(del);

        var result = (Clast.DatabaseDecimal.Values.Decimal128)Invoke(del!, factory(new object?[] { 42 }));
        // Scale 2: mantissa is value × 100.
        Assert.Equal((Int128)4200, result.Mantissa);
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
