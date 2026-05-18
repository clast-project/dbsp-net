using DbspNet.Sql.Expressions;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Parser.Ast;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Tests.Sql;

public class ExpressionCompilerTests
{
    // Helper: resolve a scalar expression against a synthetic schema, then compile it.
    private static Func<object?[], object?> Compile(string exprText, Schema schema)
    {
        var ast = new Parser(Lexer.Tokenize(exprText)).ParseExpression();
        var resolved = ResolveViaPublicApi(ast, schema);
        return ExpressionCompiler.CompileScalar(resolved);
    }

    private static Func<object?[], bool> CompilePred(string exprText, Schema schema)
    {
        var ast = new Parser(Lexer.Tokenize(exprText)).ParseExpression();
        var resolved = ResolveViaPublicApi(ast, schema);
        return ExpressionCompiler.CompilePredicate(resolved);
    }

    // The Resolver's scalar-expression machinery is `private static` — for
    // unit tests we go through a tiny SELECT and extract the projection.
    private static ResolvedExpression ResolveViaPublicApi(Expression ast, Schema schema)
    {
        var cat = new Catalog();
        // Fabricate a table named "t" mapping to the supplied schema.
        var colDefs = new List<ColumnDefinition>();
        foreach (var c in schema.Columns)
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
        SqlRealType => new SqlTypeSpec("REAL"),
        SqlDoubleType => new SqlTypeSpec("DOUBLE PRECISION"),
        SqlDecimalType d => new SqlTypeSpec("DECIMAL", d.Precision, d.Scale),
        SqlVarcharType v => new SqlTypeSpec("VARCHAR", v.MaxLength),
        SqlBooleanType => new SqlTypeSpec("BOOLEAN"),
        _ => throw new NotSupportedException(),
    };

    private static Schema Sch(params (string Name, SqlType Type)[] cols)
        => new(cols.Select(c => new SchemaColumn(c.Name, c.Type, Qualifier: "t")).ToList());

    // --- Arithmetic ---

    [Fact]
    public void Addition_OfIntegers_Sums()
    {
        var schema = Sch(("a", new SqlIntegerType(false)), ("b", new SqlIntegerType(false)));
        var f = Compile("a + b", schema);
        Assert.Equal(7, (int)f([3, 4])!);
    }

    [Fact]
    public void Addition_WithNull_PropagatesNull()
    {
        var schema = Sch(("a", new SqlIntegerType(true)), ("b", new SqlIntegerType(false)));
        var f = Compile("a + b", schema);
        Assert.Null(f([null, 4]));
    }

    [Fact]
    public void Division_PromotesToBigIntForMixedIntegers()
    {
        var schema = Sch(("a", new SqlIntegerType(false)), ("b", new SqlBigintType(false)));
        var f = Compile("a + b", schema);
        Assert.Equal(7L, (long)f([3, 4L])!);
    }

    [Fact]
    public void Modulo_Works()
    {
        var schema = Sch(("a", new SqlIntegerType(false)));
        var f = Compile("a % 3", schema);
        Assert.Equal(1, (int)f([10])!);
    }

    [Fact]
    public void UnaryMinus_Negates()
    {
        var schema = Sch(("a", new SqlIntegerType(false)));
        var f = Compile("-a", schema);
        Assert.Equal(-5, (int)f([5])!);
    }

    // --- Comparison ---

    [Fact]
    public void Comparison_PropagatesNull()
    {
        var schema = Sch(("a", new SqlIntegerType(true)));
        var f = Compile("a = 1", schema);
        Assert.Null(f([null]));
        Assert.Equal(true, f([1]));
        Assert.Equal(false, f([2]));
    }

    [Fact]
    public void StringComparison_IsOrdinal()
    {
        var schema = Sch(("s", new SqlVarcharType(null, false)));
        var f = Compile("s < 'n'", schema);
        Assert.Equal(true, f([Utf8String.Of("banana")]));
        Assert.Equal(false, f([Utf8String.Of("orange")]));
    }

    // --- 3VL boolean ---

    [Fact]
    public void And_FollowsThreeValuedLogic()
    {
        var schema = Sch(("a", new SqlBooleanType(true)), ("b", new SqlBooleanType(true)));
        var f = Compile("a AND b", schema);
        Assert.Equal(true, f([true, true]));
        Assert.Equal(false, f([true, false]));
        Assert.Equal(false, f([false, null]));   // FALSE dominates
        Assert.Null(f([true, null]));             // TRUE AND NULL → NULL
        Assert.Null(f([null, null]));
    }

    [Fact]
    public void Or_FollowsThreeValuedLogic()
    {
        var schema = Sch(("a", new SqlBooleanType(true)), ("b", new SqlBooleanType(true)));
        var f = Compile("a OR b", schema);
        Assert.Equal(true, f([true, null]));       // TRUE dominates
        Assert.Null(f([false, null]));              // FALSE OR NULL → NULL
    }

    [Fact]
    public void Not_ThreeValuedLogic()
    {
        var schema = Sch(("a", new SqlBooleanType(true)));
        var f = Compile("NOT a", schema);
        Assert.Equal(false, f([true]));
        Assert.Equal(true, f([false]));
        Assert.Null(f([null]));
    }

    // --- IS NULL / IS NOT NULL ---

    [Fact]
    public void IsNull_AlwaysDefinite()
    {
        var schema = Sch(("a", new SqlIntegerType(true)));
        var fn = Compile("a IS NULL", schema);
        var fnn = Compile("a IS NOT NULL", schema);
        Assert.Equal(true, fn([null]));
        Assert.Equal(false, fn([3]));
        Assert.Equal(false, fnn([null]));
        Assert.Equal(true, fnn([3]));
    }

    // --- Predicate compilation: NULL → FALSE ---

    [Fact]
    public void Predicate_NullCoercesToFalse()
    {
        var schema = Sch(("a", new SqlIntegerType(true)));
        var pred = CompilePred("a = 1", schema);
        Assert.False(pred([null]));
        Assert.True(pred([1]));
        Assert.False(pred([2]));
    }

    // --- CAST ---

    [Fact]
    public void Cast_IntegerToDouble()
    {
        var schema = Sch(("a", new SqlIntegerType(false)));
        var f = Compile("CAST(a AS DOUBLE PRECISION)", schema);
        Assert.Equal(3.0, (double)f([3])!);
    }

    [Fact]
    public void Cast_PreservesNull()
    {
        var schema = Sch(("a", new SqlIntegerType(true)));
        var f = Compile("CAST(a AS BIGINT)", schema);
        Assert.Null(f([null]));
    }

    [Fact]
    public void Cast_StringToInt_Parses()
    {
        var schema = Sch(("s", new SqlVarcharType(null, false)));
        var f = Compile("CAST(s AS INTEGER)", schema);
        Assert.Equal(42, (int)f([Utf8String.Of("42")])!);
    }

    // --- COALESCE ---

    [Fact]
    public void Coalesce_ReturnsFirstNonNull()
    {
        var schema = Sch(
            ("a", new SqlIntegerType(true)),
            ("b", new SqlIntegerType(true)),
            ("c", new SqlIntegerType(false)));
        var f = Compile("COALESCE(a, b, c)", schema);
        Assert.Equal(1, (int)f([1, 2, 3])!);
        Assert.Equal(2, (int)f([null, 2, 3])!);
        Assert.Equal(3, (int)f([null, null, 3])!);
    }

    // --- Full 3VL truth tables ---

    public static IEnumerable<object?[]> AndTruthTable => new[]
    {
        new object?[] { true,  true,  true  },
        new object?[] { true,  false, false },
        new object?[] { true,  null,  null  },
        new object?[] { false, true,  false },
        new object?[] { false, false, false },
        new object?[] { false, null,  false }, // FALSE dominates NULL
        new object?[] { null,  true,  null  },
        new object?[] { null,  false, false }, // FALSE dominates NULL
        new object?[] { null,  null,  null  },
    };

    [Theory]
    [MemberData(nameof(AndTruthTable))]
    public void And_FullTruthTable(bool? a, bool? b, bool? expected)
    {
        var schema = Sch(("a", new SqlBooleanType(true)), ("b", new SqlBooleanType(true)));
        var f = Compile("a AND b", schema);
        var result = f([a, b]);
        Assert.Equal((object?)expected, result);
    }

    public static IEnumerable<object?[]> OrTruthTable => new[]
    {
        new object?[] { true,  true,  true  },
        new object?[] { true,  false, true  },
        new object?[] { true,  null,  true  }, // TRUE dominates NULL
        new object?[] { false, true,  true  },
        new object?[] { false, false, false },
        new object?[] { false, null,  null  },
        new object?[] { null,  true,  true  }, // TRUE dominates NULL
        new object?[] { null,  false, null  },
        new object?[] { null,  null,  null  },
    };

    [Theory]
    [MemberData(nameof(OrTruthTable))]
    public void Or_FullTruthTable(bool? a, bool? b, bool? expected)
    {
        var schema = Sch(("a", new SqlBooleanType(true)), ("b", new SqlBooleanType(true)));
        var f = Compile("a OR b", schema);
        Assert.Equal((object?)expected, f([a, b]));
    }
}
