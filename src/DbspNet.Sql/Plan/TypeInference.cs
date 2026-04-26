using DbspNet.Sql.Parser.Ast;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Sql.Plan;

/// <summary>
/// SQL → CLR type resolution and numeric/string coercion rules used by
/// <see cref="Resolver"/>. These rules follow PostgreSQL where it matters
/// for v1: numeric-promotion up a fixed lattice; strings are comparable only
/// with strings; booleans only with booleans.
/// </summary>
internal static class TypeInference
{
    /// <summary>Convert a parser-level <see cref="SqlTypeSpec"/> to a concrete
    /// <see cref="SqlType"/> with the given nullability.</summary>
    public static SqlType FromSpec(SqlTypeSpec spec, bool nullable) => spec.Name switch
    {
        "INTEGER" => new SqlIntegerType(nullable),
        "BIGINT" => new SqlBigintType(nullable),
        "REAL" => new SqlRealType(nullable),
        "DOUBLE PRECISION" => new SqlDoubleType(nullable),
        "DECIMAL" => new SqlDecimalType(spec.Parameter1 ?? 38, spec.Parameter2 ?? 0, nullable),
        "VARCHAR" => new SqlVarcharType(spec.Parameter1, nullable),
        "CHAR" => new SqlVarcharType(spec.Parameter1, nullable),
        "BOOLEAN" => new SqlBooleanType(nullable),
        _ => throw new ResolveException($"unsupported type '{spec.Name}'"),
    };

    public static bool IsNumeric(SqlType t) =>
        t is SqlIntegerType or SqlBigintType or SqlRealType or SqlDoubleType or SqlDecimalType;

    public static bool IsString(SqlType t) => t is SqlVarcharType;

    public static bool IsBoolean(SqlType t) => t is SqlBooleanType;

    /// <summary>
    /// Promote two numerics to their common supertype for arithmetic /
    /// comparison. Result nullability is <c>a.Nullable || b.Nullable</c>.
    /// </summary>
    public static SqlType CommonNumericType(SqlType a, SqlType b)
    {
        if (!IsNumeric(a) || !IsNumeric(b))
        {
            throw new ResolveException($"numeric operand expected; got {a.Display} and {b.Display}");
        }

        var nullable = a.Nullable || b.Nullable;
        var rank = Math.Max(NumericRank(a), NumericRank(b));
        return rank switch
        {
            1 => new SqlIntegerType(nullable),
            2 => new SqlBigintType(nullable),
            3 => CombineDecimal(a, b, nullable),
            4 => new SqlRealType(nullable),
            5 => new SqlDoubleType(nullable),
            _ => throw new InvalidOperationException(),
        };
    }

    public static SqlType CommonComparableType(SqlType a, SqlType b)
    {
        if (IsNumeric(a) && IsNumeric(b))
        {
            return CommonNumericType(a, b);
        }

        if (IsString(a) && IsString(b))
        {
            var nullable = a.Nullable || b.Nullable;
            // Preserve max length if both sides have one, otherwise leave open.
            int? len = null;
            if (a is SqlVarcharType va && b is SqlVarcharType vb && va.MaxLength is { } la && vb.MaxLength is { } lb)
            {
                len = Math.Max(la, lb);
            }

            return new SqlVarcharType(len, nullable);
        }

        if (IsBoolean(a) && IsBoolean(b))
        {
            return new SqlBooleanType(a.Nullable || b.Nullable);
        }

        throw new ResolveException($"types {a.Display} and {b.Display} are not comparable");
    }

    private static int NumericRank(SqlType t) => t switch
    {
        SqlIntegerType => 1,
        SqlBigintType => 2,
        SqlDecimalType => 3,
        SqlRealType => 4,
        SqlDoubleType => 5,
        _ => 0,
    };

    private static SqlDecimalType CombineDecimal(SqlType a, SqlType b, bool nullable)
    {
        var pa = a is SqlDecimalType da ? da.Precision : 38;
        var sa = a is SqlDecimalType da2 ? da2.Scale : 0;
        var pb = b is SqlDecimalType db ? db.Precision : 38;
        var sb = b is SqlDecimalType db2 ? db2.Scale : 0;
        return new SqlDecimalType(Math.Max(pa, pb), Math.Max(sa, sb), nullable);
    }

    /// <summary>
    /// Type returned by a comparison operator on the given operand types —
    /// always BOOLEAN, nullable iff either operand is nullable.
    /// </summary>
    public static SqlType ComparisonResult(SqlType a, SqlType b)
    {
        // Validate comparability eagerly (throws if not).
        _ = CommonComparableType(a, b);
        return new SqlBooleanType(a.Nullable || b.Nullable);
    }
}
