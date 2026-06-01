// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
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
        "DATE" => new SqlDateType(nullable),
        "TIME" => new SqlTimeType(nullable),
        "TIMESTAMP" => new SqlTimestampType(nullable),
        "INTERVAL" => new SqlIntervalType(
            spec.IntervalQualifier ?? throw new ResolveException("INTERVAL requires a field qualifier"),
            nullable),
        _ => throw new ResolveException($"unsupported type '{spec.Name}'"),
    };

    public static bool IsNumeric(SqlType t) =>
        t is SqlIntegerType or SqlBigintType or SqlRealType or SqlDoubleType or SqlDecimalType;

    public static bool IsString(SqlType t) => t is SqlVarcharType;

    public static bool IsBoolean(SqlType t) => t is SqlBooleanType;

    /// <summary>
    /// True for date/time/timestamp types. Temporal types are comparable and
    /// equatable only with their own kind — DATE with DATE, TIME with TIME,
    /// TIMESTAMP with TIMESTAMP. No implicit promotion across temporal kinds.
    /// </summary>
    public static bool IsTemporal(SqlType t) =>
        t is SqlDateType or SqlTimeType or SqlTimestampType;

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

        if (a is SqlDateType && b is SqlDateType)
        {
            return new SqlDateType(a.Nullable || b.Nullable);
        }

        if (a is SqlTimeType && b is SqlTimeType)
        {
            return new SqlTimeType(a.Nullable || b.Nullable);
        }

        if (a is SqlTimestampType && b is SqlTimestampType)
        {
            return new SqlTimestampType(a.Nullable || b.Nullable);
        }

        // Two intervals are comparable; the result keeps the left qualifier
        // when both share an interval class, else falls back to a generic
        // day-time qualifier (comparison itself is by (months, micros)).
        if (a is SqlIntervalType ia && b is SqlIntervalType ib)
        {
            var nullable = a.Nullable || b.Nullable;
            return ia.IsYearMonth == ib.IsYearMonth
                ? new SqlIntervalType(ia.Qualifier, nullable)
                : new SqlIntervalType(IntervalQualifier.DayToSecond, nullable);
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
    /// Operator-specific result type for decimal arithmetic, following the
    /// SQL Server / Substrait promotion rules implemented in
    /// <see cref="Clast.DatabaseDecimal.Arithmetic.DecimalTypeRules"/>:
    /// addition / subtraction grow precision by one (carry digit), multiplication
    /// grows scale by the sum of operand scales, division extends scale to
    /// preserve fractional precision, all clamped to 38 with scale-borrowing.
    ///
    /// <para>Both operands must be at numeric rank ≤ 3 (INT / BIGINT / DECIMAL).
    /// Non-decimal numeric operands are promoted to a DECIMAL of matching
    /// precision (INT → DECIMAL(10, 0), BIGINT → DECIMAL(19, 0)).</para>
    /// </summary>
    public static SqlDecimalType DecimalArithmeticType(
        BinaryOperator op, SqlType a, SqlType b)
    {
        var nullable = a.Nullable || b.Nullable;
        var ad = ToDecimalForArithmetic(a);
        var bd = ToDecimalForArithmetic(b);

        var result = op switch
        {
            BinaryOperator.Add => Clast.DatabaseDecimal.Arithmetic.DecimalTypeRules.Add(ad, bd),
            BinaryOperator.Subtract => Clast.DatabaseDecimal.Arithmetic.DecimalTypeRules.Subtract(ad, bd),
            BinaryOperator.Multiply => Clast.DatabaseDecimal.Arithmetic.DecimalTypeRules.Multiply(ad, bd),
            BinaryOperator.Divide => Clast.DatabaseDecimal.Arithmetic.DecimalTypeRules.Divide(ad, bd),
            BinaryOperator.Modulo => Clast.DatabaseDecimal.Arithmetic.DecimalTypeRules.Modulus(ad, bd),
            _ => throw new InvalidOperationException(
                $"DecimalArithmeticType called for non-arithmetic operator {op}"),
        };

        return new SqlDecimalType(result.Precision, result.Scale, nullable);
    }

    /// <summary>
    /// Map a numeric SQL type to its <see cref="Clast.DatabaseDecimal.DecimalType"/>
    /// for arithmetic. INT, BIGINT, and DECIMAL pass through; other numerics
    /// (REAL, DOUBLE) shouldn't reach this path — the resolver routes them
    /// through <see cref="CommonNumericType"/> which returns a float type
    /// instead of a decimal.
    /// </summary>
    private static Clast.DatabaseDecimal.DecimalType ToDecimalForArithmetic(SqlType t) => t switch
    {
        SqlDecimalType d => Clast.DatabaseDecimal.DecimalType.Numeric(d.Precision, d.Scale),
        SqlIntegerType => Clast.DatabaseDecimal.DecimalType.Numeric(10, 0),
        SqlBigintType => Clast.DatabaseDecimal.DecimalType.Numeric(19, 0),
        _ => throw new InvalidOperationException(
            $"non-decimal type {t.Display} reached decimal arithmetic path"),
    };

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
