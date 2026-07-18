// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using Clast.DatabaseDecimal;
using Clast.DatabaseDecimal.Arithmetic;
using Clast.DatabaseDecimal.Values;

namespace DbspNet.Sql.Expressions;

/// <summary>
/// Runtime helpers invoked from compiled expression trees for
/// <see cref="Decimal128"/> arithmetic and comparison. Wraps the
/// <c>Clast.DatabaseDecimal</c> kernels so the expression compiler can resolve a
/// single <see cref="System.Reflection.MethodInfo"/> per operation rather
/// than juggling overload selection at code-emit time.
/// </summary>
/// <remarks>
/// All decimal arithmetic in DbspNet flows through these — operand and
/// result <see cref="DecimalType"/> are baked in as constants at compile
/// time, so the kernels see fixed scales and the runtime path is a simple
/// integer-arithmetic dispatch.
/// </remarks>
internal static class DecimalRuntime
{
    public static Decimal128 Add(Decimal128 left, DecimalType lt, Decimal128 right, DecimalType rt, DecimalType result) =>
        AddKernel.Add(left, lt, right, rt, result);

    public static Decimal128 Subtract(Decimal128 left, DecimalType lt, Decimal128 right, DecimalType rt, DecimalType result) =>
        AddKernel.Subtract(left, lt, right, rt, result);

    public static Decimal128 Multiply(Decimal128 left, DecimalType lt, Decimal128 right, DecimalType rt, DecimalType result) =>
        MultiplyKernel.Multiply(left, lt, right, rt, result);

    public static Decimal128 Divide(Decimal128 left, DecimalType lt, Decimal128 right, DecimalType rt, DecimalType result) =>
        DivideKernel.Divide(left, lt, right, rt, result);

    public static Decimal128 Modulus(Decimal128 left, DecimalType lt, Decimal128 right, DecimalType rt, DecimalType result) =>
        ModulusKernel.Modulus(left, lt, right, rt, result);

    /// <summary>
    /// Compare two decimals, rescaling to their common scale first. Returns
    /// negative / zero / positive following <see cref="IComparable.CompareTo"/>
    /// semantics.
    /// </summary>
    public static int Compare(Decimal128 left, DecimalType lt, Decimal128 right, DecimalType rt)
    {
        var commonScale = Math.Max(lt.Scale, rt.Scale);
        var l = ScaleHelper.Rescale128(left.Mantissa, lt.Scale, commonScale);
        var r = ScaleHelper.Rescale128(right.Mantissa, rt.Scale, commonScale);
        return l.CompareTo(r);
    }

    /// <summary>
    /// Promote a <see cref="long"/> to <see cref="Decimal128"/> at scale 0.
    /// Used when an integer operand appears in decimal arithmetic.
    /// </summary>
    public static Decimal128 FromInt64(long value) => new(value);

    /// <summary>Promote an <see cref="int"/> to <see cref="Decimal128"/> at scale 0.</summary>
    public static Decimal128 FromInt32(int value) => new(value);

    /// <summary>
    /// Convert a floating-point value to <see cref="Decimal128"/> at
    /// <paramref name="type"/>'s scale — <c>CAST(DOUBLE/REAL AS DECIMAL(p, s))</c>.
    /// Scales by <c>10^s</c> and rounds half-to-even to the mantissa. Only the source
    /// double's ~15–16 significant digits are preserved (a double never held more), the
    /// same lossy contract as Spark / DuckDB / Feldera. NaN / Infinity throw.
    /// </summary>
    public static Decimal128 FromDouble(double value, DecimalType type)
    {
        if (double.IsNaN(value) || double.IsInfinity(value))
        {
            throw new OverflowException($"cannot cast {value} to DECIMAL");
        }

        var scaled = Math.Round(value * Math.Pow(10, type.Scale), MidpointRounding.ToEven);
        return new Decimal128((Int128)scaled);
    }

    /// <summary>
    /// Convert a <see cref="Decimal128"/> to <see cref="double"/> at
    /// <paramref name="scale"/> — <c>CAST(DECIMAL(p, s) AS DOUBLE/REAL)</c>.
    /// Divides the mantissa by <c>10^s</c> so the fractional digits are
    /// preserved: <c>CAST(60.7834 AS DOUBLE)</c> yields <c>60.7834</c>, not
    /// the integer part. Only the double's ~15–16 significant digits survive
    /// (a 38-digit decimal never fit), the same lossy contract as
    /// Spark / DuckDB / Feldera and the inverse of <see cref="FromDouble"/>.
    /// </summary>
    public static double ToDouble(Decimal128 value, int scale) =>
        (double)value.Mantissa / Math.Pow(10, scale);

    /// <summary>
    /// Narrow a <see cref="Int256"/> SUM accumulator back to <see cref="Decimal128"/>.
    /// Throws <see cref="OverflowException"/> if the accumulator's upper 128
    /// bits aren't a sign-extension of the lower bits — i.e. the running
    /// total exceeded the result type's <see cref="Int128"/> capacity. Used
    /// at the boundary between SUM/AVG state (Int256, no silent wrap on per-row
    /// multiply) and Decimal128 output values.
    /// </summary>
    public static Decimal128 NarrowToDecimal128(Int256 value)
    {
        var narrowed = (Int128)value;
        if ((Int256)narrowed != value)
        {
            throw new OverflowException(
                "decimal SUM/AVG result exceeds Decimal128 (Int128) range");
        }

        return new Decimal128(narrowed);
    }

    /// <summary>
    /// <c>ABS</c> on a fixed-point decimal: just absolute the mantissa. The
    /// type's precision and scale are unchanged.
    /// </summary>
    public static Decimal128 Abs(Decimal128 v) => new(Int128.Abs(v.Mantissa));

    /// <summary>
    /// <c>FLOOR</c> rounded toward −∞. Result has the same type (precision /
    /// scale) as input — fractional digits become zeros, e.g.
    /// <c>FLOOR(1.75)</c> at scale 2 → <c>1.00</c>.
    /// </summary>
    public static Decimal128 Floor(Decimal128 v, byte scale)
    {
        if (scale == 0)
        {
            return v;
        }

        var pow = ScaleHelper.Rescale128(Int128.One, 0, scale);
        var quotient = v.Mantissa / pow;
        var remainder = v.Mantissa % pow;
        if (remainder == Int128.Zero)
        {
            return v;
        }

        // Integer division truncates toward zero. For positive numbers
        // truncation == floor; for negative with a fraction we have to
        // step one further down to round toward −∞.
        var floorMantissa = v.Mantissa >= 0
            ? quotient * pow
            : (quotient - Int128.One) * pow;
        return new Decimal128(floorMantissa);
    }

    /// <summary><c>CEIL</c> rounded toward +∞. See <see cref="Floor"/>.</summary>
    public static Decimal128 Ceil(Decimal128 v, byte scale)
    {
        if (scale == 0)
        {
            return v;
        }

        var pow = ScaleHelper.Rescale128(Int128.One, 0, scale);
        var quotient = v.Mantissa / pow;
        var remainder = v.Mantissa % pow;
        if (remainder == Int128.Zero)
        {
            return v;
        }

        var ceilMantissa = v.Mantissa >= 0
            ? (quotient + Int128.One) * pow
            : quotient * pow;
        return new Decimal128(ceilMantissa);
    }

    /// <summary>
    /// <c>ROUND</c> to <paramref name="digits"/> decimal places, rounding half
    /// away from zero (<c>ROUND(2.5) = 3</c>, <c>ROUND(-2.5) = -3</c>) — the SQL
    /// convention (Calcite/PostgreSQL-numeric/SQL Server/Oracle/DuckDB/Spark).
    /// The Clast.DatabaseDecimal <c>Rescale128</c> rounds half to even, so the
    /// rounding step is done here directly on the Int128 mantissa. Result type is
    /// the same as input — dropped digits are filled back with trailing zeros so
    /// <c>ROUND(1.236, 2)</c> at scale 3 returns mantissa 1240 (= 1.240).
    /// Negative <paramref name="digits"/> rounds before the decimal point
    /// (e.g. <c>ROUND(123.4, -1)</c> → <c>120.0</c> at scale 1).
    /// </summary>
    public static Decimal128 Round(Decimal128 v, byte scale, int digits)
    {
        if (digits >= scale)
        {
            return v;
        }

        // Drop precision from `scale` to `digits`, rounding half away from zero.
        // pow = 10^(scale - digits); q = trunc(m / pow); if the dropped remainder
        // is >= half a unit, step the quotient one further from zero.
        var pow = ScaleHelper.Rescale128(Int128.One, 0, scale - digits);   // exact 10^drop
        var m = v.Mantissa;
        var q = m / pow;                       // truncates toward zero
        var r = m - (q * pow);                 // remainder, carries the sign of m
        var rAbs = r < Int128.Zero ? -r : r;
        if (rAbs * 2 >= pow)
        {
            q += m < Int128.Zero ? -Int128.One : Int128.One;
        }

        // Pad zeros back to the column's scale so the result type matches.
        return new Decimal128(q * pow);
    }
}
