// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using Clast.DatabaseDecimal.Values;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful.Aggregators;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Sql.Compiler;

/// <summary>
/// Shared plumbing for the <c>APPROX_PERCENTILE</c> / <c>MEDIAN</c> /
/// <c>PERCENTILE_CONT</c> aggregators on both compile paths: a numeric runtime
/// value → <see cref="double"/> conversion (<see cref="ToDouble"/>) and a "fold
/// a multiset's signed weights into a <see cref="DdSketch"/>" helper
/// (<see cref="FoldSigned"/>).
/// </summary>
internal static class DdSketchSupport
{
    /// <summary>
    /// Fold every non-NULL value carried by <paramref name="rows"/> into
    /// <paramref name="sketch"/> with its <b>signed</b> weight — a positive
    /// weight inserts, a negative weight retracts. NULL-valued rows are skipped
    /// (SQL aggregates ignore NULL). Because <see cref="DdSketch.Add"/> is
    /// commutative integer addition over bucket counts, folding the present
    /// multiset and folding a per-tick delta into the running sketch agree
    /// exactly — the aggregator is invertible like SUM/COUNT.
    /// </summary>
    public static void FoldSigned<TRow>(
        DdSketch sketch, ZSet<TRow, Z64> rows, Func<TRow, object?> argExtract, int decimalScale)
        where TRow : notnull
    {
        foreach (var (row, weight) in rows)
        {
            var value = argExtract(row);
            if (value is null)
            {
                continue;
            }

            sketch.Add(ToDouble(value, decimalScale), weight.Value);
        }
    }

    /// <summary>
    /// Convert a boxed numeric runtime value to <see cref="double"/>. Every
    /// numeric type the resolver admits as a percentile argument is handled
    /// explicitly; <paramref name="decimalScale"/> is the argument column's
    /// declared <c>DECIMAL</c> scale (ignored for the non-decimal types).
    /// </summary>
    public static double ToDouble(object value, int decimalScale) => value switch
    {
        double d => d,
        float f => f,
        long l => l,
        int i => i,
        short s => s,
        byte b => b,
        // Decimal128 carries the raw integer mantissa; the column's scale fixes
        // the decimal point. (double)Int128 is exact for the magnitudes we see.
        Decimal128 dec => (double)dec.Mantissa / Math.Pow(10, decimalScale),
        _ => throw new NotSupportedException(
            $"APPROX_PERCENTILE cannot handle a value of type {value.GetType()}"),
    };

    /// <summary>The declared <c>DECIMAL</c> scale of <paramref name="type"/>, or 0
    /// for the non-decimal numeric types (which need no rescaling).</summary>
    public static int DecimalScaleOf(SqlType? type) => type is SqlDecimalType d ? d.Scale : 0;
}
