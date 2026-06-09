// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using Clast.DatabaseDecimal.Values;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful.Aggregators;
using DbspNet.Sql.Expressions;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Sql.Compiler;

/// <summary>
/// Shared plumbing for the <c>APPROX_PERCENTILE</c> / <c>MEDIAN</c> /
/// <c>PERCENTILE_CONT</c> / <c>PERCENTILE_DISC</c> aggregators on every compile
/// path (structural circuit, spine/trace, typed, and the batch evaluator).
/// Picks one of two estimators by the argument's <see cref="SqlType"/>:
/// <list type="bullet">
/// <item><b>numeric / INTERVAL</b> → the relative-error <see cref="DdSketch"/>
/// (folding values as <see cref="double"/>). DDSketch is the right model for
/// durations and keeps state bounded by dynamic range.</item>
/// <item><b>DATE / TIMESTAMP</b> → the exact <see cref="OrderedQuantileSketch"/>
/// (folding the value's integer day-/microsecond key). DDSketch's relative-error
/// bound is on the huge epoch-offset magnitude and so useless for absolute
/// dates/timestamps; the exact sketch answers the true quantile and still keeps
/// the incremental≡batch property.</item>
/// </list>
/// </summary>
internal static class DdSketchSupport
{
    /// <summary>The identity reconstruct for the numeric path — the DDSketch
    /// estimate is itself the (boxed) <see cref="double"/> result.</summary>
    public static readonly Func<double, object> DoubleIdentity = d => d;

    /// <summary>
    /// Fold every non-NULL value carried by <paramref name="rows"/> into
    /// <paramref name="sketch"/> with its <b>signed</b> weight — a positive
    /// weight inserts, a negative weight retracts. NULL-valued rows are skipped
    /// (SQL aggregates ignore NULL). Each value is mapped to a <see cref="double"/>
    /// by <paramref name="toDouble"/> (numeric rescale, or the interval class
    /// component). Because <see cref="DdSketch.Add"/> is commutative integer
    /// addition over bucket counts, folding the present multiset and folding a
    /// per-tick delta into the running sketch agree exactly.
    /// </summary>
    public static void FoldSigned<TRow>(
        DdSketch sketch, IMultiset<TRow, Z64> rows, Func<TRow, object?> argExtract, Func<object, double> toDouble)
        where TRow : notnull
    {
        foreach (var (row, weight) in rows)
        {
            var value = argExtract(row);
            if (value is null)
            {
                continue;
            }

            sketch.Add(toDouble(value), weight.Value);
        }
    }

    /// <summary>
    /// The exact-path counterpart of <see cref="FoldSigned{TRow}"/>: fold every
    /// non-NULL value's integer key (via <paramref name="toKey"/>) into the exact
    /// <see cref="OrderedQuantileSketch"/> with its signed weight.
    /// </summary>
    public static void FoldSignedExact<TRow>(
        OrderedQuantileSketch sketch, IMultiset<TRow, Z64> rows, Func<TRow, object?> argExtract, Func<object, long> toKey)
        where TRow : notnull
    {
        foreach (var (row, weight) in rows)
        {
            var value = argExtract(row);
            if (value is null)
            {
                continue;
            }

            sketch.Add(toKey(value), weight.Value);
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

    /// <summary>The numeric value→double mapping for <paramref name="type"/>,
    /// capturing its decimal scale.</summary>
    public static Func<object, double> NumericToDouble(SqlType? type)
    {
        var scale = DecimalScaleOf(type);
        return v => ToDouble(v, scale);
    }

    /// <summary>An <c>INTERVAL</c> value → double: the year-month class folds its
    /// month count, the day-time class its microsecond count (only the component
    /// matching the class is non-zero).</summary>
    public static Func<object, double> IntervalToDouble(IntervalQualifier q) =>
        IntervalQualifiers.IsYearMonth(q)
            ? v => ((Interval)v).Months
            : v => ((Interval)v).Micros;

    /// <summary>Rebuild an <c>INTERVAL</c> from a DDSketch estimate, placing the
    /// rounded magnitude in the component matching the qualifier's class.</summary>
    public static Func<double, object> IntervalFromDouble(IntervalQualifier q) =>
        IntervalQualifiers.IsYearMonth(q)
            ? d => new Interval((int)Math.Round(d, MidpointRounding.AwayFromZero), 0)
            : d => new Interval(0, (long)Math.Round(d, MidpointRounding.AwayFromZero));

    /// <summary>True for the temporal types whose quantiles are answered exactly
    /// (DATE / TIMESTAMP), as opposed to the DDSketch-approximated numeric and
    /// INTERVAL types.</summary>
    public static bool IsExactQuantileType(SqlType type) => type is SqlDateType or SqlTimestampType;

    /// <summary>A DATE/TIMESTAMP value → its integer sort key (days / microseconds
    /// since the epoch).</summary>
    public static Func<object, long> ExactToKey(SqlType type) => type switch
    {
        SqlDateType => v => ((Date32)v).Days,
        SqlTimestampType => v => ((Timestamp)v).Microseconds,
        _ => throw new NotSupportedException($"no exact quantile key for {type.Display}"),
    };

    /// <summary>Rebuild a DATE/TIMESTAMP from an integer sort key.</summary>
    public static Func<long, object> ExactFromKey(SqlType type) => type switch
    {
        SqlDateType => k => new Date32((int)k),
        SqlTimestampType => k => new Timestamp(k),
        _ => throw new NotSupportedException($"no exact quantile reconstruct for {type.Display}"),
    };

    /// <summary>
    /// Build the structural-path quantile aggregator for <paramref name="call"/>,
    /// dispatching on the argument type. Shared by <c>PlanToCircuit</c> (flat and
    /// spine/trace) and the <c>BatchPlanEvaluator</c> so every path agrees — the
    /// incremental≡batch equality depends on it.
    /// </summary>
    public static SqlAggregator BuildStructuralPercentile(AggregateCall call)
    {
        var argType = call.Argument!.Type;
        var extract = ExpressionCompiler.CompileScalar(call.Argument!);
        var fraction = call.Fraction!.Value;

        if (IsExactQuantileType(argType))
        {
            return new SqlExactQuantileAggregator(
                extract, fraction, call.Discrete, ExactToKey(argType), ExactFromKey(argType));
        }

        if (argType is SqlIntervalType iv)
        {
            return new SqlApproxPercentileAggregator(
                extract, fraction, IntervalToDouble(iv.Qualifier), IntervalFromDouble(iv.Qualifier));
        }

        return new SqlApproxPercentileAggregator(
            extract, fraction, NumericToDouble(argType), DoubleIdentity);
    }
}
