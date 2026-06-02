// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections.Generic;
using Clast.DatabaseDecimal.Values;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful.Aggregators;
using DbspNet.Sql.Expressions;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Sql.Compiler;

/// <summary>
/// Per-aggregate SQL semantics over a per-group multiset of input rows.
/// NULL-valued argument rows are skipped (SQL semantics); weight handling
/// follows the Core aggregator conventions — SUM/COUNT/AVG use all weights,
/// MIN/MAX use only positive-weight rows.
/// </summary>
/// <remarks>
/// Subclasses override <see cref="Update"/> for incremental aggregates (SUM,
/// COUNT, AVG): instead of rescanning the post-delta multiset, they fold the
/// per-group delta into opaque per-key state. Non-invertible aggregates
/// (MIN, MAX) inherit the default <see cref="Update"/>, which rescans
/// <c>afterMultiset</c>; they remain correct but not incrementalized.
/// </remarks>
internal abstract class SqlAggregator
{
    public abstract object? Compute(ZSet<StructuralRow, Z64> rows);

    /// <summary>
    /// Produce the new aggregate given the prior value, per-group delta, and
    /// post-delta multiset. The default implementation falls back to
    /// <see cref="Compute"/> — subclasses that can incrementalize override it.
    /// </summary>
    public virtual object? Update(
        ref object? state,
        object? oldValue,
        ZSet<StructuralRow, Z64> delta,
        ZSet<StructuralRow, Z64> after) => Compute(after);
}

internal sealed class SqlCountStarAggregator : SqlAggregator
{
    private sealed class CountState
    {
        public long Count;
    }

    public override object? Compute(ZSet<StructuralRow, Z64> rows)
    {
        var total = 0L;
        foreach (var (_, w) in rows)
        {
            total += w.Value;
        }

        return total;
    }

    public override object? Update(
        ref object? state,
        object? oldValue,
        ZSet<StructuralRow, Z64> delta,
        ZSet<StructuralRow, Z64> after)
    {
        var s = state as CountState ?? new CountState();
        foreach (var (_, w) in delta)
        {
            s.Count += w.Value;
        }

        state = s;
        return s.Count;
    }
}

internal sealed class SqlCountAggregator : SqlAggregator
{
    private readonly Func<StructuralRow, object?> _argExtract;

    public SqlCountAggregator(Func<StructuralRow, object?> argExtract)
    {
        _argExtract = argExtract;
    }

    private sealed class CountState
    {
        public long Count;
    }

    public override object? Compute(ZSet<StructuralRow, Z64> rows)
    {
        var total = 0L;
        foreach (var (row, w) in rows)
        {
            if (_argExtract(row) is null)
            {
                continue;
            }

            total += w.Value;
        }

        return total;
    }

    public override object? Update(
        ref object? state,
        object? oldValue,
        ZSet<StructuralRow, Z64> delta,
        ZSet<StructuralRow, Z64> after)
    {
        var s = state as CountState ?? new CountState();
        foreach (var (row, w) in delta)
        {
            if (_argExtract(row) is null)
            {
                continue;
            }

            s.Count += w.Value;
        }

        state = s;
        return s.Count;
    }
}

internal sealed class SqlSumAggregator : SqlAggregator
{
    private readonly Func<StructuralRow, object?> _argExtract;
    private readonly SqlType _resultType;

    public SqlSumAggregator(Func<StructuralRow, object?> argExtract, SqlType resultType)
    {
        _argExtract = argExtract;
        _resultType = resultType;
    }

    // State tracks the running sum plus the count of *distinct* non-null-valued
    // rows currently present in the group's multiset (with non-zero weight).
    // That distinct-row count — not the net weight — is what matches Compute's
    // "any non-null row observed" null-vs-value decision, since Z-set weights
    // can cancel across rows (e.g. {(5,+1), (3,-1)} has net non-null weight 0
    // yet SUM is 5·1 + 3·(-1) = 2, not NULL).
    private sealed class SumStateLong
    {
        public long Sum;
        public long DistinctNonNullRows;
    }

    private sealed class SumStateDouble
    {
        public double Sum;
        public long DistinctNonNullRows;
    }

    private sealed class SumStateDecimal128
    {
        // Mantissa accumulator at the column's scale (scale-implicit, same
        // for every input row from a single column). Widened to Int256 so
        // intermediate per-row mantissa × weight products can't silently
        // wrap when row weights or values approach Int128 capacity. Narrowed
        // back to Int128 at output via DecimalRuntime.NarrowToDecimal128,
        // which throws OverflowException if the final running total
        // legitimately exceeds the result type's range.
        public Int256 Sum;
        public long DistinctNonNullRows;
    }

    public override object? Compute(ZSet<StructuralRow, Z64> rows)
    {
        switch (_resultType)
        {
            case SqlBigintType:
                {
                    long sum = 0;
                    var any = false;
                    foreach (var (row, w) in rows)
                    {
                        var v = _argExtract(row);
                        if (v is null)
                        {
                            continue;
                        }

                        sum = checked(sum + (Convert.ToInt64(v, System.Globalization.CultureInfo.InvariantCulture) * w.Value));
                        any = true;
                    }

                    return any ? sum : null;
                }

            case SqlDoubleType:
                {
                    double sum = 0;
                    var any = false;
                    foreach (var (row, w) in rows)
                    {
                        var v = _argExtract(row);
                        if (v is null)
                        {
                            continue;
                        }

                        sum += Convert.ToDouble(v, System.Globalization.CultureInfo.InvariantCulture) * w.Value;
                        any = true;
                    }

                    return any ? sum : null;
                }

            case SqlDecimalType:
                {
                    Int256 sum = Int256.Zero;
                    var any = false;
                    foreach (var (row, w) in rows)
                    {
                        var v = _argExtract(row);
                        if (v is null)
                        {
                            continue;
                        }

                        // Int256 multiply: Int128 mantissa widens implicitly,
                        // long weight widens implicitly. No silent wrap.
                        sum += (Int256)((Decimal128)v).Mantissa * w.Value;
                        any = true;
                    }

                    return any ? (object)DecimalRuntime.NarrowToDecimal128(sum) : null;
                }

            default:
                throw new InvalidOperationException($"SUM not supported on result type {_resultType.Display}");
        }
    }

    public override object? Update(
        ref object? state,
        object? oldValue,
        ZSet<StructuralRow, Z64> delta,
        ZSet<StructuralRow, Z64> after)
    {
        switch (_resultType)
        {
            case SqlBigintType:
                {
                    var s = state as SumStateLong ?? new SumStateLong();
                    foreach (var (row, w) in delta)
                    {
                        var v = _argExtract(row);
                        if (v is null)
                        {
                            continue;
                        }

                        var afterW = after.WeightOf(row);
                        var beforeW = afterW.Value - w.Value;
                        s.Sum = checked(s.Sum + (Convert.ToInt64(v, System.Globalization.CultureInfo.InvariantCulture) * w.Value));
                        if (beforeW == 0 && afterW.Value != 0)
                        {
                            s.DistinctNonNullRows++;
                        }
                        else if (beforeW != 0 && afterW.Value == 0)
                        {
                            s.DistinctNonNullRows--;
                        }
                    }

                    state = s;
                    return s.DistinctNonNullRows > 0 ? (object)s.Sum : null;
                }

            case SqlDoubleType:
                {
                    var s = state as SumStateDouble ?? new SumStateDouble();
                    foreach (var (row, w) in delta)
                    {
                        var v = _argExtract(row);
                        if (v is null)
                        {
                            continue;
                        }

                        var afterW = after.WeightOf(row);
                        var beforeW = afterW.Value - w.Value;
                        s.Sum += Convert.ToDouble(v, System.Globalization.CultureInfo.InvariantCulture) * w.Value;
                        if (beforeW == 0 && afterW.Value != 0)
                        {
                            s.DistinctNonNullRows++;
                        }
                        else if (beforeW != 0 && afterW.Value == 0)
                        {
                            s.DistinctNonNullRows--;
                        }
                    }

                    state = s;
                    return s.DistinctNonNullRows > 0 ? (object)s.Sum : null;
                }

            case SqlDecimalType:
                {
                    var s = state as SumStateDecimal128 ?? new SumStateDecimal128();
                    foreach (var (row, w) in delta)
                    {
                        var v = _argExtract(row);
                        if (v is null)
                        {
                            continue;
                        }

                        var afterW = after.WeightOf(row);
                        var beforeW = afterW.Value - w.Value;
                        s.Sum += (Int256)((Decimal128)v).Mantissa * w.Value;
                        if (beforeW == 0 && afterW.Value != 0)
                        {
                            s.DistinctNonNullRows++;
                        }
                        else if (beforeW != 0 && afterW.Value == 0)
                        {
                            s.DistinctNonNullRows--;
                        }
                    }

                    state = s;
                    return s.DistinctNonNullRows > 0
                        ? (object)DecimalRuntime.NarrowToDecimal128(s.Sum)
                        : null;
                }

            default:
                throw new InvalidOperationException($"SUM not supported on result type {_resultType.Display}");
        }
    }
}

internal sealed class SqlMinMaxAggregator : SqlAggregator
{
    private readonly Func<StructuralRow, object?> _argExtract;
    private readonly bool _wantMin;

    public SqlMinMaxAggregator(Func<StructuralRow, object?> argExtract, bool wantMin)
    {
        _argExtract = argExtract;
        _wantMin = wantMin;
    }

    /// <summary>
    /// Per-group incremental state. <see cref="Counts"/> maps each distinct
    /// non-null value <i>v</i> to the number of rows currently in the
    /// post-delta multiset whose extracted value equals <i>v</i> AND whose
    /// net weight is strictly positive. <see cref="Active"/> mirrors the
    /// keys of <see cref="Counts"/> with positive counts, sorted by the
    /// runtime <see cref="IComparable"/> ordering, so that <c>Min</c> /
    /// <c>Max</c> are O(log n) on the distinct-value count.
    /// </summary>
    private sealed class State
    {
        public Dictionary<object, long> Counts = new();
        public SortedSet<object> Active = new(ComparableComparer.Instance);
    }

    private sealed class ComparableComparer : IComparer<object>
    {
        public static readonly ComparableComparer Instance = new();

        public int Compare(object? x, object? y) => ((IComparable)x!).CompareTo(y);
    }

    public override object? Compute(ZSet<StructuralRow, Z64> rows)
    {
        object? best = null;
        foreach (var (row, w) in rows)
        {
            if (!Z64.IsPositive(w))
            {
                continue;
            }

            var v = _argExtract(row);
            if (v is null)
            {
                continue;
            }

            if (best is null)
            {
                best = v;
                continue;
            }

            var cmp = ((IComparable)v).CompareTo(best);
            if (_wantMin ? cmp < 0 : cmp > 0)
            {
                best = v;
            }
        }

        return best;
    }

    public override object? Update(
        ref object? state,
        object? oldValue,
        ZSet<StructuralRow, Z64> delta,
        ZSet<StructuralRow, Z64> after)
    {
        var s = state as State ?? new State();
        foreach (var (row, w) in delta)
        {
            var v = _argExtract(row);
            if (v is null)
            {
                continue;
            }

            // Detect transitions of *this row's* net weight across the
            // positive/non-positive boundary. The set of values eligible
            // for MIN/MAX is "values appearing in any positive-weight row",
            // so per-value membership flips only on these transitions.
            var afterW = after.WeightOf(row).Value;
            var beforeW = afterW - w.Value;
            var wasPositive = beforeW > 0;
            var isPositive = afterW > 0;
            if (wasPositive == isPositive)
            {
                continue;
            }

            if (isPositive)
            {
                var c = s.Counts.GetValueOrDefault(v, 0L) + 1;
                s.Counts[v] = c;
                if (c == 1)
                {
                    s.Active.Add(v);
                }
            }
            else
            {
                var c = s.Counts[v] - 1;
                if (c == 0)
                {
                    s.Counts.Remove(v);
                    s.Active.Remove(v);
                }
                else
                {
                    s.Counts[v] = c;
                }
            }
        }

        state = s;
        if (s.Active.Count == 0)
        {
            return null;
        }

        return _wantMin ? s.Active.Min : s.Active.Max;
    }
}

internal sealed class SqlAvgAggregator : SqlAggregator
{
    private readonly Func<StructuralRow, object?> _argExtract;
    private readonly SqlType _resultType;

    public SqlAvgAggregator(Func<StructuralRow, object?> argExtract, SqlType resultType)
    {
        _argExtract = argExtract;
        _resultType = resultType;
    }

    private sealed class AvgStateDouble
    {
        public double Sum;
        public long NonNullCount;
    }

    private sealed class AvgStateDecimal128
    {
        // Int256 accumulator for the same reason as SumStateDecimal128.
        // Division by count typically narrows the result back into Int128
        // range, but per-row × weight can transiently exceed it.
        public Int256 Sum;
        public long NonNullCount;
    }

    public override object? Compute(ZSet<StructuralRow, Z64> rows)
    {
        if (_resultType is SqlDecimalType)
        {
            Int256 sum = Int256.Zero;
            long count = 0;
            foreach (var (row, w) in rows)
            {
                var v = _argExtract(row);
                if (v is null)
                {
                    continue;
                }

                sum += (Int256)((Decimal128)v).Mantissa * w.Value;
                count += w.Value;
            }

            // Result scale matches input column scale; integer-divide the
            // accumulator by count (truncating) before narrowing back to
            // Decimal128. Postgres-style precision extension is deferred.
            return count == 0
                ? null
                : (object)DecimalRuntime.NarrowToDecimal128(sum / (Int256)count);
        }
        else
        {
            double sum = 0;
            long count = 0;
            foreach (var (row, w) in rows)
            {
                var v = _argExtract(row);
                if (v is null)
                {
                    continue;
                }

                sum += Convert.ToDouble(v, System.Globalization.CultureInfo.InvariantCulture) * w.Value;
                count += w.Value;
            }

            return count == 0 ? null : sum / count;
        }
    }

    public override object? Update(
        ref object? state,
        object? oldValue,
        ZSet<StructuralRow, Z64> delta,
        ZSet<StructuralRow, Z64> after)
    {
        if (_resultType is SqlDecimalType)
        {
            var s = state as AvgStateDecimal128 ?? new AvgStateDecimal128();
            foreach (var (row, w) in delta)
            {
                var v = _argExtract(row);
                if (v is null)
                {
                    continue;
                }

                s.Sum += (Int256)((Decimal128)v).Mantissa * w.Value;
                s.NonNullCount += w.Value;
            }

            state = s;
            return s.NonNullCount == 0
                ? null
                : (object)DecimalRuntime.NarrowToDecimal128(s.Sum / (Int256)s.NonNullCount);
        }
        else
        {
            var s = state as AvgStateDouble ?? new AvgStateDouble();
            foreach (var (row, w) in delta)
            {
                var v = _argExtract(row);
                if (v is null)
                {
                    continue;
                }

                s.Sum += Convert.ToDouble(v, System.Globalization.CultureInfo.InvariantCulture) * w.Value;
                s.NonNullCount += w.Value;
            }

            state = s;
            return s.NonNullCount == 0 ? null : (object)(s.Sum / s.NonNullCount);
        }
    }
}

/// <summary>
/// SQL <c>APPROX_COUNT_DISTINCT</c> — a HyperLogLog estimate of the number of
/// distinct non-NULL argument values in the group. Returns a non-nullable
/// <c>BIGINT</c> (0 for an all-NULL group, like <c>COUNT(col)</c>).
/// </summary>
/// <remarks>
/// The sketch is non-invertible under retractions (a register holds a max and
/// cannot be un-set), so <see cref="Update"/> is incremental only while a tick
/// is insert-only — then it merges the new values into the running sketch.
/// Any tick carrying a retraction rebuilds the sketch from the post-delta
/// multiset. Because the sketch is a deterministic function of the present
/// value set either way, the incremental result is identical to a from-scratch
/// batch recompute — not merely within the estimator's error bound.
/// </remarks>
internal sealed class SqlApproxCountDistinctAggregator : SqlAggregator
{
    private readonly Func<StructuralRow, object?> _argExtract;

    public SqlApproxCountDistinctAggregator(Func<StructuralRow, object?> argExtract)
    {
        _argExtract = argExtract;
    }

    public override object? Compute(ZSet<StructuralRow, Z64> rows)
    {
        var sketch = new HyperLogLog();
        HllSupport.FoldPositive(sketch, rows, _argExtract);
        return sketch.EstimateCardinality();
    }

    public override object? Update(
        ref object? state,
        object? oldValue,
        ZSet<StructuralRow, Z64> delta,
        ZSet<StructuralRow, Z64> after)
    {
        var sketch = state as HyperLogLog;
        if (sketch is not null && IsInsertOnly(delta))
        {
            // Present-value set only grows this tick — merge the new values
            // (idempotent, so already-present values are harmless).
            HllSupport.FoldPositive(sketch, delta, _argExtract);
        }
        else
        {
            // A retraction may have dropped a value; rebuild from the
            // post-delta multiset.
            sketch ??= new HyperLogLog();
            sketch.Clear();
            HllSupport.FoldPositive(sketch, after, _argExtract);
        }

        state = sketch;
        return sketch.EstimateCardinality();
    }

    private static bool IsInsertOnly(ZSet<StructuralRow, Z64> delta)
    {
        foreach (var (_, weight) in delta)
        {
            if (!Z64.IsPositive(weight))
            {
                return false;
            }
        }

        return true;
    }
}

/// <summary>
/// SQL <c>APPROX_PERCENTILE(expr, fraction)</c> / <c>MEDIAN</c> /
/// <c>PERCENTILE_CONT</c> over a <b>numeric or INTERVAL</b> argument — a DDSketch
/// estimate of the value at the requested <see cref="_fraction"/> quantile of
/// the non-NULL argument values. <see cref="_toDouble"/> maps each value to the
/// double the sketch folds (numeric rescale, or the interval class component);
/// <see cref="_fromDouble"/> rebuilds the result type from the estimate (identity
/// for numeric → <c>DOUBLE</c>, or an <c>INTERVAL</c> reconstruct). Returns NULL
/// for an empty / all-NULL group, like MIN/MAX.
/// </summary>
/// <remarks>
/// The sketch holds a signed per-bucket count, so it is fully invertible: a
/// retraction folds in a negative weight rather than forcing a rebuild. The
/// running estimate is therefore maintained incrementally on every tick (the
/// SUM/COUNT pattern, not the rebuild-on-retraction HyperLogLog pattern), and
/// because the bucket map is a deterministic function of the present multiset
/// the incremental result equals a from-scratch batch recompute exactly.
/// </remarks>
internal sealed class SqlApproxPercentileAggregator : SqlAggregator
{
    private readonly Func<StructuralRow, object?> _argExtract;
    private readonly double _fraction;
    private readonly Func<object, double> _toDouble;
    private readonly Func<double, object> _fromDouble;

    public SqlApproxPercentileAggregator(
        Func<StructuralRow, object?> argExtract,
        double fraction,
        Func<object, double> toDouble,
        Func<double, object> fromDouble)
    {
        _argExtract = argExtract;
        _fraction = fraction;
        _toDouble = toDouble;
        _fromDouble = fromDouble;
    }

    public override object? Compute(ZSet<StructuralRow, Z64> rows)
    {
        var sketch = new DdSketch();
        DdSketchSupport.FoldSigned(sketch, rows, _argExtract, _toDouble);
        return sketch.EstimateQuantile(_fraction) is double d ? _fromDouble(d) : null;
    }

    public override object? Update(
        ref object? state,
        object? oldValue,
        ZSet<StructuralRow, Z64> delta,
        ZSet<StructuralRow, Z64> after)
    {
        var sketch = state as DdSketch ?? new DdSketch();
        DdSketchSupport.FoldSigned(sketch, delta, _argExtract, _toDouble);
        state = sketch;
        return sketch.EstimateQuantile(_fraction) is double d ? _fromDouble(d) : null;
    }
}

/// <summary>
/// SQL quantiles over a <b>DATE / TIMESTAMP</b> argument — answered <b>exactly</b>
/// via an <see cref="OrderedQuantileSketch"/> over the value's integer day-/
/// microsecond key (DDSketch's relative-error bound on the epoch-offset magnitude
/// is useless for absolute dates/timestamps). <see cref="_discrete"/> selects
/// <c>PERCENTILE_DISC</c> (a true set member) vs <c>PERCENTILE_CONT</c>/MEDIAN
/// (interpolated). <see cref="_toKey"/>/<see cref="_fromKey"/> convert between the
/// boxed temporal value and its sort key. Returns NULL for an empty / all-NULL
/// group.
/// </summary>
/// <remarks>
/// The sketch holds a signed per-value count, so — exactly like
/// <see cref="SqlApproxPercentileAggregator"/> — it is fully invertible and the
/// incremental result equals a from-scratch batch recompute exactly.
/// </remarks>
internal sealed class SqlExactQuantileAggregator : SqlAggregator
{
    private readonly Func<StructuralRow, object?> _argExtract;
    private readonly double _fraction;
    private readonly bool _discrete;
    private readonly Func<object, long> _toKey;
    private readonly Func<long, object> _fromKey;

    public SqlExactQuantileAggregator(
        Func<StructuralRow, object?> argExtract,
        double fraction,
        bool discrete,
        Func<object, long> toKey,
        Func<long, object> fromKey)
    {
        _argExtract = argExtract;
        _fraction = fraction;
        _discrete = discrete;
        _toKey = toKey;
        _fromKey = fromKey;
    }

    public override object? Compute(ZSet<StructuralRow, Z64> rows)
    {
        var sketch = new OrderedQuantileSketch();
        DdSketchSupport.FoldSignedExact(sketch, rows, _argExtract, _toKey);
        return sketch.EstimateQuantile(_fraction, _discrete) is long k ? _fromKey(k) : null;
    }

    public override object? Update(
        ref object? state,
        object? oldValue,
        ZSet<StructuralRow, Z64> delta,
        ZSet<StructuralRow, Z64> after)
    {
        var sketch = state as OrderedQuantileSketch ?? new OrderedQuantileSketch();
        DdSketchSupport.FoldSignedExact(sketch, delta, _argExtract, _toKey);
        state = sketch;
        return sketch.EstimateQuantile(_fraction, _discrete) is long k ? _fromKey(k) : null;
    }
}

/// <summary>
/// Runs all of a query's aggregates in a single pass over the per-group
/// multiset, packing the results into a <see cref="StructuralRow"/> whose
/// columns line up with the resolver's declared aggregate order.
/// </summary>
internal sealed class CompositeAggregator : IAggregator<StructuralRow, StructuralRow>
{
    private readonly IReadOnlyList<SqlAggregator> _aggs;
    private readonly IRowCodec<StructuralRow> _codec;
    private readonly Schema _outputSchema;

    public CompositeAggregator(
        IReadOnlyList<SqlAggregator> aggs,
        IRowCodec<StructuralRow> codec,
        Schema outputSchema)
    {
        _aggs = aggs;
        _codec = codec;
        _outputSchema = outputSchema;
    }

    public Optional<StructuralRow> Compute(ZSet<StructuralRow, Z64> multiset)
    {
        ArgumentNullException.ThrowIfNull(multiset);
        // Linear-preserving emission gate (DBSP paper §7.2-7.4 plus
        // Feldera's Aggregator trait contract: "return None if the
        // total weight of each key is zero"). The dict-shape
        // IsEmpty check that was here before broke linearity — two
        // Z-sets equal under DBSP arithmetic could give different
        // answers — blocking projection pushdown. See the memory
        // note project_projection_pushdown_blocked for the full
        // diagnosis.
        if (Z64.IsZero(multiset.SumWeights()))
        {
            return Optional<StructuralRow>.None;
        }

        var results = new object?[_aggs.Count];
        for (var i = 0; i < _aggs.Count; i++)
        {
            results[i] = _aggs[i].Compute(multiset);
        }

        return Optional<StructuralRow>.Some(_codec.BuildRow(_outputSchema, results));
    }

    public Optional<StructuralRow> Update(
        ref object? state,
        Optional<StructuralRow> oldValue,
        ZSet<StructuralRow, Z64> delta,
        ZSet<StructuralRow, Z64> afterMultiset)
    {
        ArgumentNullException.ThrowIfNull(delta);
        ArgumentNullException.ThrowIfNull(afterMultiset);

        // Always advance the sub-aggregators' per-key state — they
        // track weight transitions across ticks (e.g.
        // SqlSumAggregator.DistinctNonNullRows) and would corrupt
        // if we short-circuited on the linear emission gate. The
        // gate is applied to the *output wrapping* below.
        var subStates = state as object?[] ?? new object?[_aggs.Count];
        var results = new object?[_aggs.Count];
        for (var i = 0; i < _aggs.Count; i++)
        {
            var slot = subStates[i];
            var oldSub = oldValue.HasValue ? oldValue.Value[i] : null;
            results[i] = _aggs[i].Update(ref slot, oldSub, delta, afterMultiset);
            subStates[i] = slot;
        }

        state = subStates;

        // Linear-preserving emission gate (DBSP paper §7.2-7.4 +
        // Feldera's Aggregator trait: "return None if the total
        // weight of each key is zero"). See the matching note in
        // Compute above.
        if (Z64.IsZero(afterMultiset.SumWeights()))
        {
            return Optional<StructuralRow>.None;
        }

        return Optional<StructuralRow>.Some(_codec.BuildRow(_outputSchema, results));
    }
}
