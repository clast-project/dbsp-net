using System.Collections.Generic;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful.Aggregators;
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

    private sealed class SumStateDecimal
    {
        public decimal Sum;
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
                    decimal sum = 0;
                    var any = false;
                    foreach (var (row, w) in rows)
                    {
                        var v = _argExtract(row);
                        if (v is null)
                        {
                            continue;
                        }

                        sum += Convert.ToDecimal(v, System.Globalization.CultureInfo.InvariantCulture) * w.Value;
                        any = true;
                    }

                    return any ? sum : null;
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
                    var s = state as SumStateDecimal ?? new SumStateDecimal();
                    foreach (var (row, w) in delta)
                    {
                        var v = _argExtract(row);
                        if (v is null)
                        {
                            continue;
                        }

                        var afterW = after.WeightOf(row);
                        var beforeW = afterW.Value - w.Value;
                        s.Sum += Convert.ToDecimal(v, System.Globalization.CultureInfo.InvariantCulture) * w.Value;
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

    // MIN/MAX use the default Update: Compute(after). Incrementalizing them
    // requires retraction-aware structures (heap / sorted trace); deferred.
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

    private sealed class AvgStateDecimal
    {
        public decimal Sum;
        public long NonNullCount;
    }

    public override object? Compute(ZSet<StructuralRow, Z64> rows)
    {
        if (_resultType is SqlDecimalType)
        {
            decimal sum = 0;
            long count = 0;
            foreach (var (row, w) in rows)
            {
                var v = _argExtract(row);
                if (v is null)
                {
                    continue;
                }

                sum += Convert.ToDecimal(v, System.Globalization.CultureInfo.InvariantCulture) * w.Value;
                count += w.Value;
            }

            return count == 0 ? null : sum / count;
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
            var s = state as AvgStateDecimal ?? new AvgStateDecimal();
            foreach (var (row, w) in delta)
            {
                var v = _argExtract(row);
                if (v is null)
                {
                    continue;
                }

                s.Sum += Convert.ToDecimal(v, System.Globalization.CultureInfo.InvariantCulture) * w.Value;
                s.NonNullCount += w.Value;
            }

            state = s;
            return s.NonNullCount == 0 ? null : (object)(s.Sum / s.NonNullCount);
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
        if (multiset.IsEmpty)
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
        if (afterMultiset.IsEmpty)
        {
            return Optional<StructuralRow>.None;
        }

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
        return Optional<StructuralRow>.Some(_codec.BuildRow(_outputSchema, results));
    }
}
