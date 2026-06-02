// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using Clast.DatabaseDecimal.Values;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful.Aggregators;
using DbspNet.Sql.Expressions;

namespace DbspNet.Sql.Compiler;

/// <summary>
/// Typed-row counterpart to <see cref="SqlAggregator"/>. Compute and
/// Update operate over <see cref="ZSet{TIn,Z64}"/> where <c>TIn</c> is
/// the per-schema emitted input row struct.
/// </summary>
/// <remarks>
/// <para><b>NULL handling (Phase N4).</b> Non-nullable arg variants
/// (<see cref="TypedSumLongAggregator{TIn}"/> etc.) stay on the
/// fast path — extractor is <c>Func&lt;TIn, T&gt;</c>, state is a
/// plain accumulator. Nullable-arg variants
/// (<see cref="TypedSumLongNullableAggregator{TIn}"/> etc.) take
/// <c>Func&lt;TIn, T?&gt;</c>, skip rows whose extracted arg
/// reports <c>HasValue == false</c>, and track DistinctNonNullRows
/// parallel to the structural variant so they return <c>null</c>
/// when every row in a (non-empty) group has a NULL arg.</para>
/// <para><b>Empty-group handling.</b>
/// <see cref="TypedCompositeAggregator{TIn,TAgg}"/> short-circuits to
/// <see cref="Optional{TAgg}.None"/> whenever the per-group
/// multiset's sum of weights is zero — the linear-preserving "group
/// is present" gate per the DBSP paper §7.2-7.4 and Feldera's
/// <c>Aggregator</c> trait contract. SQL's "SUM over empty group =
/// NULL" semantics never reach the typed aggregators because the
/// gate fires first. SQL's "SUM over all-NULL group = NULL" goes
/// through the nullable variants' DistinctNonNullRows check above.</para>
/// </remarks>
internal abstract class TypedSqlAggregator<TIn>
    where TIn : notnull
{
    public abstract Type ResultClrType { get; }

    /// <summary>
    /// Compute the aggregate value over the given multiset. Returns
    /// <c>null</c> only for nullable-arg variants whose group has
    /// no positive contributions (SQL NULL); non-nullable variants
    /// always return a definite value.
    /// </summary>
    public abstract object? Compute(ZSet<TIn, Z64> rows);

    public virtual object? Update(ref object? state, ZSet<TIn, Z64> delta, ZSet<TIn, Z64> after)
        => Compute(after);
}

/// <summary>
/// <c>COUNT(*)</c>: sum of weights across the multiset. With non-null
/// arguments, <c>COUNT(col)</c> reduces to the same calculation and
/// can be served by this class too.
/// </summary>
internal sealed class TypedCountStarAggregator<TIn> : TypedSqlAggregator<TIn>
    where TIn : notnull
{
    public override Type ResultClrType => typeof(long);

    public override object? Compute(ZSet<TIn, Z64> rows)
    {
        var total = 0L;
        foreach (var (_, w) in rows)
        {
            total += w.Value;
        }

        return total;
    }

    public override object? Update(ref object? state, ZSet<TIn, Z64> delta, ZSet<TIn, Z64> after)
    {
        var s = state is long prior ? prior : 0L;
        foreach (var (_, w) in delta)
        {
            s += w.Value;
        }

        state = s;
        return s;
    }
}

/// <summary>
/// <c>SUM</c>-of-non-null with a <c>long</c> running total. Used for
/// SQL <c>SUM</c> over INT or BIGINT columns (per Postgres semantics
/// SUM(INT) returns BIGINT, so int args are widened to long here).
/// </summary>
internal sealed class TypedSumLongAggregator<TIn> : TypedSqlAggregator<TIn>
    where TIn : notnull
{
    private readonly Func<TIn, long> _argExtract;

    public TypedSumLongAggregator(Func<TIn, long> argExtract)
    {
        _argExtract = argExtract;
    }

    public override Type ResultClrType => typeof(long);

    public override object? Compute(ZSet<TIn, Z64> rows)
    {
        long sum = 0;
        foreach (var (row, w) in rows)
        {
            sum = checked(sum + _argExtract(row) * w.Value);
        }

        return sum;
    }

    public override object? Update(ref object? state, ZSet<TIn, Z64> delta, ZSet<TIn, Z64> after)
    {
        var s = state is long prior ? prior : 0L;
        foreach (var (row, w) in delta)
        {
            s = checked(s + _argExtract(row) * w.Value);
        }

        state = s;
        return s;
    }
}

/// <summary><c>SUM</c>-of-non-null with a <c>double</c> running total.</summary>
internal sealed class TypedSumDoubleAggregator<TIn> : TypedSqlAggregator<TIn>
    where TIn : notnull
{
    private readonly Func<TIn, double> _argExtract;

    public TypedSumDoubleAggregator(Func<TIn, double> argExtract)
    {
        _argExtract = argExtract;
    }

    public override Type ResultClrType => typeof(double);

    public override object? Compute(ZSet<TIn, Z64> rows)
    {
        double sum = 0;
        foreach (var (row, w) in rows)
        {
            sum += _argExtract(row) * w.Value;
        }

        return sum;
    }

    public override object? Update(ref object? state, ZSet<TIn, Z64> delta, ZSet<TIn, Z64> after)
    {
        var s = state is double prior ? prior : 0.0;
        foreach (var (row, w) in delta)
        {
            s += _argExtract(row) * w.Value;
        }

        state = s;
        return s;
    }
}

/// <summary>
/// <c>SUM</c>-of-non-null with a <see cref="Decimal128"/> running
/// total. Like the structural variant, accumulates in <see cref="Int256"/>
/// so per-row <c>mantissa × weight</c> can't silently wrap; narrows
/// back to <see cref="Decimal128"/> at output via
/// <see cref="DecimalRuntime.NarrowToDecimal128"/> (which throws
/// <see cref="OverflowException"/> if the running total exceeds
/// Int128 capacity).
/// </summary>
internal sealed class TypedSumDecimalAggregator<TIn> : TypedSqlAggregator<TIn>
    where TIn : notnull
{
    private readonly Func<TIn, Decimal128> _argExtract;

    public TypedSumDecimalAggregator(Func<TIn, Decimal128> argExtract)
    {
        _argExtract = argExtract;
    }

    public override Type ResultClrType => typeof(Decimal128);

    public override object? Compute(ZSet<TIn, Z64> rows)
    {
        Int256 sum = Int256.Zero;
        foreach (var (row, w) in rows)
        {
            sum += (Int256)_argExtract(row).Mantissa * w.Value;
        }

        return DecimalRuntime.NarrowToDecimal128(sum);
    }

    public override object? Update(ref object? state, ZSet<TIn, Z64> delta, ZSet<TIn, Z64> after)
    {
        var s = state is Int256 prior ? prior : Int256.Zero;
        foreach (var (row, w) in delta)
        {
            s += (Int256)_argExtract(row).Mantissa * w.Value;
        }

        state = s;
        return DecimalRuntime.NarrowToDecimal128(s);
    }
}

/// <summary>
/// <c>AVG</c>-of-non-null over a DECIMAL column. State is sum +
/// count (in <see cref="Int256"/> to handle the same overflow case
/// SUM does); result rescales to the column's scale at output.
/// </summary>
internal sealed class TypedAvgDecimalAggregator<TIn> : TypedSqlAggregator<TIn>
    where TIn : notnull
{
    private readonly Func<TIn, Decimal128> _argExtract;

    public TypedAvgDecimalAggregator(Func<TIn, Decimal128> argExtract)
    {
        _argExtract = argExtract;
    }

    private sealed class AvgState
    {
        public Int256 Sum;
        public long Count;
    }

    public override Type ResultClrType => typeof(Decimal128);

    public override object? Compute(ZSet<TIn, Z64> rows)
    {
        Int256 sum = Int256.Zero;
        long count = 0;
        foreach (var (row, w) in rows)
        {
            sum += (Int256)_argExtract(row).Mantissa * w.Value;
            count += w.Value;
        }

        return DecimalRuntime.NarrowToDecimal128(sum / (Int256)count);
    }

    public override object? Update(ref object? state, ZSet<TIn, Z64> delta, ZSet<TIn, Z64> after)
    {
        var s = state as AvgState ?? new AvgState();
        foreach (var (row, w) in delta)
        {
            s.Sum += (Int256)_argExtract(row).Mantissa * w.Value;
            s.Count += w.Value;
        }

        state = s;
        return DecimalRuntime.NarrowToDecimal128(s.Sum / (Int256)s.Count);
    }
}

/// <summary>
/// <c>AVG</c>-of-non-null over a numeric column. Output is always
/// <c>double</c>; for an empty group the operator drops it before
/// asking us so we never divide by zero here.
/// </summary>
internal sealed class TypedAvgDoubleAggregator<TIn> : TypedSqlAggregator<TIn>
    where TIn : notnull
{
    private readonly Func<TIn, double> _argExtract;

    public TypedAvgDoubleAggregator(Func<TIn, double> argExtract)
    {
        _argExtract = argExtract;
    }

    private sealed class AvgState
    {
        public double Sum;
        public long Count;
    }

    public override Type ResultClrType => typeof(double);

    public override object? Compute(ZSet<TIn, Z64> rows)
    {
        double sum = 0;
        long count = 0;
        foreach (var (row, w) in rows)
        {
            sum += _argExtract(row) * w.Value;
            count += w.Value;
        }

        // Guarded by the operator: empty group skips the aggregator.
        return sum / count;
    }

    public override object? Update(ref object? state, ZSet<TIn, Z64> delta, ZSet<TIn, Z64> after)
    {
        var s = state as AvgState ?? new AvgState();
        foreach (var (row, w) in delta)
        {
            s.Sum += _argExtract(row) * w.Value;
            s.Count += w.Value;
        }

        state = s;
        return s.Sum / s.Count;
    }
}

// ---- Nullable-arg variants (Phase N4) ----

/// <summary>
/// <c>COUNT(col)</c> over a nullable arg: counts the per-row weight
/// for every row whose extracted arg has a value, skipping rows
/// where it's NULL. Result is always non-null <c>long</c> (matches
/// SQL <c>COUNT(col)</c> over an empty/all-null group: returns 0
/// — but the linear-emission gate fires before that on a truly
/// empty group; an all-null non-empty group still emits 0).
/// </summary>
internal sealed class TypedCountNullableAggregator<TIn> : TypedSqlAggregator<TIn>
    where TIn : notnull
{
    private readonly Func<TIn, bool> _hasValue;

    public TypedCountNullableAggregator(Func<TIn, bool> hasValue)
    {
        _hasValue = hasValue;
    }

    public override Type ResultClrType => typeof(long);

    public override object? Compute(ZSet<TIn, Z64> rows)
    {
        var total = 0L;
        foreach (var (row, w) in rows)
        {
            if (_hasValue(row)) total += w.Value;
        }

        return total;
    }

    public override object? Update(ref object? state, ZSet<TIn, Z64> delta, ZSet<TIn, Z64> after)
    {
        var s = state is long prior ? prior : 0L;
        foreach (var (row, w) in delta)
        {
            if (_hasValue(row)) s += w.Value;
        }

        state = s;
        return s;
    }
}

/// <summary>
/// <c>SUM</c>-of-non-null with <c>long</c> running total and
/// nullable-arg extraction. Mirrors
/// <see cref="SqlSumAggregator"/>'s DistinctNonNullRows tracking
/// so the result is <c>null</c> iff no current row contributes a
/// non-null arg.
/// </summary>
internal sealed class TypedSumLongNullableAggregator<TIn> : TypedSqlAggregator<TIn>
    where TIn : notnull
{
    private readonly Func<TIn, long?> _argExtract;

    public TypedSumLongNullableAggregator(Func<TIn, long?> argExtract)
    {
        _argExtract = argExtract;
    }

    private sealed class SumState
    {
        public long Sum;
        public long DistinctNonNullRows;
    }

    public override Type ResultClrType => typeof(long);

    public override object? Compute(ZSet<TIn, Z64> rows)
    {
        long sum = 0;
        var any = false;
        foreach (var (row, w) in rows)
        {
            var v = _argExtract(row);
            if (!v.HasValue) continue;
            sum = checked(sum + v.GetValueOrDefault() * w.Value);
            any = true;
        }

        return any ? (object)sum : null;
    }

    public override object? Update(ref object? state, ZSet<TIn, Z64> delta, ZSet<TIn, Z64> after)
    {
        var s = state as SumState ?? new SumState();
        foreach (var (row, w) in delta)
        {
            var v = _argExtract(row);
            if (!v.HasValue) continue;
            var afterW = after.WeightOf(row).Value;
            var beforeW = afterW - w.Value;
            s.Sum = checked(s.Sum + v.GetValueOrDefault() * w.Value);
            if (beforeW == 0 && afterW != 0) s.DistinctNonNullRows++;
            else if (beforeW != 0 && afterW == 0) s.DistinctNonNullRows--;
        }

        state = s;
        return s.DistinctNonNullRows > 0 ? (object)s.Sum : null;
    }
}

/// <summary><c>SUM</c>-of-non-null with <c>double</c> running total and nullable-arg extraction.</summary>
internal sealed class TypedSumDoubleNullableAggregator<TIn> : TypedSqlAggregator<TIn>
    where TIn : notnull
{
    private readonly Func<TIn, double?> _argExtract;

    public TypedSumDoubleNullableAggregator(Func<TIn, double?> argExtract)
    {
        _argExtract = argExtract;
    }

    private sealed class SumState
    {
        public double Sum;
        public long DistinctNonNullRows;
    }

    public override Type ResultClrType => typeof(double);

    public override object? Compute(ZSet<TIn, Z64> rows)
    {
        double sum = 0;
        var any = false;
        foreach (var (row, w) in rows)
        {
            var v = _argExtract(row);
            if (!v.HasValue) continue;
            sum += v.GetValueOrDefault() * w.Value;
            any = true;
        }

        return any ? (object)sum : null;
    }

    public override object? Update(ref object? state, ZSet<TIn, Z64> delta, ZSet<TIn, Z64> after)
    {
        var s = state as SumState ?? new SumState();
        foreach (var (row, w) in delta)
        {
            var v = _argExtract(row);
            if (!v.HasValue) continue;
            var afterW = after.WeightOf(row).Value;
            var beforeW = afterW - w.Value;
            s.Sum += v.GetValueOrDefault() * w.Value;
            if (beforeW == 0 && afterW != 0) s.DistinctNonNullRows++;
            else if (beforeW != 0 && afterW == 0) s.DistinctNonNullRows--;
        }

        state = s;
        return s.DistinctNonNullRows > 0 ? (object)s.Sum : null;
    }
}

/// <summary><c>SUM</c>-of-non-null with <see cref="Decimal128"/> running total and nullable-arg extraction.</summary>
internal sealed class TypedSumDecimalNullableAggregator<TIn> : TypedSqlAggregator<TIn>
    where TIn : notnull
{
    private readonly Func<TIn, Decimal128?> _argExtract;

    public TypedSumDecimalNullableAggregator(Func<TIn, Decimal128?> argExtract)
    {
        _argExtract = argExtract;
    }

    private sealed class SumState
    {
        public Int256 Sum;
        public long DistinctNonNullRows;
    }

    public override Type ResultClrType => typeof(Decimal128);

    public override object? Compute(ZSet<TIn, Z64> rows)
    {
        Int256 sum = Int256.Zero;
        var any = false;
        foreach (var (row, w) in rows)
        {
            var v = _argExtract(row);
            if (!v.HasValue) continue;
            sum += (Int256)v.GetValueOrDefault().Mantissa * w.Value;
            any = true;
        }

        return any ? (object)DecimalRuntime.NarrowToDecimal128(sum) : null;
    }

    public override object? Update(ref object? state, ZSet<TIn, Z64> delta, ZSet<TIn, Z64> after)
    {
        var s = state as SumState ?? new SumState();
        foreach (var (row, w) in delta)
        {
            var v = _argExtract(row);
            if (!v.HasValue) continue;
            var afterW = after.WeightOf(row).Value;
            var beforeW = afterW - w.Value;
            s.Sum += (Int256)v.GetValueOrDefault().Mantissa * w.Value;
            if (beforeW == 0 && afterW != 0) s.DistinctNonNullRows++;
            else if (beforeW != 0 && afterW == 0) s.DistinctNonNullRows--;
        }

        state = s;
        return s.DistinctNonNullRows > 0
            ? (object)DecimalRuntime.NarrowToDecimal128(s.Sum)
            : null;
    }
}

/// <summary><c>AVG</c>-of-non-null over a numeric column with nullable-arg extraction. Returns <c>null</c> for an all-NULL group.</summary>
internal sealed class TypedAvgDoubleNullableAggregator<TIn> : TypedSqlAggregator<TIn>
    where TIn : notnull
{
    private readonly Func<TIn, double?> _argExtract;

    public TypedAvgDoubleNullableAggregator(Func<TIn, double?> argExtract)
    {
        _argExtract = argExtract;
    }

    private sealed class AvgState
    {
        public double Sum;
        public long NonNullCount;
    }

    public override Type ResultClrType => typeof(double);

    public override object? Compute(ZSet<TIn, Z64> rows)
    {
        double sum = 0;
        long count = 0;
        foreach (var (row, w) in rows)
        {
            var v = _argExtract(row);
            if (!v.HasValue) continue;
            sum += v.GetValueOrDefault() * w.Value;
            count += w.Value;
        }

        return count == 0 ? null : (object)(sum / count);
    }

    public override object? Update(ref object? state, ZSet<TIn, Z64> delta, ZSet<TIn, Z64> after)
    {
        var s = state as AvgState ?? new AvgState();
        foreach (var (row, w) in delta)
        {
            var v = _argExtract(row);
            if (!v.HasValue) continue;
            s.Sum += v.GetValueOrDefault() * w.Value;
            s.NonNullCount += w.Value;
        }

        state = s;
        return s.NonNullCount == 0 ? null : (object)(s.Sum / s.NonNullCount);
    }
}

/// <summary><c>AVG</c>-of-non-null over a DECIMAL column with nullable-arg extraction.</summary>
internal sealed class TypedAvgDecimalNullableAggregator<TIn> : TypedSqlAggregator<TIn>
    where TIn : notnull
{
    private readonly Func<TIn, Decimal128?> _argExtract;

    public TypedAvgDecimalNullableAggregator(Func<TIn, Decimal128?> argExtract)
    {
        _argExtract = argExtract;
    }

    private sealed class AvgState
    {
        public Int256 Sum;
        public long NonNullCount;
    }

    public override Type ResultClrType => typeof(Decimal128);

    public override object? Compute(ZSet<TIn, Z64> rows)
    {
        Int256 sum = Int256.Zero;
        long count = 0;
        foreach (var (row, w) in rows)
        {
            var v = _argExtract(row);
            if (!v.HasValue) continue;
            sum += (Int256)v.GetValueOrDefault().Mantissa * w.Value;
            count += w.Value;
        }

        return count == 0
            ? null
            : (object)DecimalRuntime.NarrowToDecimal128(sum / (Int256)count);
    }

    public override object? Update(ref object? state, ZSet<TIn, Z64> delta, ZSet<TIn, Z64> after)
    {
        var s = state as AvgState ?? new AvgState();
        foreach (var (row, w) in delta)
        {
            var v = _argExtract(row);
            if (!v.HasValue) continue;
            s.Sum += (Int256)v.GetValueOrDefault().Mantissa * w.Value;
            s.NonNullCount += w.Value;
        }

        state = s;
        return s.NonNullCount == 0
            ? null
            : (object)DecimalRuntime.NarrowToDecimal128(s.Sum / (Int256)s.NonNullCount);
    }
}

/// <summary>
/// Typed <c>MIN</c> / <c>MAX</c> over a per-group multiset with a
/// non-null arg. State mirrors the structural variant —
/// <see cref="Counts"/> tracks how many distinct rows currently have
/// positive net weight for each value, and <see cref="Active"/> is
/// the sorted set of values with positive count, so MIN/MAX is
/// O(log n) on the distinct-value count. Returns <c>null</c> when
/// the multiset has no positive-weight rows (the linear gate emits
/// when net weight is non-zero, which can be the negative-weight
/// case — well-formed DBSP streams shouldn't produce one, but it's
/// still a valid Z-set state during retractions); the SQL output
/// column is nullable accordingly.
/// </summary>
internal sealed class TypedSqlMinMaxAggregator<TIn, T> : TypedSqlAggregator<TIn>
    where TIn : notnull
    where T : IComparable<T>
{
    private readonly Func<TIn, T> _argExtract;
    private readonly bool _wantMin;

    public TypedSqlMinMaxAggregator(Func<TIn, T> argExtract, bool wantMin)
    {
        _argExtract = argExtract;
        _wantMin = wantMin;
    }

    private sealed class State
    {
        public Dictionary<T, long> Counts = new();
        public SortedSet<T> Active = new(); // Comparer<T>.Default
    }

    public override Type ResultClrType => typeof(T);

    public override object? Compute(ZSet<TIn, Z64> rows)
    {
        T best = default!;
        var hasBest = false;
        foreach (var (row, w) in rows)
        {
            if (!Z64.IsPositive(w))
            {
                continue;
            }

            var v = _argExtract(row);
            if (!hasBest)
            {
                best = v;
                hasBest = true;
                continue;
            }

            var cmp = v.CompareTo(best);
            if (_wantMin ? cmp < 0 : cmp > 0)
            {
                best = v;
            }
        }

        return hasBest ? (object)best! : null;
    }

    public override object? Update(ref object? state, ZSet<TIn, Z64> delta, ZSet<TIn, Z64> after)
    {
        var s = state as State ?? new State();
        foreach (var (row, w) in delta)
        {
            var v = _argExtract(row);
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

        return (_wantMin ? s.Active.Min : s.Active.Max)!;
    }
}

/// <summary>
/// Typed <c>MIN</c> / <c>MAX</c> with nullable-arg extraction (Phase
/// N5). Skips rows whose extracted arg is <c>null</c> (SQL semantics)
/// and tracks distinct positive-weight non-null values just like the
/// non-null variant. Returns <c>null</c> for an all-NULL group or
/// when no row has positive weight.
/// </summary>
internal sealed class TypedSqlMinMaxNullableAggregator<TIn, T> : TypedSqlAggregator<TIn>
    where TIn : notnull
    where T : struct, IComparable<T>
{
    private readonly Func<TIn, T?> _argExtract;
    private readonly bool _wantMin;

    public TypedSqlMinMaxNullableAggregator(Func<TIn, T?> argExtract, bool wantMin)
    {
        _argExtract = argExtract;
        _wantMin = wantMin;
    }

    private sealed class State
    {
        public Dictionary<T, long> Counts = new();
        public SortedSet<T> Active = new();
    }

    public override Type ResultClrType => typeof(T);

    public override object? Compute(ZSet<TIn, Z64> rows)
    {
        T best = default;
        var hasBest = false;
        foreach (var (row, w) in rows)
        {
            if (!Z64.IsPositive(w)) continue;
            var v = _argExtract(row);
            if (!v.HasValue) continue;
            var raw = v.GetValueOrDefault();
            if (!hasBest)
            {
                best = raw;
                hasBest = true;
                continue;
            }

            var cmp = raw.CompareTo(best);
            if (_wantMin ? cmp < 0 : cmp > 0) best = raw;
        }

        return hasBest ? (object)best : null;
    }

    public override object? Update(ref object? state, ZSet<TIn, Z64> delta, ZSet<TIn, Z64> after)
    {
        var s = state as State ?? new State();
        foreach (var (row, w) in delta)
        {
            var v = _argExtract(row);
            if (!v.HasValue) continue;
            var raw = v.GetValueOrDefault();
            var afterW = after.WeightOf(row).Value;
            var beforeW = afterW - w.Value;
            var wasPositive = beforeW > 0;
            var isPositive = afterW > 0;
            if (wasPositive == isPositive) continue;

            if (isPositive)
            {
                var c = s.Counts.GetValueOrDefault(raw, 0L) + 1;
                s.Counts[raw] = c;
                if (c == 1) s.Active.Add(raw);
            }
            else
            {
                var c = s.Counts[raw] - 1;
                if (c == 0)
                {
                    s.Counts.Remove(raw);
                    s.Active.Remove(raw);
                }
                else
                {
                    s.Counts[raw] = c;
                }
            }
        }

        state = s;
        if (s.Active.Count == 0) return null;
        return (_wantMin ? s.Active.Min : s.Active.Max);
    }
}

/// <summary>
/// Typed <c>APPROX_COUNT_DISTINCT</c>: HyperLogLog estimate of the distinct
/// non-NULL argument values in the group. The argument extractor returns a
/// boxed value (<c>null</c> for SQL NULL — a no-value <c>Nullable&lt;T&gt;</c>
/// boxes to a null reference), so one implementation covers every argument
/// type. Mirrors <see cref="SqlApproxCountDistinctAggregator"/>: insert-only
/// ticks merge into the running sketch, retraction ticks rebuild from the
/// post-delta multiset, and the result is identical to a batch recompute.
/// </summary>
internal sealed class TypedApproxCountDistinctAggregator<TIn> : TypedSqlAggregator<TIn>
    where TIn : notnull
{
    private readonly Func<TIn, object?> _argExtract;

    public TypedApproxCountDistinctAggregator(Func<TIn, object?> argExtract)
    {
        _argExtract = argExtract;
    }

    public override Type ResultClrType => typeof(long);

    public override object? Compute(ZSet<TIn, Z64> rows)
    {
        var sketch = new HyperLogLog();
        HllSupport.FoldPositive(sketch, rows, _argExtract);
        return sketch.EstimateCardinality();
    }

    public override object? Update(ref object? state, ZSet<TIn, Z64> delta, ZSet<TIn, Z64> after)
    {
        var sketch = state as HyperLogLog;
        if (sketch is not null && IsInsertOnly(delta))
        {
            HllSupport.FoldPositive(sketch, delta, _argExtract);
        }
        else
        {
            sketch ??= new HyperLogLog();
            sketch.Clear();
            HllSupport.FoldPositive(sketch, after, _argExtract);
        }

        state = sketch;
        return sketch.EstimateCardinality();
    }

    private static bool IsInsertOnly(ZSet<TIn, Z64> delta)
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
/// Typed <c>APPROX_PERCENTILE</c> / <c>MEDIAN</c> / <c>PERCENTILE_CONT</c>:
/// DDSketch estimate of the requested quantile of the non-NULL argument values.
/// The boxing argument extractor (<c>null</c> for SQL NULL) lets one
/// implementation cover every numeric argument type. Mirrors
/// <see cref="SqlApproxPercentileAggregator"/>: the sketch is invertible, so
/// every tick folds the signed delta into the running state and the result
/// equals a batch recompute exactly.
/// </summary>
internal sealed class TypedApproxPercentileAggregator<TIn> : TypedSqlAggregator<TIn>
    where TIn : notnull
{
    private readonly Func<TIn, object?> _argExtract;
    private readonly double _fraction;
    private readonly int _decimalScale;

    public TypedApproxPercentileAggregator(
        Func<TIn, object?> argExtract, double fraction, int decimalScale)
    {
        _argExtract = argExtract;
        _fraction = fraction;
        _decimalScale = decimalScale;
    }

    public override Type ResultClrType => typeof(double);

    public override object? Compute(ZSet<TIn, Z64> rows)
    {
        var sketch = new DdSketch();
        DdSketchSupport.FoldSigned(sketch, rows, _argExtract, _decimalScale);
        return sketch.EstimateQuantile(_fraction);
    }

    public override object? Update(ref object? state, ZSet<TIn, Z64> delta, ZSet<TIn, Z64> after)
    {
        var sketch = state as DdSketch ?? new DdSketch();
        DdSketchSupport.FoldSigned(sketch, delta, _argExtract, _decimalScale);
        state = sketch;
        return sketch.EstimateQuantile(_fraction);
    }
}

/// <summary>
/// Typed composite that runs all of a query's aggregates over the
/// per-group multiset and packs their results into the emitted
/// aggregate-output row <typeparamref name="TAgg"/>.
/// </summary>
/// <remarks>
/// State is an <c>object?[]</c> of per-aggregator scratch slots —
/// same shape as <see cref="CompositeAggregator"/>. Results are
/// fed through <paramref name="packResults"/>, which is a typed
/// factory built from <see cref="TypedRowEmitter"/>'s typed-fields
/// ctor (boxing happens once per agg per tick — small constant cost).
/// </remarks>
internal sealed class TypedCompositeAggregator<TIn, TAgg> : IAggregator<TIn, TAgg>
    where TIn : notnull
    where TAgg : notnull
{
    private readonly TypedSqlAggregator<TIn>[] _aggs;
    private readonly Func<object?[], TAgg> _packResults;

    public TypedCompositeAggregator(
        TypedSqlAggregator<TIn>[] aggs,
        Func<object?[], TAgg> packResults)
    {
        _aggs = aggs;
        _packResults = packResults;
    }

    public Optional<TAgg> Compute(ZSet<TIn, Z64> multiset)
    {
        if (Z64.IsZero(multiset.SumWeights()))
        {
            return Optional<TAgg>.None;
        }

        var results = new object?[_aggs.Length];
        for (var i = 0; i < _aggs.Length; i++)
        {
            results[i] = _aggs[i].Compute(multiset);
        }

        return Optional<TAgg>.Some(_packResults(results));
    }

    public Optional<TAgg> Update(
        ref object? state,
        Optional<TAgg> oldValue,
        ZSet<TIn, Z64> delta,
        ZSet<TIn, Z64> afterMultiset)
    {
        // Advance the sub-aggregators' state unconditionally — they
        // track weight transitions across ticks; short-circuiting on
        // the linear emission gate would corrupt that bookkeeping
        // for cancelling-weight groups. The gate applies to the
        // output wrapping below.
        var subStates = state as object?[] ?? new object?[_aggs.Length];
        var results = new object?[_aggs.Length];
        for (var i = 0; i < _aggs.Length; i++)
        {
            var slot = subStates[i];
            results[i] = _aggs[i].Update(ref slot, delta, afterMultiset);
            subStates[i] = slot;
        }

        state = subStates;

        if (Z64.IsZero(afterMultiset.SumWeights()))
        {
            return Optional<TAgg>.None;
        }

        return Optional<TAgg>.Some(_packResults(results));
    }
}
