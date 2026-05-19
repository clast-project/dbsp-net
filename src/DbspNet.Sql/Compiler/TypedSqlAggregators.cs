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
/// <para><b>NULL handling.</b> The typed-row pipeline rejects nullable
/// columns at the schema gate, so every extracted argument is
/// non-null by construction. That collapses the structural
/// aggregators' NULL-skip logic (e.g. <c>SqlSumAggregator</c>'s
/// <c>DistinctNonNullRows</c> tracking) to nothing — Update state is
/// just the running accumulator.</para>
/// <para><b>Empty-group handling.</b> <see cref="IncrementalAggregateOp"/>
/// already drops empty groups before the aggregator is consulted (via
/// the <c>after.IsEmpty</c> short-circuit inside
/// <see cref="TypedCompositeAggregator{TIn,TAgg}"/>), so SQL's "SUM
/// over empty group = NULL" semantics never arise on the typed
/// path.</para>
/// </remarks>
internal abstract class TypedSqlAggregator<TIn>
    where TIn : notnull
{
    public abstract Type ResultClrType { get; }

    public abstract object Compute(ZSet<TIn, Z64> rows);

    public virtual object Update(ref object? state, ZSet<TIn, Z64> delta, ZSet<TIn, Z64> after)
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

    public override object Compute(ZSet<TIn, Z64> rows)
    {
        var total = 0L;
        foreach (var (_, w) in rows)
        {
            total += w.Value;
        }

        return total;
    }

    public override object Update(ref object? state, ZSet<TIn, Z64> delta, ZSet<TIn, Z64> after)
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

    public override object Compute(ZSet<TIn, Z64> rows)
    {
        long sum = 0;
        foreach (var (row, w) in rows)
        {
            sum = checked(sum + _argExtract(row) * w.Value);
        }

        return sum;
    }

    public override object Update(ref object? state, ZSet<TIn, Z64> delta, ZSet<TIn, Z64> after)
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

    public override object Compute(ZSet<TIn, Z64> rows)
    {
        double sum = 0;
        foreach (var (row, w) in rows)
        {
            sum += _argExtract(row) * w.Value;
        }

        return sum;
    }

    public override object Update(ref object? state, ZSet<TIn, Z64> delta, ZSet<TIn, Z64> after)
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

    public override object Compute(ZSet<TIn, Z64> rows)
    {
        Int256 sum = Int256.Zero;
        foreach (var (row, w) in rows)
        {
            sum += (Int256)_argExtract(row).Mantissa * w.Value;
        }

        return DecimalRuntime.NarrowToDecimal128(sum);
    }

    public override object Update(ref object? state, ZSet<TIn, Z64> delta, ZSet<TIn, Z64> after)
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

    public override object Compute(ZSet<TIn, Z64> rows)
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

    public override object Update(ref object? state, ZSet<TIn, Z64> delta, ZSet<TIn, Z64> after)
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

    public override object Compute(ZSet<TIn, Z64> rows)
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

    public override object Update(ref object? state, ZSet<TIn, Z64> delta, ZSet<TIn, Z64> after)
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

/// <summary>
/// Typed <c>MIN</c> / <c>MAX</c> over a per-group multiset. State
/// mirrors the structural variant — <see cref="Counts"/> tracks how
/// many distinct rows currently have positive net weight for each
/// value, and <see cref="Active"/> is the sorted set of values with
/// positive count, so MIN/MAX is O(log n) on the distinct-value
/// count. The transition detection (was-positive vs is-positive)
/// per delta row is identical to <c>SqlMinMaxAggregator.Update</c>;
/// only the boxing through <c>object</c> is gone.
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

    public override object Compute(ZSet<TIn, Z64> rows)
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

        // The operator guarantees a non-empty multiset by the time
        // Compute is called, but defensively guard against an
        // all-non-positive-weight multiset (an ill-formed input —
        // structural returns null here; we throw because the typed
        // pipeline can't carry NULL).
        if (!hasBest)
        {
            throw new InvalidOperationException(
                "typed MIN/MAX on multiset with no positive-weight rows");
        }

        return best!;
    }

    public override object Update(ref object? state, ZSet<TIn, Z64> delta, ZSet<TIn, Z64> after)
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
            throw new InvalidOperationException(
                "typed MIN/MAX on multiset with no positive-weight rows");
        }

        return (_wantMin ? s.Active.Min : s.Active.Max)!;
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
        if (multiset.IsEmpty)
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
        if (afterMultiset.IsEmpty)
        {
            return Optional<TAgg>.None;
        }

        var subStates = state as object?[] ?? new object?[_aggs.Length];
        var results = new object?[_aggs.Length];
        for (var i = 0; i < _aggs.Length; i++)
        {
            var slot = subStates[i];
            results[i] = _aggs[i].Update(ref slot, delta, afterMultiset);
            subStates[i] = slot;
        }

        state = subStates;
        return Optional<TAgg>.Some(_packResults(results));
    }
}
