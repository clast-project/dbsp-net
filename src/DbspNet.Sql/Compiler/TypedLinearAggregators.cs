// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Buffers;
using System.Buffers.Binary;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Clast.DatabaseDecimal.Values;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful.Aggregators;
using DbspNet.Sql.Expressions;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Sql.Compiler;

/// <summary>
/// Compile-time switch for the trace-free linear aggregate of
/// docs/design-state-representation.md §4G. When on (the default), a typed
/// <c>GROUP BY</c> whose aggregates are all COUNT / SUM / AVG / COUNT(DISTINCT)
/// compiles to <c>LinearAggregateOp</c>, which keeps no input trace; otherwise the
/// trace-backed <c>IncrementalAggregateOp</c>. <c>DBSPNET_LINEAR_AGG=off</c>
/// switches it off process-wide (for A/B measurement).
/// </summary>
internal static class LinearAggregateMode
{
    private static readonly bool EnvironmentOff = string.Equals(
        Environment.GetEnvironmentVariable("DBSPNET_LINEAR_AGG")?.Trim(), "off", StringComparison.OrdinalIgnoreCase);

    // Stored as the inverse of an opt-out flag so the [ThreadStatic] zero value
    // means "on" on every thread (see JoinColumnPruningMode).
    [ThreadStatic]
    private static bool _disabled;

    internal static bool Enabled
    {
        get => !_disabled && !EnvironmentOff;
        set => _disabled = !value;
    }
}

/// <summary>Per-group state (de)serialisation helpers for the linear aggregators.</summary>
internal static class LinearStateIo
{
    public static void WriteInt64(IBufferWriter<byte> w, long v)
    {
        BinaryPrimitives.WriteInt64LittleEndian(w.GetSpan(8), v);
        w.Advance(8);
    }

    public static long ReadInt64(ref ReadOnlySpan<byte> r)
    {
        var v = BinaryPrimitives.ReadInt64LittleEndian(r);
        r = r[8..];
        return v;
    }

    public static void WriteDouble(IBufferWriter<byte> w, double v) => WriteInt64(w, BitConverter.DoubleToInt64Bits(v));

    public static double ReadDouble(ref ReadOnlySpan<byte> r) => BitConverter.Int64BitsToDouble(ReadInt64(ref r));

    public static void WriteBlittable<T>(IBufferWriter<byte> w, T v)
        where T : struct
    {
        var size = Unsafe.SizeOf<T>();
        MemoryMarshal.Write(w.GetSpan(size), in v);
        w.Advance(size);
    }

    public static T ReadBlittable<T>(ref ReadOnlySpan<byte> r)
        where T : struct
    {
        var v = MemoryMarshal.Read<T>(r);
        r = r[Unsafe.SizeOf<T>()..];
        return v;
    }
}

/// <summary>Byte codec for a COUNT(DISTINCT) value type: blittable structs and <see cref="Utf8String"/>.</summary>
internal static class LinearValueCodec<T>
    where T : struct
{
    private static readonly bool IsUtf8 = typeof(T) == typeof(Utf8String);

    public static readonly bool Supported = IsUtf8 || !RuntimeHelpers.IsReferenceOrContainsReferences<T>();

    public static void Write(IBufferWriter<byte> w, T value)
    {
        if (IsUtf8)
        {
            var s = Unsafe.As<T, Utf8String>(ref value).Span;
            LinearStateIo.WriteInt64(w, s.Length);
            s.CopyTo(w.GetSpan(s.Length));
            w.Advance(s.Length);
            return;
        }

        LinearStateIo.WriteBlittable(w, value);
    }

    public static T Read(ref ReadOnlySpan<byte> r)
    {
        if (IsUtf8)
        {
            var len = (int)LinearStateIo.ReadInt64(ref r);
            var u = Utf8String.FromBytes(r[..len].ToArray());
            r = r[len..];
            return Unsafe.As<Utf8String, T>(ref u);
        }

        return LinearStateIo.ReadBlittable<T>(ref r);
    }
}

/// <summary>
/// One linear unit of a <see cref="TypedLinearCompositeAggregator{TIn,TAgg}"/>: a
/// trace-free counterpart of a <see cref="TypedSqlAggregator{TIn}"/>. State is a
/// mutable per-group object created by <see cref="NewState"/> and folded in place.
/// </summary>
internal abstract class TypedLinearAggregator<TIn>
    where TIn : notnull
{
    public virtual int ResultCount => 1;

    public abstract object NewState();

    public abstract void Fold(object state, ZSet<TIn, Z64> delta);

    public abstract void ResultInto(object state, object?[] results, int[] slots);

    public abstract bool IsZero(object state);

    public abstract void Write(IBufferWriter<byte> writer, object state);

    public abstract object Read(ref ReadOnlySpan<byte> reader);
}

/// <summary>Running (sum, non-NULL weight) cell, shared by the integer SUM/AVG/COUNT units.</summary>
internal sealed class LinearLongCell
{
    public long Sum;
    public long N;
}

internal sealed class LinearDoubleCell
{
    public double Sum;
    public long N;
}

internal sealed class LinearWideCell
{
    public Int256 Sum;
    public long N;
}

/// <summary>
/// <c>COUNT(*)</c> / <c>COUNT(x)</c>: the summed weight of rows (whose argument is
/// non-NULL). Same fold as <see cref="TypedCountStarAggregator{TIn}"/> /
/// <see cref="TypedCountNullableAggregator{TIn}"/>.
/// </summary>
internal sealed class LinearCountAggregator<TIn> : TypedLinearAggregator<TIn>
    where TIn : notnull
{
    private readonly Func<TIn, bool>? _hasValue;

    public LinearCountAggregator(Func<TIn, bool>? hasValue)
    {
        _hasValue = hasValue;
    }

    public override object NewState() => new LinearLongCell();

    public override void Fold(object state, ZSet<TIn, Z64> delta)
    {
        var s = (LinearLongCell)state;
        foreach (var (row, w) in delta)
        {
            if (_hasValue is null || _hasValue(row)) s.N += w.Value;
        }
    }

    public override void ResultInto(object state, object?[] results, int[] slots)
        => results[slots[0]] = ((LinearLongCell)state).N;

    public override bool IsZero(object state) => ((LinearLongCell)state).N == 0;

    public override void Write(IBufferWriter<byte> writer, object state)
        => LinearStateIo.WriteInt64(writer, ((LinearLongCell)state).N);

    public override object Read(ref ReadOnlySpan<byte> reader)
        => new LinearLongCell { N = LinearStateIo.ReadInt64(ref reader) };
}

/// <summary>
/// <c>SUM</c> over a BIGINT accumulator. The NULL rule is "NULL iff the summed
/// weight of non-NULL-argument rows is zero" — for a non-NULL argument that weight
/// is the group's own (non-zero whenever a result is asked), so one class serves
/// both nullabilities. Overflow: the same <c>checked(sum + v * w)</c> per delta row
/// as <see cref="TypedSumLongAggregator{TIn}"/>.
/// </summary>
internal sealed class LinearSumLongAggregator<TIn> : TypedLinearAggregator<TIn>
    where TIn : notnull
{
    private readonly Func<TIn, long?> _arg;

    public LinearSumLongAggregator(Func<TIn, long?> arg)
    {
        _arg = arg;
    }

    public override object NewState() => new LinearLongCell();

    public override void Fold(object state, ZSet<TIn, Z64> delta)
    {
        var s = (LinearLongCell)state;
        foreach (var (row, w) in delta)
        {
            var v = _arg(row);
            if (!v.HasValue) continue;
            s.Sum = checked(s.Sum + v.GetValueOrDefault() * w.Value);
            s.N += w.Value;
        }
    }

    public override void ResultInto(object state, object?[] results, int[] slots)
    {
        var s = (LinearLongCell)state;
        results[slots[0]] = s.N != 0 ? s.Sum : null;
    }

    public override bool IsZero(object state)
    {
        var s = (LinearLongCell)state;
        return s.N == 0 && s.Sum == 0;
    }

    public override void Write(IBufferWriter<byte> writer, object state)
    {
        var s = (LinearLongCell)state;
        LinearStateIo.WriteInt64(writer, s.Sum);
        LinearStateIo.WriteInt64(writer, s.N);
    }

    public override object Read(ref ReadOnlySpan<byte> reader)
        => new LinearLongCell { Sum = LinearStateIo.ReadInt64(ref reader), N = LinearStateIo.ReadInt64(ref reader) };
}

/// <summary>
/// <c>SUM</c> (<paramref name="avg"/> = false) or <c>AVG</c> over a DOUBLE accumulator.
/// The float sum is not exact, so emptiness is judged by the integer weight only;
/// the sum is persisted bit-exactly.
/// </summary>
internal sealed class LinearDoubleAggregator<TIn> : TypedLinearAggregator<TIn>
    where TIn : notnull
{
    private readonly Func<TIn, double?> _arg;
    private readonly bool _avg;

    public LinearDoubleAggregator(Func<TIn, double?> arg, bool avg)
    {
        _arg = arg;
        _avg = avg;
    }

    public override object NewState() => new LinearDoubleCell();

    public override void Fold(object state, ZSet<TIn, Z64> delta)
    {
        var s = (LinearDoubleCell)state;
        foreach (var (row, w) in delta)
        {
            var v = _arg(row);
            if (!v.HasValue) continue;
            s.Sum += v.GetValueOrDefault() * w.Value;
            s.N += w.Value;
        }
    }

    public override void ResultInto(object state, object?[] results, int[] slots)
    {
        var s = (LinearDoubleCell)state;
        results[slots[0]] = s.N == 0 ? null : _avg ? s.Sum / s.N : s.Sum;
    }

    public override bool IsZero(object state) => ((LinearDoubleCell)state).N == 0;

    public override void Write(IBufferWriter<byte> writer, object state)
    {
        var s = (LinearDoubleCell)state;
        LinearStateIo.WriteDouble(writer, s.Sum);
        LinearStateIo.WriteInt64(writer, s.N);
    }

    public override object Read(ref ReadOnlySpan<byte> reader)
        => new LinearDoubleCell { Sum = LinearStateIo.ReadDouble(ref reader), N = LinearStateIo.ReadInt64(ref reader) };
}

/// <summary>
/// <c>SUM</c> / <c>AVG</c> over a DECIMAL argument, accumulated in <see cref="Int256"/>
/// and narrowed at output exactly as <see cref="TypedSumDecimalAggregator{TIn}"/> /
/// <see cref="TypedAvgDecimalAggregator{TIn}"/> do (same overflow behaviour).
/// </summary>
internal sealed class LinearDecimalAggregator<TIn> : TypedLinearAggregator<TIn>
    where TIn : notnull
{
    private readonly Func<TIn, Decimal128?> _arg;
    private readonly bool _avg;

    public LinearDecimalAggregator(Func<TIn, Decimal128?> arg, bool avg)
    {
        _arg = arg;
        _avg = avg;
    }

    public override object NewState() => new LinearWideCell();

    public override void Fold(object state, ZSet<TIn, Z64> delta)
    {
        var s = (LinearWideCell)state;
        foreach (var (row, w) in delta)
        {
            var v = _arg(row);
            if (!v.HasValue) continue;
            s.Sum += (Int256)v.GetValueOrDefault().Mantissa * w.Value;
            s.N += w.Value;
        }
    }

    public override void ResultInto(object state, object?[] results, int[] slots)
    {
        var s = (LinearWideCell)state;
        results[slots[0]] = s.N == 0
            ? null
            : DecimalRuntime.NarrowToDecimal128(_avg ? s.Sum / (Int256)s.N : s.Sum);
    }

    public override bool IsZero(object state)
    {
        var s = (LinearWideCell)state;
        return s.N == 0 && s.Sum == Int256.Zero;
    }

    public override void Write(IBufferWriter<byte> writer, object state)
    {
        var s = (LinearWideCell)state;
        LinearStateIo.WriteBlittable(writer, s.Sum);
        LinearStateIo.WriteInt64(writer, s.N);
    }

    public override object Read(ref ReadOnlySpan<byte> reader)
        => new LinearWideCell { Sum = LinearStateIo.ReadBlittable<Int256>(ref reader), N = LinearStateIo.ReadInt64(ref reader) };
}

/// <summary>
/// Open-addressing table (linear probing, backward-shift deletion) from an unboxed
/// value to <c>k</c> per-member <b>summed weights</b>, plus each member's count of
/// values whose summed weight is positive. The trace-free counterpart of
/// <see cref="DistinctValueTable{T}"/>, which counts positive-weight <i>rows</i> and so
/// needs the trace to see row-level transitions.
/// </summary>
internal sealed class DistinctWeightTable<T>
    where T : struct
{
    private const int MinCapacity = 4;

    private readonly int _k;
    private int[] _hashes;   // 0 = empty slot
    private T[] _keys;
    private long[] _weights; // _k per slot
    private int _count;
    private int _shift;

    public DistinctWeightTable(int k)
    {
        _k = k;
        _hashes = new int[MinCapacity];
        _keys = new T[MinCapacity];
        _weights = new long[MinCapacity * k];
        _shift = 32 - BitOperations.Log2(MinCapacity);
        Distinct = new long[k];
    }

    /// <summary>Per member: number of values whose summed weight is positive.</summary>
    public long[] Distinct { get; }

    public int Count => _count;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int Hash(T key)
    {
        var h = (int)((uint)EqualityComparer<T>.Default.GetHashCode(key) * 0x9E3779B9u);
        return h == 0 ? 1 : h;
    }

    /// <summary>Add <paramref name="weight"/> to <paramref name="key"/>'s weight for every member in <paramref name="mask"/>.</summary>
    public void Add(T key, ulong mask, long weight)
    {
        if (weight == 0 || mask == 0)
        {
            return;
        }

        var hash = Hash(key);
        var slot = FindSlot(key, hash);
        if (slot < 0)
        {
            if (_count + 1 > _hashes.Length - (_hashes.Length >> 2))
            {
                Grow();
            }

            slot = EmptySlotFor(hash);
            _hashes[slot] = hash;
            _keys[slot] = key;
            _count++;
        }

        var baseIdx = slot * _k;
        while (mask != 0)
        {
            var j = BitOperations.TrailingZeroCount(mask);
            mask &= mask - 1;
            ref var cell = ref _weights[baseIdx + j];
            var before = cell;
            var after = checked(before + weight);
            cell = after;
            if (before > 0 != after > 0)
            {
                Distinct[j] += after > 0 ? 1 : -1;
            }
        }

        for (var j = 0; j < _k; j++)
        {
            if (_weights[baseIdx + j] != 0)
            {
                return;
            }
        }

        RemoveAt(slot);
    }

    public void Write(IBufferWriter<byte> w)
    {
        LinearStateIo.WriteInt64(w, _count);
        for (var s = 0; s < _hashes.Length; s++)
        {
            if (_hashes[s] == 0) continue;
            LinearValueCodec<T>.Write(w, _keys[s]);
            for (var j = 0; j < _k; j++)
            {
                LinearStateIo.WriteInt64(w, _weights[s * _k + j]);
            }
        }
    }

    public void ReadInto(ref ReadOnlySpan<byte> r)
    {
        var n = LinearStateIo.ReadInt64(ref r);
        for (var i = 0; i < n; i++)
        {
            var key = LinearValueCodec<T>.Read(ref r);
            for (var j = 0; j < _k; j++)
            {
                Add(key, 1UL << j, LinearStateIo.ReadInt64(ref r));
            }
        }
    }

    private int FindSlot(T key, int hash)
    {
        var mask = _hashes.Length - 1;
        var i = (int)((uint)hash >> _shift);
        while (true)
        {
            var h = _hashes[i];
            if (h == 0)
            {
                return -1;
            }

            if (h == hash && EqualityComparer<T>.Default.Equals(_keys[i], key))
            {
                return i;
            }

            i = (i + 1) & mask;
        }
    }

    private int EmptySlotFor(int hash)
    {
        var mask = _hashes.Length - 1;
        var i = (int)((uint)hash >> _shift);
        while (_hashes[i] != 0)
        {
            i = (i + 1) & mask;
        }

        return i;
    }

    private void RemoveAt(int hole)
    {
        var mask = _hashes.Length - 1;
        var i = hole;
        while (true)
        {
            i = (i + 1) & mask;
            var h = _hashes[i];
            if (h == 0)
            {
                break;
            }

            var ideal = (int)((uint)h >> _shift);
            var distHole = (hole - ideal) & mask;
            var distI = (i - ideal) & mask;
            if (distHole < distI)
            {
                _hashes[hole] = h;
                _keys[hole] = _keys[i];
                Array.Copy(_weights, i * _k, _weights, hole * _k, _k);
                hole = i;
            }
        }

        _hashes[hole] = 0;
        _keys[hole] = default;
        Array.Clear(_weights, hole * _k, _k);
        _count--;
    }

    private void Grow()
    {
        var oldHashes = _hashes;
        var oldKeys = _keys;
        var oldWeights = _weights;
        var cap = oldHashes.Length * 2;
        _hashes = new int[cap];
        _keys = new T[cap];
        _weights = new long[cap * _k];
        _shift = 32 - BitOperations.Log2((uint)cap);
        for (var s = 0; s < oldHashes.Length; s++)
        {
            var h = oldHashes[s];
            if (h == 0)
            {
                continue;
            }

            var d = EmptySlotFor(h);
            _hashes[d] = h;
            _keys[d] = oldKeys[s];
            Array.Copy(oldWeights, s * _k, _weights, d * _k, _k);
        }
    }
}

/// <summary>
/// Trace-free <c>COUNT(DISTINCT x)</c> members sharing one unboxed base <c>x</c>
/// (FILTER variants included), like <see cref="TypedSharedCountDistinctAggregator{TIn,T}"/>,
/// but counting the values whose <b>summed weight</b> is positive rather than the
/// values held by some positive-weight row. The two agree whenever the integrated
/// group has no negative row weights (every well-formed table); see §4G.
/// </summary>
internal sealed class LinearCountDistinctAggregator<TIn, T> : TypedLinearAggregator<TIn>
    where TIn : notnull
    where T : struct
{
    private readonly Func<TIn, T?> _base;
    private readonly Func<TIn, bool>?[] _predicates;

    public LinearCountDistinctAggregator(Func<TIn, T?> baseExtract, Func<TIn, bool>?[] predicates)
    {
        if (predicates.Length is 0 or > 64)
        {
            throw new ArgumentOutOfRangeException(nameof(predicates));
        }

        _base = baseExtract;
        _predicates = predicates;
    }

    public override int ResultCount => _predicates.Length;

    public override object NewState() => new DistinctWeightTable<T>(_predicates.Length);

    public override void Fold(object state, ZSet<TIn, Z64> delta)
    {
        var table = (DistinctWeightTable<T>)state;
        foreach (var (row, w) in delta)
        {
            var v = _base(row);
            if (!v.HasValue) continue;
            var mask = 0UL;
            for (var j = 0; j < _predicates.Length; j++)
            {
                var p = _predicates[j];
                if (p is null || p(row))
                {
                    mask |= 1UL << j;
                }
            }

            table.Add(v.GetValueOrDefault(), mask, w.Value);
        }
    }

    public override void ResultInto(object state, object?[] results, int[] slots)
    {
        var d = ((DistinctWeightTable<T>)state).Distinct;
        for (var j = 0; j < slots.Length; j++)
        {
            results[slots[j]] = d[j];
        }
    }

    public override bool IsZero(object state) => ((DistinctWeightTable<T>)state).Count == 0;

    public override void Write(IBufferWriter<byte> writer, object state)
        => ((DistinctWeightTable<T>)state).Write(writer);

    public override object Read(ref ReadOnlySpan<byte> reader)
    {
        var table = new DistinctWeightTable<T>(_predicates.Length);
        table.ReadInto(ref reader);
        return table;
    }
}

/// <summary>
/// The trace-free composite: runs every linear unit over a group's delta and packs
/// the results into <typeparamref name="TAgg"/>, with the same result slots as
/// <see cref="TypedCompositeAggregator{TIn,TAgg}"/>. Per-group state is one
/// <c>object[]</c> of unit states.
/// </summary>
internal sealed class TypedLinearCompositeAggregator<TIn, TAgg> : ILinearAggregator<TIn, TAgg>
    where TIn : notnull
    where TAgg : notnull
{
    private readonly TypedLinearAggregator<TIn>[] _units;
    private readonly int[][] _slots;
    private readonly int _resultCount;
    private readonly Func<object?[], TAgg> _packResults;

    public TypedLinearCompositeAggregator(
        TypedLinearAggregator<TIn>[] units,
        int[][] slots,
        int resultCount,
        Func<object?[], TAgg> packResults)
    {
        _units = units;
        _slots = slots;
        _resultCount = resultCount;
        _packResults = packResults;
    }

    public void Fold(ref object? state, ZSet<TIn, Z64> delta)
    {
        if (state is not object[] states)
        {
            states = new object[_units.Length];
            for (var i = 0; i < _units.Length; i++) states[i] = _units[i].NewState();
            state = states;
        }

        for (var i = 0; i < _units.Length; i++)
        {
            _units[i].Fold(states[i], delta);
        }
    }

    public TAgg Result(object? state)
    {
        var states = (object[])state!;
        var results = new object?[_resultCount];
        for (var i = 0; i < _units.Length; i++)
        {
            _units[i].ResultInto(states[i], results, _slots[i]);
        }

        return _packResults(results);
    }

    public bool IsZero(object? state)
    {
        if (state is not object[] states) return true;
        for (var i = 0; i < _units.Length; i++)
        {
            if (!_units[i].IsZero(states[i])) return false;
        }

        return true;
    }

    public void WriteState(IBufferWriter<byte> writer, object? state)
    {
        var states = (object[])state!;
        for (var i = 0; i < _units.Length; i++)
        {
            _units[i].Write(writer, states[i]);
        }
    }

    public object? ReadState(ReadOnlySpan<byte> blob)
    {
        var states = new object[_units.Length];
        for (var i = 0; i < _units.Length; i++)
        {
            states[i] = _units[i].Read(ref blob);
        }

        return states;
    }
}
