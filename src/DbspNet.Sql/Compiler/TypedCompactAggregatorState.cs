// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Numerics;
using System.Runtime.CompilerServices;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;

namespace DbspNet.Sql.Compiler;

/// <summary>
/// Compile-time switch for the compact aggregator sub-state of
/// docs/design-state-representation.md §4 (variant B): unboxed, shared
/// COUNT(DISTINCT) tables and the trace-reading MIN/MAX. Read by
/// <see cref="TypedPlanCompiler"/> when it builds a typed composite; the old
/// boxed <see cref="TypedCountDistinctAggregator{TIn}"/> and the
/// <c>Dictionary</c>+<c>SortedSet</c> MIN/MAX stay as the fallback.
/// </summary>
internal static class CompactAggregatorStateMode
{
    // Stored as the inverse of an opt-out flag so the [ThreadStatic] zero value
    // means "on" on every thread (see JoinColumnPruningMode).
    [ThreadStatic]
    private static bool _distinctDisabled;

    [ThreadStatic]
    private static bool _minMaxDisabled;

    /// <summary>Unboxed, filter-shared COUNT(DISTINCT) state.</summary>
    internal static bool CompactDistinct
    {
        get => !_distinctDisabled;
        set => _distinctDisabled = !value;
    }

    /// <summary>MIN/MAX keeps only (best, count) and rescans the group on loss of the best.</summary>
    internal static bool CompactMinMax
    {
        get => !_minMaxDisabled;
        set => _minMaxDisabled = !value;
    }
}

/// <summary>
/// Open-addressing (linear probing, backward-shift deletion) table from an
/// <b>unboxed</b> value to <c>k</c> per-member row counts, plus each member's
/// running distinct-value count. One instance is the per-group state of a
/// <see cref="TypedSharedCountDistinctAggregator{TIn,T}"/>: replaces k
/// <c>Dictionary&lt;object,long&gt;</c> (one box + one entry per value per
/// member) with one key slot and k <c>int</c> counters per value.
/// </summary>
internal sealed class DistinctValueTable<T>
    where T : struct
{
    private const int MinCapacity = 4;

    private readonly int _k;
    private int[] _hashes;   // 0 = empty slot; otherwise the mixed hash (never 0)
    private T[] _keys;
    private int[] _counts;   // _k counters per slot
    private int _count;
    private int _shift;      // slot = (uint)hash >> _shift

    public DistinctValueTable(int k)
    {
        _k = k;
        _hashes = new int[MinCapacity];
        _keys = new T[MinCapacity];
        _counts = new int[MinCapacity * k];
        _shift = 32 - BitOperations.Log2(MinCapacity);
        Distinct = new long[k];
    }

    /// <summary>Per member: number of values whose counter is positive.</summary>
    public long[] Distinct { get; }

    public int Count => _count;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int Hash(T key)
    {
        // Fibonacci mix so sequential ids (bidder, auction) spread; never 0.
        var h = (int)((uint)EqualityComparer<T>.Default.GetHashCode(key) * 0x9E3779B9u);
        return h == 0 ? 1 : h;
    }

    /// <summary>Row with <paramref name="key"/> became positive for every member in <paramref name="mask"/>.</summary>
    public void Insert(T key, ulong mask)
    {
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
            if (_counts[baseIdx + j]++ == 0)
            {
                Distinct[j]++;
            }
        }
    }

    /// <summary>Row with <paramref name="key"/> stopped being positive for every member in <paramref name="mask"/>.</summary>
    public void Remove(T key, ulong mask)
    {
        var slot = FindSlot(key, Hash(key));
        if (slot < 0)
        {
            throw new InvalidOperationException("COUNT(DISTINCT) state lost a value it never counted.");
        }

        var baseIdx = slot * _k;
        while (mask != 0)
        {
            var j = BitOperations.TrailingZeroCount(mask);
            mask &= mask - 1;
            var c = --_counts[baseIdx + j];
            if (c == 0)
            {
                Distinct[j]--;
            }
            else if (c < 0)
            {
                throw new InvalidOperationException("COUNT(DISTINCT) state lost a value it never counted.");
            }
        }

        for (var j = 0; j < _k; j++)
        {
            if (_counts[baseIdx + j] != 0)
            {
                return;
            }
        }

        RemoveAt(slot);
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

    // Backward-shift deletion: pull later members of the probe run into the hole
    // whenever the hole lies cyclically between their ideal slot and their slot.
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
            // Can the entry at i move to the hole? Only if its ideal slot is not
            // cyclically in (hole, i].
            var distHole = (hole - ideal) & mask;
            var distI = (i - ideal) & mask;
            if (distHole < distI)
            {
                _hashes[hole] = h;
                _keys[hole] = _keys[i];
                Array.Copy(_counts, i * _k, _counts, hole * _k, _k);
                hole = i;
            }
        }

        _hashes[hole] = 0;
        _keys[hole] = default;
        Array.Clear(_counts, hole * _k, _k);
        _count--;
    }

    private void Grow()
    {
        var oldHashes = _hashes;
        var oldKeys = _keys;
        var oldCounts = _counts;
        var cap = oldHashes.Length * 2;
        _hashes = new int[cap];
        _keys = new T[cap];
        _counts = new int[cap * _k];
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
            Array.Copy(oldCounts, s * _k, _counts, d * _k, _k);
        }
    }
}

/// <summary>
/// One or more <c>COUNT(DISTINCT x)</c> aggregates over the same <b>unboxed</b>
/// base argument <c>x</c>, each optionally filtered — the lowered form of
/// <c>COUNT(DISTINCT x) FILTER (WHERE p)</c> is <c>COUNT(DISTINCT CASE WHEN p
/// THEN x END)</c>. All members share one <see cref="DistinctValueTable{T}"/>
/// per group (one key, k counters), the base is extracted once per delta row,
/// the post-delta weight is probed once per row (and only when some member's
/// predicate holds), and no value is boxed. Same per-row positive-weight
/// transition semantics as <see cref="TypedCountDistinctAggregator{TIn}"/>, so
/// incremental ≡ batch exactly.
/// </summary>
internal sealed class TypedSharedCountDistinctAggregator<TIn, T> : TypedSqlAggregator<TIn>
    where TIn : notnull
    where T : struct
{
    private readonly Func<TIn, T?> _base;
    private readonly Func<TIn, bool>?[] _predicates;   // null = unfiltered member
    private readonly ulong _allMask;

    public TypedSharedCountDistinctAggregator(Func<TIn, T?> baseExtract, Func<TIn, bool>?[] predicates)
    {
        if (predicates.Length is 0 or > 64)
        {
            throw new ArgumentOutOfRangeException(nameof(predicates));
        }

        _base = baseExtract;
        _predicates = predicates;
        _allMask = predicates.Length == 64 ? ulong.MaxValue : (1UL << predicates.Length) - 1;
    }

    public override Type ResultClrType => typeof(long);

    public override int ResultCount => _predicates.Length;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ulong MaskFor(TIn row)
    {
        var mask = 0UL;
        for (var j = 0; j < _predicates.Length; j++)
        {
            var p = _predicates[j];
            if (p is null || p(row))
            {
                mask |= 1UL << j;
            }
        }

        return mask;
    }

    public override object? Compute(IMultiset<TIn, Z64> rows)
    {
        var results = new object?[_predicates.Length];
        var slots = new int[_predicates.Length];
        for (var j = 0; j < slots.Length; j++) slots[j] = j;
        ComputeInto(rows, results, slots);
        return results[0];
    }

    public override void ComputeInto(IMultiset<TIn, Z64> rows, object?[] results, int[] slots)
    {
        var table = new DistinctValueTable<T>(_predicates.Length);
        foreach (var (row, w) in rows)
        {
            if (!Z64.IsPositive(w)) continue;
            var v = _base(row);
            if (!v.HasValue) continue;
            var mask = MaskFor(row);
            if (mask != 0) table.Insert(v.GetValueOrDefault(), mask);
        }

        Publish(table, results, slots);
    }

    public override object? Update(ref object? state, ZSet<TIn, Z64> delta, IMultiset<TIn, Z64> after)
    {
        var results = new object?[_predicates.Length];
        var slots = new int[_predicates.Length];
        for (var j = 0; j < slots.Length; j++) slots[j] = j;
        UpdateInto(ref state, delta, after, results, slots);
        return results[0];
    }

    public override void UpdateInto(
        ref object? state, ZSet<TIn, Z64> delta, IMultiset<TIn, Z64> after, object?[] results, int[] slots)
    {
        var table = state as DistinctValueTable<T> ?? new DistinctValueTable<T>(_predicates.Length);
        foreach (var (row, w) in delta)
        {
            var v = _base(row);
            if (!v.HasValue) continue;
            var mask = MaskFor(row);
            if (mask == 0) continue;
            var afterW = after.WeightOf(row).Value;
            var wasPositive = afterW - w.Value > 0;
            var isPositive = afterW > 0;
            if (wasPositive == isPositive) continue;
            if (isPositive) table.Insert(v.GetValueOrDefault(), mask);
            else table.Remove(v.GetValueOrDefault(), mask);
        }

        state = table;
        Publish(table, results, slots);
    }

    private static void Publish(DistinctValueTable<T> table, object?[] results, int[] slots)
    {
        var d = table.Distinct;
        for (var j = 0; j < slots.Length; j++)
        {
            results[slots[j]] = d[j];
        }
    }
}

/// <summary>
/// Typed <c>MIN</c>/<c>MAX</c> whose per-group state is only the current best
/// value and how many positive-weight rows hold it — not a second copy of the
/// group (<c>Dictionary</c> + <c>SortedSet</c>) next to the operator's trace.
/// Insertions fold in O(1); a retraction that removes the last row holding the
/// best value (or any inconsistency) triggers one rescan of the post-delta
/// multiset at the end of the tick, which reads the trace the operator already
/// keeps. Exact: the state invariant is "best = extreme non-NULL value over
/// positive-weight rows of the group, count = rows holding it", re-established
/// from the full post-delta multiset whenever the incremental fold can't.
/// </summary>
/// <remarks>
/// Cost model differs from the sorted-set variant: retraction-heavy workloads
/// that repeatedly retract the current extreme (sliding windows) pay O(group)
/// per such tick instead of O(log n) per row. Switch off with
/// <see cref="CompactAggregatorStateMode.CompactMinMax"/>.
/// </remarks>
internal sealed class TypedSqlMinMaxCompactAggregator<TIn, T> : TypedSqlAggregator<TIn>
    where TIn : notnull
    where T : struct, IComparable<T>
{
    private readonly Func<TIn, T?> _argExtract;
    private readonly bool _wantMin;

    public TypedSqlMinMaxCompactAggregator(Func<TIn, T?> argExtract, bool wantMin)
    {
        _argExtract = argExtract;
        _wantMin = wantMin;
    }

    private sealed class State
    {
        public T Best;
        public long Count;   // positive-weight rows holding Best; 0 = no best
    }

    public override Type ResultClrType => typeof(T);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool Better(T v, T best)
    {
        var cmp = v.CompareTo(best);
        return _wantMin ? cmp < 0 : cmp > 0;
    }

    public override object? Compute(IMultiset<TIn, Z64> rows)
    {
        var s = new State();
        Rescan(s, rows);
        return s.Count > 0 ? s.Best : null;
    }

    private void Rescan(State s, IMultiset<TIn, Z64> rows)
    {
        s.Count = 0;
        s.Best = default;
        foreach (var (row, w) in rows)
        {
            if (!Z64.IsPositive(w)) continue;
            var v = _argExtract(row);
            if (!v.HasValue) continue;
            var raw = v.GetValueOrDefault();
            if (s.Count == 0 || Better(raw, s.Best))
            {
                s.Best = raw;
                s.Count = 1;
            }
            else if (raw.CompareTo(s.Best) == 0)
            {
                s.Count++;
            }
        }
    }

    public override object? Update(ref object? state, ZSet<TIn, Z64> delta, IMultiset<TIn, Z64> after)
    {
        var s = state as State ?? new State();
        var dirty = false;
        foreach (var (row, w) in delta)
        {
            var v = _argExtract(row);
            if (!v.HasValue) continue;
            var afterW = after.WeightOf(row).Value;
            var wasPositive = afterW - w.Value > 0;
            var isPositive = afterW > 0;
            if (wasPositive == isPositive) continue;

            var raw = v.GetValueOrDefault();
            if (isPositive)
            {
                if (s.Count == 0 || Better(raw, s.Best))
                {
                    s.Best = raw;
                    s.Count = 1;
                }
                else if (raw.CompareTo(s.Best) == 0)
                {
                    s.Count++;
                }
            }
            else
            {
                if (s.Count == 0 || Better(raw, s.Best))
                {
                    dirty = true;   // inconsistent with the state: recover from the trace
                    break;
                }

                if (raw.CompareTo(s.Best) == 0 && --s.Count == 0)
                {
                    dirty = true;   // lost the extreme: the next one is only in the trace
                    break;
                }
            }
        }

        if (dirty)
        {
            Rescan(s, after);
        }

        state = s;
        return s.Count > 0 ? s.Best : null;
    }
}
