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

    // Null = the default (see MinMaxDefault).
    [ThreadStatic]
    private static MinMaxStateKind? _minMax;

    /// <summary>The default MIN/MAX state when nothing has been set on this thread.</summary>
    internal const MinMaxStateKind MinMaxDefault = MinMaxStateKind.Hybrid;

    /// <summary>Unboxed, filter-shared COUNT(DISTINCT) state.</summary>
    internal static bool CompactDistinct
    {
        get => !_distinctDisabled;
        set => _distinctDisabled = !value;
    }

    /// <summary>Which per-group MIN/MAX state the typed compiler builds.</summary>
    internal static MinMaxStateKind MinMax
    {
        get => _minMax ?? MinMaxDefault;
        set => _minMax = value;
    }
}

/// <summary>Per-group state of a typed MIN/MAX (design-state-representation §4B).</summary>
internal enum MinMaxStateKind
{
    /// <summary><c>Dictionary&lt;T,long&gt;</c> + <c>SortedSet&lt;T&gt;</c> per group: O(log n) per change.</summary>
    Legacy,

    /// <summary>(best, count) only; rescans the group every time the best is lost.
    /// 5-25x slower than legacy when a sliding window retracts the extreme every tick (§4B.1).</summary>
    RescanOnly,

    /// <summary>(best, count) until the group's best is first retracted, then one rescan
    /// promotes the group to a compact sorted (value, count) run (§4B.2).</summary>
    Hybrid,
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
/// Sorted multiset of <b>unboxed</b> values with a row count per value: the
/// promoted per-group state of the hybrid MIN/MAX
/// (<see cref="TypedSqlMinMaxCompactAggregator{TIn,T}"/>). A chunked sorted
/// array — a short array of chunks, each a sorted run of up to
/// <see cref="ChunkMax"/> inline (value, count) entries — so an insert or
/// remove is a binary search plus a memmove inside one chunk, and the
/// minimum / maximum are read off the ends in O(1). Replaces the legacy
/// <c>Dictionary&lt;T,long&gt;</c> + <c>SortedSet&lt;T&gt;</c> pair (an entry
/// plus a tree-node object per distinct value) with ~<c>sizeof(T)+8</c> bytes
/// per distinct value. Values are keyed by <see cref="IComparable{T}.CompareTo"/>,
/// so values that compare equal (Decimal128 1.0 vs 1.00) share one entry, which
/// keeps the first representative seen — the same as the (best, count) state.
/// </summary>
internal sealed class SortedValueCounts<T>
    where T : struct, IComparable<T>
{
    /// <summary>Largest chunk; a full chunk splits in two.</summary>
    internal const int ChunkMax = 128;

    private const int MinCapacity = 4;

    // A chunk that falls below this merges with a neighbour when they fit.
    private const int MergeBelow = ChunkMax / 4;

    private Chunk[] _chunks;
    private int _chunkCount;

    private struct Entry
    {
        public T Value;
        public long Count;
    }

    private sealed class Chunk
    {
        public Entry[] Entries;
        public int Size;

        public Chunk(int capacity) => Entries = new Entry[capacity];

        public T Last => Entries[Size - 1].Value;
    }

    public SortedValueCounts()
    {
        _chunks = [new Chunk(MinCapacity)];
        _chunkCount = 1;
    }

    /// <summary>Number of distinct values.</summary>
    public int DistinctCount { get; private set; }

    public bool IsEmpty => DistinctCount == 0;

    public T Min => _chunks[0].Entries[0].Value;

    public T Max
    {
        get
        {
            var c = _chunks[_chunkCount - 1];
            return c.Entries[c.Size - 1].Value;
        }
    }

    /// <summary>Builds from the first <paramref name="length"/> <paramref name="values"/>, sorting them in place.</summary>
    public static SortedValueCounts<T> FromUnsorted(T[] values, int length)
    {
        var result = new SortedValueCounts<T>();
        if (length == 0)
        {
            return result;
        }

        Array.Sort(values, 0, length, ValueComparer.Instance);

        // Fill chunks to 3/4 so the next few inserts don't split immediately.
        const int fill = ChunkMax * 3 / 4;
        var chunks = new List<Chunk>();
        Chunk? cur = null;
        for (var i = 0; i < length; i++)
        {
            var v = values[i];
            if (cur is not null && cur.Entries[cur.Size - 1].Value.CompareTo(v) == 0)
            {
                cur.Entries[cur.Size - 1].Count++;
                continue;
            }

            if (cur is null || cur.Size == fill)
            {
                var remaining = length - i;   // upper bound on the distinct values left
                cur = new Chunk(Math.Max(MinCapacity, Math.Min(ChunkMax, remaining)));
                chunks.Add(cur);
            }
            else if (cur.Size == cur.Entries.Length)
            {
                Array.Resize(ref cur.Entries, Math.Min(ChunkMax, cur.Entries.Length * 2));
            }

            cur.Entries[cur.Size++] = new Entry { Value = v, Count = 1 };
            result.DistinctCount++;
        }

        result._chunks = chunks.ToArray();
        result._chunkCount = chunks.Count;
        return result;
    }

    /// <summary>Approximate retained bytes (objects and arrays), for measurement.</summary>
    public long ApproximateBytes()
    {
        var entry = Unsafe.SizeOf<Entry>();
        long bytes = 32 + 24 + (8L * _chunks.Length);   // this + chunk array
        for (var i = 0; i < _chunkCount; i++)
        {
            bytes += 24 + 24 + ((long)entry * _chunks[i].Entries.Length);
        }

        return bytes;
    }

    // First chunk whose last value is >= v; the last chunk if v is above everything.
    private int ChunkFor(T v)
    {
        int lo = 0, hi = _chunkCount - 1;
        while (lo < hi)
        {
            var mid = (lo + hi) >>> 1;
            if (_chunks[mid].Last.CompareTo(v) < 0)
            {
                lo = mid + 1;
            }
            else
            {
                hi = mid;
            }
        }

        return lo;
    }

    // Index of v in the chunk, or ~insertionPoint.
    private static int Search(Chunk c, T v)
    {
        int lo = 0, hi = c.Size - 1;
        var e = c.Entries;
        while (lo <= hi)
        {
            var mid = (lo + hi) >>> 1;
            var cmp = e[mid].Value.CompareTo(v);
            if (cmp == 0)
            {
                return mid;
            }

            if (cmp < 0)
            {
                lo = mid + 1;
            }
            else
            {
                hi = mid - 1;
            }
        }

        return ~lo;
    }

    /// <summary>One more positive row holds <paramref name="v"/>.</summary>
    public void Add(T v)
    {
        var ci = _chunkCount == 1 ? 0 : ChunkFor(v);
        var c = _chunks[ci];
        var idx = c.Size == 0 ? ~0 : Search(c, v);
        if (idx >= 0)
        {
            c.Entries[idx].Count++;
            return;
        }

        idx = ~idx;
        if (c.Size == c.Entries.Length)
        {
            if (c.Entries.Length < ChunkMax)
            {
                Array.Resize(ref c.Entries, Math.Min(ChunkMax, c.Entries.Length * 2));
            }
            else if (idx == c.Size)
            {
                // Appending past the top (a sliding window's new values) — ChunkFor
                // only lands past a chunk's end on the last chunk: start a new
                // chunk instead of splitting, so the full one stays full.
                var top = new Chunk(MinCapacity);
                top.Entries[0] = new Entry { Value = v, Count = 1 };
                top.Size = 1;
                InsertChunk(_chunkCount, top);
                DistinctCount++;
                return;
            }
            else if (idx == 0 && ci == 0)
            {
                // Prepending below the bottom: likewise.
                var bottom = new Chunk(MinCapacity);
                bottom.Entries[0] = new Entry { Value = v, Count = 1 };
                bottom.Size = 1;
                InsertChunk(0, bottom);
                DistinctCount++;
                return;
            }
            else
            {
                // Split the full chunk in half and insert into the half that owns idx.
                var half = c.Size / 2;
                var right = new Chunk(ChunkMax);
                Array.Copy(c.Entries, half, right.Entries, 0, c.Size - half);
                right.Size = c.Size - half;
                Array.Clear(c.Entries, half, c.Size - half);
                c.Size = half;
                InsertChunk(ci + 1, right);
                if (idx > half)
                {
                    c = right;
                    idx -= half;
                }
            }
        }

        if (idx < c.Size)
        {
            Array.Copy(c.Entries, idx, c.Entries, idx + 1, c.Size - idx);
        }

        c.Entries[idx] = new Entry { Value = v, Count = 1 };
        c.Size++;
        DistinctCount++;
    }

    /// <summary>
    /// One positive row holding <paramref name="v"/> went away. Returns false —
    /// and changes nothing — when no row held it (inconsistent with the caller).
    /// </summary>
    public bool Remove(T v)
    {
        if (DistinctCount == 0)
        {
            return false;
        }

        var ci = _chunkCount == 1 ? 0 : ChunkFor(v);
        var c = _chunks[ci];
        var idx = Search(c, v);
        if (idx < 0)
        {
            return false;
        }

        if (--c.Entries[idx].Count > 0)
        {
            return true;
        }

        c.Size--;
        if (idx < c.Size)
        {
            Array.Copy(c.Entries, idx + 1, c.Entries, idx, c.Size - idx);
        }

        c.Entries[c.Size] = default;
        DistinctCount--;
        if (_chunkCount > 1)
        {
            if (c.Size == 0)
            {
                RemoveChunk(ci);
            }
            else if (c.Size < MergeBelow)
            {
                MergeSmall(ci);
            }
        }

        return true;
    }

    // Merge chunk ci into its smaller neighbour when the two fit in half a chunk,
    // so random deletes can't leave many near-empty chunks behind.
    private void MergeSmall(int ci)
    {
        var left = ci > 0 ? ci - 1 : -1;
        var right = ci + 1 < _chunkCount ? ci + 1 : -1;
        var other = left < 0 ? right
            : right < 0 ? left
            : _chunks[left].Size <= _chunks[right].Size ? left : right;
        if (other < 0)
        {
            return;
        }

        var (a, b) = other < ci ? (other, ci) : (ci, other);
        var ca = _chunks[a];
        var cb = _chunks[b];
        if (ca.Size + cb.Size > ChunkMax / 2)
        {
            return;
        }

        if (ca.Entries.Length < ca.Size + cb.Size)
        {
            Array.Resize(ref ca.Entries, ChunkMax / 2);
        }

        Array.Copy(cb.Entries, 0, ca.Entries, ca.Size, cb.Size);
        ca.Size += cb.Size;
        RemoveChunk(b);
    }

    private void InsertChunk(int at, Chunk c)
    {
        if (_chunkCount == _chunks.Length)
        {
            Array.Resize(ref _chunks, _chunks.Length * 2);
        }

        Array.Copy(_chunks, at, _chunks, at + 1, _chunkCount - at);
        _chunks[at] = c;
        _chunkCount++;
    }

    private void RemoveChunk(int at)
    {
        _chunkCount--;
        Array.Copy(_chunks, at + 1, _chunks, at, _chunkCount - at);
        _chunks[_chunkCount] = null!;
    }

    /// <summary>Every (value, count) in ascending order — for tests.</summary>
    internal IEnumerable<(T Value, long Count)> Entries()
    {
        for (var i = 0; i < _chunkCount; i++)
        {
            var c = _chunks[i];
            for (var j = 0; j < c.Size; j++)
            {
                yield return (c.Entries[j].Value, c.Entries[j].Count);
            }
        }
    }

    private sealed class ValueComparer : IComparer<T>
    {
        public static readonly ValueComparer Instance = new();

        public int Compare(T x, T y) => x.CompareTo(y);
    }
}

/// <summary>
/// Typed <c>MIN</c>/<c>MAX</c> whose per-group state starts as only the current
/// best value and how many positive-weight rows hold it — not a second copy of
/// the group (<c>Dictionary</c> + <c>SortedSet</c>) next to the operator's
/// trace. Insertions fold in O(1). A retraction that removes the last row
/// holding the best value (or any inconsistency) needs the next-best value,
/// which only the trace has, so the aggregator rescans the post-delta multiset
/// once at the end of the tick.
/// </summary>
/// <remarks>
/// <para><b>Rescan-only</b> (<c>promote = false</c>, §4B.1): every loss of the
/// best rescans the group — O(group) per such tick, 5-25x slower than the
/// sorted state when a sliding window retracts the extreme every tick.</para>
/// <para><b>Hybrid</b> (<c>promote = true</c>, §4B.2): the first loss of the best
/// rescans once into a <see cref="SortedValueCounts{T}"/> and the group stays
/// promoted — O(log n) per change from then on. Append-only groups never pay for
/// the sorted state. A promoted group demotes only when it becomes empty: losing
/// the extreme once is the evidence that it will be lost again, and a wrong
/// demotion costs another O(group) rescan.</para>
/// <para>State invariant (both modes): the extreme non-NULL value over the
/// group's positive-weight rows and the rows holding it (compact), or every
/// such value with its row count (promoted); re-established from the full
/// post-delta multiset whenever the incremental fold can't. Restore folds a
/// fresh state from the trace, so a restored group starts compact again.</para>
/// </remarks>
internal sealed class TypedSqlMinMaxCompactAggregator<TIn, T> : TypedSqlAggregator<TIn>
    where TIn : notnull
    where T : struct, IComparable<T>
{
    private readonly Func<TIn, T?> _argExtract;
    private readonly bool _wantMin;
    private readonly bool _promote;

    public TypedSqlMinMaxCompactAggregator(Func<TIn, T?> argExtract, bool wantMin, bool promote)
    {
        _argExtract = argExtract;
        _wantMin = wantMin;
        _promote = promote;
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

    // One entry per positive-weight row with a non-NULL value (the same row-count
    // semantics as the transition fold).
    private SortedValueCounts<T> BuildSorted(IMultiset<TIn, Z64> rows)
    {
        // Per-thread scratch: a promotion is one-time per group but a sliding
        // workload promotes every group, so don't regrow a fresh array each time.
        var values = s_scratch ?? new T[64];
        var n = 0;
        foreach (var (row, w) in rows)
        {
            if (!Z64.IsPositive(w)) continue;
            var v = _argExtract(row);
            if (!v.HasValue) continue;
            if (n == values.Length) Array.Resize(ref values, n * 2);
            values[n++] = v.GetValueOrDefault();
        }

        var result = SortedValueCounts<T>.FromUnsorted(values, n);
        if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
        {
            Array.Clear(values, 0, n);   // don't keep (e.g. string-backed) values alive
        }

        s_scratch = values.Length <= ScratchKeepMax ? values : null;
        return result;
    }

    // Largest scratch kept between promotions (elements).
    private const int ScratchKeepMax = 1 << 16;

    [ThreadStatic]
    private static T[]? s_scratch;

    private object? Emit(SortedValueCounts<T> sorted)
        => sorted.IsEmpty ? null : (_wantMin ? sorted.Min : sorted.Max);

    public override object? Update(ref object? state, ZSet<TIn, Z64> delta, IMultiset<TIn, Z64> after)
    {
        if (state is SortedValueCounts<T> sorted)
        {
            return UpdatePromoted(ref state, sorted, delta, after);
        }

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
            if (_promote)
            {
                // One rescan, into the sorted state the group keeps from now on.
                var promoted = BuildSorted(after);
                state = promoted.IsEmpty ? null : promoted;
                return Emit(promoted);
            }

            Rescan(s, after);
        }

        state = s;
        return s.Count > 0 ? s.Best : null;
    }

    private object? UpdatePromoted(
        ref object? state, SortedValueCounts<T> sorted, ZSet<TIn, Z64> delta, IMultiset<TIn, Z64> after)
    {
        foreach (var (row, w) in delta)
        {
            var v = _argExtract(row);
            if (!v.HasValue) continue;
            var afterW = after.WeightOf(row).Value;
            var wasPositive = afterW - w.Value > 0;
            var isPositive = afterW > 0;
            if (wasPositive == isPositive) continue;

            if (isPositive)
            {
                sorted.Add(v.GetValueOrDefault());
            }
            else if (!sorted.Remove(v.GetValueOrDefault()))
            {
                // Inconsistent with the state: rebuild from the trace.
                sorted = BuildSorted(after);
                break;
            }
        }

        // An emptied group demotes: its next row starts a fresh (best, count).
        state = sorted.IsEmpty ? null : sorted;
        return Emit(sorted);
    }
}
