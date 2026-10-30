// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace DbspNet.Core.Collections;

/// <summary>
/// Whether Z-sets are backed by <see cref="ChunkedMap{TKey,TValue}"/> instead of
/// <see cref="Dictionary{TKey,TValue}"/> (docs/design-chunked-deltas.md). Default off;
/// <c>DBSPNET_ZSET_MAP=chunked</c> selects it for the process.
/// </summary>
internal static class ZSetMapMode
{
    internal static readonly bool Chunked = string.Equals(
        Environment.GetEnvironmentVariable("DBSPNET_ZSET_MAP")?.Trim(), "chunked", StringComparison.OrdinalIgnoreCase);
}

/// <summary>
/// <see cref="Dictionary{TKey,TValue}"/>'s algorithm — entries chained from a bucket index, a
/// LIFO free list — with both arrays split into chunks that never reach the 85,000-byte
/// large-object threshold (docs/design-chunked-deltas.md). Every array is born young, so a
/// per-tick delta that dies within its tick is never card-scanned on the large-object heap, and
/// growth past one chunk appends a chunk instead of copying the entries.
/// </summary>
/// <remarks>
/// <para>Enumeration order is exactly <see cref="Dictionary{TKey,TValue}"/>'s for the same
/// sequence of operations: a new entry takes the free-list head if there is one, else the next
/// position; <see cref="Remove"/> pushes onto the free list; growth never moves an entry. That is
/// what keeps emission order, view digests and snapshot files identical across the seam.</para>
/// <para>Below one chunk it is three objects (the map, one entry array, one bucket array) grown
/// by doubling, like a <see cref="Dictionary{TKey,TValue}"/>; the chunk directories appear only
/// past one chunk. Capacities are powers of two, so allocation sizes differ slightly.</para>
/// <para>Not thread-safe; no version checks on enumeration (callers do not mutate while
/// enumerating).</para>
/// </remarks>
internal sealed class ChunkedMap<TKey, TValue> : IEnumerable<KeyValuePair<TKey, TValue>>
    where TKey : notnull
{
    private const int StartOfFreeList = -3;
    private const int BucketShift = 14;              // 16,384 ints = 64 KB per bucket chunk
    private const int BucketChunk = 1 << BucketShift;
    private const int LohBytes = 85_000;

    // Largest power of two whose entry array stays under the large-object threshold.
    private static readonly int EntryShift = ComputeEntryShift();
    private static readonly int EntryChunk = 1 << EntryShift;

    private Entry[] _entries0 = [];
    private Entry[][]? _entryChunks;   // non-null once past one chunk; [0] is _entries0
    private int _entryCapacity;        // positions available without growing
    private int[] _buckets0 = [];
    private int[][]? _bucketChunks;    // non-null once the index is past one chunk
    private int _bucketCount;          // power of two; 0 = not yet initialised
    private int _bucketShiftDown;      // 32 − log2(_bucketCount)
    private int _count;                // positions used, including free ones
    private int _freeList = -1;
    private int _freeCount;

    public ChunkedMap()
    {
    }

    public ChunkedMap(int capacity)
    {
        if (capacity > 0)
        {
            Initialize(capacity);
        }
    }

    private struct Entry
    {
        public uint HashCode;
        public int Next;       // chain link (−1 = end); StartOfFreeList − next-free when free
        public TKey Key;
        public TValue Value;
    }

    public int Count => _count - _freeCount;

    /// <summary>Entries the map holds without growing (for size reports).</summary>
    public int Capacity => _entryCapacity;

    /// <summary>Bytes in the entry and bucket arrays (for size reports).</summary>
    internal long ArrayBytes => ((long)_entryCapacity * Unsafe.SizeOf<Entry>()) + ((long)_bucketCount * sizeof(int));

    /// <summary>The largest single array, in bytes — stays under 85,000 by construction.</summary>
    internal long LargestArrayBytes =>
        Math.Max((long)Math.Min(_entryCapacity, EntryChunk) * Unsafe.SizeOf<Entry>(), (long)Math.Min(_bucketCount, BucketChunk) * sizeof(int));

    internal static int EntriesPerChunk => EntryChunk;

    public IEnumerable<TKey> Keys
    {
        get
        {
            for (var i = 0; i < _count; i++)
            {
                if (EntryAt(i).Next >= -1)
                {
                    yield return EntryAt(i).Key;
                }
            }
        }
    }

    public TValue this[TKey key]
    {
        set => Insert(key, value);
    }

    /// <summary>A copy holding the live entries compactly, in order — what
    /// <c>new Dictionary(dictionary)</c> produces.</summary>
    public static ChunkedMap<TKey, TValue> CopyOf(ChunkedMap<TKey, TValue> source)
    {
        ArgumentNullException.ThrowIfNull(source);
        var copy = new ChunkedMap<TKey, TValue>(source.Count);
        for (var i = 0; i < source._count; i++)
        {
            ref var e = ref source.EntryAt(i);
            if (e.Next >= -1)
            {
                copy.Insert(e.Key, e.Value);
            }
        }

        return copy;
    }

    public bool TryGetValue(TKey key, out TValue value)
    {
        var i = Find(key);
        if (i >= 0)
        {
            value = EntryAt(i).Value;
            return true;
        }

        value = default!;
        return false;
    }

    public bool ContainsKey(TKey key) => Find(key) >= 0;

    public bool Remove(TKey key)
    {
        if (_bucketCount == 0)
        {
            return false;
        }

        var hash = (uint)EqualityComparer<TKey>.Default.GetHashCode(key);
        ref var bucket = ref BucketAt(BucketOf(hash));
        var previous = -1;
        for (var i = bucket - 1; i >= 0;)
        {
            ref var e = ref EntryAt(i);
            if (e.HashCode == hash && EqualityComparer<TKey>.Default.Equals(e.Key, key))
            {
                if (previous < 0)
                {
                    bucket = e.Next + 1;
                }
                else
                {
                    EntryAt(previous).Next = e.Next;
                }

                e.Next = StartOfFreeList - _freeList;
                if (RuntimeHelpers.IsReferenceOrContainsReferences<TKey>())
                {
                    e.Key = default!;
                }

                if (RuntimeHelpers.IsReferenceOrContainsReferences<TValue>())
                {
                    e.Value = default!;
                }

                _freeList = i;
                _freeCount++;
                return true;
            }

            previous = i;
            i = e.Next;
        }

        return false;
    }

    /// <summary>Empties the map, keeping its capacity (as <c>Dictionary.Clear</c>).</summary>
    public void Clear()
    {
        if (_count == 0)
        {
            return;
        }

        if (_bucketChunks is null)
        {
            Array.Clear(_buckets0);
        }
        else
        {
            foreach (var chunk in _bucketChunks)
            {
                Array.Clear(chunk);
            }
        }

        if (_entryChunks is null)
        {
            Array.Clear(_entries0, 0, _count);
        }
        else
        {
            foreach (var chunk in _entryChunks)
            {
                if (chunk is not null)
                {
                    Array.Clear(chunk);
                }
            }
        }

        _count = 0;
        _freeList = -1;
        _freeCount = 0;
    }

    public Enumerator GetEnumerator() => new(this);

    IEnumerator<KeyValuePair<TKey, TValue>> IEnumerable<KeyValuePair<TKey, TValue>>.GetEnumerator() => GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    private static int ComputeEntryShift()
    {
        var size = Unsafe.SizeOf<Entry>();
        var shift = 4;
        while (((2L << shift) * size) + 32 < LohBytes)
        {
            shift++;
        }

        return shift;
    }

    private static int RoundUpPow2(int n) => n <= 4 ? 4 : (int)System.Numerics.BitOperations.RoundUpToPowerOf2((uint)n);

    private void Initialize(int capacity)
    {
        var size = RoundUpPow2(capacity);
        _entries0 = new Entry[Math.Min(size, EntryChunk)];
        _entryCapacity = _entries0.Length;
        AllocateBuckets(size * 2);
    }

    private void AllocateBuckets(int size)
    {
        _bucketCount = size;
        _bucketShiftDown = 32 - System.Numerics.BitOperations.Log2((uint)size);
        if (size <= BucketChunk)
        {
            _buckets0 = new int[size];
            _bucketChunks = null;
        }
        else
        {
            _bucketChunks = new int[size >> BucketShift][];
            for (var c = 0; c < _bucketChunks.Length; c++)
            {
                _bucketChunks[c] = new int[BucketChunk];
            }

            _buckets0 = [];
        }
    }

    private int BucketOf(uint hash) => (int)((hash * 0x9E3779B9u) >> _bucketShiftDown);

    // Unchecked two-level access: every index reaching these is below _bucketCount / _count,
    // which the arrays cover by construction, so the bounds checks are dead weight on the
    // probe path (they made a 1M-entry lookup 1.4× a Dictionary's).
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ref int BucketAt(int b)
    {
        if (_bucketChunks is null)
        {
            return ref Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(_buckets0), b);
        }

        var chunk = Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(_bucketChunks), b >> BucketShift);
        return ref Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(chunk), b & (BucketChunk - 1));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ref Entry EntryAt(int i)
    {
        if (_entryChunks is null)
        {
            return ref Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(_entries0), i);
        }

        var chunk = Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(_entryChunks), i >> EntryShift);
        return ref Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(chunk), i & (EntryChunk - 1));
    }

    private int Find(TKey key)
    {
        if (_bucketCount == 0)
        {
            return -1;
        }

        var hash = (uint)EqualityComparer<TKey>.Default.GetHashCode(key);
        for (var i = BucketAt(BucketOf(hash)) - 1; i >= 0;)
        {
            ref var e = ref EntryAt(i);
            if (e.HashCode == hash && EqualityComparer<TKey>.Default.Equals(e.Key, key))
            {
                return i;
            }

            i = e.Next;
        }

        return -1;
    }

    // Set-or-add, as the Dictionary indexer setter.
    private void Insert(TKey key, TValue value)
    {
        if (_bucketCount == 0)
        {
            Initialize(0);
        }

        var hash = (uint)EqualityComparer<TKey>.Default.GetHashCode(key);
        ref var bucket = ref BucketAt(BucketOf(hash));
        for (var i = bucket - 1; i >= 0;)
        {
            ref var e = ref EntryAt(i);
            if (e.HashCode == hash && EqualityComparer<TKey>.Default.Equals(e.Key, key))
            {
                e.Value = value;
                return;
            }

            i = e.Next;
        }

        int index;
        if (_freeCount > 0)
        {
            index = _freeList;
            _freeList = StartOfFreeList - EntryAt(index).Next;
            _freeCount--;
        }
        else
        {
            if (_count == _entryCapacity)
            {
                GrowEntries();
                bucket = ref BucketAt(BucketOf(hash));
            }

            index = _count;
            _count++;
        }

        ref var fresh = ref EntryAt(index);
        fresh.HashCode = hash;
        fresh.Next = bucket - 1;
        fresh.Key = key;
        fresh.Value = value;
        bucket = index + 1;

        if (_count > _bucketCount >> 1)
        {
            Rehash(_bucketCount * 2);
        }
    }

    private void GrowEntries()
    {
        if (_entryChunks is null && _entries0.Length < EntryChunk)
        {
            // Still one array: double it, as Dictionary does.
            Array.Resize(ref _entries0, Math.Min(EntryChunk, _entries0.Length * 2));
            _entryCapacity = _entries0.Length;
            return;
        }

        // Past one chunk: append a chunk; nothing moves.
        _entryChunks ??= [_entries0];
        var c = _entryCapacity >> EntryShift;
        if (c == _entryChunks.Length)
        {
            Array.Resize(ref _entryChunks, _entryChunks.Length * 2);
        }

        _entryChunks[c] = new Entry[EntryChunk];
        _entryCapacity += EntryChunk;
    }

    private void Rehash(int size)
    {
        AllocateBuckets(size);
        for (var i = 0; i < _count; i++)
        {
            ref var e = ref EntryAt(i);
            if (e.Next >= -1)
            {
                ref var bucket = ref BucketAt(BucketOf(e.HashCode));
                e.Next = bucket - 1;
                bucket = i + 1;
            }
        }
    }

    /// <summary>Enumerates live entries in position order, as Dictionary's enumerator does.</summary>
    public struct Enumerator : IEnumerator<KeyValuePair<TKey, TValue>>
    {
        // Walks one chunk at a time: the chunk in hand, the position in it, and how many of its
        // positions are in use.
        private readonly ChunkedMap<TKey, TValue> _map;
        private Entry[] _chunk;
        private int _chunkIndex;
        private int _offset;
        private int _limit;
        private KeyValuePair<TKey, TValue> _current;

        internal Enumerator(ChunkedMap<TKey, TValue> map)
        {
            _map = map;
            _chunkIndex = 0;
            _offset = 0;
            _chunk = map._entryChunks is null ? map._entries0 : map._entryChunks[0];
            _limit = Math.Min(map._count, _chunk.Length);
            _current = default;
        }

        public readonly KeyValuePair<TKey, TValue> Current => _current;

        readonly object IEnumerator.Current => _current;

        public bool MoveNext()
        {
            while (true)
            {
                while (_offset < _limit)
                {
                    ref var e = ref _chunk[_offset++];
                    if (e.Next >= -1)
                    {
                        _current = new(e.Key, e.Value);
                        return true;
                    }
                }

                var start = (_chunkIndex + 1) << EntryShift;
                if (_map._entryChunks is null || start >= _map._count)
                {
                    _current = default;
                    return false;
                }

                _chunkIndex++;
                _chunk = _map._entryChunks[_chunkIndex];
                _offset = 0;
                _limit = Math.Min(_map._count - start, EntryChunk);
            }
        }

        public readonly void Dispose()
        {
        }

        void IEnumerator.Reset() => throw new NotSupportedException();
    }
}
