// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Clast.DatabaseDecimal.Values;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Sql.Compiler;

/// <summary>
/// Whether operator state that supports it keeps rows as values in reference-free pages
/// (<see cref="PagedZSetStore"/>) instead of as <see cref="StructuralRow"/> objects
/// (docs/design-paged-trace-state.md). Default off. <c>DBSPNET_TRACE_STORE=paged</c> or the
/// thread-static <see cref="Paged"/> selects it. Read at compile time.
/// </summary>
internal static class TraceStoreMode
{
    private static readonly bool EnvironmentPaged = string.Equals(
        Environment.GetEnvironmentVariable("DBSPNET_TRACE_STORE")?.Trim(), "paged", StringComparison.OrdinalIgnoreCase);

    [ThreadStatic]
    private static bool? _paged;

    internal static bool Paged
    {
        get => _paged ?? EnvironmentPaged;
        set => _paged = value;
    }
}

/// <summary>
/// Rows of one schema as fixed-width records in 1 MiB <c>byte[]</c> pages, strings as bytes in
/// separate pages — none of it an object reference, so a young GC has nothing to mark or
/// relocate through it (design-row-representation §27.6–§27.7). Each record is a caller-defined
/// header (hashes, links, weights), then a null bitmap with an escape bit, then the cells.
/// </summary>
/// <remarks>
/// Cells compare against a probe's with the semantics of <c>object.Equals</c> per kind
/// (<see cref="PagedCell"/>). A row with a cell whose CLR type is not its column's kind — or with
/// the wrong arity — is kept whole, as an object, in an escape map: correct for any row, and rare
/// (<see cref="EscapedCount"/>). Slot allocation and free lists belong to the caller.
/// </remarks>
internal sealed class PagedRows
{
    private const int PageBytes = 1 << 20;
    private const int FirstPageRows = 64;

    // Strings compact when this many bytes are dead and dead bytes outnumber live ones.
    private const long CompactDeadBytes = 8L << 20;

    private readonly PagedCell[] _cells;
    private readonly int[] _cellOffsets;
    private readonly int _nullsOffset;
    private readonly int _nullBytes;
    private readonly bool _hasStrings;
    private readonly int _recordBytes;
    private readonly int _rowsPerPage;
    private readonly int _fullPageBytes;
    private readonly List<byte[]> _pages = [];
    private PagedStrings _strings = new();
    private Dictionary<int, StructuralRow>? _escaped;

    internal PagedRows(IReadOnlyList<PagedCell> cells, int headerBytes)
    {
        _cells = [.. cells];
        _cellOffsets = new int[_cells.Length];
        _nullsOffset = headerBytes;
        _nullBytes = (_cells.Length + 1 + 7) / 8;   // one bit per column, plus the escape bit
        var offset = _nullsOffset + _nullBytes;
        for (var i = 0; i < _cells.Length; i++)
        {
            _cellOffsets[i] = offset;
            offset += _cells[i].Width;
            _hasStrings |= _cells[i] is PagedStringCell;
        }

        _recordBytes = offset;
        _rowsPerPage = Math.Max(1, PageBytes / _recordBytes);
        _fullPageBytes = _rowsPerPage * _recordBytes;
    }

    internal static PagedCell[] CellsFor(Schema schema)
    {
        ArgumentNullException.ThrowIfNull(schema);
        var cells = new PagedCell[schema.Count];
        for (var i = 0; i < cells.Length; i++)
        {
            cells[i] = PagedCell.For(schema[i].Type.ClrType);
        }

        return cells;
    }

    public int Columns => _cells.Length;

    /// <summary>Slots ever allocated; every slot below it has a record.</summary>
    public int HighWater { get; private set; }

    public int EscapedCount => _escaped?.Count ?? 0;

    public long BytesAllocated
    {
        get
        {
            long total = _strings.Allocated;
            foreach (var page in _pages)
            {
                total += page.Length;
            }

            return total;
        }
    }

    public bool ShouldCompact => _strings.Dead > CompactDeadBytes && _strings.Dead > _strings.Live;

    /// <summary>A fresh slot past the high-water mark.</summary>
    public int Append()
    {
        var fresh = HighWater++;
        var page = fresh / _rowsPerPage;
        var needed = (fresh % _rowsPerPage + 1) * _recordBytes;
        if (page == _pages.Count)
        {
            // Small first page, so a tiny trace does not hold a megabyte; full pages after.
            _pages.Add(new byte[page == 0 ? Math.Min(_fullPageBytes, FirstPageRows * _recordBytes) : _fullPageBytes]);
        }
        else if (_pages[page].Length < needed)
        {
            var grown = _pages[page];
            Array.Resize(ref grown, Math.Min(_fullPageBytes, Math.Max(needed, grown.Length * 2)));
            _pages[page] = grown;
        }

        return fresh;
    }

    public Span<byte> Record(int slot) =>
        _pages[slot / _rowsPerPage].AsSpan(slot % _rowsPerPage * _recordBytes, _recordBytes);

    public int ReadInt32(int slot, int offset) => BinaryPrimitives.ReadInt32LittleEndian(Record(slot)[offset..]);

    public void WriteInt32(int slot, int offset, int value) =>
        BinaryPrimitives.WriteInt32LittleEndian(Record(slot)[offset..], value);

    public long ReadInt64(int slot, int offset) => BinaryPrimitives.ReadInt64LittleEndian(Record(slot)[offset..]);

    public void WriteInt64(int slot, int offset, long value) =>
        BinaryPrimitives.WriteInt64LittleEndian(Record(slot)[offset..], value);

    /// <summary>Stores <paramref name="row"/>'s cells in <paramref name="slot"/> (header untouched).</summary>
    public void Write(int slot, StructuralRow row)
    {
        var record = Record(slot);
        var nulls = record.Slice(_nullsOffset, _nullBytes);
        nulls.Clear();
        var fits = row.Count == _cells.Length;
        for (var i = 0; fits && i < _cells.Length; i++)
        {
            var cell = row[i];
            fits = cell is null || _cells[i].Accepts(cell);
        }

        if (!fits)
        {
            SetBit(nulls, _cells.Length);
            (_escaped ??= []).Add(slot, row);
            return;
        }

        for (var i = 0; i < _cells.Length; i++)
        {
            var cell = row[i];
            if (cell is null)
            {
                SetBit(nulls, i);
            }
            else
            {
                _cells[i].Write(cell, record.Slice(_cellOffsets[i], _cells[i].Width), _strings);
            }
        }
    }

    public bool Matches(int slot, StructuralRow probe)
    {
        var record = Record(slot);
        var nulls = record.Slice(_nullsOffset, _nullBytes);
        if (GetBit(nulls, _cells.Length))
        {
            return _escaped![slot].Equals(probe);
        }

        if (probe.Count != _cells.Length)
        {
            return false;
        }

        for (var i = 0; i < _cells.Length; i++)
        {
            var cell = probe[i];
            if (GetBit(nulls, i))
            {
                if (cell is not null)
                {
                    return false;
                }
            }
            else if (cell is null || !_cells[i].Matches(record.Slice(_cellOffsets[i], _cells[i].Width), cell, _strings))
            {
                return false;
            }
        }

        return true;
    }

    public bool IsEscaped(int slot) => GetBit(Record(slot).Slice(_nullsOffset, _nullBytes), _cells.Length);

    public StructuralRow EscapedRow(int slot) => _escaped![slot];

    /// <summary>One cell, decoded (boxed) — what a <see cref="PagedRowView"/> reads.</summary>
    public object? Cell(int slot, int column)
    {
        var record = Record(slot);
        return GetBit(record.Slice(_nullsOffset, _nullBytes), column)
            ? null
            : _cells[column].Read(record.Slice(_cellOffsets[column], _cells[column].Width), _strings);
    }

    /// <summary>A real row of the slot's values, carrying <paramref name="hash"/> (the hash the
    /// stored row had).</summary>
    public StructuralRow Materialize(int slot, int hash)
    {
        if (IsEscaped(slot))
        {
            return _escaped![slot];
        }

        var values = new object?[_cells.Length];
        for (var i = 0; i < values.Length; i++)
        {
            values[i] = Cell(slot, i);
        }

        return new StructuralRow(values, hash);
    }

    /// <summary>A row whose cells are read from the slot on access; the escaped row itself if
    /// the slot holds one. Valid only until the slot is freed.</summary>
    public StructuralRow View(int slot, int hash) =>
        IsEscaped(slot) ? _escaped![slot] : new PagedRowView(this, slot, hash);

    /// <summary>Gives up the slot's cells (string bytes, escaped row); the caller frees the slot.</summary>
    public void Release(int slot)
    {
        var record = Record(slot);
        var nulls = record.Slice(_nullsOffset, _nullBytes);
        if (GetBit(nulls, _cells.Length))
        {
            _escaped!.Remove(slot);
            return;
        }

        if (_hasStrings)
        {
            for (var i = 0; i < _cells.Length; i++)
            {
                if (!GetBit(nulls, i))
                {
                    _cells[i].Release(record.Slice(_cellOffsets[i], _cells[i].Width), _strings);
                }
            }
        }
    }

    /// <summary>Rewrites the string bytes of every slot <paramref name="live"/> accepts into fresh
    /// pages. Strings already handed out keep pointing at the old pages, which never change.</summary>
    public void CompactStrings(Func<int, bool> live)
    {
        var target = new PagedStrings();
        for (var slot = 0; slot < HighWater; slot++)
        {
            if (!live(slot) || IsEscaped(slot))
            {
                continue;
            }

            var record = Record(slot);
            var nulls = record.Slice(_nullsOffset, _nullBytes);
            for (var i = 0; i < _cells.Length; i++)
            {
                if (!GetBit(nulls, i))
                {
                    _cells[i].Relocate(record.Slice(_cellOffsets[i], _cells[i].Width), _strings, target);
                }
            }
        }

        _strings = target;
    }

    private static bool GetBit(ReadOnlySpan<byte> bits, int i) => (bits[i >> 3] & (1 << (i & 7))) != 0;

    private static void SetBit(Span<byte> bits, int i) => bits[i >> 3] |= (byte)(1 << (i & 7));
}

/// <summary>
/// A stored row read in place: <see cref="Count"/> and the indexer decode from the slot on
/// each access (boxing the cell). It is handed to code that only copies cells out — a join's
/// combine — and must not be kept: the slot can be freed and reused by a later tick.
/// </summary>
internal sealed class PagedRowView : StructuralRow
{
    private readonly PagedRows _rows;
    private readonly int _slot;

    internal PagedRowView(PagedRows rows, int slot, int hash)
        : base(hash)
    {
        _rows = rows;
        _slot = slot;
    }

    public override int Count => _rows.Columns;

    public override object? this[int index] => _rows.Cell(_slot, index);
}

/// <summary>Fibonacci hashing for the stores' bucket arrays: the top bits of hash × 2^32/φ.
/// Masking the low bits directly chained badly on ivm-bench's fact_holdings view at W=8 (13.5 s
/// to integrate vs 0.26 s for the Dictionary, which reduces modulo a prime); why those rows' low
/// bits cluster is not pinned down.</summary>
internal static class PagedBuckets
{
    public static int Of(int hash, int shift) => (int)(((uint)hash * 0x9E3779B9u) >> shift);

    public static int ShiftFor(int size) => 32 - System.Numerics.BitOperations.Log2((uint)size);
}

/// <summary>
/// The integral of a Z-set of rows, held as values in <see cref="PagedRows"/> with an <c>int[]</c>
/// slot index: each record's header is (hash, chain link, weight). The row's hash is the one its
/// <see cref="StructuralRow"/> already carries, stored and never recomputed.
/// </summary>
/// <remarks>Slots are allocated exactly as <see cref="Dictionary{TKey,TValue}"/> allocates entries
/// (a LIFO free list, else append), so <see cref="Materialize"/> enumerates in the order the
/// equivalent in-place <see cref="ZSet{TKey,TWeight}"/> would — which keeps snapshot files
/// byte-identical between the two representations.</remarks>
internal sealed class PagedZSetStore : IIntegratedViewStore<StructuralRow>
{
    private const int HashOffset = 0;
    private const int NextOffset = 4;
    private const int WeightOffset = 8;
    private const int HeaderBytes = 16;

    private readonly PagedRows _rows;
    private int[] _buckets = new int[16];   // slot + 1; 0 = empty
    private int _bucketShift = PagedBuckets.ShiftFor(16);
    private int _freeHead = -1;
    private int _count;

    internal PagedZSetStore(IReadOnlyList<PagedCell> cells)
    {
        _rows = new PagedRows(cells, HeaderBytes);
    }

    /// <summary>A store for rows of <paramref name="schema"/>, its cell kinds from the column types.</summary>
    internal static PagedZSetStore For(Schema schema) => new(PagedRows.CellsFor(schema));

    public int Count => _count;

    /// <summary>Rows kept as objects because a cell did not fit its column's kind.</summary>
    internal int EscapedCount => _rows.EscapedCount;

    /// <summary>Bytes held in pages (records and strings), for reports.</summary>
    internal long PageBytesAllocated => _rows.BytesAllocated;

    public void Integrate(ZSet<StructuralRow, Z64> delta)
    {
        ArgumentNullException.ThrowIfNull(delta);
        foreach (var (row, weight) in delta)
        {
            Add(row, weight.Value);
        }

        if (_rows.ShouldCompact)
        {
            _rows.CompactStrings(slot => Weight(slot) != 0);
        }
    }

    public Z64 WeightOf(StructuralRow row)
    {
        ArgumentNullException.ThrowIfNull(row);
        var hash = row.GetHashCode();
        for (var slot = _buckets[PagedBuckets.Of(hash, _bucketShift)] - 1; slot >= 0; slot = Next(slot))
        {
            if (Hash(slot) == hash && _rows.Matches(slot, row))
            {
                return new Z64(Weight(slot));
            }
        }

        return Z64.Zero;
    }

    public ZSet<StructuralRow, Z64> Materialize()
    {
        var entries = new Dictionary<StructuralRow, Z64>(_count);
        for (var slot = 0; slot < _rows.HighWater; slot++)
        {
            var weight = Weight(slot);
            if (weight != 0)
            {
                entries.Add(_rows.Materialize(slot, Hash(slot)), new Z64(weight));
            }
        }

        return new ZSet<StructuralRow, Z64>(entries);
    }

    public void Clear()
    {
        // Retract in slot order, as MergeInPlace(Negate()) removes in enumeration order, so the
        // free list — and the slot order of whatever is integrated next — matches the Z-set's.
        for (var slot = 0; slot < _rows.HighWater; slot++)
        {
            if (Weight(slot) != 0)
            {
                Unlink(slot);
                Free(slot);
            }
        }
    }

    private void Add(StructuralRow row, long weight)
    {
        if (weight == 0)
        {
            return;
        }

        var hash = row.GetHashCode();
        var bucket = PagedBuckets.Of(hash, _bucketShift);
        var previous = -1;
        for (var slot = _buckets[bucket] - 1; slot >= 0; slot = Next(slot))
        {
            if (Hash(slot) == hash && _rows.Matches(slot, row))
            {
                var sum = checked(Weight(slot) + weight);
                if (sum == 0)
                {
                    SetNext(previous, bucket, Next(slot));
                    Free(slot);
                }
                else
                {
                    _rows.WriteInt64(slot, WeightOffset, sum);
                }

                return;
            }

            previous = slot;
        }

        int fresh;
        if (_freeHead >= 0)
        {
            fresh = _freeHead;
            _freeHead = Next(fresh);
        }
        else
        {
            fresh = _rows.Append();
        }

        _rows.WriteInt32(fresh, HashOffset, hash);
        _rows.WriteInt32(fresh, NextOffset, _buckets[bucket] - 1);
        _rows.WriteInt64(fresh, WeightOffset, weight);
        _buckets[bucket] = fresh + 1;
        _rows.Write(fresh, row);
        _count++;
        if (_count > _buckets.Length)
        {
            Rehash(_buckets.Length * 2);
        }
    }

    // Releases a slot already unlinked from its bucket chain.
    private void Free(int slot)
    {
        _rows.Release(slot);
        _rows.WriteInt64(slot, WeightOffset, 0);
        _rows.WriteInt32(slot, NextOffset, _freeHead);
        _freeHead = slot;
        _count--;
    }

    private void Unlink(int slot)
    {
        var bucket = PagedBuckets.Of(Hash(slot), _bucketShift);
        var previous = -1;
        for (var s = _buckets[bucket] - 1; s >= 0; s = Next(s))
        {
            if (s == slot)
            {
                SetNext(previous, bucket, Next(slot));
                return;
            }

            previous = s;
        }

        throw new InvalidOperationException("paged store: slot not in its bucket chain");
    }

    private void SetNext(int previous, int bucket, int next)
    {
        if (previous < 0)
        {
            _buckets[bucket] = next + 1;
        }
        else
        {
            _rows.WriteInt32(previous, NextOffset, next);
        }
    }

    private void Rehash(int size)
    {
        var shift = PagedBuckets.ShiftFor(size);
        var buckets = new int[size];
        for (var slot = 0; slot < _rows.HighWater; slot++)
        {
            if (Weight(slot) != 0)
            {
                var bucket = PagedBuckets.Of(Hash(slot), shift);
                _rows.WriteInt32(slot, NextOffset, buckets[bucket] - 1);
                buckets[bucket] = slot + 1;
            }
        }

        _buckets = buckets;
        _bucketShift = shift;
    }

    private int Hash(int slot) => _rows.ReadInt32(slot, HashOffset);

    private int Next(int slot) => _rows.ReadInt32(slot, NextOffset);

    private long Weight(int slot) => _rows.ReadInt64(slot, WeightOffset);
}

/// <summary>
/// The integral of an indexed Z-set — a join trace — held as values: keys in one
/// <see cref="PagedRows"/>, values in another, two <c>int[]</c> hash indexes (key → group, and
/// (group, value) → value slot), and each group's values on a linked list.
/// </summary>
/// <remarks>
/// <para>Order mirrors <see cref="IndexedZSet{TKey,TValue,TWeight}"/> exactly. Groups take key
/// slots as its outer <see cref="Dictionary{TKey,TValue}"/> takes entries (LIFO free list, else
/// append). A group's list is its inner Z-set's entry array: a new value is appended, a retracted
/// one stays on the list as a tombstone and is the first reused by the group's next insert. So
/// joins over the store emit in the same order, and snapshots are byte-identical.</para>
/// <para>With <c>views</c>, <see cref="KeyOf"/> / <see cref="ValueAt"/> return
/// <see cref="PagedRowView"/>s (a 40-byte handle; cells boxed when read) — only for a combine
/// that copies cells out. Without, they return materialised rows.</para>
/// </remarks>
internal sealed class PagedIndexedStore : IIndexedTraceStore<StructuralRow, StructuralRow, Z64>
{
    // Key record header.
    private const int KHash = 0;
    private const int KNext = 4;    // bucket chain; free-list link when the slot is free
    private const int KHead = 8;    // first value slot on the group's list (−1 = none)
    private const int KTail = 12;
    private const int KCount = 16;  // live values
    private const int KFree = 20;   // top of the group's tombstone stack (−1 = none)
    private const int KLive = 24;   // 1 while the slot holds a group
    private const int KeyHeaderBytes = 28;

    // Value record header.
    private const int VHash = 0;
    private const int VNext = 4;    // bucket chain; tombstone-stack / free-list link otherwise
    private const int VGroupNext = 8;
    private const int VGroup = 12;
    private const int VWeight = 16; // 0 = tombstone or free
    private const int ValueHeaderBytes = 24;

    private readonly PagedRows _keys;
    private readonly PagedRows _values;
    private readonly bool _views;
    private int[] _keyBuckets = new int[16];
    private int _keyShift = PagedBuckets.ShiftFor(16);
    private int[] _valueBuckets = new int[16];
    private int _valueShift = PagedBuckets.ShiftFor(16);
    private int _keyFree = -1;
    private int _valueFree = -1;
    private int _groupCount;
    private int _valueCount;

    internal PagedIndexedStore(IReadOnlyList<PagedCell> keyCells, IReadOnlyList<PagedCell> valueCells, bool views)
    {
        _keys = new PagedRows(keyCells, KeyHeaderBytes);
        _values = new PagedRows(valueCells, ValueHeaderBytes);
        _views = views;
    }

    internal static PagedIndexedStore For(Schema keySchema, Schema valueSchema, bool views) =>
        new(PagedRows.CellsFor(keySchema), PagedRows.CellsFor(valueSchema), views);

    public int GroupCount => _groupCount;

    internal int EscapedCount => _keys.EscapedCount + _values.EscapedCount;

    internal long PageBytesAllocated => _keys.BytesAllocated + _values.BytesAllocated;

    public void Integrate(IndexedZSet<StructuralRow, StructuralRow, Z64> delta)
    {
        ArgumentNullException.ThrowIfNull(delta);
        foreach (var (key, deltaGroup) in delta)
        {
            var g = FindGroup(key);
            if (g < 0)
            {
                // _groups[key] = deltaGroup.Clone(): the delta group's entries, in its order.
                g = NewGroup(key);
                foreach (var (value, weight) in deltaGroup)
                {
                    InsertValue(g, value, value.GetHashCode(), weight.Value);
                }
            }
            else
            {
                foreach (var (value, weight) in deltaGroup)
                {
                    MergeValue(g, value, weight.Value);
                }

                if (_keys.ReadInt32(g, KCount) == 0)
                {
                    RemoveGroup(g);
                }
            }
        }

        if (_keys.ShouldCompact)
        {
            _keys.CompactStrings(slot => _keys.ReadInt32(slot, KLive) != 0);
        }

        if (_values.ShouldCompact)
        {
            _values.CompactStrings(slot => _values.ReadInt64(slot, VWeight) != 0);
        }
    }

    public int FindGroup(StructuralRow key)
    {
        var hash = key.GetHashCode();
        for (var g = _keyBuckets[PagedBuckets.Of(hash, _keyShift)] - 1; g >= 0; g = _keys.ReadInt32(g, KNext))
        {
            if (_keys.ReadInt32(g, KHash) == hash && _keys.Matches(g, key))
            {
                return g;
            }
        }

        return -1;
    }

    public int FirstGroup() => NextLiveGroup(0);

    public int NextGroup(int group) => NextLiveGroup(group + 1);

    public StructuralRow KeyOf(int group)
    {
        var hash = _keys.ReadInt32(group, KHash);
        return _views ? _keys.View(group, hash) : _keys.Materialize(group, hash);
    }

    public int FirstValue(int group) => LiveFrom(_keys.ReadInt32(group, KHead));

    public int NextValue(int value) => LiveFrom(_values.ReadInt32(value, VGroupNext));

    public StructuralRow ValueAt(int value)
    {
        var hash = _values.ReadInt32(value, VHash);
        return _views ? _values.View(value, hash) : _values.Materialize(value, hash);
    }

    public Z64 WeightAt(int value) => new(_values.ReadInt64(value, VWeight));

    public IndexedZSet<StructuralRow, StructuralRow, Z64> Materialize()
    {
        var groups = new Dictionary<StructuralRow, ZSet<StructuralRow, Z64>>(_groupCount);
        for (var g = FirstGroup(); g >= 0; g = NextGroup(g))
        {
            var entries = new Dictionary<StructuralRow, Z64>(_keys.ReadInt32(g, KCount));
            for (var v = FirstValue(g); v >= 0; v = NextValue(v))
            {
                entries.Add(_values.Materialize(v, _values.ReadInt32(v, VHash)), WeightAt(v));
            }

            groups.Add(_keys.Materialize(g, _keys.ReadInt32(g, KHash)), new ZSet<StructuralRow, Z64>(entries));
        }

        return new IndexedZSet<StructuralRow, StructuralRow, Z64>(groups);
    }

    public IReadOnlyList<StructuralRow> DropKeysBelow(long threshold, Func<StructuralRow, long> monotoneKey)
    {
        ArgumentNullException.ThrowIfNull(monotoneKey);
        List<(int Group, StructuralRow Key)>? removed = null;
        for (var g = FirstGroup(); g >= 0; g = NextGroup(g))
        {
            var key = _keys.Materialize(g, _keys.ReadInt32(g, KHash));
            if (monotoneKey(key) < threshold)
            {
                (removed ??= []).Add((g, key));
            }
        }

        if (removed is null)
        {
            return [];
        }

        var keys = new StructuralRow[removed.Count];
        for (var i = 0; i < removed.Count; i++)
        {
            RemoveGroup(removed[i].Group);
            keys[i] = removed[i].Key;
        }

        return keys;
    }

    private int NextLiveGroup(int from)
    {
        for (var g = from; g < _keys.HighWater; g++)
        {
            if (_keys.ReadInt32(g, KLive) != 0)
            {
                return g;
            }
        }

        return -1;
    }

    // The first value at or after `slot` on a group list that is not a tombstone.
    private int LiveFrom(int slot)
    {
        while (slot >= 0 && _values.ReadInt64(slot, VWeight) == 0)
        {
            slot = _values.ReadInt32(slot, VGroupNext);
        }

        return slot;
    }

    private int NewGroup(StructuralRow key)
    {
        int g;
        if (_keyFree >= 0)
        {
            g = _keyFree;
            _keyFree = _keys.ReadInt32(g, KNext);
        }
        else
        {
            g = _keys.Append();
        }

        var hash = key.GetHashCode();
        var bucket = PagedBuckets.Of(hash, _keyShift);
        _keys.WriteInt32(g, KHash, hash);
        _keys.WriteInt32(g, KNext, _keyBuckets[bucket] - 1);
        _keys.WriteInt32(g, KHead, -1);
        _keys.WriteInt32(g, KTail, -1);
        _keys.WriteInt32(g, KCount, 0);
        _keys.WriteInt32(g, KFree, -1);
        _keys.WriteInt32(g, KLive, 1);
        _keys.Write(g, key);
        _keyBuckets[bucket] = g + 1;
        _groupCount++;
        if (_groupCount > _keyBuckets.Length)
        {
            RehashKeys(_keyBuckets.Length * 2);
        }

        return g;
    }

    private void MergeValue(int g, StructuralRow value, long weight)
    {
        if (weight == 0)
        {
            return;
        }

        var hash = value.GetHashCode();
        var bucket = PagedBuckets.Of(ValueBucketHash(g, hash), _valueShift);
        var previous = -1;
        for (var v = _valueBuckets[bucket] - 1; v >= 0; v = _values.ReadInt32(v, VNext))
        {
            if (_values.ReadInt32(v, VGroup) == g && _values.ReadInt32(v, VHash) == hash && _values.Matches(v, value))
            {
                var sum = checked(_values.ReadInt64(v, VWeight) + weight);
                if (sum != 0)
                {
                    _values.WriteInt64(v, VWeight, sum);
                    return;
                }

                // Retracted: unlink from its bucket and leave it on the group list as a tombstone,
                // top of the group's reuse stack — Dictionary.Remove's free-list push.
                var next = _values.ReadInt32(v, VNext);
                if (previous < 0)
                {
                    _valueBuckets[bucket] = next + 1;
                }
                else
                {
                    _values.WriteInt32(previous, VNext, next);
                }

                _values.Release(v);
                _values.WriteInt64(v, VWeight, 0);
                _values.WriteInt32(v, VNext, _keys.ReadInt32(g, KFree));
                _keys.WriteInt32(g, KFree, v);
                _keys.WriteInt32(g, KCount, _keys.ReadInt32(g, KCount) - 1);
                _valueCount--;
                return;
            }

            previous = v;
        }

        InsertValue(g, value, hash, weight);
    }

    // A value new to the group: into its most recent tombstone if it has one, else appended.
    private void InsertValue(int g, StructuralRow value, int hash, long weight)
    {
        int v;
        var tombstone = _keys.ReadInt32(g, KFree);
        if (tombstone >= 0)
        {
            v = tombstone;
            _keys.WriteInt32(g, KFree, _values.ReadInt32(v, VNext));
        }
        else
        {
            if (_valueFree >= 0)
            {
                v = _valueFree;
                _valueFree = _values.ReadInt32(v, VNext);
            }
            else
            {
                v = _values.Append();
            }

            _values.WriteInt32(v, VGroupNext, -1);
            _values.WriteInt32(v, VGroup, g);
            var tail = _keys.ReadInt32(g, KTail);
            if (tail < 0)
            {
                _keys.WriteInt32(g, KHead, v);
            }
            else
            {
                _values.WriteInt32(tail, VGroupNext, v);
            }

            _keys.WriteInt32(g, KTail, v);
        }

        var bucket = PagedBuckets.Of(ValueBucketHash(g, hash), _valueShift);
        _values.WriteInt32(v, VHash, hash);
        _values.WriteInt64(v, VWeight, weight);
        _values.WriteInt32(v, VNext, _valueBuckets[bucket] - 1);
        _values.Write(v, value);
        _valueBuckets[bucket] = v + 1;
        _keys.WriteInt32(g, KCount, _keys.ReadInt32(g, KCount) + 1);
        _valueCount++;
        if (_valueCount > _valueBuckets.Length)
        {
            RehashValues(_valueBuckets.Length * 2);
        }
    }

    private void RemoveGroup(int g)
    {
        // Every slot on the list — live values out of their buckets, tombstones as they are —
        // goes to the shared free list.
        for (var v = _keys.ReadInt32(g, KHead); v >= 0;)
        {
            var next = _values.ReadInt32(v, VGroupNext);
            if (_values.ReadInt64(v, VWeight) != 0)
            {
                UnlinkValue(v);
                _values.Release(v);
                _values.WriteInt64(v, VWeight, 0);
                _valueCount--;
            }

            _values.WriteInt32(v, VNext, _valueFree);
            _valueFree = v;
            v = next;
        }

        var bucket = PagedBuckets.Of(_keys.ReadInt32(g, KHash), _keyShift);
        var previous = -1;
        for (var s = _keyBuckets[bucket] - 1; s >= 0; s = _keys.ReadInt32(s, KNext))
        {
            if (s == g)
            {
                var next = _keys.ReadInt32(g, KNext);
                if (previous < 0)
                {
                    _keyBuckets[bucket] = next + 1;
                }
                else
                {
                    _keys.WriteInt32(previous, KNext, next);
                }

                break;
            }

            previous = s;
        }

        _keys.Release(g);
        _keys.WriteInt32(g, KLive, 0);
        _keys.WriteInt32(g, KNext, _keyFree);
        _keyFree = g;
        _groupCount--;
    }

    private void UnlinkValue(int v)
    {
        var bucket = PagedBuckets.Of(ValueBucketHash(_values.ReadInt32(v, VGroup), _values.ReadInt32(v, VHash)), _valueShift);
        var previous = -1;
        for (var s = _valueBuckets[bucket] - 1; s >= 0; s = _values.ReadInt32(s, VNext))
        {
            if (s == v)
            {
                var next = _values.ReadInt32(v, VNext);
                if (previous < 0)
                {
                    _valueBuckets[bucket] = next + 1;
                }
                else
                {
                    _values.WriteInt32(previous, VNext, next);
                }

                return;
            }

            previous = s;
        }

        throw new InvalidOperationException("paged store: value not in its bucket chain");
    }

    private void RehashKeys(int size)
    {
        var shift = PagedBuckets.ShiftFor(size);
        var buckets = new int[size];
        for (var g = 0; g < _keys.HighWater; g++)
        {
            if (_keys.ReadInt32(g, KLive) != 0)
            {
                var bucket = PagedBuckets.Of(_keys.ReadInt32(g, KHash), shift);
                _keys.WriteInt32(g, KNext, buckets[bucket] - 1);
                buckets[bucket] = g + 1;
            }
        }

        _keyBuckets = buckets;
        _keyShift = shift;
    }

    private void RehashValues(int size)
    {
        var shift = PagedBuckets.ShiftFor(size);
        var buckets = new int[size];
        for (var v = 0; v < _values.HighWater; v++)
        {
            if (_values.ReadInt64(v, VWeight) != 0)
            {
                var bucket = PagedBuckets.Of(ValueBucketHash(_values.ReadInt32(v, VGroup), _values.ReadInt32(v, VHash)), shift);
                _values.WriteInt32(v, VNext, buckets[bucket] - 1);
                buckets[bucket] = v + 1;
            }
        }

        _valueBuckets = buckets;
        _valueShift = shift;
    }

    private static int ValueBucketHash(int group, int valueHash) => valueHash ^ (int)((uint)group * 0x85EBCA6Bu);
}
/// <summary>
/// How one column's cells live in a <see cref="PagedRows"/> record: a fixed-width encoding,
/// decoding back to the identical boxed value, and an equality check against a probe cell that
/// gives exactly what <c>object.Equals(stored, probe)</c> would.
/// </summary>
internal abstract class PagedCell
{
    public abstract int Width { get; }

    /// <summary>Whether a non-null <paramref name="cell"/> can be stored in this column; if not,
    /// the row escapes.</summary>
    public abstract bool Accepts(object cell);

    public abstract void Write(object cell, Span<byte> destination, PagedStrings strings);

    public abstract object Read(ReadOnlySpan<byte> source, PagedStrings strings);

    public abstract bool Matches(ReadOnlySpan<byte> source, object cell, PagedStrings strings);

    public virtual void Release(ReadOnlySpan<byte> source, PagedStrings strings)
    {
    }

    public virtual void Relocate(Span<byte> source, PagedStrings from, PagedStrings to)
    {
    }

    internal static PagedCell For(Type clr) =>
        clr == typeof(long) ? new PagedValueCell<long>()
        : clr == typeof(int) ? new PagedValueCell<int>()
        : clr == typeof(double) ? new PagedValueCell<double>()
        : clr == typeof(float) ? new PagedValueCell<float>()
        : clr == typeof(bool) ? new PagedValueCell<bool>()
        : clr == typeof(Date32) ? new PagedValueCell<Date32>()
        : clr == typeof(Time64) ? new PagedValueCell<Time64>()
        : clr == typeof(Timestamp) ? new PagedValueCell<Timestamp>()
        : clr == typeof(Interval) ? new PagedValueCell<Interval>()
        : clr == typeof(Decimal128) ? new PagedValueCell<Decimal128>()
        : clr == typeof(Utf8String) ? new PagedStringCell()
        : PagedEscapeCell.Instance;
}

/// <summary>An unmanaged value stored as its raw bytes, so it decodes bit-for-bit (−0.0, NaN
/// payloads, decimal scale) and compares with <see cref="IEquatable{T}"/> — what
/// <c>T.Equals(object)</c> does for a probe of the same type.</summary>
internal sealed class PagedValueCell<T> : PagedCell
    where T : unmanaged, IEquatable<T>
{
    public override int Width => Unsafe.SizeOf<T>();

    public override bool Accepts(object cell) => cell is T;

    public override void Write(object cell, Span<byte> destination, PagedStrings strings)
    {
        var value = (T)cell;
        MemoryMarshal.Write(destination, in value);
    }

    public override object Read(ReadOnlySpan<byte> source, PagedStrings strings) => MemoryMarshal.Read<T>(source);

    public override bool Matches(ReadOnlySpan<byte> source, object cell, PagedStrings strings) =>
        cell is T value ? MemoryMarshal.Read<T>(source).Equals(value) : Read(source, strings).Equals(cell);
}

/// <summary>A <see cref="Utf8String"/>: (page, offset, length) into the store's string pages.</summary>
internal sealed class PagedStringCell : PagedCell
{
    public override int Width => 12;

    public override bool Accepts(object cell) => cell is Utf8String;

    public override void Write(object cell, Span<byte> destination, PagedStrings strings)
    {
        var bytes = ((Utf8String)cell).Span;
        var (page, offset) = strings.Append(bytes);
        Encode(destination, page, offset, bytes.Length);
    }

    // No copy: string pages are append-only, so the bytes behind a handed-out string never
    // change (compaction writes fresh pages and leaves the old ones to whoever still holds them).
    public override object Read(ReadOnlySpan<byte> source, PagedStrings strings) =>
        Utf8String.FromBytes(strings.Memory(
            BinaryPrimitives.ReadInt32LittleEndian(source),
            BinaryPrimitives.ReadInt32LittleEndian(source[4..]),
            BinaryPrimitives.ReadInt32LittleEndian(source[8..])));

    public override bool Matches(ReadOnlySpan<byte> source, object cell, PagedStrings strings) =>
        cell is Utf8String value ? Bytes(source, strings).SequenceEqual(value.Span) : Read(source, strings).Equals(cell);

    public override void Release(ReadOnlySpan<byte> source, PagedStrings strings) =>
        strings.Release(BinaryPrimitives.ReadInt32LittleEndian(source[8..]));

    public override void Relocate(Span<byte> source, PagedStrings from, PagedStrings to)
    {
        var bytes = Bytes(source, from);
        var (page, offset) = to.Append(bytes);
        Encode(source, page, offset, bytes.Length);
    }

    private static ReadOnlySpan<byte> Bytes(ReadOnlySpan<byte> source, PagedStrings strings) =>
        strings.Slice(
            BinaryPrimitives.ReadInt32LittleEndian(source),
            BinaryPrimitives.ReadInt32LittleEndian(source[4..]),
            BinaryPrimitives.ReadInt32LittleEndian(source[8..]));

    private static void Encode(Span<byte> destination, int page, int offset, int length)
    {
        BinaryPrimitives.WriteInt32LittleEndian(destination, page);
        BinaryPrimitives.WriteInt32LittleEndian(destination[4..], offset);
        BinaryPrimitives.WriteInt32LittleEndian(destination[8..], length);
    }
}

/// <summary>A column with no stored kind (an untyped NULL column, or a CLR type the store does
/// not know): only NULL fits, anything else sends the row to the escape map.</summary>
internal sealed class PagedEscapeCell : PagedCell
{
    public static PagedEscapeCell Instance { get; } = new();

    public override int Width => 0;

    public override bool Accepts(object cell) => false;

    public override void Write(object cell, Span<byte> destination, PagedStrings strings) =>
        throw new InvalidOperationException("paged store: escape column holds no values");

    public override object Read(ReadOnlySpan<byte> source, PagedStrings strings) =>
        throw new InvalidOperationException("paged store: escape column holds no values");

    public override bool Matches(ReadOnlySpan<byte> source, object cell, PagedStrings strings) => false;
}

/// <summary>Append-only UTF-8 bytes for a store's string cells, in reference-free pages; the
/// first page starts small and doubles to 1 MiB. Freed bytes are only counted — the store
/// rebuilds its strings when enough are dead.</summary>
internal sealed class PagedStrings
{
    private const int PageBytes = 1 << 20;
    private const int FirstPageBytes = 4096;
    private readonly List<byte[]> _pages = [];
    private int _current = -1;
    private int _offset;

    public long Live { get; private set; }

    public long Dead { get; private set; }

    public long Allocated
    {
        get
        {
            long total = 0;
            foreach (var page in _pages)
            {
                total += page.Length;
            }

            return total;
        }
    }

    public (int Page, int Offset) Append(ReadOnlySpan<byte> bytes)
    {
        if (bytes.IsEmpty)
        {
            return (0, 0);
        }

        Live += bytes.Length;
        if (bytes.Length > PageBytes / 4)
        {
            _pages.Add(bytes.ToArray());   // its own page; the current page stays current
            return (_pages.Count - 1, 0);
        }

        if (_current < 0 || _offset + bytes.Length > _pages[_current].Length)
        {
            var page = _current < 0 ? null : _pages[_current];
            if (page is not null && page.Length < PageBytes && _offset + bytes.Length <= PageBytes)
            {
                Array.Resize(ref page, Math.Min(PageBytes, Math.Max(_offset + bytes.Length, page.Length * 2)));
                _pages[_current] = page;
            }
            else
            {
                _pages.Add(new byte[_current < 0 ? Math.Max(FirstPageBytes, bytes.Length) : PageBytes]);
                _current = _pages.Count - 1;
                _offset = 0;
            }
        }

        bytes.CopyTo(_pages[_current].AsSpan(_offset));
        var at = (_current, _offset);
        _offset += bytes.Length;
        return at;
    }

    public ReadOnlySpan<byte> Slice(int page, int offset, int length) =>
        length == 0 ? default : _pages[page].AsSpan(offset, length);

    public ReadOnlyMemory<byte> Memory(int page, int offset, int length) =>
        length == 0 ? default : _pages[page].AsMemory(offset, length);

    public void Release(int length)
    {
        Live -= length;
        Dead += length;
    }
}
