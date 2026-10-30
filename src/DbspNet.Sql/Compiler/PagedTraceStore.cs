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
/// The integral of a Z-set of rows of one schema, held as values: each row is a fixed-width
/// record in 1 MiB <c>byte[]</c> pages (hash, chain link, weight, null bitmap, cells), strings
/// are bytes in separate pages, and the hash index is an <c>int[]</c> of slots. None of it holds
/// an object reference, so a young GC has nothing to mark or relocate through it — the cost the
/// row-per-object state pays per inserted row (design-row-representation §27.6–§27.7).
/// </summary>
/// <remarks>
/// <para>The row's hash is the one its <see cref="StructuralRow"/> already carries, stored and
/// never recomputed; equality compares stored cells against the probe's with the semantics of
/// <c>object.Equals</c> per kind (<see cref="PagedCell"/>). A row with a cell whose CLR type is
/// not its column's kind — or with the wrong arity — is kept whole, as an object, in an escape
/// map; that is correct for any row and should be rare (<see cref="EscapedCount"/>).</para>
/// <para>Slots are allocated exactly as <see cref="Dictionary{TKey,TValue}"/> allocates entries
/// (a LIFO free list, else append), so <see cref="Materialize"/> enumerates in the order the
/// equivalent in-place <see cref="ZSet{TKey,TWeight}"/> would — which keeps snapshot files
/// byte-identical between the two representations.</para>
/// </remarks>
internal sealed class PagedZSetStore : IIntegratedViewStore<StructuralRow>
{
    private const int PageBytes = 1 << 20;
    private const int FirstPageRows = 64;
    private const int HashOffset = 0;
    private const int NextOffset = 4;
    private const int WeightOffset = 8;
    private const int NullsOffset = 16;

    // Strings compact when this many bytes are dead and dead bytes outnumber live ones.
    private const long CompactDeadBytes = 8L << 20;

    private readonly PagedCell[] _cells;
    private readonly int[] _cellOffsets;
    private readonly bool _hasStrings;
    private readonly int _recordBytes;
    private readonly int _rowsPerPage;
    private readonly int _fullPageBytes;
    private readonly List<byte[]> _pages = [];
    private PagedStrings _strings = new();
    private Dictionary<int, StructuralRow>? _escaped;
    private int[] _buckets = new int[16];   // slot + 1; 0 = empty
    private int _highWater;
    private int _freeHead = -1;
    private int _count;

    internal PagedZSetStore(IReadOnlyList<PagedCell> cells)
    {
        _cells = [.. cells];
        _cellOffsets = new int[_cells.Length];
        var offset = NullsOffset + NullBytes(_cells.Length);
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

    /// <summary>A store for rows of <paramref name="schema"/>, its cell kinds from the column types.</summary>
    internal static PagedZSetStore For(Schema schema)
    {
        ArgumentNullException.ThrowIfNull(schema);
        var cells = new PagedCell[schema.Count];
        for (var i = 0; i < cells.Length; i++)
        {
            cells[i] = PagedCell.For(schema[i].Type.ClrType);
        }

        return new PagedZSetStore(cells);
    }

    public int Count => _count;

    /// <summary>Rows kept as objects because a cell did not fit its column's kind.</summary>
    internal int EscapedCount => _escaped?.Count ?? 0;

    /// <summary>Bytes held in pages (records and strings), for reports.</summary>
    internal long PageBytesAllocated
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

    public void Integrate(ZSet<StructuralRow, Z64> delta)
    {
        ArgumentNullException.ThrowIfNull(delta);
        foreach (var (row, weight) in delta)
        {
            Add(row, weight.Value);
        }

        if (_strings.Dead > CompactDeadBytes && _strings.Dead > _strings.Live)
        {
            CompactStrings();
        }
    }

    public Z64 WeightOf(StructuralRow row)
    {
        ArgumentNullException.ThrowIfNull(row);
        var hash = row.GetHashCode();
        for (var slot = _buckets[hash & (_buckets.Length - 1)] - 1; slot >= 0; slot = Next(slot))
        {
            if (Hash(slot) == hash && Matches(slot, row))
            {
                return new Z64(Weight(slot));
            }
        }

        return Z64.Zero;
    }

    public ZSet<StructuralRow, Z64> Materialize()
    {
        var entries = new Dictionary<StructuralRow, Z64>(_count);
        for (var slot = 0; slot < _highWater; slot++)
        {
            var weight = Weight(slot);
            if (weight != 0)
            {
                entries.Add(RowAt(slot), new Z64(weight));
            }
        }

        return new ZSet<StructuralRow, Z64>(entries);
    }

    public void Clear()
    {
        // Retract in slot order, as MergeInPlace(Negate()) removes in enumeration order, so the
        // free list — and the slot order of whatever is integrated next — matches the Z-set's.
        for (var slot = 0; slot < _highWater; slot++)
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
        var bucket = hash & (_buckets.Length - 1);
        var previous = -1;
        for (var slot = _buckets[bucket] - 1; slot >= 0; slot = Next(slot))
        {
            if (Hash(slot) == hash && Matches(slot, row))
            {
                var sum = checked(Weight(slot) + weight);
                if (sum == 0)
                {
                    SetNext(previous, bucket, Next(slot));
                    Free(slot);
                }
                else
                {
                    SetWeight(slot, sum);
                }

                return;
            }

            previous = slot;
        }

        var fresh = Allocate();
        var record = Record(fresh);
        BinaryPrimitives.WriteInt32LittleEndian(record[HashOffset..], hash);
        BinaryPrimitives.WriteInt32LittleEndian(record[NextOffset..], _buckets[bucket] - 1);
        BinaryPrimitives.WriteInt64LittleEndian(record[WeightOffset..], weight);
        _buckets[bucket] = fresh + 1;
        Write(fresh, record, row);
        _count++;
        if (_count > _buckets.Length)
        {
            Rehash(_buckets.Length * 2);
        }
    }

    private void Write(int slot, Span<byte> record, StructuralRow row)
    {
        var nulls = record.Slice(NullsOffset, NullBytes(_cells.Length));
        nulls.Clear();
        var fits = row.Count == _cells.Length;
        for (var i = 0; fits && i < _cells.Length; i++)
        {
            var cell = row[i];
            fits = cell is null || _cells[i].Accepts(cell);
        }

        if (!fits)
        {
            SetBit(nulls, _cells.Length);   // the escape bit
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

    private bool Matches(int slot, StructuralRow probe)
    {
        var record = Record(slot);
        var nulls = record.Slice(NullsOffset, NullBytes(_cells.Length));
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

    private StructuralRow RowAt(int slot)
    {
        var record = Record(slot);
        var nulls = record.Slice(NullsOffset, NullBytes(_cells.Length));
        if (GetBit(nulls, _cells.Length))
        {
            return _escaped![slot];
        }

        var values = new object?[_cells.Length];
        for (var i = 0; i < values.Length; i++)
        {
            if (!GetBit(nulls, i))
            {
                values[i] = _cells[i].Read(record.Slice(_cellOffsets[i], _cells[i].Width), _strings);
            }
        }

        return new StructuralRow(values);
    }

    private int Allocate()
    {
        if (_freeHead >= 0)
        {
            var slot = _freeHead;
            _freeHead = Next(slot);
            return slot;
        }

        var fresh = _highWater++;
        var page = fresh / _rowsPerPage;
        var needed = (fresh % _rowsPerPage + 1) * _recordBytes;
        if (page == _pages.Count)
        {
            // Small first page, so a tiny view does not hold a megabyte; full pages after.
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

    // Releases a slot already unlinked from its bucket chain.
    private void Free(int slot)
    {
        var record = Record(slot);
        var nulls = record.Slice(NullsOffset, NullBytes(_cells.Length));
        if (GetBit(nulls, _cells.Length))
        {
            _escaped!.Remove(slot);
        }
        else if (_hasStrings)
        {
            for (var i = 0; i < _cells.Length; i++)
            {
                if (!GetBit(nulls, i))
                {
                    _cells[i].Release(record.Slice(_cellOffsets[i], _cells[i].Width), _strings);
                }
            }
        }

        SetWeight(slot, 0);
        BinaryPrimitives.WriteInt32LittleEndian(record[NextOffset..], _freeHead);
        _freeHead = slot;
        _count--;
    }

    private void Unlink(int slot)
    {
        var bucket = Hash(slot) & (_buckets.Length - 1);
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
            BinaryPrimitives.WriteInt32LittleEndian(Record(previous)[NextOffset..], next);
        }
    }

    private void Rehash(int size)
    {
        var buckets = new int[size];
        for (var slot = 0; slot < _highWater; slot++)
        {
            if (Weight(slot) != 0)
            {
                var bucket = Hash(slot) & (size - 1);
                BinaryPrimitives.WriteInt32LittleEndian(Record(slot)[NextOffset..], buckets[bucket] - 1);
                buckets[bucket] = slot + 1;
            }
        }

        _buckets = buckets;
    }

    private void CompactStrings()
    {
        var target = new PagedStrings();
        for (var slot = 0; slot < _highWater; slot++)
        {
            if (Weight(slot) == 0)
            {
                continue;
            }

            var record = Record(slot);
            var nulls = record.Slice(NullsOffset, NullBytes(_cells.Length));
            if (GetBit(nulls, _cells.Length))
            {
                continue;
            }

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

    private Span<byte> Record(int slot) =>
        _pages[slot / _rowsPerPage].AsSpan(slot % _rowsPerPage * _recordBytes, _recordBytes);

    private int Hash(int slot) => BinaryPrimitives.ReadInt32LittleEndian(Record(slot)[HashOffset..]);

    private int Next(int slot) => BinaryPrimitives.ReadInt32LittleEndian(Record(slot)[NextOffset..]);

    private long Weight(int slot) => BinaryPrimitives.ReadInt64LittleEndian(Record(slot)[WeightOffset..]);

    private void SetWeight(int slot, long weight) =>
        BinaryPrimitives.WriteInt64LittleEndian(Record(slot)[WeightOffset..], weight);

    // One bit per column, plus the escape bit after them.
    private static int NullBytes(int columns) => (columns + 1 + 7) / 8;

    private static bool GetBit(ReadOnlySpan<byte> bits, int i) => (bits[i >> 3] & (1 << (i & 7))) != 0;

    private static void SetBit(Span<byte> bits, int i) => bits[i >> 3] |= (byte)(1 << (i & 7));
}

/// <summary>
/// How one column's cells live in a <see cref="PagedZSetStore"/> record: a fixed-width encoding,
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

    public override object Read(ReadOnlySpan<byte> source, PagedStrings strings) =>
        Utf8String.FromBytes(Bytes(source, strings).ToArray());

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

    public void Release(int length)
    {
        Live -= length;
        Dead += length;
    }
}
