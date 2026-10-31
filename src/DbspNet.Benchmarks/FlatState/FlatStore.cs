// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace DbspNet.Benchmarks.FlatState;

// The flat store behind arms 2 and 3 of the flat-state probe (docs/design-flat-state-probe.md).
// Everything is fixed-size pages of unmanaged values reached through raw pointers; the two arms
// differ ONLY in where a page comes from (IPageSource): pinned managed arrays on the POH, which
// the GC owns but never traces into, or NativeMemory, which it never sees. The code that reads
// and writes the pages is the same JIT-specialised code in both.

/// <summary>Where a page's memory comes from.</summary>
internal unsafe interface IPageSource
{
    static abstract string Name { get; }

    static abstract byte* Alloc(nuint bytes, out object? owner);

    static abstract void Free(byte* p, nuint bytes, object? owner);
}

/// <summary>Arm 2: pinned, uninitialised managed arrays (the pinned-object heap). No references
/// inside, so the GC never traces them, but they count against its budget and it frees them.</summary>
internal unsafe struct PohPages : IPageSource
{
    public static string Name => "poh";

    public static byte* Alloc(nuint bytes, out object? owner)
    {
        var a = GC.AllocateUninitializedArray<byte>(checked((int)bytes), pinned: true);
        owner = a;
        return (byte*)Unsafe.AsPointer(ref MemoryMarshal.GetArrayDataReference(a));
    }

    public static void Free(byte* p, nuint bytes, object? owner)
    {
        // Dropping the owner reference is the free; the GC reclaims the array.
    }
}

/// <summary>Arm 3: NativeMemory, invisible to the GC.</summary>
internal unsafe struct NativePages : IPageSource
{
    public static string Name => "native";

    public static byte* Alloc(nuint bytes, out object? owner)
    {
        owner = null;
        Interlocked.Add(ref NativeAccount.Bytes, (long)bytes);
        return (byte*)NativeMemory.Alloc(bytes);
    }

    public static void Free(byte* p, nuint bytes, object? owner)
    {
        Interlocked.Add(ref NativeAccount.Bytes, -(long)bytes);
        NativeMemory.Free(p);
    }
}

internal static class NativeAccount
{
    public static long Bytes;
}

/// <summary>Marks a page that points into a memory-mapped checkpoint: never freed by the source.</summary>
internal sealed class MappedPage
{
    public static readonly MappedPage Instance = new();
}

/// <summary>A growable column of <typeparamref name="T"/> in fixed pages of 2^16 elements. Growth
/// adds a page and never copies, so a column never needs one contiguous block.</summary>
internal sealed unsafe class PagedColumn<T, TP> : IDisposable
    where T : unmanaged
    where TP : struct, IPageSource
{
    public const int Shift = 16;
    public const int PageElems = 1 << Shift;
    public const long Mask = PageElems - 1;

    private nint[] _pages = new nint[8];
    private object?[] _owners = new object?[8];
    private int _count;

    public static nuint PageBytes => (nuint)(PageElems * sizeof(T));

    public int PageCount => _count;

    public long Capacity => (long)_count << Shift;

    public ref T this[long i]
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => ref ((T*)_pages[(int)(i >> Shift)])[i & Mask];
    }

    public T* Page(int p) => (T*)_pages[p];

    public T* At(long i) => (T*)_pages[(int)(i >> Shift)] + (i & Mask);

    public void Ensure(long n)
    {
        while (Capacity < n)
        {
            AddPage(TP.Alloc(PageBytes, out var owner), owner);
        }
    }

    public void AdoptMapped(byte* p) => AddPage(p, MappedPage.Instance);

    /// <summary>Fill elements [0, n) with <paramref name="value"/>, page by page.</summary>
    public void Fill(T value, long n)
    {
        for (long at = 0; at < n; at += PageElems)
        {
            new Span<T>(Page((int)(at >> Shift)), (int)Math.Min(PageElems, n - at)).Fill(value);
        }
    }

    public void Dispose()
    {
        for (var i = 0; i < _count; i++)
        {
            if (_owners[i] is not MappedPage)
            {
                TP.Free((byte*)_pages[i], PageBytes, _owners[i]);
            }

            _owners[i] = null;
            _pages[i] = 0;
        }

        _count = 0;
    }

    private void AddPage(byte* p, object? owner)
    {
        if (_count == _pages.Length)
        {
            Array.Resize(ref _pages, _count * 2);
            Array.Resize(ref _owners, _count * 2);
        }

        _pages[_count] = (nint)p;
        _owners[_count] = owner;
        _count++;
    }
}

/// <summary>Strings — UTF-16 chars or UTF-8 bytes — appended into paged storage. A string never straddles a page.
/// Freed strings only count as garbage; the owner compacts when garbage passes half.</summary>
internal sealed unsafe class StrArena<TC, TP> : IDisposable
    where TC : unmanaged
    where TP : struct, IPageSource
{
    private PagedColumn<TC, TP> _chars = new();

    public long Used { get; set; }

    public long Garbage { get; set; }

    public PagedColumn<TC, TP> Chars => _chars;

    public bool NeedsCompaction => Used > (4L << 20) && Garbage * 2 > Used;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public long Append(ReadOnlySpan<TC> s)
    {
        var page = PagedColumn<TC, TP>.PageElems;
        if (s.Length > page)
        {
            throw new InvalidOperationException($"string of {s.Length} elements exceeds a page");
        }

        var off = Used & PagedColumn<TC, TP>.Mask;
        if (off + s.Length > page)
        {
            Garbage += page - off;
            Used += page - off;
        }

        _chars.Ensure(Used + s.Length);
        s.CopyTo(new Span<TC>(_chars.At(Used), s.Length));
        var start = Used;
        Used += s.Length;
        return start;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ReadOnlySpan<TC> Get(long start, int length) => new(_chars.At(start), length);

    /// <summary>Forget the contents but keep the pages (an output batch reused every tick).</summary>
    public void Reset()
    {
        Used = 0;
        Garbage = 0;
    }

    public void Dispose() => _chars.Dispose();
}

/// <summary>Hashes shared by every arm and the engine sink, so checksums agree across arms.</summary>
internal static class FlatHash
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ulong Mix(ulong x)
    {
        x ^= x >> 33;
        x *= 0xff51afd7ed558ccdUL;
        x ^= x >> 33;
        x *= 0xc4ceb9fe1a85ec53UL;
        x ^= x >> 33;
        return x;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ulong Long(long v) => Mix((ulong)v);

    public static ulong Str(ReadOnlySpan<char> s)
    {
        var h = 0xcbf29ce484222325UL;
        foreach (var c in s)
        {
            h = (h ^ c) * 0x100000001b3UL;
        }

        return Mix(h ^ (ulong)s.Length);
    }

    /// <summary>UTF-8 bytes hashed as code units: equal to <see cref="Str(ReadOnlySpan{char})"/>
    /// for ASCII, which is all the Nexmark generator emits (the cross-arm checksum checks it).</summary>
    public static ulong Str(ReadOnlySpan<byte> s)
    {
        var h = 0xcbf29ce484222325UL;
        foreach (var c in s)
        {
            h = (h ^ c) * 0x100000001b3UL;
        }

        return Mix(h ^ (ulong)s.Length);
    }

    /// <summary>Either string representation, by element type (the JIT keeps one branch).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ulong Str<TC>(ReadOnlySpan<TC> s)
        where TC : unmanaged =>
        typeof(TC) == typeof(char)
            ? Str(MemoryMarshal.Cast<TC, char>(s))
            : Str(MemoryMarshal.Cast<TC, byte>(s));

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ulong Combine(ulong h, ulong c) => (h ^ c) * 0x9E3779B97F4A7C15UL + 0x632BE59BD9B4E019UL;

    public const ulong Seed = 0x243F6A8885A308D3UL;
}

/// <summary>Fixed-size records in pages of 2^13 records. A row's cells, weight and chain links
/// sit together, so touching a row costs one or two cache lines, as a managed object does.</summary>
internal sealed unsafe class RecordPages<TP> : IDisposable
    where TP : struct, IPageSource
{
    public const int Shift = 13;
    public const int PerPage = 1 << Shift;
    private const int Mask = PerPage - 1;

    private nint[] _pages = new nint[8];
    private object?[] _owners = new object?[8];
    private int _count;

    public RecordPages(int recordBytes) => RecordBytes = recordBytes;

    public int RecordBytes { get; }

    public nuint PageBytes => (nuint)(PerPage * RecordBytes);

    public int PageCount => _count;

    public long Capacity => (long)_count << Shift;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public byte* Rec(int slot) => (byte*)_pages[slot >> Shift] + ((slot & Mask) * RecordBytes);

    public byte* Page(int p) => (byte*)_pages[p];

    public void Ensure(long n)
    {
        while (Capacity < n)
        {
            AddPage(TP.Alloc(PageBytes, out var owner), owner);
        }
    }

    public void AdoptMapped(byte* p) => AddPage(p, MappedPage.Instance);

    public void Dispose()
    {
        for (var i = 0; i < _count; i++)
        {
            if (_owners[i] is not MappedPage)
            {
                TP.Free((byte*)_pages[i], PageBytes, _owners[i]);
            }

            _owners[i] = null;
            _pages[i] = 0;
        }

        _count = 0;
    }

    private void AddPage(byte* p, object? owner)
    {
        if (_count == _pages.Length)
        {
            Array.Resize(ref _pages, _count * 2);
            Array.Resize(ref _owners, _count * 2);
        }

        _pages[_count] = (nint)p;
        _owners[_count] = owner;
        _count++;
    }
}

/// <summary>A per-key row-index entry: the slot, and its row hash so a mismatch never touches
/// the record.</summary>
internal struct RowEntry
{
    public int Slot;
    public int Hash;
}

/// <summary>A key-index entry: the key, the head and length of its chain of slots, and — once the
/// chain has grown long — where its own row index sits in the sub-table pool.</summary>
internal struct KeyEntry
{
    public long Key;
    public int Head;
    public int Count;
    public int SubOff;
    public int SubCap;
}

/// <summary>A column's pages seen untyped, for the checkpoint writer and reader.</summary>
internal sealed class ColumnPages(Func<int> pages, Func<int, nint> page, nuint pageBytes, Action<nint> adopt, Action<int> addPages)
{
    public nuint PageBytes => pageBytes;

    public int Pages() => pages();

    public nint Page(int p) => page(p);

    public void Adopt(nint p) => adopt(p);

    public void AddPages(int n) => addPages(n);

    public static unsafe ColumnPages Of<T, TP>(PagedColumn<T, TP> c)
        where T : unmanaged
        where TP : struct, IPageSource =>
        new(() => c.PageCount, p => (nint)c.Page(p), PagedColumn<T, TP>.PageBytes, p => c.AdoptMapped((byte*)p),
            n => c.Ensure((long)(c.PageCount + n) << PagedColumn<T, TP>.Shift));

    public static unsafe ColumnPages Of<TP>(RecordPages<TP> c)
        where TP : struct, IPageSource =>
        new(() => c.PageCount, p => (nint)c.Page(p), c.PageBytes, p => c.AdoptMapped((byte*)p),
            n => c.Ensure((long)(c.PageCount + n) << RecordPages<TP>.Shift));
}

/// <summary>A Z-set of rows (NL long cells, NS string cells, cell 0 the join key) in flat pages:
/// one record per row (weight, cells, string handles, chain links, row hash), a string arena and
/// a key index (key → doubly-linked chain of slots).</summary>
/// <remarks>
/// Finding a row to consolidate or retract walks its key's chain while the chain is short; past
/// <see cref="ChainLimit"/> rows the key gets its own open-addressing row index, a power-of-two
/// block from a pooled sub-table store — the flat counterpart of arm 1's one-row-inline /
/// Dictionary-when-many bucket. Two earlier versions were slower than arm 1 for reasons of
/// structure, not memory model: column-per-page records (one row touched ~14 pages), then one
/// global whole-row index (half the bids go to a few hot auctions, and each was a random miss
/// into a table over every row).
/// <para>Record layout: weight (8) · longs (8·NL) · string starts (8·NS) · string lengths
/// (4·NS) · prev (4) · next (4) · row hash (4), rounded up to 8 bytes.</para>
/// </remarks>
internal sealed unsafe class FlatTable<TP, TC> : IDisposable
    where TP : struct, IPageSource
    where TC : unmanaged, IEquatable<TC>
{
    private const int Empty = -1;
    private const int NoKey = -2;
    private const int MinCap = PagedColumn<int, TP>.PageElems;
    private const int ChainLimit = 16;
    private const int SubClasses = PagedColumn<RowEntry, TP>.Shift + 1;

    public readonly int NL;
    public readonly int NS;
    private readonly int _offS;
    private readonly int _offN;
    private readonly int _offP;
    private readonly int _offX;
    private readonly int _offH;
    private readonly RecordPages<TP> _rec;
    private readonly PagedColumn<RowEntry, TP> _sub = new();
    private readonly int[] _subFree = new int[SubClasses];
    private PagedColumn<KeyEntry, TP> _kt = new();

    public FlatTable(int nl, int ns, bool empty = true)
    {
        NL = nl;
        NS = ns;
        _offS = 8 + (8 * nl);
        _offN = _offS + (8 * ns);
        _offP = _offN + (4 * ns);
        _offX = _offP + 4;
        _offH = _offX + 4;
        _rec = new RecordPages<TP>((_offH + 4 + 7) & ~7);
        Array.Fill(_subFree, -1);
        if (empty)
        {
            KCap = MinCap;
            _kt.Ensure(KCap);
            _kt.Fill(new KeyEntry { Head = NoKey }, KCap);
        }
    }

    public StrArena<TC, TP> Arena { get; private set; } = new();

    public int High { get; private set; }

    public int FreeHead { get; private set; } = -1;

    public int Live { get; private set; }

    public int KCap { get; private set; }

    public int KCount { get; private set; }

    public long SubUsed { get; private set; }

    /// <summary>Every page-backed part, in checkpoint order.</summary>
    public List<ColumnPages> Columns() =>
        [ColumnPages.Of(_rec), ColumnPages.Of(Arena.Chars), ColumnPages.Of(_sub), ColumnPages.Of(_kt)];

    /// <summary>The scalars a checkpoint must carry besides the pages.</summary>
    public long[] SaveScalars() =>
        [High, FreeHead, Live, Arena.Used, Arena.Garbage, KCap, KCount, SubUsed, .. _subFree.Select(f => (long)f)];

    public void LoadScalars(ReadOnlySpan<long> v)
    {
        High = (int)v[0];
        FreeHead = (int)v[1];
        Live = (int)v[2];
        Arena.Used = v[3];
        Arena.Garbage = v[4];
        KCap = (int)v[5];
        KCount = (int)v[6];
        SubUsed = v[7];
        for (var i = 0; i < SubClasses; i++)
        {
            _subFree[i] = (int)v[8 + i];
        }
    }

    public static int ScalarCount => 8 + SubClasses;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public byte* Rec(int slot) => _rec.Rec(slot);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static long Weight(byte* r) => *(long*)r;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static long Long(byte* r, int c) => ((long*)(r + 8))[c];

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ReadOnlySpan<TC> Str(byte* r, int c) => Arena.Get(((long*)(r + _offS))[c], ((int*)(r + _offN))[c]);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int NextOf(byte* r) => *(int*)(r + _offX);

    public ulong RowHash(ReadOnlySpan<long> l, ReadOnlyMemory<TC>[] s)
    {
        var h = FlatHash.Seed;
        for (var i = 0; i < NL; i++)
        {
            h = FlatHash.Combine(h, FlatHash.Long(l[i]));
        }

        for (var i = 0; i < NS; i++)
        {
            h = FlatHash.Combine(h, FlatHash.Str(s[i].Span));
        }

        return h;
    }

    public ulong RowHash(byte* r)
    {
        var h = FlatHash.Seed;
        for (var i = 0; i < NL; i++)
        {
            h = FlatHash.Combine(h, FlatHash.Long(Long(r, i)));
        }

        for (var i = 0; i < NS; i++)
        {
            h = FlatHash.Combine(h, FlatHash.Str(Str(r, i)));
        }

        return h;
    }

    /// <summary>Head of the chain for <paramref name="key"/>, or -1.</summary>
    public int ChainHead(long key)
    {
        var mask = KCap - 1;
        var i = (int)(FlatHash.Long(key) & (ulong)mask);
        while (true)
        {
            ref var e = ref _kt[i];
            if (e.Head == NoKey)
            {
                return -1;
            }

            if (e.Key == key)
            {
                return e.Head;
            }

            i = (i + 1) & mask;
        }
    }

    /// <summary>Add <paramref name="w"/> copies of the row; a weight reaching zero frees it.</summary>
    public void Upsert(ReadOnlySpan<long> l, ReadOnlyMemory<TC>[] s, long w)
    {
        var h = (int)RowHash(l, s);
        ref var k = ref KeySlot(l[0]);
        if (k.SubCap == 0)
        {
            for (var slot = k.Head; slot >= 0;)
            {
                var r = Rec(slot);
                if (*(int*)(r + _offH) == h && Equal(r, l, s))
                {
                    var nw = Weight(r) + w;
                    *(long*)r = nw;
                    if (nw == 0)
                    {
                        Unlink(ref k, r);
                        Free(slot, r);
                    }

                    return;
                }

                slot = NextOf(r);
            }

            if (w == 0)
            {
                return;
            }

            Insert(l, s, w, h, ref k);
            if (k.Count >= ChainLimit)
            {
                Promote(ref k);
            }

            return;
        }

        var mask = k.SubCap - 1;
        var i = h & mask;
        while (true)
        {
            ref var e = ref _sub[k.SubOff + i];
            if (e.Slot == Empty)
            {
                break;
            }

            if (e.Hash == h && Equal(Rec(e.Slot), l, s))
            {
                var r = Rec(e.Slot);
                var nw = Weight(r) + w;
                *(long*)r = nw;
                if (nw == 0)
                {
                    var slot = e.Slot;
                    SubDelete(k.SubOff, mask, i);
                    Unlink(ref k, r);
                    Free(slot, r);
                }

                return;
            }

            i = (i + 1) & mask;
        }

        if (w == 0)
        {
            return;
        }

        if ((k.Count + 1) * 2 > k.SubCap)
        {
            Resize(ref k, k.SubCap * 2);
            mask = k.SubCap - 1;
            i = h & mask;
            while (_sub[k.SubOff + i].Slot != Empty)
            {
                i = (i + 1) & mask;
            }
        }

        var added = Insert(l, s, w, h, ref k);
        _sub[k.SubOff + i] = new RowEntry { Slot = added, Hash = h };
    }

    /// <summary>Copy every live string into a fresh arena and drop the old one.</summary>
    public void CompactIfNeeded()
    {
        if (!Arena.NeedsCompaction)
        {
            return;
        }

        var fresh = new StrArena<TC, TP>();
        for (var slot = 0; slot < High; slot++)
        {
            var r = Rec(slot);
            if (Weight(r) == 0)
            {
                continue;
            }

            for (var c = 0; c < NS; c++)
            {
                ((long*)(r + _offS))[c] = fresh.Append(Str(r, c));
            }
        }

        Arena.Dispose();
        Arena = fresh;
    }

    /// <summary>Σ weight × row hash over live rows — equal across arms for the same state.</summary>
    public (ulong Sum, long Rows) Checksum()
    {
        ulong sum = 0;
        long rows = 0;
        for (var slot = 0; slot < High; slot++)
        {
            var r = Rec(slot);
            var w = Weight(r);
            if (w != 0)
            {
                sum += (ulong)w * RowHash(r);
                rows++;
            }
        }

        return (sum, rows);
    }

    public void Dispose()
    {
        _rec.Dispose();
        Arena.Dispose();
        _sub.Dispose();
        _kt.Dispose();
    }

    private bool Equal(byte* r, ReadOnlySpan<long> l, ReadOnlyMemory<TC>[] s)
    {
        for (var c = 0; c < NL; c++)
        {
            if (Long(r, c) != l[c])
            {
                return false;
            }
        }

        for (var c = 0; c < NS; c++)
        {
            if (!Str(r, c).SequenceEqual(s[c].Span))
            {
                return false;
            }
        }

        return true;
    }

    private int AllocSlot()
    {
        if (FreeHead >= 0)
        {
            var s = FreeHead;
            FreeHead = NextOf(Rec(s));
            return s;
        }

        var slot = High++;
        if (High > _rec.Capacity)
        {
            _rec.Ensure(High);
        }

        return slot;
    }

    private int Insert(ReadOnlySpan<long> l, ReadOnlyMemory<TC>[] s, long w, int h, ref KeyEntry k)
    {
        var slot = AllocSlot();
        var rec = Rec(slot);
        *(long*)rec = w;
        for (var c = 0; c < NL; c++)
        {
            ((long*)(rec + 8))[c] = l[c];
        }

        for (var c = 0; c < NS; c++)
        {
            ((long*)(rec + _offS))[c] = Arena.Append(s[c].Span);
            ((int*)(rec + _offN))[c] = s[c].Length;
        }

        *(int*)(rec + _offH) = h;
        var head = k.Head;
        *(int*)(rec + _offX) = head;
        *(int*)(rec + _offP) = -1;
        if (head >= 0)
        {
            *(int*)(Rec(head) + _offP) = slot;
        }

        k.Head = slot;
        k.Count++;
        Live++;
        return slot;
    }

    /// <summary>The key's chain has grown long: give it its own row index.</summary>
    private void Promote(ref KeyEntry k) => Resize(ref k, (int)System.Numerics.BitOperations.RoundUpToPowerOf2((uint)k.Count * 4));

    /// <summary>Move the key's rows into a fresh sub-table of <paramref name="cap"/> entries.</summary>
    private void Resize(ref KeyEntry k, int cap)
    {
        if (cap > PagedColumn<RowEntry, TP>.PageElems)
        {
            throw new InvalidOperationException($"key {k.Key} has {k.Count} rows: a sub-table would span pages");
        }

        if (k.SubCap > 0)
        {
            SubFree(k.SubOff, k.SubCap);
        }

        var off = SubAlloc(cap);
        var mask = cap - 1;
        for (var slot = k.Head; slot >= 0;)
        {
            var r = Rec(slot);
            var h = *(int*)(r + _offH);
            var i = h & mask;
            while (_sub[off + i].Slot != Empty)
            {
                i = (i + 1) & mask;
            }

            _sub[off + i] = new RowEntry { Slot = slot, Hash = h };
            slot = NextOf(r);
        }

        k.SubOff = off;
        k.SubCap = cap;
    }

    private int SubAlloc(int cap)
    {
        var cls = System.Numerics.BitOperations.Log2((uint)cap);
        int off;
        if (_subFree[cls] >= 0)
        {
            off = _subFree[cls];
            _subFree[cls] = _sub[off].Slot;
        }
        else
        {
            // Power-of-two blocks aligned to their size never straddle a page.
            off = checked((int)((SubUsed + cap - 1) & ~(long)(cap - 1)));
            SubUsed = off + cap;
            _sub.Ensure(SubUsed);
        }

        new Span<RowEntry>(_sub.At(off), cap).Fill(new RowEntry { Slot = Empty });
        return off;
    }

    private void SubFree(int off, int cap)
    {
        var cls = System.Numerics.BitOperations.Log2((uint)cap);
        _sub[off].Slot = _subFree[cls];
        _subFree[cls] = off;
    }

    /// <summary>Linear-probing delete with backward shift, so the sub-tables need no tombstones.</summary>
    private void SubDelete(int off, int mask, int i)
    {
        var j = i;
        while (true)
        {
            j = (j + 1) & mask;
            var e = _sub[off + j];
            if (e.Slot == Empty)
            {
                break;
            }

            var home = e.Hash & mask;
            var stays = i <= j ? (home > i && home <= j) : (home > i || home <= j);
            if (!stays)
            {
                _sub[off + i] = e;
                i = j;
            }
        }

        _sub[off + i].Slot = Empty;
    }

    private void Free(int slot, byte* r)
    {
        for (var c = 0; c < NS; c++)
        {
            Arena.Garbage += ((int*)(r + _offN))[c];
        }

        *(long*)r = 0;
        *(int*)(r + _offX) = FreeHead;
        FreeHead = slot;
        Live--;
    }

    private void Unlink(ref KeyEntry k, byte* r)
    {
        var p = *(int*)(r + _offP);
        var n = *(int*)(r + _offX);
        if (p >= 0)
        {
            *(int*)(Rec(p) + _offX) = n;
        }
        else
        {
            k.Head = n;
        }

        if (n >= 0)
        {
            *(int*)(Rec(n) + _offP) = p;
        }

        k.Count--;
    }

    private ref KeyEntry KeySlot(long key)
    {
        if ((KCount + 1) * 2 > KCap)
        {
            RehashKeys(KCap * 2);
        }

        var mask = KCap - 1;
        var i = (int)(FlatHash.Long(key) & (ulong)mask);
        while (true)
        {
            ref var e = ref _kt[i];
            if (e.Head == NoKey)
            {
                e = new KeyEntry { Key = key, Head = -1 };
                KCount++;
                return ref e;
            }

            if (e.Key == key)
            {
                return ref e;
            }

            i = (i + 1) & mask;
        }
    }

    private void RehashKeys(int cap)
    {
        var old = _kt;
        var oldCap = KCap;
        _kt = new PagedColumn<KeyEntry, TP>();
        _kt.Ensure(cap);
        _kt.Fill(new KeyEntry { Head = NoKey }, cap);
        KCap = cap;
        KCount = 0;
        var mask = cap - 1;
        for (long j = 0; j < oldCap; j++)
        {
            var e = old[j];
            if (e.Head < 0)
            {
                // Never used, or a key whose chain emptied: drop it, and its sub-table.
                if (e.Head == -1 && e.SubCap > 0)
                {
                    SubFree(e.SubOff, e.SubCap);
                }

                continue;
            }

            var i = (int)(FlatHash.Long(e.Key) & (ulong)mask);
            while (_kt[i].Head != NoKey)
            {
                i = (i + 1) & mask;
            }

            _kt[i] = e;
            KCount++;
        }

        old.Dispose();
    }
}
