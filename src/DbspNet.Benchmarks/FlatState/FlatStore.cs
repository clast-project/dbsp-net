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

/// <summary>UTF-16 strings appended into paged char storage. A string never straddles a page.
/// Freed strings only count as garbage; the owner compacts when garbage passes half.</summary>
internal sealed unsafe class CharArena<TP> : IDisposable
    where TP : struct, IPageSource
{
    private PagedColumn<char, TP> _chars = new();

    public long Used { get; set; }

    public long Garbage { get; set; }

    public PagedColumn<char, TP> Chars => _chars;

    public bool NeedsCompaction => Used > (4L << 20) && Garbage * 2 > Used;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public long Append(ReadOnlySpan<char> s)
    {
        var page = PagedColumn<char, TP>.PageElems;
        if (s.Length > page)
        {
            throw new InvalidOperationException($"string of {s.Length} chars exceeds a page");
        }

        var off = Used & PagedColumn<char, TP>.Mask;
        if (off + s.Length > page)
        {
            Garbage += page - off;
            Used += page - off;
        }

        _chars.Ensure(Used + s.Length);
        s.CopyTo(new Span<char>(_chars.At(Used), s.Length));
        var start = Used;
        Used += s.Length;
        return start;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ReadOnlySpan<char> Get(long start, int length) => new(_chars.At(start), length);

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

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ulong Combine(ulong h, ulong c) => (h ^ c) * 0x9E3779B97F4A7C15UL + 0x632BE59BD9B4E019UL;

    public const ulong Seed = 0x243F6A8885A308D3UL;
}

/// <summary>A Z-set of rows (NL long cells, NS string cells, cell 0 the join key) in flat pages:
/// slot columns, a string arena, an open-addressing row index (whole-row hash → slot, for
/// consolidation and retraction) and a key index (key → doubly-linked chain of slots).</summary>
internal sealed unsafe class FlatTable<TP> : IDisposable
    where TP : struct, IPageSource
{
    private const int Empty = -1;
    private const int Tomb = -2;
    private const int NoKey = -2;
    private const int MinCap = PagedColumn<int, TP>.PageElems;

    public readonly int NL;
    public readonly int NS;
    public readonly PagedColumn<long, TP>[] L;
    public readonly PagedColumn<long, TP>[] SStart;
    public readonly PagedColumn<int, TP>[] SLen;
    public readonly PagedColumn<long, TP> W = new();
    public readonly PagedColumn<int, TP> Hash = new();
    public readonly PagedColumn<int, TP> Prev = new();
    public readonly PagedColumn<int, TP> Next = new();

    private PagedColumn<int, TP> _rt = new();
    private PagedColumn<long, TP> _kk = new();
    private PagedColumn<int, TP> _kh = new();

    public FlatTable(int nl, int ns, bool empty = true)
    {
        NL = nl;
        NS = ns;
        L = new PagedColumn<long, TP>[nl];
        SStart = new PagedColumn<long, TP>[ns];
        SLen = new PagedColumn<int, TP>[ns];
        for (var i = 0; i < nl; i++)
        {
            L[i] = new();
        }

        for (var i = 0; i < ns; i++)
        {
            SStart[i] = new();
            SLen[i] = new();
        }

        if (empty)
        {
            RtCap = MinCap;
            _rt.Ensure(RtCap);
            _rt.Fill(Empty, RtCap);
            KCap = MinCap;
            _kk.Ensure(KCap);
            _kh.Ensure(KCap);
            _kh.Fill(NoKey, KCap);
        }
    }

    public CharArena<TP> Arena { get; private set; } = new();

    public int High { get; set; }

    public int FreeHead { get; set; } = -1;

    public int Live { get; set; }

    public int RtCap { get; set; }

    public int RtCount { get; set; }

    public int RtTomb { get; set; }

    public int KCap { get; set; }

    public int KCount { get; set; }

    public PagedColumn<int, TP> RowTable => _rt;

    public PagedColumn<long, TP> KeyKeys => _kk;

    public PagedColumn<int, TP> KeyHeads => _kh;

    /// <summary>Every column, in checkpoint order.</summary>
    public List<ColumnPages> Columns()
    {
        var all = new List<ColumnPages>();
        for (var i = 0; i < NL; i++)
        {
            all.Add(ColumnPages.Of(L[i]));
        }

        for (var i = 0; i < NS; i++)
        {
            all.Add(ColumnPages.Of(SStart[i]));
            all.Add(ColumnPages.Of(SLen[i]));
        }

        all.Add(ColumnPages.Of(W));
        all.Add(ColumnPages.Of(Hash));
        all.Add(ColumnPages.Of(Prev));
        all.Add(ColumnPages.Of(Next));
        all.Add(ColumnPages.Of(Arena.Chars));
        all.Add(ColumnPages.Of(_rt));
        all.Add(ColumnPages.Of(_kk));
        all.Add(ColumnPages.Of(_kh));
        return all;
    }

    /// <summary>A column's pages seen untyped, for the checkpoint writer and reader.</summary>
    public sealed class ColumnPages(Func<int> pages, Func<int, nint> page, nuint pageBytes, Action<nint> adopt, Action<long> ensure)
    {
        public nuint PageBytes => pageBytes;

        public int Pages() => pages();

        public nint Page(int p) => page(p);

        public void Adopt(nint p) => adopt(p);

        public void Ensure(long elements) => ensure(elements);

        public static ColumnPages Of<T>(PagedColumn<T, TP> c)
            where T : unmanaged =>
            new(() => c.PageCount, p => (nint)c.Page(p), PagedColumn<T, TP>.PageBytes, p => c.AdoptMapped((byte*)p), c.Ensure);
    }

    public ulong RowHash(ReadOnlySpan<long> l, string[] s)
    {
        var h = FlatHash.Seed;
        for (var i = 0; i < NL; i++)
        {
            h = FlatHash.Combine(h, FlatHash.Long(l[i]));
        }

        for (var i = 0; i < NS; i++)
        {
            h = FlatHash.Combine(h, FlatHash.Str(s[i]));
        }

        return h;
    }

    public ulong RowHash(int slot)
    {
        var h = FlatHash.Seed;
        for (var i = 0; i < NL; i++)
        {
            h = FlatHash.Combine(h, FlatHash.Long(L[i][slot]));
        }

        for (var i = 0; i < NS; i++)
        {
            h = FlatHash.Combine(h, FlatHash.Str(Str(i, slot)));
        }

        return h;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ReadOnlySpan<char> Str(int col, int slot) => Arena.Get(SStart[col][slot], SLen[col][slot]);

    /// <summary>Head of the chain for <paramref name="key"/>, or -1.</summary>
    public int ChainHead(long key)
    {
        var mask = KCap - 1;
        var i = (int)(FlatHash.Long(key) & (ulong)mask);
        while (true)
        {
            var h = _kh[i];
            if (h == NoKey)
            {
                return -1;
            }

            if (_kk[i] == key)
            {
                return h;
            }

            i = (i + 1) & mask;
        }
    }

    /// <summary>Add <paramref name="w"/> copies of the row; a weight reaching zero frees it.</summary>
    public void Upsert(ReadOnlySpan<long> l, string[] s, long w)
    {
        var full = RowHash(l, s);
        var h = (int)full;
        var mask = RtCap - 1;
        var i = h & mask;
        var tombAt = -1;
        while (true)
        {
            var e = _rt[i];
            if (e == Empty)
            {
                break;
            }

            if (e == Tomb)
            {
                if (tombAt < 0)
                {
                    tombAt = i;
                }
            }
            else if (Hash[e] == h && Equal(e, l, s))
            {
                var nw = W[e] + w;
                W[e] = nw;
                if (nw == 0)
                {
                    RemoveAt(e, i);
                }

                return;
            }

            i = (i + 1) & mask;
        }

        if (w == 0)
        {
            return;
        }

        if ((RtCount + RtTomb + 1) * 2 > RtCap)
        {
            RehashRows(Math.Max(RtCap, (int)System.Numerics.BitOperations.RoundUpToPowerOf2((uint)(RtCount + 1) * 4)));
            tombAt = -1;
            mask = RtCap - 1;
            i = h & mask;
            while (_rt[i] != Empty)
            {
                i = (i + 1) & mask;
            }
        }

        var slot = AllocSlot();
        for (var c = 0; c < NL; c++)
        {
            L[c][slot] = l[c];
        }

        for (var c = 0; c < NS; c++)
        {
            SStart[c][slot] = Arena.Append(s[c]);
            SLen[c][slot] = s[c].Length;
        }

        W[slot] = w;
        Hash[slot] = h;
        if (tombAt >= 0)
        {
            i = tombAt;
            RtTomb--;
        }

        _rt[i] = slot;
        RtCount++;
        Live++;
        Link(slot, l[0]);
    }

    /// <summary>Copy every live string into a fresh arena and drop the old one.</summary>
    public void CompactIfNeeded()
    {
        if (!Arena.NeedsCompaction)
        {
            return;
        }

        var fresh = new CharArena<TP>();
        for (var slot = 0; slot < High; slot++)
        {
            if (W[slot] == 0)
            {
                continue;
            }

            for (var c = 0; c < NS; c++)
            {
                SStart[c][slot] = fresh.Append(Str(c, slot));
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
            var w = W[slot];
            if (w != 0)
            {
                sum += (ulong)w * RowHash(slot);
                rows++;
            }
        }

        return (sum, rows);
    }

    public void Dispose()
    {
        foreach (var c in L)
        {
            c.Dispose();
        }

        for (var i = 0; i < NS; i++)
        {
            SStart[i].Dispose();
            SLen[i].Dispose();
        }

        W.Dispose();
        Hash.Dispose();
        Prev.Dispose();
        Next.Dispose();
        Arena.Dispose();
        _rt.Dispose();
        _kk.Dispose();
        _kh.Dispose();
    }

    private bool Equal(int slot, ReadOnlySpan<long> l, string[] s)
    {
        for (var c = 0; c < NL; c++)
        {
            if (L[c][slot] != l[c])
            {
                return false;
            }
        }

        for (var c = 0; c < NS; c++)
        {
            if (!Str(c, slot).SequenceEqual(s[c]))
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
            FreeHead = Next[s];
            return s;
        }

        var slot = High++;
        var n = (long)High;
        foreach (var c in L)
        {
            c.Ensure(n);
        }

        for (var c = 0; c < NS; c++)
        {
            SStart[c].Ensure(n);
            SLen[c].Ensure(n);
        }

        W.Ensure(n);
        Hash.Ensure(n);
        Prev.Ensure(n);
        Next.Ensure(n);
        return slot;
    }

    private void RemoveAt(int slot, int tableIndex)
    {
        _rt[tableIndex] = Tomb;
        RtTomb++;
        RtCount--;
        Unlink(slot);
        for (var c = 0; c < NS; c++)
        {
            Arena.Garbage += SLen[c][slot];
        }

        W[slot] = 0;
        Next[slot] = FreeHead;
        FreeHead = slot;
        Live--;
    }

    private void Link(int slot, long key)
    {
        var ki = KeySlot(key, create: true);
        var head = _kh[ki];
        Next[slot] = head;
        Prev[slot] = -1;
        if (head >= 0)
        {
            Prev[head] = slot;
        }

        _kh[ki] = slot;
    }

    private void Unlink(int slot)
    {
        var p = Prev[slot];
        var n = Next[slot];
        if (p >= 0)
        {
            Next[p] = n;
        }
        else
        {
            _kh[KeySlot(L[0][slot], create: false)] = n;
        }

        if (n >= 0)
        {
            Prev[n] = p;
        }
    }

    private int KeySlot(long key, bool create)
    {
        if (create && (KCount + 1) * 2 > KCap)
        {
            RehashKeys(KCap * 2);
        }

        var mask = KCap - 1;
        var i = (int)(FlatHash.Long(key) & (ulong)mask);
        while (true)
        {
            if (_kh[i] == NoKey)
            {
                if (!create)
                {
                    throw new InvalidOperationException("key missing from key index");
                }

                _kk[i] = key;
                _kh[i] = -1;
                KCount++;
                return i;
            }

            if (_kk[i] == key)
            {
                return i;
            }

            i = (i + 1) & mask;
        }
    }

    private void RehashRows(int cap)
    {
        var old = _rt;
        var oldCap = RtCap;
        _rt = new PagedColumn<int, TP>();
        _rt.Ensure(cap);
        _rt.Fill(Empty, cap);
        RtCap = cap;
        RtTomb = 0;
        var mask = cap - 1;
        for (long j = 0; j < oldCap; j++)
        {
            var e = old[j];
            if (e < 0)
            {
                continue;
            }

            var i = Hash[e] & mask;
            while (_rt[i] != Empty)
            {
                i = (i + 1) & mask;
            }

            _rt[i] = e;
        }

        old.Dispose();
    }

    private void RehashKeys(int cap)
    {
        var oldK = _kk;
        var oldH = _kh;
        var oldCap = KCap;
        _kk = new PagedColumn<long, TP>();
        _kh = new PagedColumn<int, TP>();
        _kk.Ensure(cap);
        _kh.Ensure(cap);
        _kh.Fill(NoKey, cap);
        KCap = cap;
        KCount = 0;
        var mask = cap - 1;
        for (long j = 0; j < oldCap; j++)
        {
            var head = oldH[j];
            if (head < 0)
            {
                // Never used, or a key whose chain emptied: drop it.
                continue;
            }

            var key = oldK[j];
            var i = (int)(FlatHash.Long(key) & (ulong)mask);
            while (_kh[i] != NoKey)
            {
                i = (i + 1) & mask;
            }

            _kk[i] = key;
            _kh[i] = head;
            KCount++;
        }

        oldK.Dispose();
        oldH.Dispose();
    }
}
