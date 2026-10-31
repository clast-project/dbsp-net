// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Benchmarks.FlatState;

// Nexmark q20 written by hand three times over the same algorithm:
//
//   SELECT <all 7 bid cells, 9 auction cells> FROM bid b JOIN auction a ON b.auction = a.id
//   WHERE a.category = 10
//
// Both sides are integrated (the bid side unfiltered, the auction side filtered), and a tick's
// output is  Δ(A⋈B) = A_old ⋈ ΔB + ΔA ⋈ B_new,  computed as: probe A with ΔB, fold ΔB into B,
// probe B with ΔA, fold ΔA into A. Only the representation of the state differs between arms.

/// <summary>One shard's q20 state and its per-tick output.</summary>
internal interface IQ20Worker : IDisposable
{
    void Step(List<(object?[] Row, long W)> bids, List<(object?[] Row, long W)> auctions);

    /// <summary>Σ w × output-row hash over this tick's output, and Σ w.</summary>
    (ulong Sum, long Rows) SinkTick();

    (ulong Sum, long Rows) StateChecksum();

    long ProbeOne(long auction);

    void Checkpoint(string path);
}

internal static class Q20Cells
{
    public const int BidLongs = 4;     // auction, bidder, price, date_time
    public const int BidStrs = 3;      // channel, url, extra
    public const int AuctionLongs = 7; // id, initial_bid, reserve, date_time, expires, seller, category
    public const int AuctionStrs = 3;  // item_name, description, extra

    public static void Bid(object?[] r, Span<long> l, string[] s)
    {
        ReadBidLongs(r, l);
        s[0] = Decode(r[3]);
        s[1] = Decode(r[4]);
        s[2] = Decode(r[6]);
    }

    public static void Auction(object?[] r, Span<long> l, string[] s)
    {
        ReadAuctionLongs(r, l);
        s[0] = Decode(r[1]);
        s[1] = Decode(r[2]);
        s[2] = Decode(r[9]);
    }

    /// <summary>The flat arms' view of a row: string cells as memory of <typeparamref name="TC"/>,
    /// never copied here (the table copies what it keeps).</summary>
    public static void Bid<TC>(object?[] r, Span<long> l, ReadOnlyMemory<TC>[] s)
        where TC : unmanaged
    {
        ReadBidLongs(r, l);
        s[0] = Cell<TC>(r[3]);
        s[1] = Cell<TC>(r[4]);
        s[2] = Cell<TC>(r[6]);
    }

    public static void Auction<TC>(object?[] r, Span<long> l, ReadOnlyMemory<TC>[] s)
        where TC : unmanaged
    {
        ReadAuctionLongs(r, l);
        s[0] = Cell<TC>(r[1]);
        s[1] = Cell<TC>(r[2]);
        s[2] = Cell<TC>(r[9]);
    }

    /// <summary>A string cell as a managed string: shared as-is, or decoded from UTF-8 input
    /// (which allocates — the cost a connector delivering bytes would impose).</summary>
    private static string Decode(object? o) =>
        o as string ?? System.Text.Encoding.UTF8.GetString(((Utf8String)o!).Span);

    private static ReadOnlyMemory<TC> Cell<TC>(object? o)
        where TC : unmanaged
    {
        if (typeof(TC) == typeof(char))
        {
            var m = ((string)o!).AsMemory();
            return Unsafe.As<ReadOnlyMemory<char>, ReadOnlyMemory<TC>>(ref m);
        }

        var u = ((Utf8String)o!).Memory;
        return Unsafe.As<ReadOnlyMemory<byte>, ReadOnlyMemory<TC>>(ref u);
    }

    private static void ReadBidLongs(object?[] r, Span<long> l)
    {
        l[0] = (long)r[0]!;
        l[1] = (long)r[1]!;
        l[2] = (long)r[2]!;
        l[3] = ((Timestamp)r[5]!).Microseconds;
    }

    private static void ReadAuctionLongs(object?[] r, Span<long> l)
    {
        l[0] = (long)r[0]!;
        l[1] = (long)r[3]!;
        l[2] = (long)r[4]!;
        l[3] = ((Timestamp)r[5]!).Microseconds;
        l[4] = ((Timestamp)r[6]!).Microseconds;
        l[5] = (long)r[7]!;
        l[6] = (long)r[8]!;
    }

    public static bool InFilter(object?[] auction) => (long)auction[8]! == 10;

    /// <summary>The q20 output row's hash, cells in SELECT order. Every arm and the engine sink
    /// hash through this, so their checksums are comparable.</summary>
    public static ulong OutHash<TC>(
        long auction, long bidder, long price, ReadOnlySpan<TC> channel, ReadOnlySpan<TC> url,
        long bidTime, ReadOnlySpan<TC> bidExtra, ReadOnlySpan<TC> itemName, ReadOnlySpan<TC> description,
        long initialBid, long reserve, long auctionTime, long expires, long seller, long category,
        ReadOnlySpan<TC> auctionExtra)
        where TC : unmanaged
    {
        var h = FlatHash.Seed;
        h = FlatHash.Combine(h, FlatHash.Long(auction));
        h = FlatHash.Combine(h, FlatHash.Long(bidder));
        h = FlatHash.Combine(h, FlatHash.Long(price));
        h = FlatHash.Combine(h, FlatHash.Str(channel));
        h = FlatHash.Combine(h, FlatHash.Str(url));
        h = FlatHash.Combine(h, FlatHash.Long(bidTime));
        h = FlatHash.Combine(h, FlatHash.Str(bidExtra));
        h = FlatHash.Combine(h, FlatHash.Str(itemName));
        h = FlatHash.Combine(h, FlatHash.Str(description));
        h = FlatHash.Combine(h, FlatHash.Long(initialBid));
        h = FlatHash.Combine(h, FlatHash.Long(reserve));
        h = FlatHash.Combine(h, FlatHash.Long(auctionTime));
        h = FlatHash.Combine(h, FlatHash.Long(expires));
        h = FlatHash.Combine(h, FlatHash.Long(seller));
        h = FlatHash.Combine(h, FlatHash.Long(category));
        h = FlatHash.Combine(h, FlatHash.Str(auctionExtra));
        return h;
    }

    /// <summary>The same row hash the flat tables use, for arm 1's state checksum.</summary>
    public static ulong RowHash(ReadOnlySpan<long> l, ReadOnlySpan<string> s)
    {
        var h = FlatHash.Seed;
        foreach (var v in l)
        {
            h = FlatHash.Combine(h, FlatHash.Long(v));
        }

        foreach (var v in s)
        {
            h = FlatHash.Combine(h, FlatHash.Str(v));
        }

        return h;
    }
}

// ---------------------------------------------------------------------------------------------
// Arm 1: idiomatic managed objects. A row is a class with typed fields (strings by reference,
// shared with the input), state is key → bucket, a bucket holds one row inline or a Dictionary
// (the engine's inline-single-entry shape). Output is a reused list of (bid, auction, w) structs.
// ---------------------------------------------------------------------------------------------

internal sealed class BidRow : IEquatable<BidRow>
{
    public readonly long Auction;
    public readonly long Bidder;
    public readonly long Price;
    public readonly long DateTime;
    public readonly string Channel;
    public readonly string Url;
    public readonly string Extra;
    private readonly int _hash;

    public BidRow(ReadOnlySpan<long> l, string[] s)
    {
        Auction = l[0];
        Bidder = l[1];
        Price = l[2];
        DateTime = l[3];
        Channel = s[0];
        Url = s[1];
        Extra = s[2];
        _hash = (int)Hash64();
    }

    public ulong Hash64() => Q20Cells.RowHash([Auction, Bidder, Price, DateTime], [Channel, Url, Extra]);

    public bool Equals(BidRow? o) =>
        o is not null && _hash == o._hash && Auction == o.Auction && Bidder == o.Bidder && Price == o.Price &&
        DateTime == o.DateTime && string.Equals(Channel, o.Channel, StringComparison.Ordinal) &&
        string.Equals(Url, o.Url, StringComparison.Ordinal) && string.Equals(Extra, o.Extra, StringComparison.Ordinal);

    public override bool Equals(object? obj) => Equals(obj as BidRow);

    public override int GetHashCode() => _hash;
}

internal sealed class AuctionRow : IEquatable<AuctionRow>
{
    public readonly long Id;
    public readonly long InitialBid;
    public readonly long Reserve;
    public readonly long DateTime;
    public readonly long Expires;
    public readonly long Seller;
    public readonly long Category;
    public readonly string ItemName;
    public readonly string Description;
    public readonly string Extra;
    private readonly int _hash;

    public AuctionRow(ReadOnlySpan<long> l, string[] s)
    {
        Id = l[0];
        InitialBid = l[1];
        Reserve = l[2];
        DateTime = l[3];
        Expires = l[4];
        Seller = l[5];
        Category = l[6];
        ItemName = s[0];
        Description = s[1];
        Extra = s[2];
        _hash = (int)Hash64();
    }

    public ulong Hash64() => Q20Cells.RowHash(
        [Id, InitialBid, Reserve, DateTime, Expires, Seller, Category], [ItemName, Description, Extra]);

    public bool Equals(AuctionRow? o) =>
        o is not null && _hash == o._hash && Id == o.Id && InitialBid == o.InitialBid && Reserve == o.Reserve &&
        DateTime == o.DateTime && Expires == o.Expires && Seller == o.Seller && Category == o.Category &&
        string.Equals(ItemName, o.ItemName, StringComparison.Ordinal) &&
        string.Equals(Description, o.Description, StringComparison.Ordinal) &&
        string.Equals(Extra, o.Extra, StringComparison.Ordinal);

    public override bool Equals(object? obj) => Equals(obj as AuctionRow);

    public override int GetHashCode() => _hash;
}

/// <summary>The rows under one key: one inline, or a Dictionary once there are two.</summary>
internal sealed class Bucket<T>
    where T : class, IEquatable<T>
{
    public T? One;
    public long OneW;
    public Dictionary<T, long>? Many;

    /// <summary>Adds <paramref name="w"/>; returns the change in distinct rows (−1, 0, +1).</summary>
    public int Add(T row, long w)
    {
        if (Many is not null)
        {
            ref var slot = ref System.Runtime.InteropServices.CollectionsMarshal.GetValueRefOrAddDefault(Many, row, out var existed);
            slot += w;
            if (slot == 0)
            {
                Many.Remove(row);
                return existed ? -1 : 0;
            }

            return existed ? 0 : 1;
        }

        if (One is null)
        {
            One = row;
            OneW = w;
            return 1;
        }

        if (One.Equals(row))
        {
            OneW += w;
            if (OneW == 0)
            {
                One = null;
                return -1;
            }

            return 0;
        }

        Many = new Dictionary<T, long> { [One] = OneW, [row] = w };
        One = null;
        return 1;
    }

    public bool IsEmpty => Many is null ? One is null : Many.Count == 0;
}

internal readonly record struct OutPair(BidRow Bid, AuctionRow Auction, long W);

internal sealed class ManagedQ20Worker : IQ20Worker
{
    private readonly Dictionary<long, Bucket<BidRow>> _bids = [];
    private readonly Dictionary<long, Bucket<AuctionRow>> _auctions = [];
    private readonly List<OutPair> _out = [];
    private readonly long[] _l = new long[8];
    private readonly string[] _s = new string[3];
    private long _rows;

    public void Step(List<(object?[] Row, long W)> bids, List<(object?[] Row, long W)> auctions)
    {
        _out.Clear();
        foreach (var (r, w) in bids)
        {
            Q20Cells.Bid(r, _l, _s);
            var bid = new BidRow(_l, _s);
            if (_auctions.TryGetValue(bid.Auction, out var ab))
            {
                Emit(bid, ab, w);
            }

            Fold(_bids, bid.Auction, bid, w);
        }

        foreach (var (r, w) in auctions)
        {
            if (!Q20Cells.InFilter(r))
            {
                continue;
            }

            Q20Cells.Auction(r, _l, _s);
            var auction = new AuctionRow(_l, _s);
            if (_bids.TryGetValue(auction.Id, out var bb))
            {
                if (bb.Many is null)
                {
                    if (bb.One is not null)
                    {
                        _out.Add(new OutPair(bb.One, auction, bb.OneW * w));
                    }
                }
                else
                {
                    foreach (var (b, bw) in bb.Many)
                    {
                        _out.Add(new OutPair(b, auction, bw * w));
                    }
                }
            }

            Fold(_auctions, auction.Id, auction, w);
        }
    }

    public (ulong Sum, long Rows) SinkTick()
    {
        ulong sum = 0;
        long rows = 0;
        foreach (var (b, a, w) in _out)
        {
            sum += (ulong)w * Q20Cells.OutHash<char>(
                b.Auction, b.Bidder, b.Price, b.Channel, b.Url, b.DateTime, b.Extra, a.ItemName, a.Description,
                a.InitialBid, a.Reserve, a.DateTime, a.Expires, a.Seller, a.Category, a.Extra);
            rows += w;
        }

        return (sum, rows);
    }

    public (ulong Sum, long Rows) StateChecksum()
    {
        ulong sum = 0;
        long rows = 0;
        void Add<T>(Dictionary<long, Bucket<T>> d, Func<T, ulong> hash)
            where T : class, IEquatable<T>
        {
            foreach (var b in d.Values)
            {
                if (b.Many is null)
                {
                    if (b.One is not null)
                    {
                        sum += (ulong)b.OneW * hash(b.One);
                        rows++;
                    }
                }
                else
                {
                    foreach (var (r, w) in b.Many)
                    {
                        sum += (ulong)w * hash(r);
                        rows++;
                    }
                }
            }
        }

        Add(_bids, r => r.Hash64());
        Add(_auctions, r => r.Hash64());
        return (sum, rows);
    }

    public long ProbeOne(long auction)
    {
        if (!_bids.TryGetValue(auction, out var b))
        {
            return 0;
        }

        return b.Many?.Count ?? 1;
    }

    public void Checkpoint(string path)
    {
        using var fs = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None, 1 << 20);
        using (var w = new BinaryWriter(fs, System.Text.Encoding.UTF8, leaveOpen: true))
        {
            WriteSide(w, _bids, (b, x) =>
            {
                x.Write(b.Auction); x.Write(b.Bidder); x.Write(b.Price); x.Write(b.DateTime);
                x.Write(b.Channel); x.Write(b.Url); x.Write(b.Extra);
            });
            WriteSide(w, _auctions, (a, x) =>
            {
                x.Write(a.Id); x.Write(a.InitialBid); x.Write(a.Reserve); x.Write(a.DateTime); x.Write(a.Expires);
                x.Write(a.Seller); x.Write(a.Category); x.Write(a.ItemName); x.Write(a.Description); x.Write(a.Extra);
            });
        }

        fs.Flush(flushToDisk: true);
    }

    public static ManagedQ20Worker Restore(string path)
    {
        var worker = new ManagedQ20Worker();
        using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 1 << 20);
        using var r = new BinaryReader(fs, System.Text.Encoding.UTF8);
        var l = new long[8];
        var s = new string[3];
        var n = r.ReadInt64();
        for (long i = 0; i < n; i++)
        {
            var w = r.ReadInt64();
            for (var c = 0; c < Q20Cells.BidLongs; c++)
            {
                l[c] = r.ReadInt64();
            }

            for (var c = 0; c < Q20Cells.BidStrs; c++)
            {
                s[c] = r.ReadString();
            }

            Fold(worker._bids, l[0], new BidRow(l, s), w, ref worker._rows);
        }

        n = r.ReadInt64();
        for (long i = 0; i < n; i++)
        {
            var w = r.ReadInt64();
            for (var c = 0; c < Q20Cells.AuctionLongs; c++)
            {
                l[c] = r.ReadInt64();
            }

            for (var c = 0; c < Q20Cells.AuctionStrs; c++)
            {
                s[c] = r.ReadString();
            }

            Fold(worker._auctions, l[0], new AuctionRow(l, s), w, ref worker._rows);
        }

        return worker;
    }

    public void Dispose()
    {
    }

    private static void WriteSide<T>(BinaryWriter w, Dictionary<long, Bucket<T>> d, Action<T, BinaryWriter> write)
        where T : class, IEquatable<T>
    {
        long n = 0;
        foreach (var b in d.Values)
        {
            n += b.Many?.Count ?? (b.One is null ? 0 : 1);
        }

        w.Write(n);
        foreach (var b in d.Values)
        {
            if (b.Many is null)
            {
                if (b.One is not null)
                {
                    w.Write(b.OneW);
                    write(b.One, w);
                }
            }
            else
            {
                foreach (var (row, rw) in b.Many)
                {
                    w.Write(rw);
                    write(row, w);
                }
            }
        }
    }

    private void Emit(BidRow bid, Bucket<AuctionRow> ab, long w)
    {
        if (ab.Many is null)
        {
            if (ab.One is not null)
            {
                _out.Add(new OutPair(bid, ab.One, w * ab.OneW));
            }
        }
        else
        {
            foreach (var (a, aw) in ab.Many)
            {
                _out.Add(new OutPair(bid, a, w * aw));
            }
        }
    }

    private void Fold<T>(Dictionary<long, Bucket<T>> d, long key, T row, long w)
        where T : class, IEquatable<T> => Fold(d, key, row, w, ref _rows);

    private static void Fold<T>(Dictionary<long, Bucket<T>> d, long key, T row, long w, ref long rows)
        where T : class, IEquatable<T>
    {
        ref var b = ref System.Runtime.InteropServices.CollectionsMarshal.GetValueRefOrAddDefault(d, key, out _);
        b ??= new Bucket<T>();
        rows += b.Add(row, w);
        if (b.IsEmpty)
        {
            d.Remove(key);
        }
    }
}


// ---------------------------------------------------------------------------------------------
// Arms 2 and 3: the flat store. Output is a batch of records in the same kind of pages, reused
// every tick; it copies its strings, so it never points into state that the tick may free.
// ---------------------------------------------------------------------------------------------

/// <summary>Output records: weight (8) · 10 longs (auction, bidder, price, bidTime, initialBid,
/// reserve, auctionTime, expires, seller, category) · 6 string starts (channel, url, bidExtra,
/// itemName, description, auctionExtra) · 6 string lengths.</summary>
internal sealed unsafe class FlatOutBatch<TP, TC> : IDisposable
    where TP : struct, IPageSource
    where TC : unmanaged
{
    private const int OffS = 8 + (8 * 10);
    private const int OffN = OffS + (8 * 6);
    private readonly RecordPages<TP> _rec = new(OffN + (4 * 6));

    public StrArena<TC, TP> Arena { get; } = new();

    public int Count { get; private set; }

    public void Reset()
    {
        Count = 0;
        Arena.Reset();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public byte* Add(long w)
    {
        var i = Count++;
        if (Count > _rec.Capacity)
        {
            _rec.Ensure(Count);
        }

        var r = _rec.Rec(i);
        *(long*)r = w;
        return r;
    }

    public byte* Rec(int i) => _rec.Rec(i);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ref long Long(byte* r, int c) => ref ((long*)(r + 8))[c];

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SetStr(byte* r, int c, ReadOnlySpan<TC> s)
    {
        ((long*)(r + OffS))[c] = Arena.Append(s);
        ((int*)(r + OffN))[c] = s.Length;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ReadOnlySpan<TC> Str(byte* r, int c) => Arena.Get(((long*)(r + OffS))[c], ((int*)(r + OffN))[c]);

    public void Dispose()
    {
        _rec.Dispose();
        Arena.Dispose();
    }
}

internal sealed unsafe class FlatQ20Worker<TP, TC> : IQ20Worker
    where TP : struct, IPageSource
    where TC : unmanaged, IEquatable<TC>
{
    private const long Magic = 0x3032_5154_414C_46; // "FLATQ20"
    private const int HeaderBytes = 16 << 10;

    private readonly FlatOutBatch<TP, TC> _out = new();
    private readonly long[] _l = new long[8];
    private readonly ReadOnlyMemory<TC>[] _s = new ReadOnlyMemory<TC>[3];
    private readonly FlatTable<TP, TC> _bids;
    private readonly FlatTable<TP, TC> _auctions;
    private MemoryMappedFile? _map;
    private MemoryMappedViewAccessor? _view;

    public FlatQ20Worker()
        : this(new FlatTable<TP, TC>(Q20Cells.BidLongs, Q20Cells.BidStrs), new FlatTable<TP, TC>(Q20Cells.AuctionLongs, Q20Cells.AuctionStrs))
    {
    }

    private FlatQ20Worker(FlatTable<TP, TC> bids, FlatTable<TP, TC> auctions)
    {
        _bids = bids;
        _auctions = auctions;
    }

    public void Step(List<(object?[] Row, long W)> bids, List<(object?[] Row, long W)> auctions)
    {
        _out.Reset();
        _bids.CompactIfNeeded();
        _auctions.CompactIfNeeded();
        var a = _auctions;
        var b = _bids;
        foreach (var (r, w) in bids)
        {
            Q20Cells.Bid(r, _l, _s);
            for (var slot = a.ChainHead(_l[0]); slot >= 0;)
            {
                var ar = a.Rec(slot);
                var o = _out.Add(w * FlatTable<TP, TC>.Weight(ar));
                WriteBid(o, _l, _s);
                WriteAuction(o, a, ar);
                slot = a.NextOf(ar);
            }

            b.Upsert(_l.AsSpan(0, Q20Cells.BidLongs), _s, w);
        }

        foreach (var (r, w) in auctions)
        {
            if (!Q20Cells.InFilter(r))
            {
                continue;
            }

            Q20Cells.Auction(r, _l, _s);
            for (var slot = b.ChainHead(_l[0]); slot >= 0;)
            {
                var br = b.Rec(slot);
                var o = _out.Add(FlatTable<TP, TC>.Weight(br) * w);
                WriteBid(o, b, br);
                WriteAuction(o, _l, _s);
                slot = b.NextOf(br);
            }

            a.Upsert(_l.AsSpan(0, Q20Cells.AuctionLongs), _s, w);
        }
    }

    public (ulong Sum, long Rows) SinkTick()
    {
        ulong sum = 0;
        long rows = 0;
        var o = _out;
        for (var i = 0; i < o.Count; i++)
        {
            var r = o.Rec(i);
            var w = *(long*)r;
            sum += (ulong)w * Q20Cells.OutHash(
                FlatOutBatch<TP, TC>.Long(r, 0), FlatOutBatch<TP, TC>.Long(r, 1), FlatOutBatch<TP, TC>.Long(r, 2),
                o.Str(r, 0), o.Str(r, 1), FlatOutBatch<TP, TC>.Long(r, 3), o.Str(r, 2), o.Str(r, 3), o.Str(r, 4),
                FlatOutBatch<TP, TC>.Long(r, 4), FlatOutBatch<TP, TC>.Long(r, 5), FlatOutBatch<TP, TC>.Long(r, 6),
                FlatOutBatch<TP, TC>.Long(r, 7), FlatOutBatch<TP, TC>.Long(r, 8), FlatOutBatch<TP, TC>.Long(r, 9), o.Str(r, 5));
            rows += w;
        }

        return (sum, rows);
    }

    public (ulong Sum, long Rows) StateChecksum()
    {
        var (bs, br) = _bids.Checksum();
        var (a, ar) = _auctions.Checksum();
        return (bs + a, br + ar);
    }

    public long ProbeOne(long auction)
    {
        long n = 0;
        for (var slot = _bids.ChainHead(auction); slot >= 0; slot = _bids.NextOf(_bids.Rec(slot)))
        {
            n++;
        }

        return n;
    }

    // Checkpoint: a 16 KiB header of scalars, then every part's pages, whole, in a fixed order.
    // Every page size is a multiple of 16 KiB, so every page lands page-aligned in the file and a
    // mapped restore can point the tables straight at the file.
    public void Checkpoint(string path)
    {
        using var fs = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None, 0);
        var header = new long[HeaderBytes / sizeof(long)];
        var h = 0;
        header[h++] = Magic;
        foreach (var t in new[] { _bids, _auctions })
        {
            foreach (var v in t.SaveScalars())
            {
                header[h++] = v;
            }

            foreach (var c in t.Columns())
            {
                if ((c.PageBytes & ((16 << 10) - 1)) != 0)
                {
                    throw new InvalidOperationException($"page of {c.PageBytes} bytes is not 16 KiB-aligned");
                }

                header[h++] = c.Pages();
            }
        }

        fs.Write(System.Runtime.InteropServices.MemoryMarshal.AsBytes(header.AsSpan()));
        foreach (var t in new[] { _bids, _auctions })
        {
            foreach (var c in t.Columns())
            {
                var pages = c.Pages();
                for (var p = 0; p < pages; p++)
                {
                    fs.Write(new ReadOnlySpan<byte>((void*)c.Page(p), checked((int)c.PageBytes)));
                }
            }
        }

        fs.Flush(flushToDisk: true);
    }

    /// <summary>Restore by reading every page into freshly allocated pages.</summary>
    public static FlatQ20Worker<TP, TC> Restore(string path) => Restore(path, mapped: false);

    /// <summary>Restore by mapping the checkpoint copy-on-write and pointing the tables at it:
    /// nothing is read until a page is touched.</summary>
    public static FlatQ20Worker<TP, TC> RestoreMapped(string path) => Restore(path, mapped: true);

    public void Dispose()
    {
        _out.Dispose();
        _bids.Dispose();
        _auctions.Dispose();
        if (_view is not null)
        {
            _view.SafeMemoryMappedViewHandle.ReleasePointer();
            _view.Dispose();
            _map!.Dispose();
        }
    }

    private static FlatQ20Worker<TP, TC> Restore(string path, bool mapped)
    {
        var header = new long[HeaderBytes / sizeof(long)];
        using (var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 0))
        {
            fs.ReadExactly(System.Runtime.InteropServices.MemoryMarshal.AsBytes(header.AsSpan()));
        }

        if (header[0] != Magic)
        {
            throw new InvalidDataException("not a flat q20 checkpoint");
        }

        var bids = new FlatTable<TP, TC>(Q20Cells.BidLongs, Q20Cells.BidStrs, empty: false);
        var auctions = new FlatTable<TP, TC>(Q20Cells.AuctionLongs, Q20Cells.AuctionStrs, empty: false);
        var worker = new FlatQ20Worker<TP, TC>(bids, auctions);
        var h = 1;
        var counts = new List<int>();
        foreach (var t in new[] { bids, auctions })
        {
            t.LoadScalars(header.AsSpan(h, FlatTable<TP, TC>.ScalarCount));
            h += FlatTable<TP, TC>.ScalarCount;
            foreach (var unused in t.Columns())
            {
                counts.Add((int)header[h++]);
            }
        }

        long offset = HeaderBytes;
        var k = 0;
        if (mapped)
        {
            worker._map = MemoryMappedFile.CreateFromFile(path, FileMode.Open, null, 0, MemoryMappedFileAccess.CopyOnWrite);
            worker._view = worker._map.CreateViewAccessor(0, 0, MemoryMappedFileAccess.CopyOnWrite);
            byte* basePtr = null;
            worker._view.SafeMemoryMappedViewHandle.AcquirePointer(ref basePtr);
            basePtr += worker._view.PointerOffset;
            foreach (var t in new[] { bids, auctions })
            {
                foreach (var c in t.Columns())
                {
                    var pages = counts[k++];
                    for (var p = 0; p < pages; p++)
                    {
                        c.Adopt((nint)(basePtr + offset));
                        offset += (long)c.PageBytes;
                    }
                }
            }

            return worker;
        }

        using var handle = File.OpenHandle(path, FileMode.Open, FileAccess.Read);
        foreach (var t in new[] { bids, auctions })
        {
            foreach (var c in t.Columns())
            {
                var pages = counts[k++];
                c.AddPages(pages);
                for (var p = 0; p < pages; p++)
                {
                    var span = new Span<byte>((void*)c.Page(p), checked((int)c.PageBytes));
                    var read = 0;
                    while (read < span.Length)
                    {
                        var n = RandomAccess.Read(handle, span[read..], offset + read);
                        if (n == 0)
                        {
                            throw new EndOfStreamException();
                        }

                        read += n;
                    }

                    offset += (long)c.PageBytes;
                }
            }
        }

        return worker;
    }

    private void WriteBid(byte* o, long[] l, ReadOnlyMemory<TC>[] s)
    {
        for (var c = 0; c < Q20Cells.BidLongs; c++)
        {
            FlatOutBatch<TP, TC>.Long(o, c) = l[c];
        }

        _out.SetStr(o, 0, s[0].Span);
        _out.SetStr(o, 1, s[1].Span);
        _out.SetStr(o, 2, s[2].Span);
    }

    private void WriteBid(byte* o, FlatTable<TP, TC> b, byte* br)
    {
        for (var c = 0; c < Q20Cells.BidLongs; c++)
        {
            FlatOutBatch<TP, TC>.Long(o, c) = FlatTable<TP, TC>.Long(br, c);
        }

        _out.SetStr(o, 0, b.Str(br, 0));
        _out.SetStr(o, 1, b.Str(br, 1));
        _out.SetStr(o, 2, b.Str(br, 2));
    }

    private void WriteAuction(byte* o, long[] l, ReadOnlyMemory<TC>[] s)
    {
        for (var c = 1; c < Q20Cells.AuctionLongs; c++)
        {
            FlatOutBatch<TP, TC>.Long(o, 3 + c) = l[c];
        }

        _out.SetStr(o, 3, s[0].Span);
        _out.SetStr(o, 4, s[1].Span);
        _out.SetStr(o, 5, s[2].Span);
    }

    private void WriteAuction(byte* o, FlatTable<TP, TC> a, byte* ar)
    {
        for (var c = 1; c < Q20Cells.AuctionLongs; c++)
        {
            FlatOutBatch<TP, TC>.Long(o, 3 + c) = FlatTable<TP, TC>.Long(ar, c);
        }

        _out.SetStr(o, 3, a.Str(ar, 0));
        _out.SetStr(o, 4, a.Str(ar, 1));
        _out.SetStr(o, 5, a.Str(ar, 2));
    }
}
