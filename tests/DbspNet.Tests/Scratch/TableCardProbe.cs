// Does a reference-free trace hash table remove the young-GC card-scanning cost
// (design-row-representation §27.6)? A bulk load inserts millions of young rows into old, large
// trace tables at hash-random positions; each insert dirties its own card in a large-object-heap
// Entry[] and the card stays set while the row is ephemeral, so every young GC rescans most of the
// table, in mark and again in relocate. This grows the same rows into the same tables over a
// number of ticks, on W threads (each owning its share of the tables), with per-tick delta
// dictionaries as mid-life garbage, and times the GC pause by kind.
//   TABLE_CARD_PROBE=dict        Dictionary<StructuralRow,long> (today's flat trace)
//   TABLE_CARD_PROBE=index       reference-free table: Entry {hash, next, int row, weight} plus an
//                                append-only array of 16K-row pages (new references land densely,
//                                in the newest pages only; the Entry[] is never card-scanned)
//   TABLE_CARD_PROBE=index-flat  the same, with the rows in one doubling array instead of pages
//   TABLE_CARD_PROBE=cells       the table keeps no row object at all: each row's values are copied
//                                into reference-free long pages (an upper bound for "a trace holds no
//                                young GC objects"; deltas and garbage are unchanged)
// Knobs: TABLE_CARD_ROWS (25e6), TABLE_CARD_TABLES (48), TABLE_CARD_WORKERS (1), TABLE_CARD_TICKS
// (20), TABLE_CARD_STAGES (3 delta dictionaries per tick), TABLE_CARD_GARBAGE (4 short-lived rows
// per kept row), TABLE_CARD_DELTA_CHUNK (0 = one dictionary per delta, on the large-object heap
// once it holds ~3.5K+ rows; N = the delta split into dictionaries of at most N rows). Run each mode in its own process with DOTNET_gcServer=1. No-op when unset.
using System.Diagnostics;
using System.Globalization;
using DbspNet.Core.Collections;
using Xunit;
using Xunit.Abstractions;

namespace DbspNet.Tests.Scratch;

public class TableCardProbe
{
    private const int Cells = 9;
    private const int PageRows = 16_384;

    private readonly ITestOutputHelper _out;

    public TableCardProbe(ITestOutputHelper output) => _out = output;

    private interface ITable
    {
        int Count { get; }

        void Add(StructuralRow row, long weight);
    }

    [Fact]
    public void Probe()
    {
        var mode = Environment.GetEnvironmentVariable("TABLE_CARD_PROBE");
        if (string.IsNullOrEmpty(mode))
        {
            return;
        }

        var rows = (long)Env("TABLE_CARD_ROWS", 25e6);
        var tables = (int)Env("TABLE_CARD_TABLES", 48);
        var workers = (int)Env("TABLE_CARD_WORKERS", 1);
        var ticks = (int)Env("TABLE_CARD_TICKS", 20);
        var stages = (int)Env("TABLE_CARD_STAGES", 3);
        var garbage = (int)Env("TABLE_CARD_GARBAGE", 4);
        var deltaChunk = (int)Env("TABLE_CARD_DELTA_CHUNK", 0);
        Func<ITable> make = mode switch
        {
            "dict" => () => new DictTable(),
            "index" => () => new IndexTable(paged: true),
            "index-flat" => () => new IndexTable(paged: false),
            "cells" => () => new CellTable(),
            _ => throw new ArgumentException(mode),
        };

        // Shared cell boxes (old by the time growth starts), as projections share them in the engine:
        // a row is 8 shared cells plus one fresh unique id box.
        var pool = new object[1 << 20];
        for (var i = 0; i < pool.Length; i++)
        {
            pool[i] = (long)i;
        }

        var all = Enumerable.Range(0, tables).Select(_ => make()).ToArray();
        GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
        using var events = new RowFormGcProbe.GcEventListener();
        Thread.Sleep(500);
        events.Reset();
        var counts0 = (GC.CollectionCount(0), GC.CollectionCount(1), GC.CollectionCount(2));
        var pause0 = GC.GetTotalPauseDuration();
        var alloc0 = GC.GetTotalAllocatedBytes(precise: true);
        var sw = Stopwatch.StartNew();

        var perTick = rows / ((long)ticks * workers);
        using var barrier = new Barrier(workers);
        var threads = Enumerable.Range(0, workers).Select(w => new Thread(() =>
        {
            var mine = all.Where((_, i) => i % workers == w).ToArray();
            long next = w * (rows / workers);
            for (var t = 0; t < ticks; t++)
            {
                // The tick's delta, rebuilt by a few operator stages (each a fresh dictionary that
                // dies with the tick: mid-life garbage), with short-lived intermediate rows.
                var chunkRows = deltaChunk > 0 ? deltaChunk : (int)perTick;
                var delta = new List<Dictionary<StructuralRow, long>>();
                for (long i = 0; i < perTick; i++)
                {
                    for (var g = 0; g < garbage; g++)
                    {
                        GC.KeepAlive(NewRow(pool, next, g + 1));
                    }

                    if (i % chunkRows == 0)
                    {
                        delta.Add(new Dictionary<StructuralRow, long>((int)Math.Min(chunkRows, perTick - i)));
                    }

                    delta[^1][NewRow(pool, next++, 0)] = 1;
                }

                for (var s = 1; s < stages; s++)
                {
                    delta = delta.Select(d => new Dictionary<StructuralRow, long>(d)).ToList();
                }

                var k = 0;
                foreach (var d in delta)
                {
                    foreach (var (row, weight) in d)
                    {
                        mine[k++ % mine.Length].Add(row, weight);
                    }
                }

                barrier.SignalAndWait();
            }
        })).ToArray();
        foreach (var th in threads)
        {
            th.Start();
        }

        foreach (var th in threads)
        {
            th.Join();
        }

        var buildMs = sw.Elapsed.TotalMilliseconds;
        var pauseMs = (GC.GetTotalPauseDuration() - pause0).TotalMilliseconds;
        var allocated = GC.GetTotalAllocatedBytes(precise: true) - alloc0;
        var counts = (GC.CollectionCount(0) - counts0.Item1, GC.CollectionCount(1) - counts0.Item2, GC.CollectionCount(2) - counts0.Item3);
        Thread.Sleep(1500); // runtime events reach an in-process listener asynchronously
        var gcReport = events.Report();

        GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
        var live = GC.GetGCMemoryInfo(GCKind.FullBlocking).PromotedBytes;
        var held = all.Sum(x => (long)x.Count);
        Assert.Equal(perTick * ticks * workers, held);
        GC.KeepAlive(all);
        GC.KeepAlive(pool);
        _out.WriteLine(string.Create(CultureInfo.InvariantCulture,
            $"TABLECARD {mode} delta_chunk={deltaChunk} W={workers} rows={held / 1e6:F2}M tables={tables} ticks={ticks} stages={stages} garbage={garbage} " +
            $"live={live / 1e9:F2} GB alloc={allocated / 1e9:F1} GB build={buildMs:F0} ms pause={pauseMs:F0} ms " +
            $"GCs g0/g1/g2={counts.Item1}/{counts.Item2}/{counts.Item3} | {gcReport}"));
    }

    private static double Env(string name, double dflt) =>
        double.TryParse(Environment.GetEnvironmentVariable(name), NumberStyles.Float, CultureInfo.InvariantCulture, out var v) ? v : dflt;

    // 8 shared cells and a fresh unique id (salt makes intermediate rows distinct from kept ones).
    private static StructuralRow NewRow(object[] pool, long id, int salt)
    {
        var cells = new object?[Cells];
        for (var c = 0; c < Cells - 1; c++)
        {
            cells[c] = pool[(int)((id * 31 + (c * 7919) + salt) & (pool.Length - 1))];
        }

        cells[Cells - 1] = (id << 3) | (long)salt;
        return new StructuralRow(cells);
    }

    private sealed class DictTable : ITable
    {
        private readonly Dictionary<StructuralRow, long> _d = new();

        public int Count => _d.Count;

        public void Add(StructuralRow row, long weight)
        {
            System.Runtime.InteropServices.CollectionsMarshal.GetValueRefOrAddDefault(_d, row, out _) += weight;
        }
    }

    // A chained hash table whose arrays hold no references: the GC never scans them. The rows live
    // in an append-only row store indexed by the entries.
    private sealed class IndexTable(bool paged) : ITable
    {
        private struct Entry
        {
            public int Hash;
            public int Next;
            public int Row;
            public long Weight;
        }

        private readonly List<StructuralRow[]> _pages = new();
        private StructuralRow[] _flat = new StructuralRow[16];
        private int[] _buckets = new int[16];
        private Entry[] _entries = new Entry[16];
        private int _count;

        public int Count => _count;

        public void Add(StructuralRow row, long weight)
        {
            var hash = row.GetHashCode();
            ref var bucket = ref _buckets[(uint)hash % (uint)_buckets.Length];
            for (var i = bucket - 1; i >= 0; i = _entries[i].Next)
            {
                if (_entries[i].Hash == hash && RowAt(_entries[i].Row).Equals(row))
                {
                    _entries[i].Weight += weight;
                    return;
                }
            }

            if (_count == _entries.Length)
            {
                Grow();
                bucket = ref _buckets[(uint)hash % (uint)_buckets.Length];
            }

            var r = Append(row);
            _entries[_count] = new Entry { Hash = hash, Next = bucket - 1, Row = r, Weight = weight };
            bucket = ++_count;
        }

        private StructuralRow RowAt(int r) => paged ? _pages[r / PageRows][r % PageRows] : _flat[r];

        private int Append(StructuralRow row)
        {
            var r = _count;
            if (paged)
            {
                if (r % PageRows == 0)
                {
                    _pages.Add(new StructuralRow[PageRows]);
                }

                _pages[r / PageRows][r % PageRows] = row;
            }
            else
            {
                if (r == _flat.Length)
                {
                    Array.Resize(ref _flat, r * 2);
                }

                _flat[r] = row;
            }

            return r;
        }

        private void Grow()
        {
            Array.Resize(ref _entries, _entries.Length * 2);
            _buckets = new int[_entries.Length];
            for (var i = 0; i < _count; i++)
            {
                ref var b = ref _buckets[(uint)_entries[i].Hash % (uint)_buckets.Length];
                _entries[i].Next = b - 1;
                b = i + 1;
            }
        }
    }

    // Reference-free rows: the cells live in long pages (the ids are boxed longs in this probe).
    private sealed class CellTable : ITable
    {
        private struct Entry
        {
            public int Hash;
            public int Next;
            public long Weight;
        }

        private readonly List<long[]> _pages = new();
        private int[] _buckets = new int[16];
        private Entry[] _entries = new Entry[16];
        private int _count;

        public int Count => _count;

        public void Add(StructuralRow row, long weight)
        {
            var hash = row.GetHashCode();
            ref var bucket = ref _buckets[(uint)hash % (uint)_buckets.Length];
            for (var i = bucket - 1; i >= 0; i = _entries[i].Next)
            {
                if (_entries[i].Hash == hash && Same(i, row))
                {
                    _entries[i].Weight += weight;
                    return;
                }
            }

            if (_count == _entries.Length)
            {
                Grow();
                bucket = ref _buckets[(uint)hash % (uint)_buckets.Length];
            }

            var r = _count;
            if (r % PageRows == 0)
            {
                _pages.Add(new long[PageRows * Cells]);
            }

            var page = _pages[r / PageRows];
            var at = (r % PageRows) * Cells;
            for (var c = 0; c < Cells; c++)
            {
                page[at + c] = (long)row[c]!;
            }

            _entries[_count] = new Entry { Hash = hash, Next = bucket - 1, Weight = weight };
            bucket = ++_count;
        }

        private bool Same(int r, StructuralRow row)
        {
            var page = _pages[r / PageRows];
            var at = (r % PageRows) * Cells;
            for (var c = 0; c < Cells; c++)
            {
                if (page[at + c] != (long)row[c]!)
                {
                    return false;
                }
            }

            return true;
        }

        private void Grow()
        {
            Array.Resize(ref _entries, _entries.Length * 2);
            _buckets = new int[_entries.Length];
            for (var i = 0; i < _count; i++)
            {
                ref var b = ref _buckets[(uint)_entries[i].Hash % (uint)_buckets.Length];
                _entries[i].Next = b - 1;
                b = i + 1;
            }
        }
    }
}
