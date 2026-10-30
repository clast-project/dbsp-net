// What young-generation GC pause each ROW FORM costs while a heap GROWS the way an ivm-bench
// SF=3 batch-1 bulk load grows it (design-state-representation §9.15: the batch-1 pause is all
// gen0/gen1 copying of new survivors, not full GCs — so TraceLayoutGcProbe's forced full GCs of
// a static heap are the wrong measure for rows).
//
// The heap grows chunk by chunk: rows in the census shape (TraceLayoutGcProbe.Shape: one key row
// per group, then that group's value rows), every row retained and referenced from SHARE paged
// "traces" (rows are shared by reference across traces today — every form keeps that), with
// short-lived garbage allocated between chunks. No GC.Collect while growing: gen0/gen1 run
// naturally. One forced full GC at the very end measures retained bytes.
//
//   ROW_FORM_PROBE=boxed          today: StructuralRow + object?[] + a box per cell (long / int /
//                                 Date32 / Decimal128 boxes; a boxed Utf8String over its own byte[])
//   ROW_FORM_PROBE=arena          a 32-byte handle object per row (arena ref + row index + hash);
//                                 fixed-width cells row-major in per-schema 16K-row byte[] pages
//                                 (LOH); each string a separate byte[] referenced from a 16K-entry
//                                 byte[][] page per string column (LOH page, young byte[])
//   ROW_FORM_PROBE=arena-strings  as arena, but string bytes bump-allocated in 1 MiB LOH byte[]
//                                 pages; the cell holds (global offset: long, length: int)
//   ROW_FORM_PROBE=handle-free    as arena-strings with no per-row object at all: the traces hold
//                                 long row ids (arena << 40 | index) — the upper bound
//
// Knobs (all optional):
//   ROW_FORM_SCALE=1.0     fraction of the census group counts (1.0 = 5.40M keys + 24.9M values)
//   ROW_FORM_COLUMNS=LLLIDMSS  value-row column mix: L long, I int, D Date32, M Decimal128,
//                          S string (key rows are always one long column)
//   ROW_FORM_STRLEN=12     bytes per string cell
//   ROW_FORM_SHARE=2       traces referencing every row
//   ROW_FORM_GARBAGE=3.0   short-lived garbage bytes per row, as a multiple of the BOXED row's
//                          bytes — an absolute amount, identical in every mode (batch 1:
//                          ~41 GiB allocated for 10.6 GB retained, ~3:1)
//   ROW_FORM_CHUNK=16384   rows per chunk; garbage follows each chunk
//   ROW_FORM_WINDOW=1      chunks of garbage kept alive at once (an in-flight delta survives the
//                          GCs that land inside it, as a real delta does, then dies)
//
// Run each mode in its own process under Server GC, e.g.
//   DOTNET_gcServer=1 ROW_FORM_PROBE=boxed dotnet test tests/DbspNet.Tests -c Release \
//     --filter FullyQualifiedName~RowFormGcProbe --logger "console;verbosity=detailed"
// No-op when ROW_FORM_PROBE is unset.
using System.Buffers.Binary;
using System.Diagnostics;
using System.Diagnostics.Tracing;
using System.Globalization;
using System.Runtime.CompilerServices;
using Clast.DatabaseDecimal.Values;
using DbspNet.Core.Collections;
using DbspNet.Sql.TypeSystem;
using Xunit;
using Xunit.Abstractions;

namespace DbspNet.Tests.Scratch;

public class RowFormGcProbe
{
    private const int PageRows = 16_384;          // rows per cell page / refs per trace page
    private const int StringPageBytes = 1 << 20;  // arena-strings byte pages

    private readonly ITestOutputHelper _out;

    public RowFormGcProbe(ITestOutputHelper output) => _out = output;

    [Fact]
    public void Probe()
    {
        var mode = Environment.GetEnvironmentVariable("ROW_FORM_PROBE");
        if (string.IsNullOrEmpty(mode))
        {
            return;
        }

        var scale = Env("ROW_FORM_SCALE", 1.0);
        var columns = Environment.GetEnvironmentVariable("ROW_FORM_COLUMNS") is { Length: > 0 } c ? c : "LLLIDMSS";
        var strLen = (int)Env("ROW_FORM_STRLEN", 12);
        var share = (int)Env("ROW_FORM_SHARE", 2);
        var garbageRatio = Env("ROW_FORM_GARBAGE", 3.0);
        var chunk = (int)Env("ROW_FORM_CHUNK", PageRows);
        var window = (int)Env("ROW_FORM_WINDOW", 1);

        IRowSink sink = mode switch
        {
            "boxed" => new BoxedSink(columns, strLen, share),
            "arena" => new ArenaSink(columns, strLen, share, inlineStrings: false, handles: true),
            "arena-strings" => new ArenaSink(columns, strLen, share, inlineStrings: true, handles: true),
            "handle-free" => new ArenaSink(columns, strLen, share, inlineStrings: true, handles: false),
            _ => throw new ArgumentException(mode),
        };

        // Garbage is fixed in absolute bytes so every mode allocates the same transient load; the
        // reference is the boxed form's per-row bytes (static estimate, see BoxedRowBytes).
        var garbagePerRow = (long)(garbageRatio * BoxedRowBytes(columns, strLen));
        var garbageWindow = new Queue<object?[]>();
        long garbageBytes = 0;
        long rows = 0, keys = 0;

        // Settle the startup heap so the growth phase starts from a clean slate.
        GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
        GC.WaitForPendingFinalizers();
        using var events = new GcEventListener();
        Thread.Sleep(500); // let the listener attach before growth starts
        events.Reset();

        var counts0 = (GC.CollectionCount(0), GC.CollectionCount(1), GC.CollectionCount(2));
        var pause0 = GC.GetTotalPauseDuration();
        var alloc0 = GC.GetTotalAllocatedBytes(precise: true);
        var sw = Stopwatch.StartNew();

        long v = 0, inChunk = 0;
        foreach (var (groups, size) in TraceLayoutGcProbe.Shape)
        {
            var n = (long)Math.Round(groups * scale);
            for (long g = 0; g < n; g++)
            {
                sink.AddKey(keys++);
                rows++;
                inChunk++;
                for (var j = 0; j < size; j++)
                {
                    sink.AddValue(v++);
                    rows++;
                    if (++inChunk >= chunk)
                    {
                        garbageBytes += Garbage(garbagePerRow * inChunk, garbageWindow, window);
                        inChunk = 0;
                    }
                }
            }
        }

        garbageBytes += Garbage(garbagePerRow * inChunk, garbageWindow, window);
        garbageWindow.Clear();
        var buildMs = sw.Elapsed.TotalMilliseconds;
        var pauseMs = (GC.GetTotalPauseDuration() - pause0).TotalMilliseconds;
        var allocated = GC.GetTotalAllocatedBytes(precise: true) - alloc0;
        var counts = (GC.CollectionCount(0) - counts0.Item1, GC.CollectionCount(1) - counts0.Item2, GC.CollectionCount(2) - counts0.Item3);
        Thread.Sleep(1500); // runtime events reach an in-process listener asynchronously
        var gcReport = events.Report();

        GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
        var live = GC.GetGCMemoryInfo(GCKind.FullBlocking).PromotedBytes;
        GC.KeepAlive(sink);

        var kept = allocated - garbageBytes;
        _out.WriteLine(string.Create(CultureInfo.InvariantCulture,
            $"ROWFORM {mode} cols={columns} str={strLen} share={share} garbage={garbageRatio:F1} scale={scale:F3} " +
            $"rows={rows / 1e6:F2}M (keys {keys / 1e6:F2}M) live={live / 1e9:F2} GB ({(double)live / rows:F0} B/row) " +
            $"kept-alloc={(double)kept / rows:F0} B/row est-objects/row={sink.ObjectsPerValueRow} " +
            $"alloc={allocated / 1e9:F1} GB build={buildMs:F0} ms pause={pauseMs:F0} ms " +
            $"GCs g0/g1/g2={counts.Item1}/{counts.Item2}/{counts.Item3} | {gcReport}"));
    }

    private static double Env(string name, double dflt) =>
        Environment.GetEnvironmentVariable(name) is { Length: > 0 } s ? double.Parse(s, CultureInfo.InvariantCulture) : dflt;

    // Short-lived garbage shaped like an operator's intermediate output: small rows of four boxed
    // longs collected into a delta array, alive for WINDOW chunks. Allocates until this thread has
    // allocated `target` bytes; returns what it allocated.
    private static long Garbage(long target, Queue<object?[]> window, int keep)
    {
        var start = GC.GetAllocatedBytesForCurrentThread();
        var est = (int)Math.Max(1, target / 184) + 1; // ≈184 B per garbage row (see below)
        var delta = new object?[est];
        var i = 0;
        while (GC.GetAllocatedBytesForCurrentThread() - start < target)
        {
            long x = i;
            var row = new StructuralRow(new object?[] { x, x + 1, x + 2, x + 3 });
            if (i < delta.Length)
            {
                delta[i] = row;
            }

            i++;
        }

        window.Enqueue(delta);
        while (window.Count > keep)
        {
            window.Dequeue();
        }

        return GC.GetAllocatedBytesForCurrentThread() - start;
    }

    // Static per-row byte estimate for the boxed form on 64-bit .NET (object header + method table
    // = 16 B; arrays add an 8 B length; sizes round up to 8).
    internal static long BoxedRowBytes(string columns, int strLen)
    {
        long b = 32 + Round8(24 + (8 * columns.Length)); // StructuralRow + object?[]
        foreach (var c in columns)
        {
            b += c switch
            {
                'L' => 24,
                'I' => 24,
                'D' => 24,
                'M' => Round8(16 + Unsafe.SizeOf<Decimal128>()),
                'S' => 32 + Round8(24 + strLen), // boxed Utf8String (ReadOnlyMemory) + byte[]
                _ => throw new ArgumentException(columns),
            };
        }

        return b;
    }

    private static long Round8(long x) => (x + 7) & ~7L;

    private interface IRowSink
    {
        int ObjectsPerValueRow { get; }

        void AddKey(long k);

        void AddValue(long i);
    }

    // SHARE traces, each a list of 16K-reference pages (128 KB: LOH, like a real trace's big
    // entry arrays) — so young rows are reached from old pages through the card table, as today.
    private sealed class Traces<T>(int share)
    {
        private readonly List<T[]>[] _pages = Enumerable.Range(0, share).Select(_ => new List<T[]>()).ToArray();
        private long _count;

        public void Add(T item)
        {
            var o = (int)(_count % PageRows);
            foreach (var t in _pages)
            {
                if (o == 0)
                {
                    t.Add(new T[PageRows]);
                }

                t[^1][o] = item;
            }

            _count++;
        }
    }

    private static void FillString(Span<byte> dst, long i, int col)
    {
        // Unique-ish ASCII digits, no transient string.
        var x = (ulong)(i * 31 + col);
        for (var k = dst.Length - 1; k >= 0; k--)
        {
            dst[k] = (byte)('0' + (int)(x % 10));
            x /= 10;
        }
    }

    private sealed class BoxedSink(string columns, int strLen, int share) : IRowSink
    {
        private readonly Traces<StructuralRow> _traces = new(share);

        public int ObjectsPerValueRow => 2 + columns.Length + columns.Count(c => c == 'S');

        public void AddKey(long k) => _traces.Add(new StructuralRow(new object?[] { k }));

        public void AddValue(long i)
        {
            var cells = new object?[columns.Length];
            for (var c = 0; c < columns.Length; c++)
            {
                cells[c] = columns[c] switch
                {
                    'L' => i * (c + 1),
                    'I' => (int)(i % 1000),
                    'D' => new Date32((int)(i % 20000)),
                    'M' => new Decimal128((Int128)(i * 100)),
                    'S' => MakeUtf8(i, c),
                    _ => throw new ArgumentException(columns),
                };
            }

            _traces.Add(new StructuralRow(cells));
        }

        private Utf8String MakeUtf8(long i, int c)
        {
            var bytes = new byte[strLen];
            FillString(bytes, i, c);
            return Utf8String.FromBytes(bytes);
        }
    }

    // A row's identity: which arena, which slot, its hash. 16 B header + 8 + 4 + 4 = 32 B.
    private sealed class RowHandle(Arena arena, int index, int hash)
    {
        public readonly Arena Arena = arena;
        public readonly int Index = index;
        public readonly int Hash = hash;
    }

    // One schema's rows: fixed-width cells row-major in 16K-row byte[] pages (LOH for any stride
    // ≥ 6 B), string bytes either as separate byte[] objects (byte[][] pages) or bump-allocated in
    // 1 MiB byte pages with (offset: long, length: int) in the cell.
    private sealed class Arena
    {
        private readonly string _cols;
        private readonly int _strLen;
        private readonly bool _inlineStrings;
        private readonly int[] _offset;
        private readonly int[] _stringCol; // column → string-column ordinal, or -1
        private readonly List<byte[]> _cellPages = new();
        private readonly List<byte[][]>[] _stringRefPages;
        private readonly List<byte[]> _stringPages = new();
        private int _stringUsed = StringPageBytes;
        private int _count;

        public Arena(string cols, int strLen, bool inlineStrings)
        {
            (_cols, _strLen, _inlineStrings) = (cols, strLen, inlineStrings);
            _offset = new int[cols.Length];
            _stringCol = new int[cols.Length];
            var stride = 0;
            var strings = 0;
            for (var c = 0; c < cols.Length; c++)
            {
                _offset[c] = stride;
                _stringCol[c] = cols[c] == 'S' ? strings++ : -1;
                stride += cols[c] switch
                {
                    'L' => 8,
                    'I' or 'D' => 4,
                    'M' => 16,
                    'S' => inlineStrings ? 12 : 0,
                    _ => throw new ArgumentException(cols),
                };
            }

            Stride = Math.Max(stride, 6); // keep a page ≥ 85 KB (LOH)
            _stringRefPages = Enumerable.Range(0, strings).Select(_ => new List<byte[][]>()).ToArray();
        }

        public int Stride { get; }

        // Appends one row (cell values derived from i) and returns (index, hash).
        public (int Index, int Hash) Append(long i)
        {
            var (p, o) = (_count / PageRows, _count % PageRows);
            if (o == 0)
            {
                _cellPages.Add(new byte[PageRows * Stride]);
                foreach (var refs in _stringRefPages)
                {
                    refs.Add(new byte[PageRows][]);
                }
            }

            var cell = _cellPages[p].AsSpan(o * Stride, Stride);
            var h = new HashCode();
            for (var c = 0; c < _cols.Length; c++)
            {
                var at = cell[_offset[c]..];
                switch (_cols[c])
                {
                    case 'L':
                        BinaryPrimitives.WriteInt64LittleEndian(at, i * (c + 1));
                        h.Add(i * (c + 1));
                        break;
                    case 'I':
                        BinaryPrimitives.WriteInt32LittleEndian(at, (int)(i % 1000));
                        h.Add((int)(i % 1000));
                        break;
                    case 'D':
                        BinaryPrimitives.WriteInt32LittleEndian(at, (int)(i % 20000));
                        h.Add((int)(i % 20000));
                        break;
                    case 'M':
                        BinaryPrimitives.WriteInt128LittleEndian(at, (Int128)(i * 100));
                        h.Add(i * 100);
                        break;
                    case 'S':
                        Span<byte> bytes;
                        if (_inlineStrings)
                        {
                            if (_stringUsed + _strLen > StringPageBytes)
                            {
                                _stringPages.Add(new byte[StringPageBytes]);
                                _stringUsed = 0;
                            }

                            var global = ((long)(_stringPages.Count - 1) * StringPageBytes) + _stringUsed;
                            BinaryPrimitives.WriteInt64LittleEndian(at, global);
                            BinaryPrimitives.WriteInt32LittleEndian(at[8..], _strLen);
                            bytes = _stringPages[^1].AsSpan(_stringUsed, _strLen);
                            _stringUsed += _strLen;
                        }
                        else
                        {
                            var s = new byte[_strLen];
                            _stringRefPages[_stringCol[c]][p][o] = s;
                            bytes = s;
                        }

                        FillString(bytes, i, c);
                        h.AddBytes(bytes);
                        break;
                }
            }

            return (_count++, h.ToHashCode());
        }
    }

    private sealed class ArenaSink : IRowSink
    {
        private readonly Arena _keys;
        private readonly Arena _values;
        private readonly Traces<RowHandle>? _handles;
        private readonly Traces<long>? _ids;
        private readonly int _objectsPerValueRow;

        public ArenaSink(string columns, int strLen, int share, bool inlineStrings, bool handles)
        {
            _keys = new Arena("L", strLen, inlineStrings);
            _values = new Arena(columns, strLen, inlineStrings);
            _handles = handles ? new Traces<RowHandle>(share) : null;
            _ids = handles ? null : new Traces<long>(share);
            _objectsPerValueRow = (handles ? 1 : 0) + (inlineStrings ? 0 : columns.Count(c => c == 'S'));
        }

        public int ObjectsPerValueRow => _objectsPerValueRow;

        public void AddKey(long k) => Add(_keys, 0, k);

        public void AddValue(long i) => Add(_values, 1, i);

        private void Add(Arena arena, long arenaId, long i)
        {
            var (index, hash) = arena.Append(i);
            if (_handles is not null)
            {
                _handles.Add(new RowHandle(arena, index, hash));
            }
            else
            {
                _ids!.Add((arenaId << 40) | (uint)index);
            }
        }
    }

    // Runtime GC events (keyword 0x1): pause per GC kind from suspend-begin to restart-end (copied
    // from IvmMultiBatch's IVM_GC_EVENTS listener), plus bytes promoted out of gen0 and gen1 by the
    // young GCs (GCHeapStats TotalPromotedSize0/1 — the copying the pause pays for).
    private sealed class GcEventListener : EventListener
    {
        private readonly object _gate = new();
        private readonly Dictionary<string, (int Count, double Ms)> _byKind = new(StringComparer.Ordinal);
        private DateTime _suspend;
        private string? _kind;
        private long _promoted0;
        private long _promoted1;

        public void Reset()
        {
            lock (_gate)
            {
                _byKind.Clear();
                (_promoted0, _promoted1) = (0, 0);
            }
        }

        protected override void OnEventSourceCreated(EventSource source)
        {
            if (source.Name == "Microsoft-Windows-DotNETRuntime")
            {
                EnableEvents(source, EventLevel.Informational, (EventKeywords)0x1);
            }
        }

        protected override void OnEventWritten(EventWrittenEventArgs e)
        {
            lock (_gate)
            {
                switch (e.EventName)
                {
                    case "GCSuspendEEBegin_V1":
                        _suspend = e.TimeStamp;
                        break;
                    case "GCStart_V2":
                        var depth = Field(e, "Depth");
                        var type = Field(e, "Type");
                        _kind = "gen" + depth + (type == 1 ? " bg" : type == 2 ? " fg" : " blocking");
                        if (depth == 2 && type != 1)
                        {
                            // Why a full GC blocked: 0 alloc-small, 4 alloc-large, 5/6 out of space
                            // SOH/LOH, 8 internal (e.g. a young GC escalated) — GCStart Reason.
                            _kind += " reason" + Field(e, "Reason");
                        }
                        break;
                    case "GCHeapStats_V2" when _kind is not null && (_kind.StartsWith("gen0", StringComparison.Ordinal) || _kind.StartsWith("gen1", StringComparison.Ordinal)):
                        _promoted0 += Field(e, "TotalPromotedSize0");
                        _promoted1 += Field(e, "TotalPromotedSize1");
                        break;
                    case "GCRestartEEEnd_V1":
                        if (_suspend != default)
                        {
                            var kind = _kind ?? "suspend-only (BGC phase)";
                            var (n, ms) = _byKind.GetValueOrDefault(kind);
                            _byKind[kind] = (n + 1, ms + (e.TimeStamp - _suspend).TotalMilliseconds);
                        }

                        _suspend = default;
                        _kind = null;
                        break;
                }
            }
        }

        private static long Field(EventWrittenEventArgs e, string name)
        {
            var i = e.PayloadNames!.IndexOf(name);
            return i < 0 ? 0 : Convert.ToInt64(e.Payload![i], CultureInfo.InvariantCulture);
        }

        public string Report()
        {
            lock (_gate)
            {
                var young = _byKind.Where(kv => kv.Key.StartsWith("gen0", StringComparison.Ordinal) || kv.Key.StartsWith("gen1", StringComparison.Ordinal))
                    .Sum(kv => kv.Value.Ms);
                return string.Join("; ", _byKind.OrderBy(kv => kv.Key, StringComparer.Ordinal).Select(kv =>
                        string.Create(CultureInfo.InvariantCulture, $"{kv.Key} {kv.Value.Count}x {kv.Value.Ms:F0} ms")))
                    + string.Create(CultureInfo.InvariantCulture,
                        $"; young pause {young:F0} ms; events total {_byKind.Values.Sum(v => v.Ms):F0} ms; " +
                        $"promoted g0 {_promoted0 / 1e9:F2} GB g1 {_promoted1 / 1e9:F2} GB");
            }
        }
    }
}
