// What young-generation GC pause each ROW FORM costs while a heap GROWS the way an ivm-bench
// SF=3 batch-1 bulk load grows it (design-state-representation §9.15: the batch-1 pause is all
// gen0/gen1 copying of new survivors, not full GCs — so TraceLayoutGcProbe's forced full GCs of
// a static heap are the wrong measure for rows).
//
// The heap grows chunk by chunk: rows in the census shape (TraceLayoutGcProbe.Shape: one key row
// per group, then that group's value rows), every row retained and referenced from SHARE paged
// "traces", with short-lived garbage allocated between chunks. No GC.Collect while growing:
// gen0/gen1 run naturally. One forced full GC at the very end measures retained bytes.
//
// Calibrated to the SF=3 batch-1 row census (rows held by traces: 32.5M distinct, 5.18 GB,
// ~159 B/row; object[] 58%, StructuralRow 20%; ~8.6 cells/row ≈ 2.4 double, 2.75 string, 1.0
// long, 0.9 timestamp, 0.8 int, 0.4 date, 0.35 null). Two census facts shape the defaults:
//   * CELL BOXES ARE SHARED across rows (projections copy cell references): refs per distinct box
//     ≈ 7 for numerics, ≈ 13 for strings. Every form shares a column's value across BOXSHARE /
//     STRSHARE consecutive rows; the boxed form reuses the same box object, the others store the
//     value inline (strings: the same byte[] / Utf8String box / byte-page offset).
//   * ROWS ARE BARELY SHARED across traces (30.9M rows in 1 trace, 1.63M in 2): SHARE=1.
//
//   ROW_FORM_PROBE=boxed          today: StructuralRow + object?[] + boxed cells (Double / Int64 /
//                                 Timestamp / Int32 / Date32 / Decimal128 boxes; a boxed
//                                 Utf8String over its own byte[]), boxes shared as above
//   ROW_FORM_PROBE=inline-class   one class object per row with the cells as typed fields inside
//                                 it (emitted-subclass style: header + hash + fixed-width cells +
//                                 references to shared Utf8String boxes) — no object[], no numeric
//                                 boxes. The "unboxed and shared, no arena" candidate.
//   ROW_FORM_PROBE=arena          a 32-byte handle object per row (arena ref + row index + hash);
//                                 fixed-width cells row-major in per-schema 16K-row byte[] pages
//                                 (LOH); each string a separate (shared) byte[] referenced from a
//                                 16K-entry byte[][] page per string column (LOH page, young byte[])
//   ROW_FORM_PROBE=arena-strings  as arena, but string bytes bump-allocated in 1 MiB LOH byte[]
//                                 pages; the cell holds (global offset: long, length: int)
//   ROW_FORM_PROBE=handle-free    as arena-strings with no per-row object at all: the traces hold
//                                 long row ids (arena << 40 | index) — the upper bound
//
// Knobs (all optional):
//   ROW_FORM_SCALE=1.0     fraction of the census group counts (1.0 = 5.40M keys + 24.9M values)
//   ROW_FORM_COLUMNS=FFSSSLTIN  value-row column mix: F double, S string, L long, T Timestamp,
//                          I int, D Date32, M Decimal128, N a nullable long that is always NULL
//                          (key rows are always one long column)
//   ROW_FORM_STRLEN=16     bytes per string (byte[] object = 40 B; census average ≈ 42 B)
//   ROW_FORM_BOXSHARE=7    consecutive rows sharing each numeric cell value (and box)
//   ROW_FORM_STRSHARE=13   consecutive rows sharing each string value (and its objects)
//   ROW_FORM_SHARE=1       traces referencing every row
//   ROW_FORM_GARBAGE=3.0   short-lived garbage bytes per row, as a multiple of the BOXED row's
//                          estimated bytes — an absolute amount, identical in every mode
//                          (batch 1: ~41 GiB allocated for 10.6 GB retained, ~3:1)
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
using System.Runtime.InteropServices;
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

    private sealed record Knobs(string Columns, int StrLen, int BoxShare, int StrShare, int Share);

    [Fact]
    public void Probe()
    {
        var mode = Environment.GetEnvironmentVariable("ROW_FORM_PROBE");
        if (string.IsNullOrEmpty(mode))
        {
            return;
        }

        var scale = Env("ROW_FORM_SCALE", 1.0);
        var k = new Knobs(
            Environment.GetEnvironmentVariable("ROW_FORM_COLUMNS") is { Length: > 0 } c ? c : "FFSSSLTIN",
            (int)Env("ROW_FORM_STRLEN", 16),
            Math.Max(1, (int)Env("ROW_FORM_BOXSHARE", 7)),
            Math.Max(1, (int)Env("ROW_FORM_STRSHARE", 13)),
            (int)Env("ROW_FORM_SHARE", 1));
        var garbageRatio = Env("ROW_FORM_GARBAGE", 3.0);
        var chunk = (int)Env("ROW_FORM_CHUNK", PageRows);
        var window = (int)Env("ROW_FORM_WINDOW", 1);

        IRowSink sink = mode switch
        {
            "boxed" => new BoxedSink(k),
            "inline-class" => new InlineClassSink(k),
            "arena" => new ArenaSink(k, inlineStrings: false, handles: true),
            "arena-strings" => new ArenaSink(k, inlineStrings: true, handles: true),
            "handle-free" => new ArenaSink(k, inlineStrings: true, handles: false),
            _ => throw new ArgumentException(mode),
        };

        // Garbage is fixed in absolute bytes so every mode allocates the same transient load; the
        // reference is the boxed form's per-value-row bytes (static estimate, see BoxedRowBytes).
        var garbagePerRow = (long)(garbageRatio * BoxedRowBytes(k));
        var garbageWindow = new Queue<object?[]>();
        long garbageBytes = 0, keyBytes = 0, valueBytes = 0;
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
                // Per-call allocation split (an FCall read; cheap) so value rows report on their own.
                var a = GC.GetAllocatedBytesForCurrentThread();
                sink.AddKey(keys++);
                var b = GC.GetAllocatedBytesForCurrentThread();
                keyBytes += b - a;
                rows++;
                inChunk++;
                for (var j = 0; j < size; j++)
                {
                    sink.AddValue(v++);
                    var e = GC.GetAllocatedBytesForCurrentThread();
                    valueBytes += e - b;
                    rows++;
                    if (++inChunk >= chunk)
                    {
                        garbageBytes += Garbage(garbagePerRow * inChunk, garbageWindow, window);
                        inChunk = 0;
                        e = GC.GetAllocatedBytesForCurrentThread();
                    }

                    b = e;
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
            $"ROWFORM {mode} cols={k.Columns} str={k.StrLen} boxshare={k.BoxShare} strshare={k.StrShare} share={k.Share} " +
            $"garbage={garbageRatio:F1} scale={scale:F3} rows={rows / 1e6:F2}M (keys {keys / 1e6:F2}M) " +
            $"live={live / 1e9:F2} GB ({(double)live / rows:F0} B/row) kept-alloc={(double)kept / rows:F0} B/row " +
            $"(value {(double)valueBytes / v:F0}, key {(double)keyBytes / keys:F0}) objects/value-row={sink.ObjectsPerValueRow:F2} " +
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

    // Static per-value-row byte estimate for the boxed form on 64-bit .NET (object header + method
    // table = 16 B; arrays add an 8 B length; sizes round up to 8); shared boxes amortised.
    private static double BoxedRowBytes(Knobs k)
    {
        double b = 32 + Round8(24 + (8 * k.Columns.Length)); // StructuralRow + object?[]
        foreach (var c in k.Columns)
        {
            b += c switch
            {
                'F' or 'L' or 'T' or 'I' or 'D' => 24.0 / k.BoxShare,
                'M' => Round8(16 + Unsafe.SizeOf<Decimal128>()) / (double)k.BoxShare,
                'S' => (32 + Round8(24 + k.StrLen)) / (double)k.StrShare, // Utf8String box + byte[]
                'N' => 0,
                _ => throw new ArgumentException(k.Columns),
            };
        }

        return b;
    }

    private static long Round8(long x) => (x + 7) & ~7L;

    // The logical value of column c (type t) in row i: equal across `share` consecutive rows, so
    // every form holds the same data (s is the shared value's identity).
    private static long Source(long i, char t, Knobs k) => i / (t == 'S' ? k.StrShare : k.BoxShare);

    private static long Num(char t, int c, long s) => t switch
    {
        'F' => BitConverter.DoubleToInt64Bits((s * 0.25) + c),
        'L' => s * (c + 1),
        'T' => s * 1_000_000,
        'I' => (int)(s % 1000),
        'D' => (int)(s % 20000),
        'M' => s * 100,
        _ => 0,
    };

    private static void FillString(Span<byte> dst, long s, int col)
    {
        // Unique-ish ASCII digits, no transient string.
        var x = (ulong)(s * 31 + col);
        for (var k = dst.Length - 1; k >= 0; k--)
        {
            dst[k] = (byte)('0' + (int)(x % 10));
            x /= 10;
        }
    }

    private interface IRowSink
    {
        double ObjectsPerValueRow { get; }

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

    // Remembers, per column, the last shared value's identity and its object (box / string), so a
    // form that shares objects reuses one across the rows that share the value.
    private sealed class SharedCells(int columns)
    {
        private readonly long[] _source = Enumerable.Repeat(-1L, columns).ToArray();
        private readonly object?[] _obj = new object?[columns];

        public bool TryGet(int c, long s, out object? o)
        {
            o = _obj[c];
            return _source[c] == s;
        }

        public object? Set(int c, long s, object? o)
        {
            (_source[c], _obj[c]) = (s, o);
            return o;
        }
    }

    private sealed class BoxedSink(Knobs k) : IRowSink
    {
        private readonly Traces<StructuralRow> _traces = new(k.Share);
        private readonly SharedCells _keyCells = new(1);
        private readonly SharedCells _cells = new(k.Columns.Length);

        public double ObjectsPerValueRow => 2 + k.Columns.Sum(c => c switch
        {
            'N' => 0,
            'S' => 2.0 / k.StrShare,
            _ => 1.0 / k.BoxShare,
        });

        public void AddKey(long key) => _traces.Add(new StructuralRow(new object?[] { Cell(_keyCells, 'L', 0, key) }));

        public void AddValue(long i)
        {
            var cells = new object?[k.Columns.Length];
            for (var c = 0; c < cells.Length; c++)
            {
                cells[c] = Cell(_cells, k.Columns[c], c, i);
            }

            _traces.Add(new StructuralRow(cells));
        }

        private object? Cell(SharedCells shared, char t, int c, long i)
        {
            if (t == 'N')
            {
                return null;
            }

            var s = Source(i, t, k);
            if (shared.TryGet(c, s, out var o))
            {
                return o;
            }

            var n = Num(t, c, s);
            return shared.Set(c, s, t switch
            {
                'F' => BitConverter.Int64BitsToDouble(n),
                'L' => n,
                'T' => new Timestamp(n),
                'I' => (int)n,
                'D' => new Date32((int)n),
                'M' => new Decimal128((Int128)n),
                'S' => NewUtf8(k.StrLen, s, c),
                _ => throw new ArgumentException(k.Columns),
            });
        }
    }

    private static Utf8String NewUtf8(int strLen, long s, int c)
    {
        var bytes = new byte[strLen];
        FillString(bytes, s, c);
        return Utf8String.FromBytes(bytes);
    }

    // Fixed-width layout of one schema's cells, shared by the arena and inline-class forms: a null
    // mask (only if the schema has a nullable column), then the cells in column order — 8 B for
    // F/L/T/N, 4 B for I/D, 16 B for M, and for S either 12 B (offset: long, length: int into
    // string pages) or nothing (the string is an object reference held beside the cells).
    private sealed class CellLayout
    {
        public CellLayout(string cols, bool inlineStrings)
        {
            Cols = cols;
            Offset = new int[cols.Length];
            StringOrdinal = new int[cols.Length];
            var stride = cols.Contains('N') ? (cols.Length + 7) / 8 : 0;
            for (var c = 0; c < cols.Length; c++)
            {
                Offset[c] = stride;
                StringOrdinal[c] = cols[c] == 'S' ? Strings++ : -1;
                stride += cols[c] switch
                {
                    'F' or 'L' or 'T' or 'N' => 8,
                    'I' or 'D' => 4,
                    'M' => 16,
                    'S' => inlineStrings ? 12 : 0,
                    _ => throw new ArgumentException(cols),
                };
            }

            Stride = stride;
        }

        public string Cols { get; }

        public int[] Offset { get; }

        public int[] StringOrdinal { get; }

        public int Strings { get; }

        public int Stride { get; }

        // Writes the numeric cells (and the null mask) of row i; returns their hash contribution.
        public void WriteNumeric(Span<byte> cells, long i, Knobs k, ref HashCode h)
        {
            for (var c = 0; c < Cols.Length; c++)
            {
                var t = Cols[c];
                var at = cells[Offset[c]..];
                switch (t)
                {
                    case 'S':
                        break;
                    case 'N':
                        cells[c / 8] |= (byte)(1 << (c % 8));
                        h.Add(0);
                        break;
                    case 'I' or 'D':
                        var n4 = (int)Num(t, c, Source(i, t, k));
                        BinaryPrimitives.WriteInt32LittleEndian(at, n4);
                        h.Add(n4);
                        break;
                    case 'M':
                        var m = Num(t, c, Source(i, t, k));
                        BinaryPrimitives.WriteInt128LittleEndian(at, m);
                        h.Add(m);
                        break;
                    default:
                        var n8 = Num(t, c, Source(i, t, k));
                        BinaryPrimitives.WriteInt64LittleEndian(at, n8);
                        h.Add(n8);
                        break;
                }
            }
        }
    }

    // ---- inline-class: one object per row, cells as fields inside it -------------------------
    // A real emitted subclass declares typed fields; here the fields are an [InlineArray] of longs
    // big enough for the layout's stride plus an [InlineArray] of string references, chosen per
    // schema from a few sizes — the same bytes in the same object, without codegen.
    private abstract class InlineRow
    {
        public int Hash;

        public abstract Span<byte> Cells { get; }

        public abstract Span<object?> Refs { get; }
    }

    private sealed class InlineRow<TCells, TRefs> : InlineRow
        where TCells : struct
        where TRefs : struct
    {
        private TCells _cells;
        private TRefs _refs;

        public override Span<byte> Cells =>
            MemoryMarshal.AsBytes(MemoryMarshal.CreateSpan(ref Unsafe.As<TCells, long>(ref _cells), Unsafe.SizeOf<TCells>() / 8));

        public override Span<object?> Refs =>
            MemoryMarshal.CreateSpan(ref Unsafe.As<TRefs, object?>(ref _refs), Unsafe.SizeOf<TRefs>() / 8);
    }

    [InlineArray(1)] private struct L1 { private long _e; }
    [InlineArray(2)] private struct L2 { private long _e; }
    [InlineArray(3)] private struct L3 { private long _e; }
    [InlineArray(4)] private struct L4 { private long _e; }
    [InlineArray(5)] private struct L5 { private long _e; }
    [InlineArray(6)] private struct L6 { private long _e; }
    [InlineArray(7)] private struct L7 { private long _e; }
    [InlineArray(8)] private struct L8 { private long _e; }
    [InlineArray(10)] private struct L10 { private long _e; }
    [InlineArray(12)] private struct L12 { private long _e; }
    [InlineArray(16)] private struct L16 { private long _e; }
    private struct R0 { }
    [InlineArray(1)] private struct R1 { private object? _e; }
    [InlineArray(2)] private struct R2 { private object? _e; }
    [InlineArray(3)] private struct R3 { private object? _e; }
    [InlineArray(4)] private struct R4 { private object? _e; }
    [InlineArray(6)] private struct R6 { private object? _e; }
    [InlineArray(8)] private struct R8 { private object? _e; }

    private static readonly (int N, Type T)[] CellSizes =
        [(1, typeof(L1)), (2, typeof(L2)), (3, typeof(L3)), (4, typeof(L4)), (5, typeof(L5)), (6, typeof(L6)),
         (7, typeof(L7)), (8, typeof(L8)), (10, typeof(L10)), (12, typeof(L12)), (16, typeof(L16))];

    private static readonly (int N, Type T)[] RefSizes =
        [(0, typeof(R0)), (1, typeof(R1)), (2, typeof(R2)), (3, typeof(R3)), (4, typeof(R4)), (6, typeof(R6)), (8, typeof(R8))];

    private static InlineRow<TCells, TRefs> NewInline<TCells, TRefs>()
        where TCells : struct
        where TRefs : struct => new InlineRow<TCells, TRefs>();

    private static Func<InlineRow> InlineFactory(CellLayout layout)
    {
        var longs = Math.Max(1, (layout.Stride + 7) / 8);
        var cells = CellSizes.First(s => s.N >= longs).T;
        var refs = RefSizes.First(s => s.N >= layout.Strings).T;
        return typeof(RowFormGcProbe).GetMethod(nameof(NewInline), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!
            .MakeGenericMethod(cells, refs).CreateDelegate<Func<InlineRow>>();
    }

    private sealed class InlineClassSink : IRowSink
    {
        private readonly Knobs _k;
        private readonly Traces<InlineRow> _traces;
        private readonly CellLayout _keyLayout = new("L", inlineStrings: false);
        private readonly CellLayout _layout;
        private readonly Func<InlineRow> _newKey;
        private readonly Func<InlineRow> _newValue;
        private readonly SharedCells _strings;

        public InlineClassSink(Knobs k)
        {
            _k = k;
            _traces = new(k.Share);
            _layout = new(k.Columns, inlineStrings: false);
            _newKey = InlineFactory(_keyLayout);
            _newValue = InlineFactory(_layout);
            _strings = new(k.Columns.Length);
        }

        public double ObjectsPerValueRow => 1 + (_layout.Strings * 2.0 / _k.StrShare);

        public void AddKey(long key) => _traces.Add(Fill(_newKey(), _keyLayout, key));

        public void AddValue(long i)
        {
            var row = Fill(_newValue(), _layout, i);
            var refs = row.Refs;
            var h = new HashCode();
            h.Add(row.Hash);
            for (var c = 0; c < _layout.Cols.Length; c++)
            {
                if (_layout.Cols[c] != 'S')
                {
                    continue;
                }

                // A reference to a shared boxed Utf8String, as the boxed form holds (shared ≈13x).
                var s = Source(i, 'S', _k);
                if (!_strings.TryGet(c, s, out var str))
                {
                    str = _strings.Set(c, s, NewUtf8(_k.StrLen, s, c));
                }

                refs[_layout.StringOrdinal[c]] = str;
                h.Add(s);
            }

            row.Hash = h.ToHashCode();
            _traces.Add(row);
        }

        private InlineRow Fill(InlineRow row, CellLayout layout, long i)
        {
            var h = new HashCode();
            layout.WriteNumeric(row.Cells, i, _k, ref h);
            row.Hash = h.ToHashCode();
            return row;
        }
    }

    // ---- arena forms ------------------------------------------------------------------------

    // A row's identity: which arena, which slot, its hash. 16 B header + 8 + 4 + 4 = 32 B.
    private sealed class RowHandle(Arena arena, int index, int hash)
    {
        public readonly Arena Arena = arena;
        public readonly int Index = index;
        public readonly int Hash = hash;
    }

    // One schema's rows: CellLayout rows row-major in 16K-row byte[] pages (LOH for any stride
    // ≥ 6 B), string bytes either as separate shared byte[] objects (byte[][] pages) or
    // bump-allocated in 1 MiB byte pages with (offset: long, length: int) in the cell.
    private sealed class Arena
    {
        private readonly Knobs _k;
        private readonly CellLayout _layout;
        private readonly bool _inlineStrings;
        private readonly int _stride;
        private readonly List<byte[]> _cellPages = new();
        private readonly List<byte[][]>[] _stringRefPages;
        private readonly List<byte[]> _stringPages = new();
        private readonly SharedCells _shared;   // shared byte[] per string column (arena)
        private readonly long[] _strSource;     // shared (offset) per string column (arena-strings)
        private readonly long[] _strAt;
        private int _stringUsed = StringPageBytes;
        private int _count;

        public Arena(string cols, Knobs k, bool inlineStrings)
        {
            (_k, _inlineStrings) = (k, inlineStrings);
            _layout = new CellLayout(cols, inlineStrings);
            _stride = Math.Max(_layout.Stride, 6); // keep a page ≥ 85 KB (LOH)
            // arena-strings keeps no per-row string references, so no reference pages either.
            _stringRefPages = Enumerable.Range(0, inlineStrings ? 0 : _layout.Strings).Select(_ => new List<byte[][]>()).ToArray();
            _shared = new SharedCells(cols.Length);
            _strSource = Enumerable.Repeat(-1L, cols.Length).ToArray();
            _strAt = new long[cols.Length];
        }

        // Appends one row (cell values derived from i) and returns (index, hash).
        public (int Index, int Hash) Append(long i)
        {
            var (p, o) = (_count / PageRows, _count % PageRows);
            if (o == 0)
            {
                _cellPages.Add(new byte[PageRows * _stride]);
                foreach (var refs in _stringRefPages)
                {
                    refs.Add(new byte[PageRows][]);
                }
            }

            var cells = _cellPages[p].AsSpan(o * _stride, _stride);
            var h = new HashCode();
            _layout.WriteNumeric(cells, i, _k, ref h);
            for (var c = 0; c < _layout.Cols.Length; c++)
            {
                if (_layout.Cols[c] != 'S')
                {
                    continue;
                }

                var s = Source(i, 'S', _k);
                h.Add(s);
                if (_inlineStrings)
                {
                    // Shared value → the same (offset, length); new value → bump-allocate bytes.
                    if (_strSource[c] != s)
                    {
                        if (_stringUsed + _k.StrLen > StringPageBytes)
                        {
                            _stringPages.Add(new byte[StringPageBytes]);
                            _stringUsed = 0;
                        }

                        FillString(_stringPages[^1].AsSpan(_stringUsed, _k.StrLen), s, c);
                        (_strSource[c], _strAt[c]) = (s, ((long)(_stringPages.Count - 1) * StringPageBytes) + _stringUsed);
                        _stringUsed += _k.StrLen;
                    }

                    var cell = cells[_layout.Offset[c]..];
                    BinaryPrimitives.WriteInt64LittleEndian(cell, _strAt[c]);
                    BinaryPrimitives.WriteInt32LittleEndian(cell[8..], _k.StrLen);
                }
                else
                {
                    if (!_shared.TryGet(c, s, out var bytes))
                    {
                        var b = new byte[_k.StrLen];
                        FillString(b, s, c);
                        bytes = _shared.Set(c, s, b);
                    }

                    _stringRefPages[_layout.StringOrdinal[c]][p][o] = (byte[])bytes!;
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

        public ArenaSink(Knobs k, bool inlineStrings, bool handles)
        {
            _keys = new Arena("L", k, inlineStrings);
            _values = new Arena(k.Columns, k, inlineStrings);
            _handles = handles ? new Traces<RowHandle>(k.Share) : null;
            _ids = handles ? null : new Traces<long>(k.Share);
            ObjectsPerValueRow = (handles ? 1 : 0) + (inlineStrings ? 0 : k.Columns.Count(c => c == 'S') / (double)k.StrShare);
        }

        public double ObjectsPerValueRow { get; }

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
    internal sealed class GcEventListener : EventListener
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
