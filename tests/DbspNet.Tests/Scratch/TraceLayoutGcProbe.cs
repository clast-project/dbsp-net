// What GC pause the indexed-trace container layer costs, against the rows it holds: the
// ivm-bench SF=3 batch-1 trace shape (DBSPNET_TRACE_ACCESS_PROFILE census, 2026-10-30) built
// at full scale in one of three layouts, then timed under forced full blocking GCs.
//   TRACE_LAYOUT_PROBE=rows     the value rows and keys alone, in plain arrays (baseline)
//   TRACE_LAYOUT_PROBE=indexed  the same rows in today's IndexedZSetTrace
//   TRACE_LAYOUT_PROBE=flat     the same rows in a flat prototype: key -> head index, plus
//                               value / weight / next arrays (no per-group objects)
//   TRACE_LAYOUT_PROBE=rows-paged / flat-paged   the same, with every big array split into
//                               16K-entry pages (Server GC marks one huge array on one thread)
// Run each in its own process (DOTNET_gcServer=1 to match the server). No-op when unset.
using System.Diagnostics;
using System.Globalization;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Sql.TypeSystem;
using Xunit;
using Xunit.Abstractions;

namespace DbspNet.Tests.Scratch;

public class TraceLayoutGcProbe
{
    // (groups, values per group) per census size bucket: 1, 2-4, 5-16, 17-256, 257-4096, >4096.
    private static readonly (int Groups, int Size)[] Shape =
    [
        (3_328_848, 1),
        (1_921_009, 3),   // 4.83M values / 1.92M groups ≈ 2.5
        (108_960, 6),
        (27_938, 92),
        (14_203, 741),
        (15, 197_589),
    ];

    private readonly ITestOutputHelper _out;

    public TraceLayoutGcProbe(ITestOutputHelper output) => _out = output;

    [Fact]
    public void Probe()
    {
        var layout = Environment.GetEnvironmentVariable("TRACE_LAYOUT_PROBE");
        if (string.IsNullOrEmpty(layout))
        {
            return;
        }

        var sw = Stopwatch.StartNew();
        object held = layout switch
        {
            "rows" => BuildRows(),
            "indexed" => BuildIndexed(),
            "flat" => BuildFlat(paged: false),
            "rows-paged" => BuildRowsPaged(),
            "flat-paged" => BuildFlat(paged: true),
            _ => throw new ArgumentException(layout),
        };
        var buildMs = sw.Elapsed.TotalMilliseconds;

        GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
        var promoted = GC.GetGCMemoryInfo(GCKind.FullBlocking).PromotedBytes;
        var pause0 = GC.GetTotalPauseDuration();
        const int Collections = 5;
        for (var i = 0; i < Collections; i++)
        {
            GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
        }

        var pauseMs = (GC.GetTotalPauseDuration() - pause0).TotalMilliseconds / Collections;
        GC.KeepAlive(held);
        _out.WriteLine(string.Create(
            CultureInfo.InvariantCulture,
            $"LAYOUT {layout} live={promoted / 1e9:F2} GB  full-GC pause={pauseMs:F0} ms  build={buildMs:F0} ms"));
    }

    // A value row shaped like ivm-bench's: six columns, five of them boxed scalars.
    private static StructuralRow ValueRow(long i) =>
        StructuralRow.Of(new object?[] { i, (int)(i % 1000), new Date32((int)(i % 20000)), i * 3, (int)(i % 7), "sym" });

    private static StructuralRow KeyRow(long g) => StructuralRow.Of(new object?[] { g });

    private static IEnumerable<(StructuralRow Key, int Size)> Groups()
    {
        long g = 0;
        foreach (var (groups, size) in Shape)
        {
            for (var i = 0; i < groups; i++)
            {
                yield return (KeyRow(g++), size);
            }
        }
    }

    private static (List<StructuralRow> Keys, List<StructuralRow> Values) BuildRows()
    {
        var keys = new List<StructuralRow>();
        var values = new List<StructuralRow>();
        long v = 0;
        foreach (var (key, size) in Groups())
        {
            keys.Add(key);
            for (var j = 0; j < size; j++)
            {
                values.Add(ValueRow(v++));
            }
        }

        return (keys, values);
    }

    private static IndexedZSetTrace<StructuralRow, StructuralRow, Z64> BuildIndexed()
    {
        // Integrated a batch of groups at a time, as an operator integrates its deltas.
        var trace = new IndexedZSetTrace<StructuralRow, StructuralRow, Z64>();
        var b = new IndexedZSetBuilder<StructuralRow, StructuralRow, Z64>();
        long v = 0;
        var pending = 0;
        foreach (var (key, size) in Groups())
        {
            for (var j = 0; j < size; j++)
            {
                b.Add(key, ValueRow(v++), Z64.One);
            }

            if (++pending == 50_000)
            {
                trace.Integrate(b.Build());
                b = new IndexedZSetBuilder<StructuralRow, StructuralRow, Z64>();
                pending = 0;
            }
        }

        trace.Integrate(b.Build());
        return trace;
    }

    private static List<StructuralRow[]> BuildRowsPaged()
    {
        var pages = new List<StructuralRow[]>();
        var page = new StructuralRow[PageSize];
        var n = 0;
        void Add(StructuralRow r)
        {
            if (n == PageSize)
            {
                pages.Add(page);
                page = new StructuralRow[PageSize];
                n = 0;
            }

            page[n++] = r;
        }

        long v = 0;
        foreach (var (key, size) in Groups())
        {
            Add(key);
            for (var j = 0; j < size; j++)
            {
                Add(ValueRow(v++));
            }
        }

        pages.Add(page);
        return pages;
    }

    private const int PageSize = 16_384;

    private static FlatTrace BuildFlat(bool paged)
    {
        var flat = new FlatTrace(paged);
        long v = 0;
        foreach (var (key, size) in Groups())
        {
            for (var j = 0; j < size; j++)
            {
                flat.Append(key, ValueRow(v++), 1);
            }
        }

        return flat;
    }

    // The prototype's storage only (appends; no in-group lookup): what the GC sees. Paged, the
    // value / weight / next arrays are 16K-entry pages instead of three growing arrays.
    private sealed class FlatTrace(bool paged)
    {
        private readonly Dictionary<StructuralRow, int> _head = new();
        private readonly List<StructuralRow[]> _valuePages = new();
        private readonly List<long[]> _weightPages = new();
        private readonly List<int[]> _nextPages = new();
        private StructuralRow[] _values = new StructuralRow[1024];
        private long[] _weights = new long[1024];
        private int[] _next = new int[1024];
        private int _count;

        public void Append(StructuralRow key, StructuralRow value, long weight)
        {
            var next = _head.TryGetValue(key, out var h) ? h : -1;
            _head[key] = _count;
            if (paged)
            {
                var (p, o) = (_count / PageSize, _count % PageSize);
                if (o == 0)
                {
                    _valuePages.Add(new StructuralRow[PageSize]);
                    _weightPages.Add(new long[PageSize]);
                    _nextPages.Add(new int[PageSize]);
                }

                _valuePages[p][o] = value;
                _weightPages[p][o] = weight;
                _nextPages[p][o] = next;
            }
            else
            {
                if (_count == _values.Length)
                {
                    Array.Resize(ref _values, _count * 2);
                    Array.Resize(ref _weights, _count * 2);
                    Array.Resize(ref _next, _count * 2);
                }

                _values[_count] = value;
                _weights[_count] = weight;
                _next[_count] = next;
            }

            _count++;
        }
    }
}
