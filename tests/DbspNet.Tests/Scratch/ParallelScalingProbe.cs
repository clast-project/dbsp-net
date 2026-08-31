// Kill-question probe for the structural-parallel batch-1 thesis: does an
// ALLOCATION-BOUND DBSP workload scale on the shared managed heap, or does GC /
// allocator contention flatten the speedup? Batch-1 churns 44 GiB single-threaded;
// if W workers each allocating into one GC don't get ~linear wall speedup, the
// "parallelise the program path" lever is dead regardless of how clean the Exchange
// plumbing is (ExchangeOp/ParallelCircuit are already generic over StructuralRow).
//
// Faithful by construction: each worker runs the REAL join kernel
// (IncrementalJoinCore.JoinInto) + REAL IndexedZSetTrace + StructuralRow output —
// the exact per-tuple allocation profile of batch-1's hot join/projection tier —
// over a DISJOINT shard of the 900K-row watches_history workload. Disjoint shards
// need no Exchange (independent replicas), so this measures the SCALING CEILING:
// pure op + managed-heap parallelism, no shuffle/barrier tax. If even this ceiling
// is flat, stop; if it scales, the realistic path (with exchanges at coarse 20-tick
// granularity, which Nexmark showed is cheap) is worth the compiler work.
//
// MUST run with DOTNET_gcServer=1 (batch-1's GC mode); workstation GC will mislead.
// Gated on IVM_MICRO=1.
using System.Diagnostics;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful;
using Xunit;
using Xunit.Abstractions;

namespace DbspNet.Tests.Scratch;

public class ParallelScalingProbe
{
    private readonly ITestOutputHelper _out;

    public ParallelScalingProbe(ITestOutputHelper output) => _out = output;

    private const int LeftRows = 4_500_000;   // ~5x batch-1's watches_history slice so a run lasts
                                              // seconds (thread-spawn negligible, GC steady-state)
    private const int Symbols = 3626;
    private const int Ticks = 20;
    private const int OutWidth = 8;
    private const int Repeats = 3;   // wall is timed → median of repeats

    [Fact]
    public void Measure()
    {
        if (Environment.GetEnvironmentVariable("IVM_MICRO") is not ("1" or "true" or "TRUE"))
        {
            _out.WriteLine("IVM_MICRO not set — skipping parallel-scaling probe.");
            return;
        }

        var right = BuildRight();
        var widths = new[] { 1, 2, 4, 8 };

        _out.WriteLine($"ServerGC={System.Runtime.GCSettings.IsServerGC}  procs={Environment.ProcessorCount}  left_rows={LeftRows}");
        _out.WriteLine("(each worker runs the real join+projection kernel over a disjoint symbol shard)");
        _out.WriteLine("");

        foreach (var columnar in new[] { false, true })
        {
            _out.WriteLine(columnar
                ? "--- LOW-ALLOC columnar sink (pooled cols + 1 boundary materialise) ---"
                : "--- STRUCTURAL sink (StructuralRow throughout, batch-1's rep) ---");
            double baseline = 0;
            foreach (var w in widths)
            {
                var shards = BuildShards(w);
                RunParallel(shards, right, columnar); // warmup

                var times = new List<double>(Repeats);
                long alloc = 0;
                double gcPauseMs = 0;
                for (var rep = 0; rep < Repeats; rep++)
                {
                    var allocBefore = GC.GetTotalAllocatedBytes(precise: false);
                    var pauseBefore = GC.GetTotalPauseDuration();
                    var sw = Stopwatch.StartNew();
                    RunParallel(shards, right, columnar);
                    sw.Stop();
                    times.Add(sw.Elapsed.TotalMilliseconds);
                    alloc = GC.GetTotalAllocatedBytes(precise: false) - allocBefore;
                    gcPauseMs = (GC.GetTotalPauseDuration() - pauseBefore).TotalMilliseconds;
                }

                times.Sort();
                var median = times[times.Count / 2];
                if (w == 1)
                {
                    baseline = median;
                }

                _out.WriteLine(
                    $"W={w,2}  wall {median,7:F1} ms   speedup {baseline / median,4:F2}x   " +
                    $"efficiency {100.0 * (baseline / median) / w,4:F0}%   alloc {alloc / (1024.0 * 1024.0 * 1024.0):F2} GiB   " +
                    $"gc-pause {gcPauseMs,6:F1} ms ({100.0 * gcPauseMs / median,4:F0}% of wall)");
            }

            _out.WriteLine("");
        }

        _out.WriteLine("thesis: if the columnar (low-alloc) sink SCALES BETTER than structural, then reducing");
        _out.WriteLine("per-row allocation and parallelising COMPOUND (less GC stop-the-world -> more scaling).");
    }

    // Run W workers concurrently, each over its own disjoint shard. Returns when all finish.
    private static void RunParallel(List<List<IndexedZSet<string, StructuralRow, Z64>>> shards, IndexedZSet<string, StructuralRow, Z64> right, bool columnar)
    {
        var w = shards.Count;
        if (w == 1)
        {
            if (columnar)
            {
                JoinProjShardCol(shards[0], right);
            }
            else
            {
                JoinProjShard(shards[0], right);
            }

            return;
        }

        var threads = new Thread[w];
        for (var i = 0; i < w; i++)
        {
            var shard = shards[i];
            threads[i] = new Thread(() =>
            {
                if (columnar)
                {
                    JoinProjShardCol(shard, right);
                }
                else
                {
                    JoinProjShard(shard, right);
                }
            }) { IsBackground = true };
        }

        foreach (var t in threads)
        {
            t.Start();
        }

        foreach (var t in threads)
        {
            t.Join();
        }
    }

    // The real per-worker engine work: incremental inner join (dl ⋈ R) + downstream
    // projection, StructuralRow throughout, across this shard's ticks.
    private static long JoinProjShard(List<IndexedZSet<string, StructuralRow, Z64>> leftDeltas, IndexedZSet<string, StructuralRow, Z64> right)
    {
        var leftTrace = new IndexedZSetTrace<string, StructuralRow, Z64>();
        var rightTrace = new IndexedZSetTrace<string, StructuralRow, Z64>();
        rightTrace.Integrate(right);
        long sink = 0;

        foreach (var dl in leftDeltas)
        {
            var builder = new ZSetBuilder<StructuralRow, Z64>();
            IncrementalJoinCore.JoinInto(dl, rightTrace.Current, CombineRow, null, builder);
            var joined = builder.Build();

            var proj = new ZSetBuilder<StructuralRow, Z64>(joined.Count);
            foreach (var (r, wt) in joined)
            {
                var cols = new object?[OutWidth];
                for (var k = 0; k < OutWidth; k++)
                {
                    cols[k] = r[k];
                }

                proj.Add(new StructuralRow(cols), wt);
            }

            sink += proj.Build().Count;
            leftTrace.Integrate(dl);
        }

        return sink;
    }

    // Low-alloc columnar variant: pooled per-worker column buffers reused across
    // ticks, StructuralRows built ONCE at the boundary. Same join match + trace, far
    // less per-row allocation — isolates whether lower alloc improves parallel scaling.
    private static long JoinProjShardCol(List<IndexedZSet<string, StructuralRow, Z64>> leftDeltas, IndexedZSet<string, StructuralRow, Z64> right)
    {
        var leftTrace = new IndexedZSetTrace<string, StructuralRow, Z64>();
        var rightTrace = new IndexedZSetTrace<string, StructuralRow, Z64>();
        rightTrace.Integrate(right);
        long sink = 0;

        var cols = new object?[OutWidth][];
        var cap = 0;
        long[] weights = System.Array.Empty<long>();

        foreach (var dl in leftDeltas)
        {
            var need = 0;
            foreach (var (_, g) in dl)
            {
                need += g.Count;
            }

            need = Math.Max(4, need);
            if (need > cap)
            {
                cap = need;
                for (var c = 0; c < OutWidth; c++)
                {
                    cols[c] = new object?[cap];
                }

                weights = new long[cap];
            }

            var n = 0;
            var rt = rightTrace.Current;
            foreach (var (key, lGroup) in dl)
            {
                var rGroup = rt.GroupFor(key);
                if (rGroup.IsEmpty)
                {
                    continue;
                }

                foreach (var (lv, lw) in lGroup)
                {
                    foreach (var (rv, rw) in rGroup)
                    {
                        cols[0][n] = lv[0];
                        cols[1][n] = lv[1];
                        cols[2][n] = lv[2];
                        cols[3][n] = lv[3];
                        cols[4][n] = rv[1];
                        cols[5][n] = rv[2];
                        cols[6][n] = rv[3];
                        cols[7][n] = rv[4 % rv.Count];
                        weights[n] = Z64.Multiply(lw, rw).Value;
                        n++;
                    }
                }
            }

            var outZ = new ZSetBuilder<StructuralRow, Z64>(n);
            for (var i = 0; i < n; i++)
            {
                var rowCols = new object?[OutWidth];
                for (var c = 0; c < OutWidth; c++)
                {
                    rowCols[c] = cols[c][i];
                }

                outZ.Add(new StructuralRow(rowCols), new Z64(weights[i]));
            }

            sink += outZ.Build().Count;
            leftTrace.Integrate(dl);
        }

        return sink;
    }

    private static StructuralRow CombineRow(string key, StructuralRow l, StructuralRow r)
    {
        var cols = new object?[OutWidth];
        cols[0] = l[0];
        cols[1] = l[1];
        cols[2] = l[2];
        cols[3] = l[3];
        cols[4] = r[1];
        cols[5] = r[2];
        cols[6] = r[3];
        cols[7] = r[4 % r.Count];
        return new StructuralRow(cols);
    }

    // Total work is CONSTANT across W: LeftRows rows split into w disjoint symbol
    // shards, each shard chunked into Ticks/w-ish ticks so per-tick sizes match the
    // single-worker case (same delta granularity → same per-tick alloc shape).
    private static List<List<IndexedZSet<string, StructuralRow, Z64>>> BuildShards(int w)
    {
        var shards = new List<List<IndexedZSet<string, StructuralRow, Z64>>>(w);
        for (var i = 0; i < w; i++)
        {
            shards.Add(new List<IndexedZSet<string, StructuralRow, Z64>>(Ticks));
        }

        var baseDate = new DateTime(2020, 1, 1);
        var actions = new[] { "ACTV", "CNCL", "ACTV" };
        var symbols = BuildSymbols();

        // Assign each row to a worker by symbol shard (disjoint keys per worker), and
        // within a worker into Ticks chunks so each worker still runs Ticks steps.
        var builders = new IndexedZSetBuilder<string, StructuralRow, Z64>[w];
        var perWorkerCounts = new int[w];
        for (var i = 0; i < w; i++)
        {
            builders[i] = new IndexedZSetBuilder<string, StructuralRow, Z64>();
        }

        var chunkTarget = LeftRows / w / Ticks + 1;
        for (var r = 0; r < LeftRows; r++)
        {
            var symIdx = r % Symbols;
            var worker = symIdx % w;                 // disjoint symbol -> worker map
            var sym = symbols[symIdx];
            var cols = new object?[4];
            cols[0] = "CID" + (r % 50000);
            cols[1] = sym;
            cols[2] = baseDate.AddMinutes(r);
            cols[3] = actions[r % 3];
            builders[worker].Add(sym, new StructuralRow(cols), new Z64(1));
            if (++perWorkerCounts[worker] >= chunkTarget && shards[worker].Count < Ticks - 1)
            {
                shards[worker].Add(builders[worker].Build());
                builders[worker] = new IndexedZSetBuilder<string, StructuralRow, Z64>();
                perWorkerCounts[worker] = 0;
            }
        }

        for (var i = 0; i < w; i++)
        {
            shards[i].Add(builders[i].Build());
        }

        return shards;
    }

    private static IndexedZSet<string, StructuralRow, Z64> BuildRight()
    {
        var b = new IndexedZSetBuilder<string, StructuralRow, Z64>();
        foreach (var sym in BuildSymbols())
        {
            var cols = new object?[]
            {
                sym, "CO" + sym, "Company " + sym, sym.GetHashCode() % 2 == 0 ? "NYSE" : "NASDAQ", "ACTV",
            };
            b.Add(sym, new StructuralRow(cols), new Z64(1));
        }

        return b.Build();
    }

    private static string[] BuildSymbols()
    {
        var s = new string[Symbols];
        for (var i = 0; i < Symbols; i++)
        {
            s[i] = "SYM" + i;
        }

        return s;
    }
}
