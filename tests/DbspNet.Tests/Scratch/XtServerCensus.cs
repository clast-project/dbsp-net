// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
//
// Per-tick memory census of a workload that only reaches the engine through DbspNet.Server's HTTP
// API (extended TPC-DS, docs/design-bounded-memory.md §5.3 step 0). Hosts the server's endpoints
// in-process around an engine this test owns, so ProgramRunner.AfterTick can walk its circuit, then
// runs an external driver against it and waits for that to exit.
//
//   XT_DRIVER         (required) a shell script to run (bash) once the server is up; it drives the workload
//   XT_CENSUS_FILE    (required) where the census lines go, flushed per line (an OOM loses nothing)
//   XT_PORT           (optional) default 8091
//   XT_CENSUS_STEP_GIB (optional, default 8) a full object census each time live crosses another
//                     multiple of this many GiB; TICKLIVE lines (forced compacting GC) every tick
//   DBSPNET_WORKERS   as for the server
//   XT_CHECKPOINT_AT_TICK (optional, with DBSPNET_SNAPSHOT_DIR) checkpoint after this tick and exit; a
//                     later run with DBSPNET_SNAPSHOT_DIR on a copy of that directory restores and goes on
//   XT_STATE_CENSUS_TICK (optional) a full state census after this tick (before any checkpoint there)
//   XT_CENSUS_AFTER_DEPLOY (optional) 1: a full state census once deploy (and any restore) is done; exit: then exit
using System.Diagnostics;
using System.Globalization;
using DbspNet.Core.Circuit;
using DbspNet.Server;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Xunit;

namespace DbspNet.Tests.Scratch;

public class XtServerCensus
{
    [Fact]
    public async Task RunDriverUnderCensus()
    {
        var driver = Environment.GetEnvironmentVariable("XT_DRIVER");
        var censusFile = Environment.GetEnvironmentVariable("XT_CENSUS_FILE");
        if (string.IsNullOrEmpty(driver) || string.IsNullOrEmpty(censusFile))
        {
            return;
        }

        var port = Environment.GetEnvironmentVariable("XT_PORT") is { Length: > 0 } p ? p : "8091";
        var stepGib = double.TryParse(Environment.GetEnvironmentVariable("XT_CENSUS_STEP_GIB"), CultureInfo.InvariantCulture, out var s) ? s : 8;
        await using var log = new StreamWriter(censusFile, append: false) { AutoFlush = true };

        var engine = new DbspNetEngine();
        var sw = Stopwatch.StartNew();

        // A forced compacting collection, live bytes, then the state census (by operator and field) and the
        // block residency report. What the walk does not reach (statics: the block cache, file pins) is
        // live minus the walk's total.
        void FullCensus(string tag)
        {
            var (runner, replicas) = Locate(engine);
            System.Runtime.GCSettings.LargeObjectHeapCompactionMode = System.Runtime.GCLargeObjectHeapCompactionMode.CompactOnce;
            GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
            var live = GC.GetGCMemoryInfo(GCKind.FullBlocking).PromotedBytes;
            var res = StateCensus.Walk(replicas, runner, null, 3);
            log.WriteLine(string.Create(CultureInfo.InvariantCulture,
                $"CENSUS {tag} live_mib={live / 1048576.0:F0} walked_mib={res.Buckets.Sum(b => b.Bytes) / 1048576.0:F0} walk_s={res.Took.TotalSeconds:F0} cache_mib={DbspNet.Persistence.Blocks.BlockCache.Bytes / 1048576.0:F0}"));
            foreach (var line in StateCensus.Report(tag, res, 80))
            {
                log.WriteLine(line);
            }

            foreach (var line in BlockResidency.Report(tag, BlockResidency.Walk(replicas, runner, -1), 40))
            {
                log.WriteLine(line);
            }

            log.WriteLine($"CENSUS-DONE {tag}");
        }
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseUrls($"http://127.0.0.1:{port}");
        await using var app = builder.Build();
        app.MapGet("/healthz", () => Results.Ok(new { status = "ok" }));
        app.MapPost("/compile", (CompileSpec spec) => Results.Ok(DbspNetEngine.Compile(spec)));
        // A restored deploy's drain counts from 0 again; census lines carry the circuit's tick.
        long tickBase = 0;
        app.MapPost("/deploy", async (ProgramSpec spec, CancellationToken ct) =>
        {
            var r = await engine.DeployAsync(spec, ct);
            tickBase = r.RestoredTick;
            log.WriteLine(string.Create(CultureInfo.InvariantCulture,
                $"DEPLOYED t_s={sw.Elapsed.TotalSeconds:F0} persistent={r.Persistent} restored_tick={r.RestoredTick} workers={r.Workers} deploy_s={r.CompileTimeS:F1}"));
            if (Environment.GetEnvironmentVariable("XT_CENSUS_AFTER_DEPLOY") is { Length: > 0 } after)
            {
                // §5.30: what a restored deploy holds before its first tick, as XT_STATE_CENSUS_TICK reports a
                // tick's end; "exit" ends the process once it is written.
                FullCensus($"deploy{r.RestoredTick}");
                if (after == "exit")
                {
                    log.Flush();
                    Environment.Exit(0);
                }
            }

            return Results.Ok(r);
        });
        app.MapPost("/resume", () => Results.Ok(engine.Resume()));
        app.MapPost("/wait", async (CancellationToken ct) =>
        {
            try
            {
                return Results.Ok(await engine.WaitAsync(ct));
            }
            catch (Exception ex)
            {
                log.WriteLine("WAIT FAILED " + ex.GetType().Name + ": " + ex.Message);
                log.WriteLine(ex.StackTrace);
                if (ex is AggregateException agg)
                {
                    // Where each worker's allocation failed: distinct inner stacks, with how many workers hit each.
                    foreach (var g in agg.Flatten().InnerExceptions.GroupBy(e => e.StackTrace))
                    {
                        log.WriteLine($"INNER x{g.Count()} {g.First().GetType().Name}");
                        log.WriteLine(g.Key);
                    }
                }

                return Results.Json(new { error = ex.Message, detail = ex.ToString() }, statusCode: 500);
            }
        });
        app.MapPost("/pause", () => Results.Ok(new { status = "ok" }));
        app.MapGet("/stats", () => Results.Ok(engine.GetStats()));

        var nextCensus = stepGib;
        var checkpointAt = long.TryParse(Environment.GetEnvironmentVariable("XT_CHECKPOINT_AT_TICK"), out var ca) ? ca : -1;
        DbspNet.Connectors.Abstractions.ProgramRunner.AfterTick = drainTick =>
        {
            var t = drainTick + tickBase;
            var (runner, replicas) = Locate(engine);
            var before = GC.GetGCMemoryInfo(GCKind.Any).HeapSizeBytes;
            var allocated = GC.GetTotalAllocatedBytes(precise: false);
            System.Runtime.GCSettings.LargeObjectHeapCompactionMode = System.Runtime.GCLargeObjectHeapCompactionMode.CompactOnce;
            GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
            var live = GC.GetGCMemoryInfo(GCKind.FullBlocking).PromotedBytes;
            if (Environment.GetEnvironmentVariable("XT_RELEASE_VIEW_INPUTS") == "1")
            {
                // Experiment (§5.9): what integrated views' pinned input deltas (LastDelta, read only
                // by the runner once a view has been written) keep alive past the tick.
                var released = 0;
                long releasedRows = 0;
                foreach (var rep in replicas)
                {
                    foreach (var op in rep.Operators)
                    {
                        var ot = op.GetType();
                        if (ot.IsGenericType && ot.GetGenericTypeDefinition() == typeof(DbspNet.Core.Operators.Stateful.IntegrateOp<>)
                            && ot.GetField("_input", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)?.GetValue(op) is IReleasableStream s)
                        {
                            releasedRows += (long)(s.GetType().GetProperty("LastCount", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)?.GetValue(s) ?? 0L);
                            s.Release();
                            released++;
                        }
                    }
                }

                GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
                var after = GC.GetGCMemoryInfo(GCKind.FullBlocking).PromotedBytes;
                log.WriteLine(string.Create(CultureInfo.InvariantCulture,
                    $"VIEWRELEASE tick={t} streams={released} rows={releasedRows} live_before_mib={live / 1048576.0:F0} live_after_mib={after / 1048576.0:F0}"));
                live = after;
            }

            {
                // Where live bytes sit by generation (LOH: large arrays, e.g. block bodies or pooled
                // arrays; gen2: small objects, e.g. rows), and evicted block bodies still reachable.
                var info = GC.GetGCMemoryInfo(GCKind.FullBlocking);
                var gens = string.Join(" ", Enumerable.Range(0, info.GenerationInfo.Length).Select(g =>
                    string.Create(CultureInfo.InvariantCulture, $"g{g}={info.GenerationInfo[g].SizeAfterBytes / 1048576.0:F0}")));
                var (deadCount, deadBytes) = DbspNet.Core.Operators.Stateful.Blocks.BlockAccess.Enabled
                    ? DbspNet.Persistence.Blocks.BlockCache.DroppedAlive() : (0, 0);
                log.WriteLine(string.Create(CultureInfo.InvariantCulture,
                    $"GENS tick={t} {gens} (g3=LOH g4=POH) evicted_alive={deadCount} evicted_alive_mib={deadBytes / 1048576.0:F0}"));
            }

            if (int.TryParse(Environment.GetEnvironmentVariable("XT_STATE_CENSUS_TICK"), out var censusTick) && censusTick == t)
            {
                FullCensus($"end{t}");
            }

            if (int.TryParse(Environment.GetEnvironmentVariable("XT_PAUSE_AT_TICK"), out var pauseTick) && pauseTick == t)
            {
                // Hold here (after the forced GC) for a heap dump: until XT_PAUSE_AT_TICK's resume file
                // ({census file}.resume) appears, or 20 minutes.
                log.WriteLine($"PAUSED tick={t} pid={Environment.ProcessId}");
                var until = DateTime.UtcNow.AddMinutes(20);
                while (!File.Exists(censusFile + ".resume") && DateTime.UtcNow < until)
                {
                    Thread.Sleep(1000);
                }
            }

            log.WriteLine(SpillMix($"tick={t}"));
            var rows = BlockResidency.Walk(replicas, runner, -1);
            long blocks = rows.Sum(r => r.Resident), managed = rows.Sum(r => r.ManagedEntries), small = rows.Sum(r => r.SmallRows);
            log.WriteLine(string.Create(CultureInfo.InvariantCulture,
                $"TICKLIVE tick={t} t_s={sw.Elapsed.TotalSeconds:F0} heap_before_mib={before / 1048576.0:F0} live_mib={live / 1048576.0:F0} blocks_mib={blocks / 1048576.0:F0} managed_entries={managed} small_window_rows={small} alloc_gib={allocated / (double)(1L << 30):F1} rss_mib={Environment.WorkingSet / 1048576.0:F0} writeback_est_mib={DbspNet.Core.Operators.Stateful.Blocks.OrderedPartitionStore.LiveCacheBytesTotal / 1048576.0:F0} writeback_drops={DbspNet.Core.Operators.Stateful.Blocks.OrderedPartitionStore.WriteBackDrops} spilled_runs={DbspNet.Persistence.Blocks.RunSpiller.Runs} spilled_mib={DbspNet.Persistence.Blocks.RunSpiller.Bytes / 1048576.0:F0} spill_disk_mib={DbspNet.Persistence.Blocks.RunSpiller.LiveFileBytes / 1048576.0:F0} spill_disk_peak_mib={DbspNet.Persistence.Blocks.RunSpiller.PeakFileBytes / 1048576.0:F0} spill_files={DbspNet.Persistence.Blocks.RunSpiller.LiveFiles} del_release={DbspNet.Persistence.Blocks.RunSpiller.DeletedOnRelease} del_sweep={DbspNet.Persistence.Blocks.RunSpiller.DeletedBySweep} cache_mib={DbspNet.Persistence.Blocks.BlockCache.Bytes / 1048576.0:F0} step_blocked={DbspNet.Core.Operators.Stateful.Blocks.DeltaSpill.BlockedBuilds} step_blocked_mrows={DbspNet.Core.Operators.Stateful.Blocks.DeltaSpill.BlockedRows / 1e6:F1} adopted={DbspNet.Core.Operators.Stateful.Blocks.DeltaSpill.Adopted} adopted_mrows={DbspNet.Core.Operators.Stateful.Blocks.DeltaSpill.AdoptedRows / 1e6:F1} streamed_runs={DbspNet.Core.Operators.Stateful.Blocks.BlockedIndexedTrace.StreamedRuns} giant_merges={DbspNet.Core.Operators.Stateful.Blocks.BlockedIndexedTrace.GiantMerges} giant_fallbacks={DbspNet.Core.Operators.Stateful.Blocks.BlockedIndexedTrace.GiantFallbacks} pressure={DbspNet.Core.Operators.Stateful.Blocks.MemoryPressure.Current} pressure_changes={DbspNet.Core.Operators.Stateful.Blocks.MemoryPressure.Changes} pressure_used_mib={DbspNet.Core.Operators.Stateful.Blocks.MemoryPressure.LastUsedBytes >> 20} pressure_peak_mib={DbspNet.Core.Operators.Stateful.Blocks.MemoryPressure.PeakUsedBytes >> 20} pushes={DbspNet.Core.Operators.Stateful.Blocks.PressureRelief.Pushes} pushed_entries={DbspNet.Core.Operators.Stateful.Blocks.PressureRelief.PushedEntries} kept_spilled={DbspNet.Core.Operators.Stateful.IntegrateOp<DbspNet.Core.Collections.StructuralRow>.SpilledDeltas} kept_spilled_mrows={DbspNet.Core.Operators.Stateful.IntegrateOp<DbspNet.Core.Collections.StructuralRow>.SpilledDeltaRows / 1e6:F1}"));
            if (live / (double)(1L << 30) >= nextCensus)
            {
                while (live / (double)(1L << 30) >= nextCensus)
                {
                    nextCensus += stepGib;
                }

                foreach (var line in StateCensus.Report($"tick{t}", StateCensus.Walk(replicas, runner, null, 3)))
                {
                    log.WriteLine(line);
                }

                foreach (var line in BlockResidency.Report($"tick{t}", rows))
                {
                    log.WriteLine(line);
                }
            }

            if (t == checkpointAt)
            {
                // XT_CHECKPOINT_AT_TICK (with DBSPNET_SNAPSHOT_DIR): snapshot state and source cursors after
                // this tick, then end the process. A deploy against a copy of the snapshot directory resumes
                // at the next tick (docs/design-bounded-memory.md §5.30).
                var c0 = sw.Elapsed.TotalSeconds;
                ((DbspNet.Connectors.Abstractions.ProgramRunner)runner).CheckpointAsync().AsTask().GetAwaiter().GetResult();
                log.WriteLine(string.Create(CultureInfo.InvariantCulture,
                    $"CHECKPOINTED tick={t} t_s={sw.Elapsed.TotalSeconds:F0} save_s={sw.Elapsed.TotalSeconds - c0:F1}"));
                log.Flush();
                Environment.Exit(0);
            }
        };

        // Inside a step (the huge ticks are single steps): after any operator, once the heap has
        // grown another XT_OP_STEP_GIB (default 4) since the last sample, a forced GC measures live
        // (OPLIVE); a full census whenever live crosses another XT_CENSUS_STEP_GIB. Operators that
        // ran over 2 s are logged (OPSLOW). Serial only: the hook takes a lock and is not per replica.
        var opStepGib = double.TryParse(Environment.GetEnvironmentVariable("XT_OP_STEP_GIB"), CultureInfo.InvariantCulture, out var os) ? os : 4;
        var nextHeap = (long)(opStepGib * (1L << 30));
        var nextOpCensus = stepGib;
        var lastOp = Stopwatch.GetTimestamp();
        var lastReading = double.NegativeInfinity; // at most one unforced reading per 30 s
        var gate = new object();
        var opTick = long.TryParse(Environment.GetEnvironmentVariable("XT_OP_TICK"), out var ot) ? ot : -1;

        // XT_CENSUS_AT_OPS=1801,1808: a live reading and a full census after these operators of the
        // sampled tick, whatever the heap did (what holds the heap going into a failing operator).
        var censusAt = (Environment.GetEnvironmentVariable("XT_CENSUS_AT_OPS") ?? "")
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(x => int.Parse(x, CultureInfo.InvariantCulture)).ToHashSet();
        // XT_GCDUMP_AT_OPS=2064: a whole-heap dotnet-gcdump of this process after these operators of the
        // sampled tick (once each), to XT_GCDUMP_DIR; XT_GCDUMP_EXIT=1 exits after the last one. The state
        // census reaches operators, streams and the runner but not closures or statics.
        var gcdumpAt = (Environment.GetEnvironmentVariable("XT_GCDUMP_AT_OPS") ?? "")
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(x => int.Parse(x, CultureInfo.InvariantCulture)).ToHashSet();
        var gcdumpDir = Environment.GetEnvironmentVariable("XT_GCDUMP_DIR") ?? Path.GetTempPath();
        var liveAt = (Environment.GetEnvironmentVariable("XT_LIVE_AT_OPS") ?? "")
            .Split(',', StringSplitOptions.RemoveEmptyEntries).ToDictionary(x => int.Parse(x, CultureInfo.InvariantCulture), _ => 0);
        var liveWorkers = int.Parse(Environment.GetEnvironmentVariable("DBSPNET_WORKERS") ?? "1", CultureInfo.InvariantCulture);

        // Per-op allocation (§5.35): each worker steps on its own thread, so the thread's allocated bytes
        // since its previous operator are that operator's on that worker. OPALLOC when >= 512 MiB.
        var lastAlloc = new ThreadLocal<long>(() => GC.GetAllocatedBytesForCurrentThread());
        // XT_DESCRIBE_OPS=1680-1700: once, each operator's stream fields and delegates (what the plan wired).
        var describe = (Environment.GetEnvironmentVariable("XT_DESCRIBE_OPS") ?? "").Split('-', StringSplitOptions.RemoveEmptyEntries);
        var describeFrom = describe.Length == 2 ? int.Parse(describe[0], CultureInfo.InvariantCulture) : -1;
        var describeTo = describe.Length == 2 ? int.Parse(describe[1], CultureInfo.InvariantCulture) : -1;
        var described = 0;
        RootCircuit.AfterOperatorStep = (circuit, i) =>
        {
            if (describeFrom >= 0 && Interlocked.Exchange(ref described, 1) == 0)
            {
                lock (gate)
                {
                    for (var k = describeFrom; k <= describeTo && k < circuit.Operators.Count; k++)
                    {
                        var o = circuit.Operators[k];
                        var parts = new List<string>();
                        foreach (var f in o.GetType().GetFields(System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Public))
                        {
                            var v = f.GetValue(o);
                            if (v is null) continue;
                            if (v.GetType().Name.StartsWith("Stream", StringComparison.Ordinal))
                                parts.Add($"{f.Name}=s{System.Runtime.CompilerServices.RuntimeHelpers.GetHashCode(v):x}");
                            else if (v is Delegate d)
                                parts.Add($"{f.Name}={d.Method.DeclaringType?.Name}.{d.Method.Name}" + (d.Target is { } t ? $"[{t.GetType().Name}]" : ""));
                        }

                        log.WriteLine($"OPDESC op={k} view={circuit.OperatorLabel(k) ?? "-"} type={o.GetType().Name} {string.Join(' ', parts)}");
                    }
                }
            }

            var allocNow = GC.GetAllocatedBytesForCurrentThread();
            var opAlloc = allocNow - lastAlloc.Value;
            lastAlloc.Value = allocNow;
            lock (gate)
            {
                if (opAlloc >= 512L << 20 && (opTick < 0 || circuit.TickCount == opTick))
                {
                    log.WriteLine(string.Create(CultureInfo.InvariantCulture,
                        $"OPALLOC tick={circuit.TickCount} op={i} type={circuit.Operators[i].GetType().Name} thread={Environment.CurrentManagedThreadId} alloc_mib={opAlloc >> 20} t_s={sw.Elapsed.TotalSeconds:F0}"));
                }

                var now = Stopwatch.GetTimestamp();
                var op = circuit.Operators[i];
                if (opTick >= 0 && circuit.TickCount != opTick)
                {
                    lastOp = now; // XT_OP_TICK: sample inside that tick only
                    return;
                }

                // XT_LIVE_AT_OPS=1693,1703: once every worker has stepped one of these operators of the sampled
                // tick, a full compacting GC and its live bytes (LIVEAT); XT_LIVE_EXIT=1 exits after the last.
                if (liveAt.TryGetValue(i, out var seen))
                {
                    liveAt[i] = ++seen;
                    if (seen == liveWorkers)
                    {
                        GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
                        log.WriteLine(string.Create(CultureInfo.InvariantCulture,
                            $"LIVEAT tick={circuit.TickCount} op={i} type={op.GetType().Name} live_mib={GC.GetTotalMemory(false) >> 20} t_s={sw.Elapsed.TotalSeconds:F0} copy_merges={DbspNet.Core.Operators.Stateful.Blocks.BlockedIndexedTrace.CopyMerges}"));
                        log.Flush();
                        if (liveAt.Values.All(v => v >= liveWorkers) && Environment.GetEnvironmentVariable("XT_LIVE_EXIT") == "1")
                        {
                            Environment.Exit(0);
                        }
                    }
                }

                if (gcdumpAt.Remove(i))
                {
                    var file = Path.Combine(gcdumpDir, $"tick{circuit.TickCount}-op{i}.gcdump");
                    var tool = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), ".dotnet", "tools", "dotnet-gcdump");
                    var t0 = sw.Elapsed.TotalSeconds;
                    using (var dump = Process.Start(new ProcessStartInfo(tool, $"collect -p {Environment.ProcessId} -o {file} -t 3600") { UseShellExecute = false, RedirectStandardOutput = true, RedirectStandardError = true })!)
                    {
                        var output = dump.StandardOutput.ReadToEnd() + dump.StandardError.ReadToEnd();
                        dump.WaitForExit();
                        log.WriteLine(string.Create(CultureInfo.InvariantCulture,
                            $"GCDUMP tick={circuit.TickCount} op={i} type={op.GetType().Name} exit={dump.ExitCode} file={file} took_s={sw.Elapsed.TotalSeconds - t0:F0} {output.ReplaceLineEndings(" ")}"));
                    }

                    log.Flush();
                    if (gcdumpAt.Count == 0 && Environment.GetEnvironmentVariable("XT_GCDUMP_EXIT") == "1")
                    {
                        Environment.Exit(0);
                    }

                    now = Stopwatch.GetTimestamp();
                }

                {
                    // The step's output delta size: introspection, else the operator's _output stream.
                    long outRows = -1;
                    if (op is DbspNet.Core.Circuit.IIntrospectable intro)
                    {
                        outRows = intro.LastOutputRows;
                    }
                    else if (op.GetType().GetField("_output", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)?.GetValue(op) is { } stream
                        && stream.GetType().GetProperty("LastCount", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)?.GetValue(stream) is long c)
                    {
                        outRows = c;
                    }

                    if (outRows >= 1_000_000)
                    {
                        log.WriteLine(string.Create(CultureInfo.InvariantCulture,
                            $"OPROWS tick={circuit.TickCount} op={i} type={op.GetType().Name} out_rows={outRows} op_ms={(now - lastOp) * 1000.0 / Stopwatch.Frequency:F0} heap_mib={GC.GetTotalMemory(false) / 1048576.0:F0} t_s={sw.Elapsed.TotalSeconds:F0} step_blocked={DbspNet.Core.Operators.Stateful.Blocks.DeltaSpill.BlockedBuilds}"));
                    }
                }

                var opMs = (now - lastOp) * 1000.0 / Stopwatch.Frequency;
                var heap = GC.GetTotalMemory(forceFullCollection: false);
                if (opMs > 2000)
                {
                    log.WriteLine(string.Create(CultureInfo.InvariantCulture,
                        $"OPSLOW tick={circuit.TickCount} op={i} type={op.GetType().Name} ms={opMs:F0} heap_mib={heap / 1048576.0:F0} t_s={sw.Elapsed.TotalSeconds:F0}"));
                }

                // Once per op: every replica runs it, and twelve walks under this lock distort the tick.
                var forced = censusAt.Remove(i);
                if (forced || (heap >= nextHeap && sw.Elapsed.TotalSeconds - lastReading >= 30))
                {
                    lastReading = sw.Elapsed.TotalSeconds;
                    System.Runtime.GCSettings.LargeObjectHeapCompactionMode = System.Runtime.GCLargeObjectHeapCompactionMode.CompactOnce;
                    GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
                    var live = GC.GetGCMemoryInfo(GCKind.FullBlocking).PromotedBytes;
                    var (runner, replicas) = Locate(engine);
                    var rows = BlockResidency.Walk(replicas, runner, -1);
                    long blocks = rows.Sum(r => r.Resident), managed = rows.Sum(r => r.ManagedEntries), small = rows.Sum(r => r.SmallRows);
                    log.WriteLine(string.Create(CultureInfo.InvariantCulture,
                        $"OPLIVE tick={circuit.TickCount} op={i}/{circuit.Operators.Count} type={op.GetType().Name} t_s={sw.Elapsed.TotalSeconds:F0} heap_before_mib={heap / 1048576.0:F0} live_mib={live / 1048576.0:F0} blocks_mib={blocks / 1048576.0:F0} managed_entries={managed} small_window_rows={small} rss_mib={Environment.WorkingSet / 1048576.0:F0} writeback_est_mib={DbspNet.Core.Operators.Stateful.Blocks.OrderedPartitionStore.LiveCacheBytesTotal / 1048576.0:F0} writeback_drops={DbspNet.Core.Operators.Stateful.Blocks.OrderedPartitionStore.WriteBackDrops} spilled_runs={DbspNet.Persistence.Blocks.RunSpiller.Runs} spilled_mib={DbspNet.Persistence.Blocks.RunSpiller.Bytes / 1048576.0:F0} spill_disk_mib={DbspNet.Persistence.Blocks.RunSpiller.LiveFileBytes / 1048576.0:F0} spill_disk_peak_mib={DbspNet.Persistence.Blocks.RunSpiller.PeakFileBytes / 1048576.0:F0} spill_files={DbspNet.Persistence.Blocks.RunSpiller.LiveFiles} del_release={DbspNet.Persistence.Blocks.RunSpiller.DeletedOnRelease} del_sweep={DbspNet.Persistence.Blocks.RunSpiller.DeletedBySweep} cache_mib={DbspNet.Persistence.Blocks.BlockCache.Bytes / 1048576.0:F0} step_blocked={DbspNet.Core.Operators.Stateful.Blocks.DeltaSpill.BlockedBuilds} step_blocked_mrows={DbspNet.Core.Operators.Stateful.Blocks.DeltaSpill.BlockedRows / 1e6:F1} adopted={DbspNet.Core.Operators.Stateful.Blocks.DeltaSpill.Adopted} adopted_mrows={DbspNet.Core.Operators.Stateful.Blocks.DeltaSpill.AdoptedRows / 1e6:F1} streamed_runs={DbspNet.Core.Operators.Stateful.Blocks.BlockedIndexedTrace.StreamedRuns} giant_merges={DbspNet.Core.Operators.Stateful.Blocks.BlockedIndexedTrace.GiantMerges} giant_fallbacks={DbspNet.Core.Operators.Stateful.Blocks.BlockedIndexedTrace.GiantFallbacks} pressure={DbspNet.Core.Operators.Stateful.Blocks.MemoryPressure.Current} pressure_changes={DbspNet.Core.Operators.Stateful.Blocks.MemoryPressure.Changes} pressure_used_mib={DbspNet.Core.Operators.Stateful.Blocks.MemoryPressure.LastUsedBytes >> 20} pressure_peak_mib={DbspNet.Core.Operators.Stateful.Blocks.MemoryPressure.PeakUsedBytes >> 20} pushes={DbspNet.Core.Operators.Stateful.Blocks.PressureRelief.Pushes} pushed_entries={DbspNet.Core.Operators.Stateful.Blocks.PressureRelief.PushedEntries} kept_spilled={DbspNet.Core.Operators.Stateful.IntegrateOp<DbspNet.Core.Collections.StructuralRow>.SpilledDeltas} kept_spilled_mrows={DbspNet.Core.Operators.Stateful.IntegrateOp<DbspNet.Core.Collections.StructuralRow>.SpilledDeltaRows / 1e6:F1}"));
                    log.WriteLine(SpillMix($"tick={circuit.TickCount} op={i}"));
                    // From live, not the heap before the collection: garbage once put the next reading
                    // out of reach for the rest of the tick (§5.24).
                    nextHeap = live + (long)(opStepGib * (1L << 30));
                    if (forced || live / (double)(1L << 30) >= nextOpCensus)
                    {
                        while (live / (double)(1L << 30) >= nextOpCensus)
                        {
                            nextOpCensus += stepGib;
                        }

                        foreach (var line in StateCensus.Streams($"tick{circuit.TickCount}-op{i}", replicas))
                        {
                            log.WriteLine(line);
                        }

                        log.Flush();
                        var census = StateCensus.Walk(replicas, runner, null, 3);
                        foreach (var line in StateCensus.Report($"tick{circuit.TickCount}-op{i}", census))
                        {
                            log.WriteLine(line);
                        }
                    }

                    now = Stopwatch.GetTimestamp();
                }

                lastOp = now;
            }
        };

        try
        {
            await app.StartAsync();
            log.WriteLine($"SERVER up on {port}, workers={Environment.GetEnvironmentVariable("DBSPNET_WORKERS") ?? "default"}");
            using var proc = Process.Start(new ProcessStartInfo("bash", driver) { UseShellExecute = false })!;
            await proc.WaitForExitAsync();
            log.WriteLine($"DRIVER exit={proc.ExitCode} after {sw.Elapsed.TotalSeconds:F0}s");
        }
        finally
        {
            DbspNet.Connectors.Abstractions.ProgramRunner.AfterTick = null;
            RootCircuit.AfterOperatorStep = null;
            await app.StopAsync();
        }
    }

    // The engine's runner, and its circuit's replicas (one for a serial program).
    private static (object Runner, IReadOnlyList<RootCircuit> Replicas) Locate(DbspNetEngine engine)
    {
        const System.Reflection.BindingFlags F = System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Public;
        var runner = typeof(DbspNetEngine).GetField("_runner", F)!.GetValue(engine)!;
        var program = runner.GetType().GetField("_program", F)!.GetValue(runner)!;
        foreach (var f in program.GetType().GetFields(F))
        {
            switch (f.GetValue(program))
            {
                case DbspNet.Sql.Compiler.CompiledProgram serial:
                    return (runner, [serial.Circuit]);
                case DbspNet.Sql.Compiler.ParallelCompiledProgram parallel:
                    return (runner, parallel.Circuit.Replicas);
            }
        }

        throw new InvalidOperationException("no compiled program in " + program.GetType());
    }

    // Spilled bytes by holder, read right after a forced full collection; then a sweep, and what it freed.
    private static string SpillMix(string where)
    {
        if (DbspNet.Core.Operators.Stateful.Blocks.BlockAccess.Spiller is not DbspNet.Persistence.Blocks.RunSpiller spiller)
        {
            return $"SPILLMIX {where} none";
        }

        var (state, delta, reader, dead, files) = spiller.Census();
        var disk0 = DbspNet.Persistence.Blocks.RunSpiller.LiveFileBytes;
        spiller.Sweep();
        var freed = disk0 - DbspNet.Persistence.Blocks.RunSpiller.LiveFileBytes;
        static double M(long b) => b / 1048576.0;
        return string.Create(CultureInfo.InvariantCulture,
            $"SPILLMIX {where} files={files} state_mib={M(state):F0} delta_mib={M(delta):F0} reader_mib={M(reader):F0} dead_mib={M(dead):F0} swept_mib={M(freed):F0}");
    }
}
