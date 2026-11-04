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
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseUrls($"http://127.0.0.1:{port}");
        await using var app = builder.Build();
        app.MapGet("/healthz", () => Results.Ok(new { status = "ok" }));
        app.MapPost("/compile", (CompileSpec spec) => Results.Ok(DbspNetEngine.Compile(spec)));
        app.MapPost("/deploy", async (ProgramSpec spec, CancellationToken ct) => Results.Ok(await engine.DeployAsync(spec, ct)));
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
                return Results.Json(new { error = ex.Message, detail = ex.ToString() }, statusCode: 500);
            }
        });
        app.MapPost("/pause", () => Results.Ok(new { status = "ok" }));
        app.MapGet("/stats", () => Results.Ok(engine.GetStats()));

        var nextCensus = stepGib;
        var sw = Stopwatch.StartNew();
        DbspNet.Connectors.Abstractions.ProgramRunner.AfterTick = t =>
        {
            var (runner, replicas) = Locate(engine);
            var before = GC.GetGCMemoryInfo(GCKind.Any).HeapSizeBytes;
            var allocated = GC.GetTotalAllocatedBytes(precise: false);
            System.Runtime.GCSettings.LargeObjectHeapCompactionMode = System.Runtime.GCLargeObjectHeapCompactionMode.CompactOnce;
            GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
            var live = GC.GetGCMemoryInfo(GCKind.FullBlocking).PromotedBytes;
            var rows = BlockResidency.Walk(replicas, runner, -1);
            long blocks = rows.Sum(r => r.Resident), managed = rows.Sum(r => r.ManagedEntries), small = rows.Sum(r => r.SmallRows);
            log.WriteLine(string.Create(CultureInfo.InvariantCulture,
                $"TICKLIVE tick={t} t_s={sw.Elapsed.TotalSeconds:F0} heap_before_mib={before / 1048576.0:F0} live_mib={live / 1048576.0:F0} blocks_mib={blocks / 1048576.0:F0} managed_entries={managed} small_window_rows={small} alloc_gib={allocated / (double)(1L << 30):F1} rss_mib={Environment.WorkingSet / 1048576.0:F0} writeback_est_mib={DbspNet.Core.Operators.Stateful.Blocks.OrderedPartitionStore.LiveCacheBytesTotal / 1048576.0:F0} writeback_drops={DbspNet.Core.Operators.Stateful.Blocks.OrderedPartitionStore.WriteBackDrops}"));
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
        };

        // Inside a step (the huge ticks are single steps): after any operator, once the heap has
        // grown another XT_OP_STEP_GIB (default 4) since the last sample, a forced GC measures live
        // (OPLIVE); a full census whenever live crosses another XT_CENSUS_STEP_GIB. Operators that
        // ran over 2 s are logged (OPSLOW). Serial only: the hook takes a lock and is not per replica.
        var opStepGib = double.TryParse(Environment.GetEnvironmentVariable("XT_OP_STEP_GIB"), CultureInfo.InvariantCulture, out var os) ? os : 4;
        var nextHeap = (long)(opStepGib * (1L << 30));
        var nextOpCensus = stepGib;
        var lastOp = Stopwatch.GetTimestamp();
        var gate = new object();
        RootCircuit.AfterOperatorStep = (circuit, i) =>
        {
            lock (gate)
            {
                var now = Stopwatch.GetTimestamp();
                var op = circuit.Operators[i];
                var opMs = (now - lastOp) * 1000.0 / Stopwatch.Frequency;
                var heap = GC.GetTotalMemory(forceFullCollection: false);
                if (opMs > 2000)
                {
                    log.WriteLine(string.Create(CultureInfo.InvariantCulture,
                        $"OPSLOW tick={circuit.TickCount} op={i} type={op.GetType().Name} ms={opMs:F0} heap_mib={heap / 1048576.0:F0} t_s={sw.Elapsed.TotalSeconds:F0}"));
                }

                if (heap >= nextHeap)
                {
                    System.Runtime.GCSettings.LargeObjectHeapCompactionMode = System.Runtime.GCLargeObjectHeapCompactionMode.CompactOnce;
                    GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
                    var live = GC.GetGCMemoryInfo(GCKind.FullBlocking).PromotedBytes;
                    var (runner, replicas) = Locate(engine);
                    var rows = BlockResidency.Walk(replicas, runner, -1);
                    long blocks = rows.Sum(r => r.Resident), managed = rows.Sum(r => r.ManagedEntries), small = rows.Sum(r => r.SmallRows);
                    log.WriteLine(string.Create(CultureInfo.InvariantCulture,
                        $"OPLIVE tick={circuit.TickCount} op={i}/{circuit.Operators.Count} type={op.GetType().Name} t_s={sw.Elapsed.TotalSeconds:F0} heap_before_mib={heap / 1048576.0:F0} live_mib={live / 1048576.0:F0} blocks_mib={blocks / 1048576.0:F0} managed_entries={managed} small_window_rows={small} rss_mib={Environment.WorkingSet / 1048576.0:F0} writeback_est_mib={DbspNet.Core.Operators.Stateful.Blocks.OrderedPartitionStore.LiveCacheBytesTotal / 1048576.0:F0} writeback_drops={DbspNet.Core.Operators.Stateful.Blocks.OrderedPartitionStore.WriteBackDrops}"));
                    nextHeap = Math.Max(heap, live) + (long)(opStepGib * (1L << 30));
                    if (live / (double)(1L << 30) >= nextOpCensus)
                    {
                        while (live / (double)(1L << 30) >= nextOpCensus)
                        {
                            nextOpCensus += stepGib;
                        }

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
}
