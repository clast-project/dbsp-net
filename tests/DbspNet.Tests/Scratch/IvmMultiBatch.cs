// Local multi-batch ivm-bench run (batches 1 → 2 → 3) to compare output modes: per batch,
// the drain (ingest + step) time, the output-write time, and the output's bytes on disk.
// Truncate rewrites every output view each batch; Incremental writes only the net change.
//
// Gated on env vars, no-op otherwise:
//   IVM_DATA_ROOT     local dir mirroring /data/raw/delta
//   IVM_SPEC          deploy spec JSON (dbt_to_program.py output)
//   IVM_STAGING_ROOT  multi-version staging copy (see IvmCheckpointReuse); its _pending commits
//                     are promoted one per batch
//   IVM_OUTPUT_MODE   truncate (default) | incremental
//   IVM_OUT_ROOT      (optional) output Delta root; default = temp. Wiped at start.
//   IVM_WORKERS       (optional) > 1 runs the data-parallel program on that many workers
//   IVM_TYPE_VIEWS    (optional) 1 = serial program tries the typed fast path per view
//   IVM_PAUSE_DIR     (optional) after batch 1, write <dir>/pid and wait for <dir>/go — so a
//                     heap dump (dotnet-gcdump collect -p <pid>) sees batch 1's retained state
//   IVM_SNAPSHOT_DIR  (optional) checkpoint after every batch through SnapshotCheckpointStore
//                     (serial or, with IVM_WORKERS, the one-commit parallel snapshot), timed as
//                     checkpoint_ms; then restore the last one into a fresh program, timed as
//                     restore_ms, and check its view row counts. Wiped at start. Compiles with
//                     snapshot codecs, which turn off shared arrangements, so compare drain_ms
//                     only against another run with this set.
//   IVM_RESTART_AFTER=N  (with IVM_SNAPSHOT_DIR) restore a fresh program from batch N's checkpoint
//                     and run the remaining batches on it (RESTART line; later RESULTs resumed=1)
//   IVM_RESTORE_WORKERS  (with IVM_SNAPSHOT_DIR, parallel) restore at this W instead: re-shard the
//                     last snapshot first (reshard_ms on the RESTORE line)
//   IVM_SNAPSHOT_RETAIN  (optional) snapshots to keep (default 2)
//   IVM_CHECKPOINT_DELTA=1  (with IVM_SNAPSHOT_DIR, parallel) diff consecutive retained snapshots
//                     (CheckpointDeltaProbe): DELTA / DELTAKIND lines
//   IVM_TRACE_FAMILY  (optional) spine = spine traces for every keyed stateful operator; default flat
//   IVM_CHECKPOINT_POLICY  (with IVM_SNAPSHOT_DIR) a CheckpointPolicy.Parse setting: unset = a
//                     snapshot every batch; "none" = journal only; N = journal + snapshot every N s
//   IVM_CHECKPOINT_PROFILE=1|all  (with IVM_SNAPSHOT_DIR) per checkpoint (all: every op position): wall, CPU, allocation and GC
//                     deltas, and the operator save times summed by type (CKPT/CKPTOP lines)
//   IVM_CHECKPOINT_REPEAT=1   after batch 1's checkpoint, write a second one straight away (same
//                     tick, so a fresh generation directory) and profile it the same way
//   IVM_CHECKPOINT_PREGC=1    force a full compacting GC (untimed) before every checkpoint
using System.Diagnostics;
using System.Globalization;
using System.Text.Json;
using DbspNet.Connectors.Abstractions;
using DbspNet.Connectors.EngineeredWood;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.TypeSystem;
using Xunit;
using Xunit.Abstractions;

namespace DbspNet.Tests.Scratch;

public class IvmMultiBatch
{
    private static readonly JsonSerializerOptions JsonOpts = new() { PropertyNameCaseInsensitive = true };

    private readonly ITestOutputHelper _out;

    // IVM_TRACE_FAMILY=spine compiles every keyed stateful operator onto spine traces (design-durable-state
    // Phase 2: spine vs flat); default flat.
    private static readonly TraceFamily TraceFamily =
        string.Equals(Environment.GetEnvironmentVariable("IVM_TRACE_FAMILY"), "spine", StringComparison.OrdinalIgnoreCase)
            ? TraceFamily.Spine
            : TraceFamily.Flat;

    public IvmMultiBatch(ITestOutputHelper output) => _out = output;

    [Fact]
    public async Task RunThreeBatches_Local()
    {
        var dataRoot = Environment.GetEnvironmentVariable("IVM_DATA_ROOT");
        var specPath = Environment.GetEnvironmentVariable("IVM_SPEC");
        var stagingRoot = Environment.GetEnvironmentVariable("IVM_STAGING_ROOT");
        if (string.IsNullOrEmpty(dataRoot) || string.IsNullOrEmpty(specPath) || string.IsNullOrEmpty(stagingRoot))
        {
            _out.WriteLine("IVM_DATA_ROOT / IVM_SPEC / IVM_STAGING_ROOT not set — skipping.");
            return;
        }

        var mode = (Environment.GetEnvironmentVariable("IVM_OUTPUT_MODE") ?? "truncate").Trim().ToLowerInvariant() switch
        {
            "incremental" => OutputMode.Incremental,
            _ => OutputMode.Truncate,
        };
        var outRoot = Environment.GetEnvironmentVariable("IVM_OUT_ROOT")
                      ?? Path.Combine(Path.GetTempPath(), "ivm-multibatch-out");

        // IVM_RESUME=1: a fresh process restoring the snapshot a run with IVM_STOP_AFTER=1 left
        // (outputs and snapshot kept), then batches 2-3 — a real restart, not an in-process one.
        var resume = Environment.GetEnvironmentVariable("IVM_RESUME") == "1";
        var stopAfter = int.TryParse(Environment.GetEnvironmentVariable("IVM_STOP_AFTER"), out var sa) ? sa : 0;
        if (!resume && Directory.Exists(outRoot))
        {
            Directory.Delete(outRoot, recursive: true);
        }

        RewindPendingCommits(stagingRoot);

        var snapshotDir = Environment.GetEnvironmentVariable("IVM_SNAPSHOT_DIR");
        var codecs = string.IsNullOrEmpty(snapshotDir) ? null : DbspNet.Persistence.ArrowSqlSnapshotCodecs.Instance;
        SnapshotCheckpointStore? store = null;
        var policySetting = Environment.GetEnvironmentVariable("IVM_CHECKPOINT_POLICY");
        CheckpointPolicy? policy = string.IsNullOrEmpty(policySetting) ? null : CheckpointPolicy.Parse(policySetting);
        if (codecs is not null)
        {
            if (!resume && Directory.Exists(snapshotDir))
            {
                Directory.Delete(snapshotDir, recursive: true);
            }

            Directory.CreateDirectory(snapshotDir!);
            // IVM_SNAPSHOT_RETAIN keeps more than the default 2 snapshots (IvmCheckpointDelta diffs them).
            var retain = int.TryParse(Environment.GetEnvironmentVariable("IVM_SNAPSHOT_RETAIN"), out var r) ? r : 2;
            store = new SnapshotCheckpointStore(new DbspNet.Persistence.IO.Local.LocalTableFileSystem(snapshotDir!), retain);
        }

        var spec = JsonSerializer.Deserialize<Spec>(File.ReadAllText(specPath), JsonOpts)!;
        var outputViews = IvmSpecViews.ToCompile(spec.Outputs, spec.Output_Bindings.Select(o => o.View));
        var workers = int.TryParse(Environment.GetEnvironmentVariable("IVM_WORKERS"), out var w) ? w : 1;
        ParallelCompiledProgram? CompileParallel(int? at = null)
        {
            Assert.True(
                SqlProgram.TryCompileParallel(
                    spec.Program, outputViews, at ?? workers, out var compiled, codecs,
                    new CompileOptions { TraceFamily = TraceFamily },
                    numericStringCoercion: true, nullCollation: NullCollation.Low),
                "parallel compile refused the program");
            return compiled;
        }

        var parallel = workers > 1 ? CompileParallel() : null;

        string InUri(string uri)
        {
            var rel = uri.StartsWith("/data/raw/delta/", StringComparison.Ordinal) ? uri["/data/raw/delta/".Length..] : uri.TrimStart('/');
            return rel.StartsWith("staging/", StringComparison.Ordinal)
                ? Path.Combine(stagingRoot, rel["staging/".Length..])
                : Path.Combine(dataRoot, rel);
        }

        List<IInputConnector> Inputs() => spec.Inputs
            .Select(i => (IInputConnector)new DeltaInputConnector(i.Table, InUri(i.Uri)))
            .ToList();
        var inputs = Inputs();
        List<IOutputConnector> outputs = spec.Output_Bindings
            .Select(o => (IOutputConnector)new DeltaOutputConnector(o.View, Path.Combine(outRoot, o.View), mode))
            .ToList();
        // IVM_GC_EVENTS=1: every GC pause during batch 1 (suspend → restart), by generation.
        using var gcEvents = Environment.GetEnvironmentVariable("IVM_GC_EVENTS") == "1" ? new GcPauseListener() : null;

        var serial = parallel is null ? CompileSerial() : null;
        var runner = parallel is not null
            ? await ProgramRunner.CreateAsync(parallel, inputs, outputs, store, policy)
            : await ProgramRunner.CreateAsync(serial!, inputs, outputs, store, policy);
        var checkpointProfile = Environment.GetEnvironmentVariable("IVM_CHECKPOINT_PROFILE") is "1" or "all" or "alloc";
        DbspNet.Persistence.Snapshot.ProfileSave = checkpointProfile;

        // IVM_TYPE_VIEWS=1 (serial only): each view tries the typed fast path, structural
        // fallback elsewhere (design-row-representation §23). That path disables single-use
        // inlining, so compare it against DBSPNET_INLINE_SINGLE_USE=off, not the default.
        CompiledProgram CompileSerial()
        {
            var typeViews = Environment.GetEnvironmentVariable("IVM_TYPE_VIEWS") == "1";
            var compiled = SqlProgram.Compile(
                spec.Program, outputViews, codecs, new CompileOptions { TypeEligibleProgramViews = typeViews, TraceFamily = TraceFamily },
                numericStringCoercion: true, nullCollation: NullCollation.Low);
            if (typeViews)
            {
                var (typed, fellBack) = PlanToCircuit.LastProgramTypedTally;
                _out.WriteLine($"TYPED {typed.Count} views typed, {fellBack.Count} fell back; typed: " + string.Join(", ", typed));
            }

            return compiled;
        }

        _out.WriteLine($"output mode: {mode}, workers: {workers}");
        // IVM_STATE_CENSUS=1: retained state by operator kind and structure (StateCensus) after the
        // restore (IVM_RESUME), after the first batch's drain and after its output write; objects
        // absent from the restore walk are what that batch created and kept.
        var stateCensus = Environment.GetEnvironmentVariable("IVM_STATE_CENSUS") == "1";
        HashSet<object>? censusBaseline = null;
        HashSet<object> StateCensusAt(string tag, HashSet<object>? baseline)
        {
            IReadOnlyList<DbspNet.Core.Circuit.RootCircuit> replicas = parallel is not null ? parallel.Circuit.Replicas : [serial!.Circuit];
            var depth = int.TryParse(Environment.GetEnvironmentVariable("IVM_CENSUS_DEPTH"), out var d) ? d : 3;
            var res = StateCensus.Walk(replicas, runner, baseline, depth);
            foreach (var line in StateCensus.Report(tag, res))
            {
                _out.WriteLine(line);
            }

            return res.Seen;
        }

        // IVM_BLOCK_RESIDENCY=1 (with DBSPNET_BLOCK_ACCESS=1 for read stamps): block bytes by
        // operator and evictability, and what each batch read (BlockResidency).
        var blockResidency = Environment.GetEnvironmentVariable("IVM_BLOCK_RESIDENCY") == "1";
        // IVM_PHASE_LIVE=1 (census only): live bytes at a phase boundary, after a full compacting GC,
        // and the bytes allocated since the previous boundary.
        long phaseAlloc = GC.GetTotalAllocatedBytes(precise: true);
        using var allocTypes = Environment.GetEnvironmentVariable("IVM_ALLOC_TYPES") == "1" ? new AllocTypeSampler() : null;
        AllocTypeSampler.Phase = "drain1";
        void PhaseLive(string label)
        {
            if (allocTypes is not null)
            {
                foreach (var line in allocTypes.Report(label))
                {
                    _out.WriteLine(line);
                }

                var m = System.Text.RegularExpressions.Regex.Match(label, @"^([a-z]+)(\d+)$");
                var n = int.Parse(m.Groups[2].Value, CultureInfo.InvariantCulture);
                AllocTypeSampler.Phase = m.Groups[1].Value switch
                {
                    "drain" => "output" + n,
                    "output" => "ckpt" + n,
                    _ => "drain" + (n + 1),
                };
            }

            if (Environment.GetEnvironmentVariable("IVM_PHASE_LIVE") != "1")
            {
                return;
            }

            var allocated = GC.GetTotalAllocatedBytes(precise: true);
            var sinceMib = (allocated - phaseAlloc) / 1048576.0;
            var before = GC.GetGCMemoryInfo(GCKind.Any);
            System.Runtime.GCSettings.LargeObjectHeapCompactionMode = System.Runtime.GCLargeObjectHeapCompactionMode.CompactOnce;
            GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
            var live = GC.GetGCMemoryInfo(GCKind.FullBlocking);
            phaseAlloc = GC.GetTotalAllocatedBytes(precise: true);
            _out.WriteLine(string.Create(CultureInfo.InvariantCulture,
                $"PHASELIVE {label} alloc_since_mib={sinceMib:F0} live_mib={live.PromotedBytes / 1048576.0:F0} committed_before_mib={before.TotalCommittedBytes / 1048576.0:F0} rss_mib={Environment.WorkingSet / 1048576.0:F0} writeback_est_mib={DbspNet.Core.Operators.Stateful.Blocks.OrderedPartitionStore.LiveCacheBytesTotal / 1048576.0:F0} writeback_drops={DbspNet.Core.Operators.Stateful.Blocks.OrderedPartitionStore.WriteBackDrops}"));
        }

        void ResidencyAt(string tag, int epoch)
        {
            if (!blockResidency)
            {
                return;
            }

            IReadOnlyList<DbspNet.Core.Circuit.RootCircuit> replicas = parallel is not null ? parallel.Circuit.Replicas : [serial!.Circuit];
            var sw0 = Stopwatch.StartNew();
            var rows = BlockResidency.Walk(replicas, runner, epoch);
            foreach (var line in BlockResidency.Report(tag, rows))
            {
                _out.WriteLine(line);
            }

            foreach (var (stack, n, b) in DbspNet.Core.Operators.Stateful.Blocks.BlockAccess.TakeStacks().Take(25))
            {
                _out.WriteLine(string.Create(CultureInfo.InvariantCulture, $"RESIDSTACK {tag} mb={b / 1048576.0:F1} blocks={n} {stack}"));
            }

            _out.WriteLine(string.Create(CultureInfo.InvariantCulture, $"RESID {tag} took_ms={sw0.ElapsedMilliseconds} gc_heap_gib={GC.GetTotalMemory(false) / (double)(1L << 30):F2}"));
        }

        var restartAfter = int.TryParse(Environment.GetEnvironmentVariable("IVM_RESTART_AFTER"), out var ra) ? ra : 0;
        var restarted = false;
        var firstBatch = 1;
        DateTime restoreEndUtc = DateTime.MinValue;
        if (resume)
        {
            Assert.True(store is not null, "IVM_RESUME needs IVM_SNAPSHOT_DIR");
            var allocBefore = GC.GetTotalAllocatedBytes(precise: false);
            var pauseBefore = GC.GetTotalPauseDuration();
            var jitBefore = System.Runtime.JitInfo.GetCompilationTime();
            var jitMethodsBefore = System.Runtime.JitInfo.GetCompiledMethodCount();
            var heapStart = GC.GetTotalMemory(false);
            var rs = Stopwatch.GetTimestamp();
            DbspNet.Core.Operators.Stateful.Blocks.BlockAccess.Epoch = 1;
            DbspNet.Core.Operators.Stateful.Blocks.BlockAccess.Phase = DbspNet.Core.Operators.Stateful.Blocks.BlockAccess.Restore;
            var tick = await runner.RestoreAsync();
            _out.WriteLine(string.Create(CultureInfo.InvariantCulture,
                $"RESUME tick={tick} restore_ms={Stopwatch.GetElapsedTime(rs).TotalMilliseconds:F0} " +
                $"jit_ms={(System.Runtime.JitInfo.GetCompilationTime() - jitBefore).TotalMilliseconds:F0} jit_methods={System.Runtime.JitInfo.GetCompiledMethodCount() - jitMethodsBefore} " +
                $"heap_start_gib={heapStart / (double)(1L << 30):F2} " +
                $"alloc_gib={(GC.GetTotalAllocatedBytes(precise: false) - allocBefore) / (double)(1L << 30):F2} " +
                $"gc_pause_ms={(GC.GetTotalPauseDuration() - pauseBefore).TotalMilliseconds:F0} " +
                $"heap_end_gib={GC.GetTotalMemory(false) / (double)(1L << 30):F2} " +
                $"restore_gc_ms={DbspNet.Persistence.CheckpointGcMode.LastRestoreCollectMs:F0}"));
            restarted = true;
            firstBatch = 2;
            if (stateCensus)
            {
                censusBaseline = StateCensusAt("restore", null);
            }
            ResidencyAt("restore", 1);
            restoreEndUtc = DateTime.UtcNow;
        }

        if (resume && DbspNet.Persistence.Snapshot.ProfileSave)
        {
            foreach (var g in DbspNet.Persistence.Snapshot.RestoreProfile.ToArray().GroupBy(l => l.Op).OrderByDescending(g => g.Sum(l => l.Ms)).Take(10))
            {
                _out.WriteLine(string.Create(CultureInfo.InvariantCulture,
                    $"RESTOREOP {g.Key} n={g.Count()} sum_ms={g.Sum(l => l.Ms):F0} max_ms={g.Max(l => l.Ms):F0} alloc_mib={DbspNet.Persistence.Snapshot.RestoreAlloc.Where(a => a.Op == g.Key).Sum(a => a.Bytes) / 1048576.0:F0}"));
            }
        }

        for (var batch = firstBatch; batch <= 3; batch++)
        {
            if (batch > 1)
            {
                PromotePendingCommits(stagingRoot, batch - 1);
            }

            DbspNet.Core.Operators.Stateful.Blocks.BlockAccess.Epoch = batch;
            DbspNet.Core.Operators.Stateful.Blocks.BlockAccess.Phase = DbspNet.Core.Operators.Stateful.Blocks.BlockAccess.Drain;

            // IVM_TICK_LIVE=1 (census only: a full compacting GC per tick): batch 1's memory over its
            // ticks — heap before the GC (garbage included), live after, and the block state in it.
            if (Environment.GetEnvironmentVariable("IVM_TICK_LIVE") == "1" && batch == firstBatch)
            {
                IReadOnlyList<DbspNet.Core.Circuit.RootCircuit> tickReplicas = parallel is not null ? parallel.Circuit.Replicas : [serial!.Circuit];
                var tickRunner = runner;
                DbspNet.Connectors.Abstractions.ProgramRunner.AfterTick = t =>
                {
                    var before = GC.GetGCMemoryInfo(GCKind.Any).HeapSizeBytes;
                    var committed = GC.GetGCMemoryInfo(GCKind.Any).TotalCommittedBytes;
                    var allocated = GC.GetTotalAllocatedBytes(precise: false);
                    System.Runtime.GCSettings.LargeObjectHeapCompactionMode = System.Runtime.GCLargeObjectHeapCompactionMode.CompactOnce;
                    GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
                    var live = GC.GetGCMemoryInfo(GCKind.FullBlocking).PromotedBytes;
                    var rows = BlockResidency.Walk(tickReplicas, tickRunner, -1);
                    long blocks = rows.Sum(r => r.Resident), managed = rows.Sum(r => r.ManagedEntries), small = rows.Sum(r => r.SmallRows);
                    if (int.TryParse(Environment.GetEnvironmentVariable("IVM_TICK_CENSUS"), out var censusTick) && censusTick == t)
                    {
                        foreach (var line in StateCensus.Report($"tick{t}", StateCensus.Walk(tickReplicas, tickRunner, null, 3)))
                        {
                            _out.WriteLine(line);
                        }
                    }

                    if (int.TryParse(Environment.GetEnvironmentVariable("IVM_TICK_DROPSTREAMS"), out var dropTick) && dropTick == t)
                    {
                        // Experiment: what the operators' current deltas (stream values) keep alive.
                        var cleared = 0;
                        foreach (var rep in tickReplicas)
                        {
                            foreach (var op in rep.Operators)
                            {
                                for (var ty = op.GetType(); ty is not null && ty != typeof(object); ty = ty.BaseType)
                                {
                                    foreach (var f in ty.GetFields(System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.DeclaredOnly))
                                    {
                                        if (f.GetValue(op) is { } v && v.GetType().IsGenericType && v.GetType().GetGenericTypeDefinition() == typeof(DbspNet.Core.Circuit.Stream<>))
                                        {
                                            var cur = v.GetType().GetField("Current", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!;
                                            cur.SetValue(v, null);
                                            cleared++;
                                        }
                                    }
                                }
                            }
                        }

                        GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
                        _out.WriteLine(string.Create(CultureInfo.InvariantCulture,
                            $"DROPSTREAMS tick={t} cleared={cleared} live_after_mib={GC.GetGCMemoryInfo(GCKind.FullBlocking).PromotedBytes / 1048576.0:F0}"));
                    }

                    _out.WriteLine(string.Create(CultureInfo.InvariantCulture,
                        $"TICKLIVE tick={t} heap_before_mib={before / 1048576.0:F0} committed_mib={committed / 1048576.0:F0} live_mib={live / 1048576.0:F0} blocks_mib={blocks / 1048576.0:F0} managed_entries={managed} small_window_rows={small} alloc_gib={allocated / (double)(1L << 30):F1} rss_mib={Environment.WorkingSet / 1048576.0:F0} writeback_est_mib={DbspNet.Core.Operators.Stateful.Blocks.OrderedPartitionStore.LiveCacheBytesTotal / 1048576.0:F0} writeback_drops={DbspNet.Core.Operators.Stateful.Blocks.OrderedPartitionStore.WriteBackDrops}"));
                };
            }
            else
            {
                DbspNet.Connectors.Abstractions.ProgramRunner.AfterTick = null;
            }
            var alloc0 = GC.GetTotalAllocatedBytes(precise: false);
            var pause0 = GC.GetTotalPauseDuration();
            var gen2Before = GC.CollectionCount(2);
            var jit0 = System.Runtime.JitInfo.GetCompilationTime();
            double drainAllocGib = 0;
            var sw = Stopwatch.StartNew();
            var batchStartUtc = DateTime.UtcNow;
            DateTime? drainEndUtc = null;
            DateTime? outputEndUtc = null;
            var measureAtDrain = Environment.GetEnvironmentVariable("IVM_MEASURE_AT") == "drain";
            long ticks;
            double drainMs, outputMs;
            double outputGcMs = 0;
            if (Environment.GetEnvironmentVariable("DBSPNET_PROFILE") is { Length: > 0 })
            {
                // Only RunBatchAsync builds the DBSPNET_PROFILE report (stderr / DBSPNET_PROFILE_FILE).
                ticks = await runner.RunBatchAsync();
                drainMs = sw.Elapsed.TotalMilliseconds;
                outputMs = 0;
            }
            else
            {
                ticks = await runner.DrainAsync();
                drainMs = sw.Elapsed.TotalMilliseconds;
                drainAllocGib = (GC.GetTotalAllocatedBytes(precise: false) - alloc0) / (double)(1L << 30);
                drainEndUtc = DateTime.UtcNow;
                if (DbspNet.Core.Operators.Stateful.Blocks.BlockTraceProfile.Enabled)
                {
                    _out.WriteLine(DbspNet.Core.Operators.Stateful.Blocks.BlockTraceProfile.Report());
                }
                if (stateCensus && batch == firstBatch)
                {
                    StateCensusAt("drain", censusBaseline);
                }

                if (measureAtDrain)
                {
                    // IVM_MEASURE_AT=drain: before the output write forms any view, so the
                    // census sees operator state only.
                    MeasureLive(batch, runner);
                }

                if (DbspNet.Persistence.Blocks.BlockCache.Profile)
                {
                    _out.WriteLine($"FAULTS batch={batch} phase=drain " + DbspNet.Persistence.Blocks.BlockCache.TakeProfile());
                }

                ResidencyAt($"drain{batch}", batch);
                PhaseLive($"drain{batch}");
                DbspNet.Core.Operators.Stateful.Blocks.BlockAccess.Phase = DbspNet.Core.Operators.Stateful.Blocks.BlockAccess.Write;
                var outputPause0 = GC.GetTotalPauseDuration();
                await runner.WriteOutputsAsync();
                outputGcMs = (GC.GetTotalPauseDuration() - outputPause0).TotalMilliseconds;
                outputMs = sw.Elapsed.TotalMilliseconds - drainMs;
                outputEndUtc = DateTime.UtcNow;
                PhaseLive($"output{batch}");
                if (stateCensus && batch == firstBatch)
                {
                    StateCensusAt("write", censusBaseline);
                    censusBaseline = null;
                }
            }

            if (batch == 1 && Environment.GetEnvironmentVariable("IVM_PAUSE_DIR") is { Length: > 0 } pauseDir)
            {
                File.WriteAllText(Path.Combine(pauseDir, "pid"), Environment.ProcessId.ToString(CultureInfo.InvariantCulture));
                while (!File.Exists(Path.Combine(pauseDir, "go")))
                {
                    await Task.Delay(500);
                }
            }

            if (!measureAtDrain)
            {
                MeasureLive(batch, runner);
            }

            if (batch == 1 && DbspNet.Core.Collections.TraceAccessProfile.Enabled)
            {
                // Indexed-trace group shape after the bulk load (DBSPNET_TRACE_ACCESS_PROFILE=1).
                long traces = 0, groups = 0, values = 0, singletons = 0, containerBytes = 0, flatTraces = 0, flatRows = 0;
                var groupsBySize = new long[6];
                var valuesBySize = new long[6];
                foreach (var c in DbspNet.Core.Collections.TraceAccessProfile.Snapshot())
                {
                    if (c.Shape is { } sh)
                    {
                        traces++;
                        groups += sh.Groups;
                        values += sh.Values;
                        singletons += sh.Singletons;
                        containerBytes += sh.ContainerBytes;
                        for (var i = 0; i < 6; i++)
                        {
                            groupsBySize[i] += sh.GroupsBySize[i];
                            valuesBySize[i] += sh.ValuesBySize[i];
                        }
                    }
                    else
                    {
                        flatTraces++;
                        flatRows += c.StateKeys;
                    }
                }

                _out.WriteLine(string.Create(
                    CultureInfo.InvariantCulture,
                    $"SHAPE indexed traces={traces} groups={groups} values={values} singleton_groups={singletons} container_bytes={containerBytes} | flat traces={flatTraces} rows={flatRows}"));
                _out.WriteLine("SHAPE groups by size [1, 2-4, 5-16, 17-256, 257-4096, >4096] = " + string.Join(" ", groupsBySize));
                _out.WriteLine("SHAPE values by size [1, 2-4, 5-16, 17-256, 257-4096, >4096] = " + string.Join(" ", valuesBySize));
            }

            if (batch == 1 && DbspNet.Core.Collections.DeltaSizeProfile.Enabled)
            {
                // DBSPNET_DELTA_SIZE_PROFILE=1 (with DBSPNET_PROFILE=1 for attribution).
                foreach (var line in DbspNet.Core.Collections.DeltaSizeProfile.Report())
                {
                    _out.WriteLine(line);
                }
            }

            if (batch == firstBatch && gcEvents is not null)
            {
                await Task.Delay(2000); // runtime events arrive asynchronously
                _out.WriteLine("GCPAUSE " + gcEvents.Report());
                _out.WriteLine("GCPAUSE-DRAIN " + gcEvents.Report(drainEndUtc));
                foreach (var line in gcEvents.LohAllocations())
                {
                    _out.WriteLine(line);
                }
                string PhaseOf(DateTime at) =>
                    at < restoreEndUtc ? "restore" : at < batchStartUtc ? "before" : at < drainEndUtc ? "drain" : at < outputEndUtc ? "output" : "after";
                foreach (var line in gcEvents.PerGc(PhaseOf))
                {
                    _out.WriteLine(line);
                }
            }

            // The checkpoint the server would write at the end of this batch (RunBatchAsync under
            // DBSPNET_PROFILE has already written it, so this is then a no-op).
            double checkpointMs = 0;
            double snapshotMb = 0;
            double writtenMb = 0;
            if (store is not null)
            {
                if (Environment.GetEnvironmentVariable("IVM_CHECKPOINT_PREGC") == "1")
                {
                    GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
                    GC.WaitForPendingFinalizers();
                }

                DbspNet.Core.Operators.Stateful.Blocks.BlockAccess.Phase = DbspNet.Core.Operators.Stateful.Blocks.BlockAccess.Checkpoint;
                checkpointMs = await ProfileCheckpoint($"batch{batch}", async () =>
                {
                    if (policy is null)
                    {
                        await runner.CheckpointAsync();
                    }
                    else
                    {
                        await runner.CommitBatchAsync();
                    }
                });
                if (batch == 1 && Environment.GetEnvironmentVariable("IVM_CHECKPOINT_REPEAT") == "1")
                {
                    var fs = new DbspNet.Persistence.IO.Local.LocalTableFileSystem(snapshotDir!);
                    await ProfileCheckpoint("batch1-again", async () =>
                    {
                        if (parallel is not null)
                        {
                            await DbspNet.Persistence.ParallelSnapshot.WriteAsync(parallel.Circuit, fs, retainCount: 2);
                        }
                        else
                        {
                            await DbspNet.Persistence.Snapshot.WriteAsync(serial!.Circuit, fs, retainCount: 2);
                        }
                    });
                }
                if (DbspNet.Core.Operators.Stateful.Blocks.BlockTraceProfile.Enabled)
                {
                    _out.WriteLine("CKPT " + DbspNet.Core.Operators.Stateful.Blocks.BlockTraceProfile.Report());
                }

                if (stateCensus && batch == firstBatch)
                {
                    // After the checkpoint: a blocked trace (DBSPNET_TRACE_BLOCKS) has frozen every level.
                    StateCensusAt("ckpt", null);
                }

                var pointer = Path.Combine(snapshotDir!, "current.txt");
                if (File.Exists(pointer))
                {
                    var current = File.ReadAllText(pointer).Trim();
                    var dirBytes = Directory.EnumerateFiles(Path.Combine(snapshotDir!, current), "*", SearchOption.AllDirectories)
                        .Sum(f => new FileInfo(f).Length);

                    // With incremental runs (DBSPNET_STATE_LAYOUT=runs) the state also includes the
                    // shared runs the snapshot's run indexes (runs.txt) name; this checkpoint wrote only
                    // the ones under runs/{current}.
                    var referenced = Directory.EnumerateFiles(Path.Combine(snapshotDir!, current), "runs.txt", SearchOption.AllDirectories)
                        .SelectMany(File.ReadAllLines).Where(l => l.Length > 0)
                        .SelectMany(l => l.Split('\t').Skip(5)).Distinct() // op-i/file, base, delta, count, order, runs…
                        .Sum(r => new FileInfo(Path.Combine(snapshotDir!, r)).Length);
                    var newRuns = Path.Combine(snapshotDir!, "runs", current);
                    var written = Directory.Exists(newRuns)
                        ? Directory.EnumerateFiles(newRuns, "*", SearchOption.AllDirectories).Sum(f => new FileInfo(f).Length)
                        : 0;
                    snapshotMb = (dirBytes + referenced) / (1024.0 * 1024.0);
                    writtenMb = (dirBytes + written) / (1024.0 * 1024.0);
                }
            }

            if (DbspNet.Persistence.Blocks.BlockCache.Profile)
            {
                _out.WriteLine($"FAULTS batch={batch} phase=write+ckpt " + DbspNet.Persistence.Blocks.BlockCache.TakeProfile());
            }

            ResidencyAt($"batch{batch}", batch);
            PhaseLive($"ckpt{batch}");
            if (Environment.GetEnvironmentVariable("IVM_LIVE_EACH_BATCH") == "1")
            {
                // Live bytes after this batch's checkpoint (a full compacting GC: census runs only).
                System.Runtime.GCSettings.LargeObjectHeapCompactionMode = System.Runtime.GCLargeObjectHeapCompactionMode.CompactOnce;
                GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
                var live = GC.GetGCMemoryInfo(GCKind.FullBlocking);
                _out.WriteLine(string.Create(CultureInfo.InvariantCulture,
                    $"LIVEBATCH batch={batch} promoted_mib={live.PromotedBytes / 1048576.0:F0} rss_mib={Environment.WorkingSet / 1048576.0:F0} cache_mib={DbspNet.Persistence.Blocks.BlockCache.Bytes / 1048576.0:F0} dropped_alive={DbspNet.Persistence.Blocks.BlockCache.DroppedAlive().Count} dropped_alive_mib={DbspNet.Persistence.Blocks.BlockCache.DroppedAlive().Bytes / 1048576.0:F0}"));
                if (stateCensus)
                {
                    StateCensusAt($"after{batch}", null);
                }
            }

            // What the server's /wait reports after every batch.
            var c0 = Stopwatch.GetTimestamp();
            var viewRows = runner.ViewRowCounts().Sum(kv => kv.Value);
            var countsMs = Stopwatch.GetElapsedTime(c0).TotalMilliseconds;
            var bytes = Directory.EnumerateFiles(outRoot, "*.parquet", SearchOption.AllDirectories).Sum(f => new FileInfo(f).Length);
        foreach (var kv in DbspNet.Core.Circuit.StreamRelease.OpaqueReasons.OrderByDescending(kv => kv.Value).Take(10)) { Console.WriteLine($"OPAQUE {kv.Value} {kv.Key}"); }
            _out.WriteLine(string.Create(
                CultureInfo.InvariantCulture,
                $"RESULT batch={batch} ticks={ticks} drain_ms={drainMs:F0} output_ms={outputMs:F0} counts_ms={countsMs:F0} view_rows={viewRows} parquet_mb={bytes / 1e6:F1} drain_alloc_gib={drainAllocGib:F2} gc_pause_ms={(GC.GetTotalPauseDuration() - pause0).TotalMilliseconds:F0} gen2={GC.CollectionCount(2) - gen2Before} native_output={DbspNet.Persistence.Blocks.ViewArrow.Native} opaque_ops={DbspNet.Core.Circuit.StreamRelease.OpaqueOperators} release_plan_ms={DbspNet.Core.Circuit.StreamRelease.PlanTime.TotalMilliseconds:F0} released={DbspNet.Core.Circuit.StreamRelease.Released} jit_ms={(System.Runtime.JitInfo.GetCompilationTime() - jit0).TotalMilliseconds:F0}")
                + (store is null ? "" : string.Create(CultureInfo.InvariantCulture, $" checkpoint_ms={checkpointMs:F0} snapshot_mib={snapshotMb:F1} written_mib={writtenMb:F1}"))
                + string.Create(CultureInfo.InvariantCulture, $" peak_ws_mib={System.Diagnostics.Process.GetCurrentProcess().PeakWorkingSet64 / 1048576.0:F0} output_gc_ms={outputGcMs:F0}")
                + (restarted ? " resumed=1" : "")
                + (DbspNet.Persistence.Blocks.BlockCache.Enabled
                    ? string.Create(CultureInfo.InvariantCulture, $" cache_mib={DbspNet.Persistence.Blocks.BlockCache.Bytes / 1048576.0:F0} cache_admits={DbspNet.Persistence.Blocks.BlockCache.Admissions} cache_evicts={DbspNet.Persistence.Blocks.BlockCache.Evictions}")
                    : ""));
            if (restarted)
            {
                _out.WriteLine(string.Create(CultureInfo.InvariantCulture,
                    $"COLD after_batch={batch} resolves={DbspNet.Core.Collections.ColdStats.Resolves} present={DbspNet.Core.Collections.ColdStats.ResolvedPresent} promotions={DbspNet.Core.Collections.ColdStats.Promotions} promoted_groups={DbspNet.Core.Collections.ColdStats.PromotedGroups} read_ms={DbspNet.Core.Collections.ColdStats.ReadMs:F0} promote_ms={DbspNet.Core.Collections.ColdStats.PromoteMs:F0} row_reads={DbspNet.Core.Collections.ColdStats.RowResolves.Reads} row_present={DbspNet.Core.Collections.ColdStats.RowResolves.Present} giant_waits={DbspNet.Persistence.Runs.RunColdGroups.GiantWaits.Waits} giant_wait_ms={DbspNet.Persistence.Runs.RunColdGroups.GiantWaits.Ms:F0}"));
                foreach (var (who, n) in DbspNet.Core.Collections.ColdStats.Promoters.OrderByDescending(kv => kv.Value).Take(8))
                {
                    _out.WriteLine($"PROMOTER {who}{n}");
                }
            }

            if (batch == stopAfter)
            {
                break; // IVM_STOP_AFTER: leave the snapshot (and outputs) for an IVM_RESUME run
            }

            // IVM_RESTART_AFTER=N: after batch N's checkpoint, drop the program and restore a fresh
            // one from that checkpoint (the deploy path), then run the remaining batches on it —
            // their drain_ms is the "first batch after restore" (docs/design-durable-state.md §1f).
            if (store is not null && batch == restartAfter)
            {
                parallel?.Dispose();
                parallel = null;
                runner = null!;
                foreach (var o in outputs)
                {
                    await o.DisposeAsync();
                }

                GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
                GC.WaitForPendingFinalizers();
                parallel = workers > 1 ? CompileParallel() : null;
                outputs = spec.Output_Bindings
                    .Select(o => (IOutputConnector)new DeltaOutputConnector(o.View, Path.Combine(outRoot, o.View), mode))
                    .ToList();
                runner = parallel is not null
                    ? await ProgramRunner.CreateAsync(parallel, Inputs(), outputs, store, policy)
                    : await ProgramRunner.CreateAsync(CompileSerial(), Inputs(), outputs, store, policy);
                var allocBefore = GC.GetTotalAllocatedBytes(precise: false);
                var pauseBefore = GC.GetTotalPauseDuration();
                var heapAtStart = GC.GetTotalMemory(false);
                var rs = Stopwatch.GetTimestamp();
                var tick = await runner.RestoreAsync();
                _out.WriteLine(string.Create(CultureInfo.InvariantCulture,
                    $"RESTART after_batch={batch} tick={tick} restart_ms={Stopwatch.GetElapsedTime(rs).TotalMilliseconds:F0} " +
                    $"alloc_gib={(GC.GetTotalAllocatedBytes(precise: false) - allocBefore) / (double)(1L << 30):F2} " +
                    $"gc_pause_ms={(GC.GetTotalPauseDuration() - pauseBefore).TotalMilliseconds:F0} " +
                    $"heap_start_gib={heapAtStart / (double)(1L << 30):F2} heap_end_gib={GC.GetTotalMemory(false) / (double)(1L << 30):F2} " +
                    $"restore_gc_ms={DbspNet.Persistence.CheckpointGcMode.LastRestoreCollectMs:F0}"));
                restarted = true;

                // IVM_RESTART_GC=1: a full compacting collection after the restore (diagnostic).
                if (Environment.GetEnvironmentVariable("IVM_RESTART_GC") == "1")
                {
                    var g0 = Stopwatch.GetTimestamp();
                    var heapBefore = GC.GetTotalMemory(false);
                    GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
                    _out.WriteLine(string.Create(CultureInfo.InvariantCulture,
                        $"RESTARTGC ms={Stopwatch.GetElapsedTime(g0).TotalMilliseconds:F0} heap_before_gib={heapBefore / (double)(1L << 30):F2} heap_after_gib={GC.GetTotalMemory(false) / (double)(1L << 30):F2}"));
                }

                // IVM_RESTART_PAUSE_MS: idle this long after the restore before the next batch (to
                // separate the restore's own cost from the disk still settling after a checkpoint).
                if (int.TryParse(Environment.GetEnvironmentVariable("IVM_RESTART_PAUSE_MS"), out var pause) && pause > 0)
                {
                    await Task.Delay(pause);
                }

                // IVM_RESTART_PAUSE_DIR: write {dir}/pid after the restore and wait for {dir}/go (to
                // attach dotnet-gcdump / dotnet-trace to the just-restored process).
                if (Environment.GetEnvironmentVariable("IVM_RESTART_PAUSE_DIR") is { Length: > 0 } restartPauseDir)
                {
                    File.WriteAllText(Path.Combine(restartPauseDir, "pid"), Environment.ProcessId.ToString(CultureInfo.InvariantCulture));
                    while (!File.Exists(Path.Combine(restartPauseDir, "go")))
                    {
                        await Task.Delay(500);
                    }
                }
                if (DbspNet.Persistence.Snapshot.ProfileSave)
                {
                    var loads = DbspNet.Persistence.Snapshot.RestoreProfile.ToArray();
                    foreach (var g in loads.GroupBy(l => l.Op).OrderByDescending(g => g.Sum(l => l.Ms)).Take(10))
                    {
                        _out.WriteLine(string.Create(CultureInfo.InvariantCulture,
                            $"RESTOREOP {g.Key} n={g.Count()} sum_ms={g.Sum(l => l.Ms):F0} max_ms={g.Max(l => l.Ms):F0}"));
                    }
                }
            }
        }

        if (store is not null && parallel is not null && Environment.GetEnvironmentVariable("IVM_CHECKPOINT_DELTA") == "1")
        {
            // Phase 2: bytes an incremental checkpoint would write between retained snapshots.
            var deltaFs = new DbspNet.Persistence.IO.Local.LocalTableFileSystem(snapshotDir!);
            foreach (var line in await CheckpointDeltaProbe.MeasureAsync(deltaFs, parallel))
            {
                _out.WriteLine(line);
            }
        }

        if (store is not null && stopAfter == 0)
        {
            // Restart: drop the producer, then restore its last checkpoint into a fresh program at
            // the same W (the deploy path's RestoreAsync), and check every view came back.
            var expected = runner.ViewRowCounts().OrderBy(kv => kv.Key, StringComparer.Ordinal).ToList();
            var producerTick = runner.TickCount;
            parallel?.Dispose();
            parallel = null;
            runner = null!;
            GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
            GC.WaitForPendingFinalizers();

            var restoreWorkers = int.TryParse(Environment.GetEnvironmentVariable("IVM_RESTORE_WORKERS"), out var rw) ? rw : workers;
            using var restoredParallel = restoreWorkers > 1 ? CompileParallel(restoreWorkers) : null;
            var restored = restoredParallel is not null
                ? await ProgramRunner.CreateAsync(restoredParallel, Inputs(), [], store, policy)
                : await ProgramRunner.CreateAsync(CompileSerial(), Inputs(), [], store, policy);
            double reshardMs = 0;
            if (restoreWorkers != workers)
            {
                var rs0 = Stopwatch.GetTimestamp();
                await store.ReshardAsync(restoredParallel!);
                reshardMs = Stopwatch.GetElapsedTime(rs0).TotalMilliseconds;
            }

            var r0 = Stopwatch.GetTimestamp();
            var restoredTick = await restored.RestoreAsync();
            var restoreMs = Stopwatch.GetElapsedTime(r0).TotalMilliseconds;
            var actual = restored.ViewRowCounts().OrderBy(kv => kv.Key, StringComparer.Ordinal).ToList();
            var match = restoredTick == producerTick && expected.SequenceEqual(actual);
            _out.WriteLine(string.Create(
                CultureInfo.InvariantCulture,
                $"RESTORE workers={workers} restore_workers={restoreWorkers} reshard_ms={reshardMs:F0} tick={restoredTick} restore_ms={restoreMs:F0} view_rows_match={match}"));
            Assert.True(match, "restored view row counts differ from the producer's");
        }

        foreach (var o in outputs)
        {
            await o.DisposeAsync();
        }

        parallel?.Dispose();

        // Each output's contents as read back through engineered-wood (deletion vectors
        // applied): row count and an order-independent digest, comparable across modes.
        foreach (var o in spec.Output_Bindings.OrderBy(o => o.View, StringComparer.Ordinal))
        {
            var table = await global::EngineeredWood.DeltaLake.Table.DeltaTable.OpenAsync(
                new global::EngineeredWood.IO.Local.LocalTableFileSystem(Path.Combine(outRoot, o.View)));
            long rows = 0;
            ulong digest = 0;
            await foreach (var batch in table.ReadAllAsync(null, CancellationToken.None))
            {
                for (var i = 0; i < batch.Length; i++)
                {
                    var cells = new string[batch.ColumnCount];
                    for (var c = 0; c < batch.ColumnCount; c++)
                    {
                        cells[c] = Cell(batch.Column(c), i);
                    }

                    digest += System.IO.Hashing.XxHash64.HashToUInt64(System.Text.Encoding.UTF8.GetBytes(string.Join("\u0001", cells)));
                    rows++;
                }
            }

            table.Dispose();
            _out.WriteLine(string.Create(CultureInfo.InvariantCulture, $"VIEW {o.View} rows={rows} digest={digest:x16}"));
        }
    }

    private async Task<double> ProfileCheckpoint(string label, Func<Task> checkpoint)
    {
        var g0 = GC.CollectionCount(0);
        var g1 = GC.CollectionCount(1);
        var g2 = GC.CollectionCount(2);
        var pause0 = GC.GetTotalPauseDuration();
        var alloc0 = GC.GetTotalAllocatedBytes(precise: false);
        var cpu0 = Process.GetCurrentProcess().TotalProcessorTime;
        while (DbspNet.Persistence.Snapshot.SaveProfile.TryDequeue(out _))
        {
        }

        var info0 = GC.GetGCMemoryInfo();
        var ws0 = Process.GetCurrentProcess().WorkingSet64;
        var start = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var t0 = Stopwatch.GetTimestamp();
        await checkpoint();
        var wall = Stopwatch.GetElapsedTime(t0).TotalMilliseconds;
        if (DbspNet.Persistence.Snapshot.ProfileSave)
        {
            var proc = Process.GetCurrentProcess();
            _out.WriteLine(string.Create(CultureInfo.InvariantCulture,
                $"CKPTMEM {label} start_unix_ms={start} end_unix_ms={DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()} " +
                $"heap_gib={info0.HeapSizeBytes / (double)(1L << 30):F1} committed_gib={info0.TotalCommittedBytes / (double)(1L << 30):F1} " +
                $"fragmented_gib={info0.FragmentedBytes / (double)(1L << 30):F1} load_gib={info0.MemoryLoadBytes / (double)(1L << 30):F1} " +
                $"high_load_gib={info0.HighMemoryLoadThresholdBytes / (double)(1L << 30):F1} ws_before_gib={ws0 / (double)(1L << 30):F1} " +
                $"ws_after_gib={proc.WorkingSet64 / (double)(1L << 30):F1} peak_ws_gib={proc.PeakWorkingSet64 / (double)(1L << 30):F1}"));
        }

        if (!DbspNet.Persistence.Snapshot.ProfileSave)
        {
            return wall;
        }

        var cpu = (Process.GetCurrentProcess().TotalProcessorTime - cpu0).TotalMilliseconds;
        var ops = new List<(string Dir, int Index, string Op, double Ms, long Alloc)>();
        while (DbspNet.Persistence.Snapshot.SaveProfile.TryDequeue(out var e))
        {
            ops.Add(e);
        }

        var perWorker = ops.GroupBy(o => o.Dir).Select(g => g.Sum(o => o.Ms)).ToList();
        _out.WriteLine(string.Create(CultureInfo.InvariantCulture,
            $"CKPT {label} wall_ms={wall:F0} cpu_ms={cpu:F0} alloc_gib={(GC.GetTotalAllocatedBytes(precise: false) - alloc0) / (double)(1L << 30):F2} " +
            $"gc_pause_ms={(GC.GetTotalPauseDuration() - pause0).TotalMilliseconds:F0} gc0={GC.CollectionCount(0) - g0} gc1={GC.CollectionCount(1) - g1} gc2={GC.CollectionCount(2) - g2} " +
            $"save_sum_ms={ops.Sum(o => o.Ms):F0} worker_max_ms={(perWorker.Count == 0 ? 0 : perWorker.Max()):F0} worker_min_ms={(perWorker.Count == 0 ? 0 : perWorker.Min()):F0}"));
        foreach (var g in ops.GroupBy(o => o.Op).OrderByDescending(g => Environment.GetEnvironmentVariable("IVM_CHECKPOINT_PROFILE") == "alloc" ? g.Sum(o => o.Alloc) : g.Sum(o => o.Ms)).Take(12))
        {
            _out.WriteLine(string.Create(CultureInfo.InvariantCulture,
                $"CKPTOP {label} {g.Key} n={g.Count()} sum_ms={g.Sum(o => o.Ms):F0} max_ms={g.Max(o => o.Ms):F0} alloc_mib={g.Sum(o => o.Alloc) / 1048576.0:F0}"));
        }

        // The slowest operator positions (summed over workers): which op-{i} a file belongs to.
        var all = Environment.GetEnvironmentVariable("IVM_CHECKPOINT_PROFILE") == "all";
        foreach (var g in ops.GroupBy(o => (o.Index, o.Op)).OrderByDescending(g => g.Sum(o => o.Ms)).Take(all ? int.MaxValue : 12))
        {
            _out.WriteLine(string.Create(CultureInfo.InvariantCulture,
                $"CKPTIDX {label} op-{g.Key.Index} {g.Key.Op} sum_ms={g.Sum(o => o.Ms):F0} alloc_mib={g.Sum(o => o.Alloc) / 1048576.0:F0}"));
        }

        return wall;
    }

    private static string Cell(Apache.Arrow.IArrowArray array, int i)
    {
        if (array.IsNull(i))
        {
            return "<null>";
        }

        return array switch
        {
            Apache.Arrow.StringArray s => s.GetString(i) ?? "<null>",
            Apache.Arrow.Int32Array a => a.GetValue(i)!.Value.ToString(CultureInfo.InvariantCulture),
            Apache.Arrow.Int64Array a => a.GetValue(i)!.Value.ToString(CultureInfo.InvariantCulture),
            Apache.Arrow.DoubleArray a => a.GetValue(i)!.Value.ToString("R", CultureInfo.InvariantCulture),
            Apache.Arrow.BooleanArray a => a.GetValue(i)!.Value ? "t" : "f",
            Apache.Arrow.Date32Array a => a.GetValue(i)!.Value.ToString(CultureInfo.InvariantCulture),
            Apache.Arrow.TimestampArray a => a.GetValue(i)!.Value.ToString(CultureInfo.InvariantCulture),
            Apache.Arrow.Decimal128Array a => a.GetSqlDecimal(i)?.ToString() ?? "<null>",
            Apache.Arrow.FloatArray a => a.GetValue(i)!.Value.ToString("R", CultureInfo.InvariantCulture),
            // Any other primitive array: its typed GetValue(int), formatted invariantly.
            _ => array.GetType().GetMethod("GetValue", [typeof(int)])?.Invoke(array, [i]) is IFormattable f
                ? f.ToString(null, CultureInfo.InvariantCulture)
                : throw new InvalidOperationException($"no digest rule for {array.GetType().Name}"),
        };
    }

    // Delete every _delta_log commit that has a _pending twin, back to the batch-1 state.
    private static void RewindPendingCommits(string stagingRoot)
    {
        foreach (var table in Directory.GetDirectories(stagingRoot))
        {
            var pending = Path.Combine(table, "_pending");
            if (!Directory.Exists(pending))
            {
                continue;
            }

            foreach (var p in Directory.GetFiles(pending, "*.json"))
            {
                var target = Path.Combine(table, "_delta_log", Path.GetFileName(p));
                if (File.Exists(target))
                {
                    File.Delete(target);
                }
            }
        }
    }

    private static void PromotePendingCommits(string stagingRoot, int version)
    {
        var name = version.ToString("D20", CultureInfo.InvariantCulture) + ".json";
        foreach (var table in Directory.GetDirectories(stagingRoot))
        {
            var pending = Path.Combine(table, "_pending", name);
            if (File.Exists(pending))
            {
                File.Copy(pending, Path.Combine(table, "_delta_log", name), overwrite: true);
            }
        }
    }

    private sealed record Spec(
        List<string> Program,
        List<string>? Outputs,
        List<InputBinding> Inputs,
        List<OutputBinding> Output_Bindings);

    private sealed record InputBinding(string Table, string Uri, string Mode);

    private sealed record OutputBinding(string View, string Uri, string Mode);

    // Pairs each GC's suspend-begin with its restart-end (the pause the application sees) and
    // charges it to that GC's generation and kind, from the runtime's own GC events.
    // IVM_MEASURE_LIVE / IVM_HEAP_CENSUS after batch 1: retained bytes and what holds them.
    private void MeasureLive(int batch, object runner)
    {
        if (batch == 1 && Environment.GetEnvironmentVariable("IVM_MEASURE_LIVE") == "1")
        {
            // Live bytes after a full compacting GC: batch 1's retained state, exactly.
            System.Runtime.GCSettings.LargeObjectHeapCompactionMode = System.Runtime.GCLargeObjectHeapCompactionMode.CompactOnce;
            GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
            // PromotedBytes of that full blocking GC is what survived it; GetTotalMemory under
            // Server GC also counts free space in regions and varies run to run.
            var info = GC.GetGCMemoryInfo(GCKind.FullBlocking);
            _out.WriteLine(string.Create(
                CultureInfo.InvariantCulture,
                $"LIVE promoted={info.PromotedBytes} heap={info.HeapSizeBytes} fragmented={info.FragmentedBytes}"));
        }

        if (batch == 1 && Environment.GetEnvironmentVariable("IVM_HEAP_CENSUS") == "1")
        {
            // What survives the bulk load, by kind, and how shared the trace rows are.
            var censusSw = Stopwatch.StartNew();
            foreach (var line in HeapCensus.Reachable(runner))
            {
                _out.WriteLine(line);
            }

            if (DbspNet.Core.Collections.TraceAccessProfile.Enabled)
            {
                foreach (var line in HeapCensus.Traces(DbspNet.Core.Collections.TraceAccessProfile.Snapshot()))
                {
                    _out.WriteLine(line);
                }
            }

            _out.WriteLine(string.Create(CultureInfo.InvariantCulture, $"CENSUS took {censusSw.Elapsed.TotalSeconds:F0} s"));
        }
    }

    private sealed class GcPauseListener : System.Diagnostics.Tracing.EventListener
    {
        private readonly object _gate = new();
        private readonly List<(string Kind, double Ms, double P0, double P1, DateTime At)> _pauses = new();
        private readonly Dictionary<string, double> _lohAlloc = new(StringComparer.Ordinal);

        /// <summary>LOH bytes allocated by type (sampled; IVM_LOH_ALLOC=1).</summary>
        public string[] LohAllocations()
        {
            lock (_gate)
            {
                return _lohAlloc.OrderByDescending(kv => kv.Value).Take(15)
                    .Select(kv => string.Create(CultureInfo.InvariantCulture, $"LOHALLOC {kv.Value / 1e9,8:F2} GB  {kv.Key}"))
                    .ToArray();
            }
        }
        private readonly List<(DateTime At, string Line)> _perGc = new();
        private DateTime _suspend;
        private int _reason;

        // SUSPEND_REASON (gcinterface.h): why the runtime stopped every thread.
        private static readonly string[] SuspendReasons =
            ["other", "gc", "appdomain-shutdown", "code-pitching", "shutdown", "debugger", "gc-prep", "debugger-sweep", "profiler", "rejit"];
        private string? _kind;
        private double _p0, _p1, _g0, _g1, _g2, _loh;

        // GCMarkWithType: bytes each heap marked from each root kind this GC (3 = older
        // generations via the card table). Summed, and the busiest heap, per kind.
        private readonly Dictionary<int, (double Sum, double Max)> _mark = new();

        // Mark-phase time per heap: each GCMarkWithType fires as that heap finishes the phase, so
        // the gap since the heap's previous mark event (or GCStart) is the phase's duration.
        private readonly Dictionary<int, DateTime> _heapLast = new();
        private readonly Dictionary<int, (double SumMs, double MaxMs)> _markMs = new();
        private DateTime _gcStart;
        private static readonly string[] MarkKinds =
            ["stack", "finalizeQ", "handles", "cards", "sizedref", "overflow", "dephandles", "newFQ", "steal", "bgcsaved"];

        private static double Field(System.Diagnostics.Tracing.EventWrittenEventArgs e, string name) =>
            Convert.ToDouble(e.Payload![e.PayloadNames!.IndexOf(name)], CultureInfo.InvariantCulture);

        protected override void OnEventSourceCreated(System.Diagnostics.Tracing.EventSource source)
        {
            if (source.Name == "Microsoft-Windows-DotNETRuntime")
            {
                // IVM_LOH_ALLOC=1: Verbose adds GCAllocationTick (one event per ~100 KB allocated),
                // tallied below by type for the large-object heap.
                EnableEvents(
                    source,
                    Environment.GetEnvironmentVariable("IVM_LOH_ALLOC") == "1"
                        ? System.Diagnostics.Tracing.EventLevel.Verbose
                        : System.Diagnostics.Tracing.EventLevel.Informational,
                    (System.Diagnostics.Tracing.EventKeywords)0x1);
            }
        }

        protected override void OnEventWritten(System.Diagnostics.Tracing.EventWrittenEventArgs e)
        {
            lock (_gate)
            {
                switch (e.EventName)
                {
                    case "GCAllocationTick_V4" or "GCAllocationTick_V3":
                        if (Convert.ToInt32(e.Payload![e.PayloadNames!.IndexOf("AllocationKind")], CultureInfo.InvariantCulture) == 1)
                        {
                            var typeName = e.Payload![e.PayloadNames!.IndexOf("TypeName")] as string ?? "?";
                            _lohAlloc[typeName] = _lohAlloc.GetValueOrDefault(typeName) + Field(e, "AllocationAmount64");
                        }

                        break;
                    case "GCSuspendEEBegin_V1":
                        _suspend = e.TimeStamp;
                        _reason = Convert.ToInt32(e.Payload![e.PayloadNames!.IndexOf("Reason")], CultureInfo.InvariantCulture);
                        break;
                    case "GCStart_V2":
                        var depth = Convert.ToInt32(e.Payload![e.PayloadNames!.IndexOf("Depth")], CultureInfo.InvariantCulture);
                        var type = Convert.ToInt32(e.Payload![e.PayloadNames!.IndexOf("Type")], CultureInfo.InvariantCulture);
                        _kind = "gen" + depth + (type == 1 ? " background" : type == 2 ? " foreground" : " blocking");
                        _gcStart = e.TimeStamp;
                        break;
                    case "GCMarkWithType":
                        var mk = Convert.ToInt32(e.Payload![e.PayloadNames!.IndexOf("Type")], CultureInfo.InvariantCulture);
                        var mb = Field(e, "Bytes");
                        var (ms0, mx0) = _mark.GetValueOrDefault(mk);
                        _mark[mk] = (ms0 + mb, Math.Max(mx0, mb));
                        var heap = Convert.ToInt32(e.Payload![e.PayloadNames!.IndexOf("HeapNum")], CultureInfo.InvariantCulture);
                        var since = _heapLast.TryGetValue(heap, out var last) ? last : _gcStart;
                        var phaseMs = (e.TimeStamp - since).TotalMilliseconds;
                        _heapLast[heap] = e.TimeStamp;
                        var (ts0, tx0) = _markMs.GetValueOrDefault(mk);
                        _markMs[mk] = (ts0 + phaseMs, Math.Max(tx0, phaseMs));
                        break;
                    case "GCHeapStats_V2":
                        // Bytes this GC promoted out of gen0 / gen1: the copying a young pause pays for.
                        _p0 = Field(e, "TotalPromotedSize0");
                        _p1 = Field(e, "TotalPromotedSize1");
                        _g0 = Field(e, "GenerationSize0");
                        _g1 = Field(e, "GenerationSize1");
                        _g2 = Field(e, "GenerationSize2");
                        _loh = Field(e, "GenerationSize3");
                        break;
                    case "GCRestartEEEnd_V1":
                        if (_suspend != default)
                        {
                            var kind = _kind ?? "no GC (suspension only)";
                            var pause = (e.TimeStamp - _suspend).TotalMilliseconds;
                            _pauses.Add((kind, pause, _p0, _p1, _suspend.ToUniversalTime()));
                            if (_kind is null)
                            {
                                _perGc.Add((_suspend.ToUniversalTime(), string.Create(
                                    CultureInfo.InvariantCulture,
                                    $"SUSPEND {(_reason < SuspendReasons.Length ? SuspendReasons[_reason] : _reason.ToString(CultureInfo.InvariantCulture))} {pause:F0} ms")));
                            }
                            else
                            {
                                _perGc.Add((_suspend.ToUniversalTime(), string.Create(
                                    CultureInfo.InvariantCulture,
                                    $"GC {kind} {pause:F0} ms promoted0={_p0 / 1e9:F2} promoted1={_p1 / 1e9:F2} GB | gen0={_g0 / 1e9:F2} gen1={_g1 / 1e9:F2} gen2={_g2 / 1e9:F2} loh={_loh / 1e9:F2} GB | marked ")
                                    + string.Join(" ", _mark.OrderBy(kv => kv.Key).Select(kv => string.Create(
                                        CultureInfo.InvariantCulture,
                                        $"{(kv.Key < MarkKinds.Length ? MarkKinds[kv.Key] : kv.Key.ToString(CultureInfo.InvariantCulture))}={kv.Value.Sum / 1e9:F2}GB/{_markMs.GetValueOrDefault(kv.Key).MaxMs:F0}ms")))));
                            }
                        }

                        _suspend = default;
                        _kind = null;
                        _p0 = _p1 = _g0 = _g1 = _g2 = _loh = 0;
                        _mark.Clear();
                        _markMs.Clear();
                        _heapLast.Clear();
                        break;
                }
            }
        }

        /// <summary>One line per GC (IVM_GC_EVENTS=1): pause, promoted bytes, generation sizes.</summary>
        public string[] PerGc(Func<DateTime, string>? phaseOf = null)
        {
            lock (_gate)
            {
                return _perGc.Select(g => (phaseOf is null ? "" : "[" + phaseOf(g.At) + "] ") + g.Line).ToArray();
            }
        }

        /// <summary>Pauses by kind, those that began before <paramref name="until"/> (UTC) when given.</summary>
        public string Report(DateTime? until = null)
        {
            lock (_gate)
            {
                var pauses = _pauses.Where(p => until is null || p.At < until).ToList();
                return string.Join("; ", pauses.GroupBy(p => p.Kind).OrderBy(g => g.Key, StringComparer.Ordinal).Select(g =>
                    string.Create(CultureInfo.InvariantCulture, $"{g.Key}: {g.Count()} GCs {g.Sum(p => p.Ms):F0} ms")
                    + (g.Key.StartsWith("gen", StringComparison.Ordinal)
                        ? string.Create(CultureInfo.InvariantCulture, $" (promoted {g.Sum(p => p.P0) / 1e9:F2}/{g.Sum(p => p.P1) / 1e9:F2} GB)")
                        : "")))
                    + string.Create(CultureInfo.InvariantCulture, $"; total {pauses.Sum(p => p.Ms):F0} ms")
                    + string.Create(CultureInfo.InvariantCulture, $"; young {pauses.Where(p => !p.Kind.Contains("background", StringComparison.Ordinal)).Sum(p => p.Ms):F0} ms promoted {pauses.Sum(p => p.P0) / 1e9:F2}/{pauses.Sum(p => p.P1) / 1e9:F2} GB");
            }
        }
    }
}

// IVM_ALLOC_TYPES=1: allocation by type, by phase (GCAllocationTick: one sample per ~100 KB).
internal sealed class AllocTypeSampler : System.Diagnostics.Tracing.EventListener
{
    public static volatile string Phase = "before";
    private readonly object _gate = new();
    private readonly Dictionary<(string Phase, string Type), long> _bytes = new();

    protected override void OnEventSourceCreated(System.Diagnostics.Tracing.EventSource source)
    {
        if (source.Name == "Microsoft-Windows-DotNETRuntime")
        {
            EnableEvents(source, System.Diagnostics.Tracing.EventLevel.Verbose, (System.Diagnostics.Tracing.EventKeywords)0x1);
        }
    }

    protected override void OnEventWritten(System.Diagnostics.Tracing.EventWrittenEventArgs e)
    {
        if (e.EventName is not ("GCAllocationTick_V4" or "GCAllocationTick_V3"))
        {
            return;
        }

        var type = e.Payload![e.PayloadNames!.IndexOf("TypeName")] as string ?? "?";
        var amount = Convert.ToInt64(e.Payload![e.PayloadNames!.IndexOf("AllocationAmount64")], CultureInfo.InvariantCulture);
        lock (_gate)
        {
            var k = (Phase, type);
            _bytes[k] = _bytes.GetValueOrDefault(k) + amount;
        }
    }

    public IEnumerable<string> Report(string phase, int top = 15)
    {
        lock (_gate)
        {
            var rows = _bytes.Where(kv => kv.Key.Phase == phase).OrderByDescending(kv => kv.Value).Take(top).ToList();
            var total = _bytes.Where(kv => kv.Key.Phase == phase).Sum(kv => kv.Value);
            yield return string.Create(CultureInfo.InvariantCulture, $"ALLOC {phase} total_mib={total / 1048576.0:F0}");
            foreach (var kv in rows)
            {
                yield return string.Create(CultureInfo.InvariantCulture, $"ALLOC {phase} {kv.Value / 1048576.0,8:F0} MiB  {kv.Key.Type}");
            }
        }
    }
}
