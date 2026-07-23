// Prices RECOVERY for the incremental-persistence arc (docs/design-incremental-persistence.md §5:
// "Recovery time — nothing here measures restore"). A2 replaces ProgramRunner's per-batch checkpoint
// with "WAL every batch, snapshot every N", and N is only settable honestly once we know what each
// leg of recovery costs on real SF=3 state:
//
//   recovery(N) ~= restore(snapshot)  +  N * replay(one batch of WAL)
//
// So this probe measures the two coefficients separately, plus the no-snapshot bound:
//
//   (a) snapshot-only restore  — Snapshot.ReadAsync over the real ~4 GiB checkpoint
//   (b) snapshot + WAL replay  — WalRecorder.CreateAsync, which loads the snapshot then replays
//                                only the ticks past it; replay leg = (b) - (a)
//   (c) WAL-only replay        — opt-in second recording with no snapshot at all, i.e. the
//                                "snapshot interval = infinity" bound (replays batch 1 from the log)
//
// This is also the first time the WAL runs over the real 50-view program with real data — A1 made
// that reachable (WalRecorder now takes any ICompiledCircuit) but only unit tests exercised it.
// The ingest loop below deliberately mirrors ProgramRunner.DrainAsync, except it calls
// wal.StepAsync() instead of program.Step() so each tick's inputs land in the log first.
//
// Every recovery is VERIFIED, not just timed: output-view digests captured during recording must
// match what the recovered program holds, otherwise the timing is of a wrong answer.
//
// Gated on env vars, no-op otherwise:
//   IVM_DATA_ROOT     local dir mirroring /data/raw/delta
//   IVM_SPEC          deploy spec JSON (dbt_to_program.py output)
//   IVM_SNAPSHOT_DIR  snapshot store — put it on /mnt/d, a run writes several GB
//   IVM_WAL_DIR       WAL store — likewise, batch 1's log is the whole input
//   IVM_STAGING_ROOT  multi-version staging copy (see IvmCheckpointReuse); absent => 1 batch
//   IVM_BATCHES       batches to run (default 3)
//   IVM_SNAPSHOT_AFTER  take the WAL's snapshot at the end of this batch (default 1)
//   IVM_WAL_ONLY      set to 1 to also run leg (c) — a second full recording with no snapshot
//   IVM_TRACE_FAMILY  flat (default) | spine
using System.Diagnostics;
using System.Globalization;
using System.Text.Json;
using DbspNet.Arrow;
using DbspNet.Connectors.Abstractions;
using DbspNet.Connectors.EngineeredWood;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Persistence;
using DbspNet.Persistence.IO.Local;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.TypeSystem;
using Xunit;
using Xunit.Abstractions;

namespace DbspNet.Tests.Scratch;

public class IvmRecoveryProbe
{
    private static readonly JsonSerializerOptions JsonOpts = new() { PropertyNameCaseInsensitive = true };

    private readonly ITestOutputHelper _out;

    public IvmRecoveryProbe(ITestOutputHelper output) => _out = output;

    [Fact]
    public async Task MeasureRecovery()
    {
        var dataRoot = Environment.GetEnvironmentVariable("IVM_DATA_ROOT");
        var specPath = Environment.GetEnvironmentVariable("IVM_SPEC");
        var snapshotDir = Environment.GetEnvironmentVariable("IVM_SNAPSHOT_DIR");
        var walDir = Environment.GetEnvironmentVariable("IVM_WAL_DIR");
        if (string.IsNullOrEmpty(dataRoot) || string.IsNullOrEmpty(specPath)
            || string.IsNullOrEmpty(snapshotDir) || string.IsNullOrEmpty(walDir))
        {
            _out.WriteLine("IVM_DATA_ROOT / IVM_SPEC / IVM_SNAPSHOT_DIR / IVM_WAL_DIR not set — skipping.");
            return;
        }

        var stagingRoot = Environment.GetEnvironmentVariable("IVM_STAGING_ROOT");
        var family = (Environment.GetEnvironmentVariable("IVM_TRACE_FAMILY") ?? "flat").Trim().ToLowerInvariant();
        var traceFamily = family == "spine" ? TraceFamily.Spine : TraceFamily.Flat;
        var batches = Env("IVM_BATCHES", 3);
        var snapshotAfter = Env("IVM_SNAPSHOT_AFTER", 1);
        var walOnly = Environment.GetEnvironmentVariable("IVM_WAL_ONLY") is "1" or "true" or "TRUE";
        if (string.IsNullOrEmpty(stagingRoot))
        {
            batches = 1;
        }

        var spec = JsonSerializer.Deserialize<Spec>(File.ReadAllText(specPath), JsonOpts)!;

        _out.WriteLine($"trace family   : {traceFamily}");
        _out.WriteLine($"batches        : {batches}, snapshot after batch {snapshotAfter}");
        _out.WriteLine($"snapshot dir   : {snapshotDir}");
        _out.WriteLine($"wal dir        : {walDir}");
        _out.WriteLine("");

        // ---------------- Phase 1: record ----------------
        var rec = await RecordAsync(
            spec, dataRoot, stagingRoot, snapshotDir, walDir, traceFamily, batches, snapshotAfter, takeSnapshot: true);

        // ---------------- Phase 2: measure the recovery legs ----------------
        _out.WriteLine("");
        _out.WriteLine("-- recovery --");

        // Each leg runs in its own scope, with the previous leg's circuit released and a
        // collection forced first. Without that, leg (b) restores a second ~4 GiB circuit
        // while leg (a)'s is still reachable, and the (b) - (a) subtraction charges the extra
        // GC pressure to "replay" — which is exactly how the bogus 70x in §7.3 arose.
        static void ReleaseHeap()
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
        }

        // (a) snapshot only. Restores to the end of batch `snapshotAfter`.
        ReleaseHeap();
        async Task<(double Ms, bool Ok, long Tick)> MeasureSnapshotOnly()
        {
            var snapOnly = Compile(spec, traceFamily);
            var sw = Stopwatch.StartNew();
            await Snapshot.ReadAsync(snapOnly.Circuit, new LocalTableFileSystem(snapshotDir));
            sw.Stop();
            return (sw.Elapsed.TotalMilliseconds,
                    CheckDigest(rec.DigestAtSnapshot, Digest(snapOnly), "snapshot-only restore"),
                    snapOnly.Circuit.TickCount);
        }

        var (restoreMs, okA, tickA) = await MeasureSnapshotOnly();
        _out.WriteLine(FormattableString.Invariant(
            $"  (a) snapshot restore          {restoreMs,10:F0} ms   -> tick {tickA} (end of batch {snapshotAfter}), {(okA ? "verified" : "WRONG")}"));

        // (b) snapshot + WAL replay. Restores to the end of the last batch.
        ReleaseHeap();
        async Task<(double Ms, bool Ok, long Tick)> MeasureHybrid()
        {
            var hybrid = Compile(spec, traceFamily);
            var sw = Stopwatch.StartNew();
            await using (await WalRecorder.CreateAsync(
                hybrid, new LocalTableFileSystem(walDir), new LocalTableFileSystem(snapshotDir)))
            {
                sw.Stop();
                return (sw.Elapsed.TotalMilliseconds,
                        CheckDigest(rec.DigestAtEnd, Digest(hybrid), "snapshot+WAL recovery"),
                        hybrid.Circuit.TickCount);
            }
        }

        var (hybridMs, okB, tickB) = await MeasureHybrid();
        var replayMs = hybridMs - restoreMs;
        var replayedBatches = Math.Max(1, batches - snapshotAfter);
        _out.WriteLine(FormattableString.Invariant(
            $"  (b) snapshot + WAL replay     {hybridMs,10:F0} ms   -> tick {tickB} (end of batch {batches}), {(okB ? "verified" : "WRONG")}"));
        _out.WriteLine(FormattableString.Invariant(
            $"      replay leg = (b) - (a)    {replayMs,10:F0} ms   over {replayedBatches} batch(es) = {replayMs / replayedBatches:F0} ms/batch"));

        var okD = true;

        // (d) THE ISOLATION LEG. Restore from the snapshot, then drive the remaining batches
        //     through the CONNECTORS with a plain program.Step() — no WAL anywhere. If (b)
        //     diverges and (d) does not, the fault is in WAL replay. If (d) diverges too, the
        //     fault is in snapshot restore itself, which would mean the shipping per-batch
        //     checkpoint (design-structural-parallel §10) silently corrupts on recovery.
        if (stagingRoot is { Length: > 0 } && snapshotAfter < batches)
        {
            ReleaseHeap();
            var afterRestore = Compile(spec, traceFamily);
            var swDRestore = Stopwatch.StartNew();
            await Snapshot.ReadAsync(afterRestore.Circuit, new LocalTableFileSystem(snapshotDir));
            swDRestore.Stop();
            var dRestoreMs = swDRestore.Elapsed.TotalMilliseconds;

            // Rewind staging to the snapshot point, then replay the later batches as the
            // connectors would have delivered them.
            RewindPendingCommits(stagingRoot);
            for (var b = 2; b <= snapshotAfter; b++)
            {
                PromotePendingCommits(stagingRoot, b - 1);
            }

            var conns = spec.Inputs.Select(i => new DeltaInputConnector(i.Table, InUriFor(i.Uri, dataRoot, stagingRoot))).ToList();
            var cur = new Dictionary<string, IConnectorOffset>(StringComparer.Ordinal);
            foreach (var c in conns)
            {
                await c.ResolveSchemaAsync(afterRestore.Inputs[c.Name].Schema, default);
                // The snapshot's cursors: every source consumed through the snapshot batch.
                cur[c.Name] = rec.CursorsAtSnapshot![c.Name];
            }

            var swD = Stopwatch.StartNew();
            for (var b = snapshotAfter + 1; b <= batches; b++)
            {
                PromotePendingCommits(stagingRoot, b - 1);
                await IngestAsync(conns, cur, afterRestore, wal: null);
            }

            swD.Stop();
            okD = CheckDigest(rec.DigestAtEnd, Digest(afterRestore), "restore + connector replay (no WAL)");
            _out.WriteLine(FormattableString.Invariant(
                $"  (d) restore + connectors      {dRestoreMs + swD.Elapsed.TotalMilliseconds,10:F0} ms   -> tick {afterRestore.Circuit.TickCount}, {(okD ? "verified" : "WRONG")}  (own restore {dRestoreMs:F0} ms + step leg {swD.Elapsed.TotalMilliseconds:F0} ms)"));
            _out.WriteLine(FormattableString.Invariant(
                $"      cross-check: (a) restore {restoreMs:F0} ms vs (d) restore {dRestoreMs:F0} ms — if these diverge, the (b)-(a) subtraction is not trustworthy"));
            if (okB && okD)
            {
                _out.WriteLine("      => both correct: nothing to isolate.");
            }
            else if (okD)
            {
                _out.WriteLine("      => (b) WRONG but (d) right: the fault is in WAL REPLAY.");
            }
            else
            {
                _out.WriteLine("      => (d) also wrong: the fault is in SNAPSHOT RESTORE, not the WAL.");
            }
        }

        // (c) WAL-only: a fresh recording that never snapshots, so nothing is pruned and the
        //     log still holds batch 1. This is what recovery costs at snapshot interval = infinity.
        if (walOnly)
        {
            var walOnlyDir = walDir + "-noSnap";
            _out.WriteLine("");
            _out.WriteLine("  (c) re-recording with NO snapshot to price the unbounded-interval case…");
            var rec2 = await RecordAsync(
                spec, dataRoot, stagingRoot, snapshotDir + "-unused", walOnlyDir, traceFamily,
                batches, snapshotAfter, takeSnapshot: false);

            var fromWal = Compile(spec, traceFamily);
            bool okC;
            var swC = Stopwatch.StartNew();
            await using (await WalRecorder.CreateAsync(fromWal, new LocalTableFileSystem(walOnlyDir)))
            {
                swC.Stop();
                okC = CheckDigest(rec2.DigestAtEnd, Digest(fromWal), "WAL-only recovery");
            }

            _out.WriteLine(FormattableString.Invariant(
                $"  (c) WAL-only replay           {swC.Elapsed.TotalMilliseconds,10:F0} ms   -> tick {fromWal.Circuit.TickCount} (all {batches} batches), {(okC ? "verified" : "WRONG")}"));
            _out.WriteLine(FormattableString.Invariant(
                $"      vs (b) snapshot-backed    {hybridMs,10:F0} ms   ratio {swC.Elapsed.TotalMilliseconds / Math.Max(1.0, hybridMs):F2}x"));
        }

        // ---------------- The knob ----------------
        _out.WriteLine("");
        _out.WriteLine("-- what this means for the A2 snapshot interval --");
        var perBatchReplay = replayMs / replayedBatches;

        // The replay coefficient is a DIFFERENCE of two ~35 s restores, so anything within a
        // few percent of a restore is indistinguishable from zero. Extrapolating such a
        // coefficient produces nonsense (a negative one predicts negative recovery time), so
        // say what was actually measured instead of projecting through the noise.
        var noiseFloorMs = restoreMs * 0.05;
        if (Math.Abs(perBatchReplay) < noiseFloorMs)
        {
            _out.WriteLine(FormattableString.Invariant(
                $"  replay of {replayedBatches} incremental batch(es) measured {perBatchReplay:F0} ms — below the"));
            _out.WriteLine(FormattableString.Invariant(
                $"  +/-{noiseFloorMs:F0} ms noise floor of differencing two ~{restoreMs / 1000.0:F0} s restores, i.e. not measurable."));
            _out.WriteLine(FormattableString.Invariant(
                $"  recovery is dominated by the {restoreMs / 1000.0:F1} s snapshot restore; replaying a SMALL batch is free."));
            _out.WriteLine("");
            _out.WriteLine("  Caveat: replay cost tracks the WORK in the replayed ticks, not the batch count.");
            _out.WriteLine("  These batches are ~200 rows. Replaying a BULK batch costs what that batch's step");
            _out.WriteLine("  cost originally (batch 1 here: ~60 s), so the interval is bounded by the largest");
            _out.WriteLine("  batch left in the log, not by how many batches are.");
        }
        else
        {
            _out.WriteLine(FormattableString.Invariant(
                $"  recovery(N batches since snapshot) ~= {restoreMs:F0} ms + N * {perBatchReplay:F0} ms"));
            foreach (var n in new[] { 1, 10, 100, 1000 })
            {
                _out.WriteLine(FormattableString.Invariant(
                    $"    N = {n,5}  ->  {(restoreMs + n * perBatchReplay) / 1000.0,9:F1} s"));
            }
        }

        _out.WriteLine("");
        _out.WriteLine(FormattableString.Invariant(
            $"  for reference, recording cost per batch: snapshot {rec.SnapshotMs:F0} ms, WAL append {rec.WalMsPerIncrementalBatch:F0} ms (incremental batches)"));

        // Assert LAST, so the whole report (timings + which views diverged) is printed
        // before the failure. A wrong recovery is a defect, not a measurement.
        Assert.True(okA, "snapshot-only restore produced wrong state — see report above");
        Assert.True(okB, "snapshot + WAL recovery produced wrong state — see report above");
        Assert.True(okD, "restore + connector replay produced wrong state — see report above");
    }

    // Drives batches 1..N through the WAL, mirroring ProgramRunner.DrainAsync's ingest loop but
    // stepping via the recorder so each tick's inputs are logged before they hit the circuit.
    private async Task<Recording> RecordAsync(
        Spec spec, string dataRoot, string? stagingRoot, string snapshotDir, string walDir,
        TraceFamily traceFamily, int batches, int snapshotAfter, bool takeSnapshot)
    {
        foreach (var dir in new[] { snapshotDir, walDir })
        {
            if (Directory.Exists(dir))
            {
                Directory.Delete(dir, recursive: true);
            }

            Directory.CreateDirectory(dir);
        }

        if (stagingRoot is { Length: > 0 })
        {
            RewindPendingCommits(stagingRoot);
        }

        var program = Compile(spec, traceFamily);

        var connectors = spec.Inputs
            .Select(i => new DeltaInputConnector(i.Table, InUriFor(i.Uri, dataRoot, stagingRoot)))
            .ToList();

        // Bind each source to the program's declared schema, exactly as ProgramRunner.CreateAsync does.
        var cursors = new Dictionary<string, IConnectorOffset>(StringComparer.Ordinal);
        foreach (var c in connectors)
        {
            await c.ResolveSchemaAsync(program.Inputs[c.Name].Schema, default);
            cursors[c.Name] = c.InitialOffset;
        }

        var result = new Recording();
        await using (var wal = await WalRecorder.CreateAsync(
            program, new LocalTableFileSystem(walDir), new LocalTableFileSystem(snapshotDir)))
        {
            for (var batch = 1; batch <= batches; batch++)
            {
                if (batch > 1 && stagingRoot is { Length: > 0 })
                {
                    PromotePendingCommits(stagingRoot, batch - 1);
                }

                var sw = Stopwatch.StartNew();
                var ticks = await IngestAsync(connectors, cursors, program, wal);
                sw.Stop();

                if (batch > 1)
                {
                    result.WalMsPerIncrementalBatch = sw.Elapsed.TotalMilliseconds;
                }

                _out.WriteLine(FormattableString.Invariant(
                    $"record batch {batch}: {ticks} tick(s), ingest+WAL+step {sw.Elapsed.TotalMilliseconds:F0} ms"));

                if (takeSnapshot && batch == snapshotAfter)
                {
                    var swS = Stopwatch.StartNew();
                    await wal.WriteSnapshotAsync();
                    swS.Stop();
                    result.SnapshotMs = swS.Elapsed.TotalMilliseconds;
                    result.DigestAtSnapshot = Digest(program);
                    result.CursorsAtSnapshot = new Dictionary<string, IConnectorOffset>(cursors, StringComparer.Ordinal);
                    _out.WriteLine(FormattableString.Invariant(
                        $"           snapshot at tick {program.Circuit.TickCount}: {swS.Elapsed.TotalMilliseconds:F0} ms"));
                }
            }

            result.DigestAtEnd = Digest(program);
        }

        var walBytes = DirBytes(walDir);
        var snapBytes = DirBytes(snapshotDir);
        _out.WriteLine(FormattableString.Invariant(
            $"           on disk: WAL {walBytes / (1024.0 * 1024.0):F1} MiB, snapshot {snapBytes / (1024.0 * 1024.0):F1} MiB"));
        return result;
    }

    // ProgramRunner.DrainAsync's loop, with wal.StepAsync() in place of program.Step().
    private static async Task<long> IngestAsync(
        List<DeltaInputConnector> connectors,
        Dictionary<string, IConnectorOffset> cursors,
        CompiledProgram program,
        WalRecorder? wal)
    {
        long ticks = 0;
        bool progressed;
        do
        {
            progressed = false;
            foreach (var c in connectors)
            {
                var cursor = cursors[c.Name];
                var latest = await c.LatestOffsetAsync(default);
                if (latest is null || latest.CompareTo(cursor) <= 0)
                {
                    continue;
                }

                var input = await c.NextAsync(cursor, default);
                if (input is null)
                {
                    continue;
                }

                await foreach (var vb in input.Content)
                {
                    program.Inputs[c.Name].PushArrow(vb.Batch, vb.Weights);
                }

                if (wal is null)
                {
                    program.Step();
                }
                else
                {
                    await wal.StepAsync();
                }

                ticks++;
                cursors[c.Name] = input.Offset;
                progressed = true;
            }
        }
        while (progressed);

        return ticks;
    }

    private static string InUriFor(string uri, string dataRoot, string? stagingRoot)
    {
        var rel = StripPrefix(uri, "/data/raw/delta/");
        if (stagingRoot is { Length: > 0 } && rel.StartsWith("staging/", StringComparison.Ordinal))
        {
            return Path.Combine(stagingRoot, rel["staging/".Length..]);
        }

        return Path.Combine(dataRoot, rel);
    }

    private static CompiledProgram Compile(Spec spec, TraceFamily traceFamily)
    {
        var outputViews = spec.Output_Bindings.Select(o => o.View).ToHashSet(StringComparer.Ordinal);
        return SqlProgram.Compile(
            spec.Program, outputViews,
            snapshotCodecs: ArrowSqlSnapshotCodecs.Instance,
            options: new CompileOptions { TraceFamily = traceFamily },
            numericStringCoercion: true, nullCollation: NullCollation.Low);
    }

    // Order-independent digest of every output view: row count and a weighted sum of row hashes.
    // Cheap enough to run on a 4 GiB state, strong enough that a wrong recovery cannot match.
    private static Dictionary<string, (long Rows, long Hash)> Digest(CompiledProgram program)
    {
        var d = new Dictionary<string, (long, long)>(StringComparer.Ordinal);
        foreach (var (name, output) in program.Outputs)
        {
            long rows = 0;
            long hash = 0;
            foreach (var (row, weight) in output.CurrentView)
            {
                rows += weight.Value;
                var h = 17L;
                for (var i = 0; i < output.Schema.Count; i++)
                {
                    h = unchecked((h * 486187739L) + (row[i]?.GetHashCode() ?? 0));
                }

                hash = unchecked(hash + (h * weight.Value));
            }

            d[name] = (rows, hash);
        }

        return d;
    }

    // Reports EVERY differing view, not just the first — when a recovery is wrong, the
    // pattern across views (which ones, and by what ratio) is the diagnosis.
    private bool CheckDigest(
        Dictionary<string, (long Rows, long Hash)>? expected,
        Dictionary<string, (long Rows, long Hash)> actual,
        string what)
    {
        Assert.NotNull(expected);
        var bad = new List<string>();
        foreach (var (view, e) in expected!.OrderBy(kv => kv.Key, StringComparer.Ordinal))
        {
            if (!actual.TryGetValue(view, out var a))
            {
                bad.Add($"    {view,-28} MISSING");
                continue;
            }

            if (a == e)
            {
                continue;
            }

            var ratio = e.Rows != 0 ? (double)a.Rows / e.Rows : double.NaN;
            bad.Add(FormattableString.Invariant(
                $"    {view,-28} rows {e.Rows,10} -> {a.Rows,10}  ({ratio,5:F2}x){(a.Rows == e.Rows ? "  [same rows, different content]" : "")}"));
        }

        if (bad.Count == 0)
        {
            return true;
        }

        _out.WriteLine($"  !! {what}: {bad.Count}/{expected.Count} output views differ");
        foreach (var line in bad)
        {
            _out.WriteLine(line);
        }

        return false;
    }

    private static long DirBytes(string dir) =>
        Directory.Exists(dir)
            ? Directory.EnumerateFiles(dir, "*", SearchOption.AllDirectories).Sum(f => new FileInfo(f).Length)
            : 0;

    private static int Env(string name, int fallback) =>
        int.TryParse(Environment.GetEnvironmentVariable(name), NumberStyles.Integer,
            CultureInfo.InvariantCulture, out var v) ? v : fallback;

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

    private static string StripPrefix(string s, string prefix) =>
        s.StartsWith(prefix, StringComparison.Ordinal) ? s[prefix.Length..] : s.TrimStart('/');

    private sealed class Recording
    {
        public double SnapshotMs { get; set; }

        public double WalMsPerIncrementalBatch { get; set; }

        public Dictionary<string, (long Rows, long Hash)>? DigestAtSnapshot { get; set; }

        public Dictionary<string, IConnectorOffset>? CursorsAtSnapshot { get; set; }

        public Dictionary<string, (long Rows, long Hash)>? DigestAtEnd { get; set; }
    }

    private sealed record Spec(
        List<string> Program,
        List<InputBinding> Inputs,
        List<OutputBinding> Output_Bindings);

    private sealed record InputBinding(string Table, string Uri, string Mode);

    private sealed record OutputBinding(string View, string Uri, string Mode);
}
