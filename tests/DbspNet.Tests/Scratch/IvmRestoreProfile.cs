// Prices the RESTORE path one level below IvmRecoveryProbe (docs/design-incremental-persistence.md
// §11, the lazy/file-backed-restore arc). §10 apportioned restore across operator KINDS and found it
// deserialize-bound — "most of restore is reading bytes back, not rebuilding structure". That makes
// the next question "what is deserialize made of?", which this probe answers by splitting every
// codec load into read / decode / extract / materialize / index, and every operator's post-codec
// work into integrate / rebuild.
//
// It also splits the run in two so the expensive half is paid once:
//
//   IVM_RESTORE_MODE=record    ingest batch 1, snapshot, then (if IVM_BATCHES>1) keep ingesting so
//                              the end-of-run digest is recorded too. Writes restore-probe.json
//                              beside the snapshot: digests + connector cursors at the snapshot.
//   IVM_RESTORE_MODE=profile   (default) compile, restore that snapshot with the stage profile on,
//                              VERIFY against the recorded digest, print the apportionment.
//   IVM_RESTORE_MODE=replay    restore, then replay the held-back batches through the connectors
//                              with DBSPNET_TRACE_ACCESS_PROFILE=1 counting which restored keys the
//                              resumed pipeline actually touches. VERIFIES the end digest.
//
// Verification is not optional: §7.2 was a restore that silently produced wrong state, and a timing
// of a wrong answer is worthless. Every mode that restores checks a recorded digest.
//
// Env (as IvmRecoveryProbe, plus IVM_RESTORE_MODE):
//   IVM_DATA_ROOT IVM_SPEC IVM_SNAPSHOT_DIR IVM_STAGING_ROOT IVM_BATCHES IVM_TRACE_FAMILY
//   IVM_RESTORE_REPEAT   restore this many times in profile mode (default 1) — restore wall varies
//                        ~15% run to run (§9.1), so the split matters more than any single total.
using System.Diagnostics;
using System.Globalization;
using System.Text.Json;
using DbspNet.Arrow;
using DbspNet.Connectors.Abstractions;
using DbspNet.Connectors.EngineeredWood;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Persistence;
using DbspNet.Persistence.IO.Local;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.TypeSystem;
using Xunit;
using Xunit.Abstractions;

namespace DbspNet.Tests.Scratch;

public class IvmRestoreProfile
{
    private const string SidecarName = "restore-probe.json";

    private static readonly JsonSerializerOptions JsonOpts = new() { PropertyNameCaseInsensitive = true };

    private readonly ITestOutputHelper _out;

    public IvmRestoreProfile(ITestOutputHelper output) => _out = output;

    [Fact]
    public async Task ProfileRestore()
    {
        var dataRoot = Environment.GetEnvironmentVariable("IVM_DATA_ROOT");
        var specPath = Environment.GetEnvironmentVariable("IVM_SPEC");
        var snapshotDir = Environment.GetEnvironmentVariable("IVM_SNAPSHOT_DIR");
        if (string.IsNullOrEmpty(dataRoot) || string.IsNullOrEmpty(specPath) || string.IsNullOrEmpty(snapshotDir))
        {
            _out.WriteLine("IVM_DATA_ROOT / IVM_SPEC / IVM_SNAPSHOT_DIR not set — skipping.");
            return;
        }

        var stagingRoot = Environment.GetEnvironmentVariable("IVM_STAGING_ROOT");
        var mode = (Environment.GetEnvironmentVariable("IVM_RESTORE_MODE") ?? "profile").Trim().ToLowerInvariant();
        var family = (Environment.GetEnvironmentVariable("IVM_TRACE_FAMILY") ?? "flat").Trim().ToLowerInvariant();
        var traceFamily = family == "spine" ? TraceFamily.Spine : TraceFamily.Flat;
        var batches = Env("IVM_BATCHES", 3);
        if (string.IsNullOrEmpty(stagingRoot))
        {
            batches = 1;
        }

        var spec = JsonSerializer.Deserialize<Spec>(File.ReadAllText(specPath), JsonOpts)!;
        _out.WriteLine($"mode {mode}, family {traceFamily}, batches {batches}, snapshot {snapshotDir}");

        switch (mode)
        {
            case "record":
                await RecordAsync(spec, dataRoot, stagingRoot, snapshotDir, traceFamily, batches);
                return;
            case "replay":
                await ReplayAsync(spec, dataRoot, stagingRoot, snapshotDir, traceFamily, batches);
                return;
            default:
                await ProfileAsync(spec, snapshotDir, traceFamily);
                return;
        }
    }

    // ---------------- record ----------------

    private async Task RecordAsync(
        Spec spec, string dataRoot, string? stagingRoot, string snapshotDir,
        TraceFamily traceFamily, int batches)
    {
        if (Directory.Exists(snapshotDir))
        {
            Directory.Delete(snapshotDir, recursive: true);
        }

        Directory.CreateDirectory(snapshotDir);
        if (stagingRoot is { Length: > 0 })
        {
            RewindPendingCommits(stagingRoot);
        }

        var program = Compile(spec, traceFamily);
        var connectors = spec.Inputs
            .Select(i => new DeltaInputConnector(i.Table, InUriFor(i.Uri, dataRoot, stagingRoot)))
            .ToList();

        var cursors = new Dictionary<string, IConnectorOffset>(StringComparer.Ordinal);
        foreach (var c in connectors)
        {
            await c.ResolveSchemaAsync(program.Inputs[c.Name].Schema, default);
            cursors[c.Name] = c.InitialOffset;
        }

        var sw = Stopwatch.StartNew();
        var ticks = await IngestAsync(connectors, cursors, program);
        sw.Stop();
        _out.WriteLine(FormattableString.Invariant(
            $"record batch 1: {ticks} tick(s), ingest+step {sw.Elapsed.TotalMilliseconds:F0} ms"));

        var swS = Stopwatch.StartNew();
        await Snapshot.WriteAsync(program.Circuit, new LocalTableFileSystem(snapshotDir));
        swS.Stop();
        _out.WriteLine(FormattableString.Invariant(
            $"snapshot at tick {program.Circuit.TickCount}: {swS.Elapsed.TotalMilliseconds:F0} ms, {DirBytes(snapshotDir) / (1024.0 * 1024.0):F1} MiB"));

        var sidecar = new Sidecar
        {
            Tick = program.Circuit.TickCount,
            DigestAtSnapshot = Digest(program),
            Cursors = cursors.ToDictionary(kv => kv.Key, kv => kv.Value.Serialize(), StringComparer.Ordinal),
        };

        for (var b = 2; b <= batches; b++)
        {
            PromotePendingCommits(stagingRoot!, b - 1);
            var swB = Stopwatch.StartNew();
            var t = await IngestAsync(connectors, cursors, program);
            swB.Stop();
            _out.WriteLine(FormattableString.Invariant(
                $"record batch {b}: {t} tick(s), ingest+step {swB.Elapsed.TotalMilliseconds:F0} ms"));
        }

        sidecar.DigestAtEnd = Digest(program);
        sidecar.EndTick = program.Circuit.TickCount;
        File.WriteAllText(Path.Combine(snapshotDir, SidecarName), JsonSerializer.Serialize(sidecar));
        _out.WriteLine($"wrote {SidecarName}: snapshot tick {sidecar.Tick}, end tick {sidecar.EndTick}");
    }

    // ---------------- profile ----------------

    private async Task ProfileAsync(Spec spec, string snapshotDir, TraceFamily traceFamily)
    {
        var sidecar = ReadSidecar(snapshotDir);
        var repeat = Env("IVM_RESTORE_REPEAT", 1);
        for (var r = 0; r < repeat; r++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();

            SnapshotRestoreProfile.Reset();
            PartitionedWindowAggregateLoadProfile.Reset();
            // IVM_RESTORE_DEGREE=1 gives the sequential walk with the stage profile; anything
            // higher measures the shipping concurrent restore, where per-operator timings would be
            // meaningless (ReadAsync forces sequential while ProfileLoad is set, so ask for it
            // explicitly rather than silently getting the slow path).
            var degree = Env("IVM_RESTORE_DEGREE", Snapshot.DefaultRestoreParallelism);
            Snapshot.ProfileLoad = degree <= 1 && Environment.GetEnvironmentVariable("IVM_NO_STAGE_PROFILE") is null;

            var program = Compile(spec, traceFamily);
            var alloc0 = GC.GetTotalAllocatedBytes(precise: false);
            var sw = Stopwatch.StartNew();
            await Snapshot.ReadAsync(program.Circuit, new LocalTableFileSystem(snapshotDir), degree);
            sw.Stop();
            var allocMiB = (GC.GetTotalAllocatedBytes(precise: false) - alloc0) / (1024.0 * 1024.0);
            Snapshot.ProfileLoad = false;

            if (Environment.GetEnvironmentVariable("IVM_DUMP_CELL_TYPES") is "1")
            {
                DumpCellTypes(program);
            }

            var ok = CheckDigest(sidecar.DigestAtSnapshot, Digest(program), "restore");
            _out.WriteLine("");
            _out.WriteLine(FormattableString.Invariant(
                $"== restore #{r + 1}: {sw.Elapsed.TotalMilliseconds:F0} ms, {allocMiB:F0} MiB allocated, degree {degree}, tick {program.Circuit.TickCount}, {(ok ? "verified" : "WRONG")} =="));
            if (Snapshot.LastLoadProfile.Count > 0 && SnapshotRestoreProfile.Files > 0)
            {
                ReportStages(sw.Elapsed.TotalMilliseconds);
                ReportKinds();
            }

            Assert.True(ok, "restore produced wrong state — see report above");
        }
    }

    // Which CLR types actually reach an output row? The answer decides whether a cross-process
    // digest can use object.GetHashCode() at all (§11.1): Utf8String hashes with XxHash3 and is
    // process-stable, a raw string is not.
    private void DumpCellTypes(CompiledProgram program)
    {
        _out.WriteLine("  cell CLR types per output view:");
        foreach (var (name, output) in program.Outputs.OrderBy(kv => kv.Key, StringComparer.Ordinal))
        {
            var types = new SortedSet<string>(StringComparer.Ordinal);
            var seen = 0;
            foreach (var (row, _) in output.CurrentView)
            {
                for (var i = 0; i < output.Schema.Count; i++)
                {
                    types.Add(row[i]?.GetType().Name ?? "null");
                }

                if (++seen >= 200)
                {
                    break;
                }
            }

            _out.WriteLine($"    {name,-28} {string.Join(", ", types)}");
        }
    }

    private void ReportStages(double totalMs)
    {
        var p = SnapshotRestoreProfile.Legs();
        var sum = p.Sum(x => x.Ms);
        _out.WriteLine(FormattableString.Invariant(
            $"  {SnapshotRestoreProfile.Files} files, {SnapshotRestoreProfile.Rows:N0} rows, {SnapshotRestoreProfile.Bytes / (1024.0 * 1024.0):F1} MiB on disk"));
        _out.WriteLine("  stage                        ms     % of restore   % of stages");
        foreach (var (name, ms) in p)
        {
            _out.WriteLine(FormattableString.Invariant(
                $"    {name,-22} {ms,8:F0}   {ms / totalMs * 100,8:F1}%   {ms / sum * 100,8:F1}%"));
        }

        _out.WriteLine(FormattableString.Invariant(
            $"    {"(of extract: VARCHAR)",-22} {SnapshotRestoreProfile.ExtractStringMs,8:F0}   {SnapshotRestoreProfile.ExtractStringMs / totalMs * 100,8:F1}%   over {SnapshotRestoreProfile.StringColumns} of {SnapshotRestoreProfile.Columns} columns"));
        _out.WriteLine(FormattableString.Invariant(
            $"    {"[sum of stages]",-22} {sum,8:F0}   {sum / totalMs * 100,8:F1}%"));
        _out.WriteLine(FormattableString.Invariant(
            $"    {"[unattributed]",-22} {totalMs - sum,8:F0}   {(totalMs - sum) / totalMs * 100,8:F1}%"));
        if (SnapshotRestoreProfile.Rows > 0)
        {
            _out.WriteLine(FormattableString.Invariant(
                $"  per row: {sum * 1e6 / SnapshotRestoreProfile.Rows:F0} ns of attributed restore over {SnapshotRestoreProfile.Rows:N0} rows"));
        }
    }

    private void ReportKinds()
    {
        var prof = Snapshot.LastLoadProfile;
        if (prof.Count == 0)
        {
            return;
        }

        var total = prof.Sum(x => x.Ms);
        _out.WriteLine(FormattableString.Invariant($"  by operator kind ({prof.Count} ops, {total:F0} ms):"));
        foreach (var g in prof.GroupBy(x => x.Operator).OrderByDescending(g => g.Sum(x => x.Ms)))
        {
            var ms = g.Sum(x => x.Ms);
            _out.WriteLine(FormattableString.Invariant(
                $"    {g.Key,-42} {ms,8:F0} ms ({ms / total * 100,5:F1}%) x{g.Count()}"));
        }
    }

    // ---------------- replay (touch fraction) ----------------

    private async Task ReplayAsync(
        Spec spec, string dataRoot, string? stagingRoot, string snapshotDir,
        TraceFamily traceFamily, int batches)
    {
        var sidecar = ReadSidecar(snapshotDir);
        var program = Compile(spec, traceFamily);
        var sw = Stopwatch.StartNew();
        await Snapshot.ReadAsync(program.Circuit, new LocalTableFileSystem(snapshotDir));
        sw.Stop();
        var okRestore = CheckDigest(sidecar.DigestAtSnapshot, Digest(program), "restore");
        _out.WriteLine(FormattableString.Invariant(
            $"restore {sw.Elapsed.TotalMilliseconds:F0} ms, tick {program.Circuit.TickCount}, {(okRestore ? "verified" : "WRONG")}"));

        if (!TraceAccessProfileIsOn())
        {
            _out.WriteLine("DBSPNET_TRACE_ACCESS_PROFILE not set — replay will time but not count touches.");
        }

        RewindPendingCommits(stagingRoot!);
        var connectors = spec.Inputs
            .Select(i => new DeltaInputConnector(i.Table, InUriFor(i.Uri, dataRoot, stagingRoot)))
            .ToList();
        var cursors = new Dictionary<string, IConnectorOffset>(StringComparer.Ordinal);
        foreach (var c in connectors)
        {
            await c.ResolveSchemaAsync(program.Inputs[c.Name].Schema, default);
            cursors[c.Name] = c.ParseOffset(sidecar.Cursors![c.Name]);
        }

        TraceAccessProfile.ResetCounts();
        TraceAccessProfile.Arm();
        var swR = Stopwatch.StartNew();
        for (var b = 2; b <= batches; b++)
        {
            PromotePendingCommits(stagingRoot!, b - 1);
            await IngestAsync(connectors, cursors, program);
        }

        swR.Stop();
        TraceAccessProfile.Disarm();

        var okEnd = CheckDigest(sidecar.DigestAtEnd, Digest(program), "restore + replay");
        _out.WriteLine(FormattableString.Invariant(
            $"replay of batches 2..{batches}: {swR.Elapsed.TotalMilliseconds:F0} ms -> tick {program.Circuit.TickCount}, {(okEnd ? "verified" : "WRONG")}"));

        ReportTouch();
        Assert.True(okRestore, "restore produced wrong state");
        Assert.True(okEnd, "restore + replay produced wrong state");
    }

    private void ReportTouch()
    {
        var counters = TraceAccessProfile.Snapshot();
        if (counters.Count == 0)
        {
            _out.WriteLine("no trace-state counters registered (profile flag off).");
            return;
        }

        var live = counters.Where(c => c.StateKeys > 0).ToList();
        long keys = live.Sum(c => (long)c.StateKeys);
        long touched = live.Sum(c => (long)c.DistinctProbed);
        long probes = live.Sum(c => c.Probes);
        long scans = live.Sum(c => c.Scans);
        var scanned = live.Where(c => c.Scans > 0).Sum(c => (long)c.StateKeys);

        _out.WriteLine("");
        _out.WriteLine(FormattableString.Invariant(
            $"-- what the resumed ticks touched ({live.Count} non-empty trace collections) --"));
        _out.WriteLine(FormattableString.Invariant(
            $"  restored keys                 {keys,14:N0}"));
        _out.WriteLine(FormattableString.Invariant(
            $"  distinct keys probed          {touched,14:N0}  ({touched * 100.0 / Math.Max(1, keys):F4}% of restored)"));
        _out.WriteLine(FormattableString.Invariant(
            $"  probes (incl. repeats)        {probes,14:N0}"));
        _out.WriteLine(FormattableString.Invariant(
            $"  full scans of a trace         {scans,14:N0}  over {live.Count(c => c.Scans > 0)} collections"));
        _out.WriteLine(FormattableString.Invariant(
            $"  keys in scanned collections   {scanned,14:N0}  ({scanned * 100.0 / Math.Max(1, keys):F1}% of restored — laziness cannot skip these)"));

        _out.WriteLine("  largest collections:");
        foreach (var c in live.OrderByDescending(c => c.StateKeys).Take(12))
        {
            _out.WriteLine(FormattableString.Invariant(
                $"    {c.Kind,-8} keys {c.StateKeys,10:N0}  probed {c.DistinctProbed,8:N0} ({c.DistinctProbed * 100.0 / c.StateKeys,6:F3}%)  scans {c.Scans,4}"));
        }
    }

    private static bool TraceAccessProfileIsOn() =>
        Environment.GetEnvironmentVariable("DBSPNET_TRACE_ACCESS_PROFILE") is "1" or "true" or "TRUE";

    // ---------------- shared plumbing (mirrors IvmRecoveryProbe) ----------------

    private static async Task<long> IngestAsync(
        List<DeltaInputConnector> connectors,
        Dictionary<string, IConnectorOffset> cursors,
        CompiledProgram program)
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

                program.Step();
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
        var outputViews = IvmSpecViews.ToCompile(spec.Outputs, spec.Output_Bindings.Select(o => o.View));
        return SqlProgram.Compile(
            spec.Program, outputViews,
            snapshotCodecs: ArrowSqlSnapshotCodecs.Instance,
            options: new CompileOptions { TraceFamily = traceFamily },
            numericStringCoercion: true, nullCollation: NullCollation.Low);
    }

    private static Dictionary<string, DigestEntry> Digest(CompiledProgram program)
    {
        var d = new Dictionary<string, DigestEntry>(StringComparer.Ordinal);
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
                    h = unchecked((h * 486187739L) + StableCell(row[i]));
                }

                hash = unchecked(hash + (h * weight.Value));
            }

            d[name] = new DigestEntry(rows, hash);
        }

        return d;
    }

    // Process-INDEPENDENT cell hash. object.GetHashCode() is randomized per process for
    // string, which is fine inside one run (IvmRecoveryProbe records and verifies in the same
    // process) but silently reports every string-bearing view as "differs" when the digest is
    // written by the record run and read by a later one. Everything here depends only on the
    // value.
    private static long StableCell(object? value) => value switch
    {
        null => 0,
        string s => StableHash.Of(s),
        int i => StableHash.Of(i),
        long l => StableHash.Of(l),
        short sh => StableHash.Of(sh),
        byte b => StableHash.Of(b),
        bool bo => bo ? 1 : 2,
        double d => StableHash.Of(BitConverter.DoubleToInt64Bits(d)),
        float f => StableHash.Of(BitConverter.SingleToInt32Bits(f)),
        decimal m => StableHash.Of(m.ToString(CultureInfo.InvariantCulture)),
        DateTime dt => StableHash.Of(dt.Ticks),
        DateTimeOffset dto => StableHash.Of(dto.UtcTicks),
        TimeSpan ts => StableHash.Of(ts.Ticks),
        byte[] bytes => StableHash.Of(bytes),
        _ => StableHash.Of(Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty),
    };

    private bool CheckDigest(
        Dictionary<string, DigestEntry>? expected, Dictionary<string, DigestEntry> actual, string what)
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

            if (a.Rows == e.Rows && a.Hash == e.Hash)
            {
                continue;
            }

            bad.Add(FormattableString.Invariant($"    {view,-28} rows {e.Rows,10} -> {a.Rows,10}"));
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

    private static Sidecar ReadSidecar(string snapshotDir)
    {
        var path = Path.Combine(snapshotDir, SidecarName);
        if (!File.Exists(path))
        {
            throw new InvalidOperationException(
                $"{path} missing — run IVM_RESTORE_MODE=record first (it writes the digests this mode verifies against).");
        }

        return JsonSerializer.Deserialize<Sidecar>(File.ReadAllText(path), JsonOpts)!;
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

    private sealed class Sidecar
    {
        public long Tick { get; set; }

        public long EndTick { get; set; }

        public Dictionary<string, DigestEntry>? DigestAtSnapshot { get; set; }

        public Dictionary<string, DigestEntry>? DigestAtEnd { get; set; }

        public Dictionary<string, string>? Cursors { get; set; }
    }

    public sealed record DigestEntry(long Rows, long Hash);

    private sealed record Spec(
        List<string> Program,
        List<string>? Outputs,
        List<InputBinding> Inputs,
        List<OutputBinding> Output_Bindings);

    private sealed record InputBinding(string Table, string Uri, string Mode);

    private sealed record OutputBinding(string View, string Uri, string Mode);
}
