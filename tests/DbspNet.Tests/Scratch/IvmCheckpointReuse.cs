// Phase 0 of the incremental-persistence arc (docs/persistence.md, design-structural-parallel §10):
// measure how much of a checkpoint is REUSABLE across consecutive ivm-bench batches — the number
// that gates whether a reference-manifest snapshot (Track B) is worth building at all.
//
// Snapshot.WriteAsync rewrites every operator's whole state every batch (O(state)). Feldera's
// transaction commit is incremental (O(delta)), and ivm-bench measures Feldera WITH persistence
// inside the batch window. So the question this probe answers is: on real SF=3 state, after a real
// batch-2 / batch-3 delta, what fraction of the bytes we rewrite are byte-for-byte identical to
// what we already wrote last batch?
//
// Method: run batches 1..N against a multi-version staging copy, keeping every checkpoint
// (retainCount high), then content-hash every file of every snap-T and diff consecutive
// snapshots. Two granularities fall out of the same measurement:
//   * FLAT traces write one file per operator, so a match means "this operator's ENTIRE state was
//     untouched by the batch" — reuse at operator granularity.
//   * SPINE traces write one file per immutable LSM batch, so a match means "this spine batch was
//     not compacted away" — reuse at batch granularity, which is what Track B would exploit.
// Run it once per family (IVM_TRACE_FAMILY=flat|spine) to get both, plus the spine-backed vs flat
// split of the snapshot's bytes (RecursiveCteOp and IntegrateOp have no spine sibling).
//
// Content-hash, not object identity, is deliberate: it is exactly what a content-addressed batch
// store could skip, it survives the positional `batch_i` renaming that compaction causes, and it
// needs no production-code change. It slightly OVERSTATES reuse where two distinct batches happen
// to serialise identically (rare and small), and understates it if serialisation is not
// deterministic for equal state (it is: sorted columnar arrays through a fixed Arrow codec).
//
// Gated on env vars, no-op otherwise:
//   IVM_DATA_ROOT     local dir mirroring /data/raw/delta
//   IVM_SPEC          deploy spec JSON (dbt_to_program.py output)
//   IVM_SNAPSHOT_DIR  where checkpoints go — put it on /mnt/d, a full run writes several GB
//   IVM_STAGING_ROOT  (optional) multi-version staging copy; each table has a _pending/ dir whose
//                     commit JSONs are promoted into _delta_log/ one per batch. Absent ⇒ 1 batch.
//   IVM_TRACE_FAMILY  flat (default) | spine
//   IVM_BATCHES       batches to run (default 3)
//   IVM_OUT_ROOT      (optional) output Delta root; default = temp
using System.Diagnostics;
using System.Globalization;
using System.Security.Cryptography;
using System.Text.Json;
using DbspNet.Connectors.Abstractions;
using DbspNet.Connectors.EngineeredWood;
using DbspNet.Persistence;
using DbspNet.Persistence.IO.Local;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.TypeSystem;
using Xunit;
using Xunit.Abstractions;

namespace DbspNet.Tests.Scratch;

public class IvmCheckpointReuse
{
    private static readonly JsonSerializerOptions JsonOpts = new() { PropertyNameCaseInsensitive = true };

    private readonly ITestOutputHelper _out;

    public IvmCheckpointReuse(ITestOutputHelper output) => _out = output;

    [Fact]
    public async Task MeasureCheckpointReuse()
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
        var outRoot = Environment.GetEnvironmentVariable("IVM_OUT_ROOT")
                      ?? Path.Combine(Path.GetTempPath(), "ivm-reuse-out");
        var family = (Environment.GetEnvironmentVariable("IVM_TRACE_FAMILY") ?? "flat").Trim().ToLowerInvariant();
        var traceFamily = family == "spine" ? TraceFamily.Spine : TraceFamily.Flat;
        var batches = int.TryParse(
            Environment.GetEnvironmentVariable("IVM_BATCHES"), NumberStyles.Integer,
            CultureInfo.InvariantCulture, out var b) ? b : 3;
        if (string.IsNullOrEmpty(stagingRoot))
        {
            batches = 1; // no pending commits to promote — only batch 1 has input
        }

        // Start from a clean snapshot dir so retained snap-T dirs are exactly this run's.
        if (Directory.Exists(snapshotDir))
        {
            Directory.Delete(snapshotDir, recursive: true);
        }

        Directory.CreateDirectory(snapshotDir);

        // …and from a staging copy rewound to version 0. A previous run promoted its
        // pending commits into _delta_log and left them there; without this, batch 1
        // would ingest batches 2 and 3 as well (visible as a first batch that drains
        // 3x the ticks it should) and every later "unchanged" reading would be an
        // artifact of nothing having been ingested.
        if (stagingRoot is { Length: > 0 })
        {
            var rewound = RewindPendingCommits(stagingRoot);
            _out.WriteLine($"rewound staging: removed {rewound} previously-promoted commit(s)");
        }

        _out.WriteLine($"trace family : {traceFamily}");
        _out.WriteLine($"batches      : {batches}");
        _out.WriteLine($"snapshot dir : {snapshotDir}");
        _out.WriteLine($"staging root : {stagingRoot ?? "(spec default — single batch)"}");
        _out.WriteLine("");

        var spec = JsonSerializer.Deserialize<Spec>(File.ReadAllText(specPath), JsonOpts)!;
        var outputViews = spec.Output_Bindings.Select(o => o.View).ToHashSet(StringComparer.Ordinal);

        var options = new CompileOptions { TraceFamily = traceFamily };
        var compileSw = Stopwatch.StartNew();
        var program = SqlProgram.Compile(
            spec.Program, outputViews,
            snapshotCodecs: ArrowSqlSnapshotCodecs.Instance,
            options: options,
            numericStringCoercion: true, nullCollation: NullCollation.Low);
        compileSw.Stop();
        _out.WriteLine($"compiled {program.Circuit.Operators.Count} operators in {compileSw.ElapsedMilliseconds} ms");

        // op index -> short type name, for the spine/flat split. Operators is internal;
        // DbspNet.Tests is an InternalsVisibleTo friend of DbspNet.Core.
        var opKind = new Dictionary<int, string>();
        for (var i = 0; i < program.Circuit.Operators.Count; i++)
        {
            opKind[i] = ShortName(program.Circuit.Operators[i].GetType());
        }

        string InUri(string uri)
        {
            var rel = StripPrefix(uri, "/data/raw/delta/");
            if (stagingRoot is { Length: > 0 } && rel.StartsWith("staging/", StringComparison.Ordinal))
            {
                return Path.Combine(stagingRoot, rel["staging/".Length..]);
            }

            return Path.Combine(dataRoot, rel);
        }

        var inputs = spec.Inputs
            .Select(i => (IInputConnector)new DeltaInputConnector(i.Table, InUri(i.Uri)))
            .ToList();
        var outputs = spec.Output_Bindings
            .Select(o => (IOutputConnector)new DeltaOutputConnector(o.View, Path.Combine(outRoot, o.View), OutputMode.Truncate))
            .ToList();

        // Retain every checkpoint this run writes so the diff can run offline at the end.
        var store = new SnapshotCheckpointStore(new LocalTableFileSystem(snapshotDir), retainCount: batches + 1);
        var runner = await ProgramRunner.CreateAsync(program, inputs, outputs, store);

        var snaps = new List<SnapView>();
        for (var batch = 1; batch <= batches; batch++)
        {
            if (batch > 1)
            {
                var promoted = PromotePendingCommits(stagingRoot!, batch - 1);
                _out.WriteLine($"batch {batch}: promoted {promoted} pending Delta commit(s) (version {batch - 1})");
            }

            // RunBatchAsync's own phases, unrolled so the checkpoint can be timed on its
            // own — the whole point is how much of a durable batch the save costs.
            var sw = Stopwatch.StartNew();
            var ticks = await runner.DrainAsync();
            var stepMs = sw.Elapsed.TotalMilliseconds;
            await runner.WriteOutputsAsync();
            var outMs = sw.Elapsed.TotalMilliseconds - stepMs;
            await runner.CheckpointAsync();
            var saveMs = sw.Elapsed.TotalMilliseconds - stepMs - outMs;
            sw.Stop();

            var view = ReadLatestSnapshot(snapshotDir, opKind);
            view.Batch = batch;
            view.Ticks = ticks;
            view.StepMs = stepMs;
            view.OutMs = outMs;
            view.SaveMs = saveMs;
            snaps.Add(view);

            _out.WriteLine(FormattableString.Invariant(
                $"batch {batch}: {ticks} tick(s)  step {stepMs:F0} ms  outputs {outMs:F0} ms  save {saveMs:F0} ms  ->  snap-{view.Tick} = {Mib(view.TotalBytes):F1} MiB in {view.Files.Count} file(s)"));
        }

        Report(snaps, traceFamily);

        Assert.NotEmpty(snaps);
    }

    private void Report(List<SnapView> snaps, TraceFamily family)
    {
        _out.WriteLine("");
        _out.WriteLine("=============== CHECKPOINT REUSE ===============");
        _out.WriteLine($"trace family: {family}");

        // ---- (1) whole-checkpoint reuse, batch over batch ----
        _out.WriteLine("");
        _out.WriteLine("-- whole checkpoint: bytes byte-identical to the previous checkpoint --");
        _out.WriteLine("  batch  snap      files    total MiB   unchanged MiB   %unchanged   changed MiB");
        for (var i = 0; i < snaps.Count; i++)
        {
            var s = snaps[i];
            if (i == 0)
            {
                _out.WriteLine(FormattableString.Invariant(
                    $"  {s.Batch,5}  {s.Tick,-8}  {s.Files.Count,7}  {Mib(s.TotalBytes),10:F1}  {"(baseline)",14}  {"-",11}  {Mib(s.TotalBytes),11:F1}"));
                continue;
            }

            var prev = snaps[i - 1];
            var (reused, _) = ReuseAgainst(s, prev);
            var pct = s.TotalBytes > 0 ? reused * 100.0 / s.TotalBytes : 0.0;
            _out.WriteLine(FormattableString.Invariant(
                $"  {s.Batch,5}  {s.Tick,-8}  {s.Files.Count,7}  {Mib(s.TotalBytes),10:F1}  {Mib(reused),14:F1}  {pct,10:F1}%  {Mib(s.TotalBytes - reused),11:F1}"));
        }

        // What the save costs today, and what it would cost if the unchanged bytes were
        // simply not rewritten (bytes-proportional projection — an upper bound on the win,
        // since a reference-manifest commit still pays per-batch bookkeeping).
        _out.WriteLine("");
        _out.WriteLine("-- durable batch: where the wall-clock goes, and the O(delta) projection --");
        _out.WriteLine("  batch    step ms   outputs ms    save ms   save %batch   projected save ms   projected %batch");
        for (var i = 0; i < snaps.Count; i++)
        {
            var s = snaps[i];
            var total = s.StepMs + s.OutMs + s.SaveMs;
            var sharePct = total > 0 ? s.SaveMs * 100.0 / total : 0.0;
            if (i == 0)
            {
                _out.WriteLine(FormattableString.Invariant(
                    $"  {s.Batch,5}  {s.StepMs,9:F0}  {s.OutMs,11:F0}  {s.SaveMs,9:F0}  {sharePct,11:F1}%  {"(baseline)",19}  {"-",17}"));
                continue;
            }

            var (reused, _) = ReuseAgainst(s, snaps[i - 1]);
            var frac = s.TotalBytes > 0 ? 1.0 - (double)reused / s.TotalBytes : 1.0;
            var projSave = s.SaveMs * frac;
            var projTotal = s.StepMs + s.OutMs + projSave;
            var projShare = projTotal > 0 ? projSave * 100.0 / projTotal : 0.0;
            _out.WriteLine(FormattableString.Invariant(
                $"  {s.Batch,5}  {s.StepMs,9:F0}  {s.OutMs,11:F0}  {s.SaveMs,9:F0}  {sharePct,11:F1}%  {projSave,19:F0}  {projShare,16:F1}%"));
        }

        // ---- (2) split by operator kind, on the last checkpoint ----
        var last = snaps[^1];
        var prevLast = snaps.Count > 1 ? snaps[^2] : null;
        _out.WriteLine("");
        _out.WriteLine($"-- snapshot bytes by operator kind (snap-{last.Tick}), and how much of each was unchanged --");
        _out.WriteLine("  operator kind                        ops    files      MiB     %of snap   unchanged MiB   %unchanged  spine?");

        var prevHashes = prevLast is null ? null : HashCounts(prevLast);
        var byKind = last.Files
            .GroupBy(f => f.Kind)
            .Select(g => (Kind: g.Key, Ops: g.Select(f => f.OpIndex).Distinct().Count(),
                          Files: g.Count(), Bytes: g.Sum(f => f.Length), Items: g.ToList()))
            .OrderByDescending(g => g.Bytes)
            .ToList();

        long spineBytes = 0, flatBytes = 0, spineReuse = 0, flatReuse = 0;
        foreach (var g in byKind)
        {
            long reused = 0;
            if (prevHashes is not null)
            {
                var pool = new Dictionary<string, int>(prevHashes, StringComparer.Ordinal);
                foreach (var f in g.Items)
                {
                    if (pool.TryGetValue(f.Hash, out var n) && n > 0)
                    {
                        pool[f.Hash] = n - 1;
                        reused += f.Length;
                    }
                }
            }

            var isSpine = g.Kind.StartsWith("Spine", StringComparison.Ordinal);
            if (isSpine) { spineBytes += g.Bytes; spineReuse += reused; }
            else { flatBytes += g.Bytes; flatReuse += reused; }

            var share = last.TotalBytes > 0 ? g.Bytes * 100.0 / last.TotalBytes : 0.0;
            var rpct = g.Bytes > 0 ? reused * 100.0 / g.Bytes : 0.0;
            _out.WriteLine(FormattableString.Invariant(
                $"  {g.Kind,-34}  {g.Ops,4}  {g.Files,7}  {Mib(g.Bytes),9:F1}  {share,9:F1}%  {Mib(reused),14:F1}  {rpct,10:F1}%  {(isSpine ? "yes" : "no"),6}"));
        }

        _out.WriteLine("");
        _out.WriteLine("-- spine-backed vs flat split of snapshot bytes --");
        var tot = last.TotalBytes;
        _out.WriteLine(FormattableString.Invariant(
            $"  spine-backed : {Mib(spineBytes),9:F1} MiB ({(tot > 0 ? spineBytes * 100.0 / tot : 0),5:F1}%)   unchanged {Mib(spineReuse),9:F1} MiB ({(spineBytes > 0 ? spineReuse * 100.0 / spineBytes : 0),5:F1}%)"));
        _out.WriteLine(FormattableString.Invariant(
            $"  flat         : {Mib(flatBytes),9:F1} MiB ({(tot > 0 ? flatBytes * 100.0 / tot : 0),5:F1}%)   unchanged {Mib(flatReuse),9:F1} MiB ({(flatBytes > 0 ? flatReuse * 100.0 / flatBytes : 0),5:F1}%)"));

        // ---- (3) the biggest individual operators ----
        _out.WriteLine("");
        _out.WriteLine($"-- top 20 operators by snapshot bytes (snap-{last.Tick}) --");
        _out.WriteLine("   op   kind                                files       MiB   unchanged MiB   %unchanged");
        var byOp = last.Files
            .GroupBy(f => f.OpIndex)
            .Select(g => (Op: g.Key, Kind: g.First().Kind, Files: g.Count(), Bytes: g.Sum(f => f.Length), Items: g.ToList()))
            .OrderByDescending(g => g.Bytes)
            .Take(20);
        foreach (var g in byOp)
        {
            long reused = 0;
            if (prevHashes is not null)
            {
                var pool = new Dictionary<string, int>(prevHashes, StringComparer.Ordinal);
                foreach (var f in g.Items)
                {
                    if (pool.TryGetValue(f.Hash, out var n) && n > 0)
                    {
                        pool[f.Hash] = n - 1;
                        reused += f.Length;
                    }
                }
            }

            var rpct = g.Bytes > 0 ? reused * 100.0 / g.Bytes : 0.0;
            _out.WriteLine(FormattableString.Invariant(
                $"  {g.Op,4}   {g.Kind,-34}  {g.Files,5}  {Mib(g.Bytes),9:F1}  {Mib(reused),14:F1}  {rpct,10:F1}%"));
        }

        _out.WriteLine("");
        _out.WriteLine("=============== END CHECKPOINT REUSE ===============");
    }

    // Bytes of `s` whose content hash also appears in `prev` (multiset match, so N identical
    // files in `s` only claim reuse against N identical files in `prev`).
    private static (long Reused, int Files) ReuseAgainst(SnapView s, SnapView prev)
    {
        var pool = HashCounts(prev);
        long bytes = 0;
        var files = 0;
        foreach (var f in s.Files)
        {
            if (pool.TryGetValue(f.Hash, out var n) && n > 0)
            {
                pool[f.Hash] = n - 1;
                bytes += f.Length;
                files++;
            }
        }

        return (bytes, files);
    }

    private static Dictionary<string, int> HashCounts(SnapView v)
    {
        var d = new Dictionary<string, int>(StringComparer.Ordinal);
        foreach (var f in v.Files)
        {
            d[f.Hash] = d.TryGetValue(f.Hash, out var n) ? n + 1 : 1;
        }

        return d;
    }

    // Delete every _delta_log commit that has a _pending twin, i.e. everything a prior
    // run promoted, leaving the copy back at version 0 (the batch-1 state).
    private static int RewindPendingCommits(string stagingRoot)
    {
        var removed = 0;
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
                    removed++;
                }
            }
        }

        return removed;
    }

    // Promote each staging table's pending commit for `version` into its _delta_log.
    private static int PromotePendingCommits(string stagingRoot, int version)
    {
        var name = version.ToString("D20", CultureInfo.InvariantCulture) + ".json";
        var promoted = 0;
        foreach (var table in Directory.GetDirectories(stagingRoot))
        {
            var pending = Path.Combine(table, "_pending", name);
            if (!File.Exists(pending))
            {
                continue;
            }

            var target = Path.Combine(table, "_delta_log", name);
            File.Copy(pending, target, overwrite: true);
            promoted++;
        }

        return promoted;
    }

    private static SnapView ReadLatestSnapshot(string snapshotDir, Dictionary<int, string> opKind)
    {
        var current = File.ReadAllText(Path.Combine(snapshotDir, "current.txt")).Trim();
        var dir = Path.Combine(snapshotDir, current);
        var tick = long.Parse(current.AsSpan("snap-".Length), CultureInfo.InvariantCulture);

        var files = new List<SnapFile>();
        foreach (var path in Directory.EnumerateFiles(dir, "*", SearchOption.AllDirectories))
        {
            var rel = Path.GetRelativePath(dir, path).Replace('\\', '/');
            if (rel == "manifest.json")
            {
                continue; // metadata, not state — and it changes every batch by construction
            }

            var opIndex = -1;
            var slash = rel.IndexOf('/');
            if (slash > 0 && rel.StartsWith("op-", StringComparison.Ordinal))
            {
                _ = int.TryParse(rel.AsSpan(3, slash - 3), NumberStyles.Integer, CultureInfo.InvariantCulture, out opIndex);
            }

            var info = new FileInfo(path);
            files.Add(new SnapFile(
                rel, opIndex, opKind.TryGetValue(opIndex, out var k) ? k : "(unknown)",
                info.Length, HashFile(path)));
        }

        return new SnapView { Tick = tick, Dir = dir, Files = files };
    }

    private static string HashFile(string path)
    {
        using var sha = SHA256.Create();
        using var fs = File.OpenRead(path);
        return Convert.ToHexString(sha.ComputeHash(fs));
    }

    private static string ShortName(Type t)
    {
        var n = t.Name;
        var tick = n.IndexOf('`');
        return tick >= 0 ? n[..tick] : n;
    }

    private static double Mib(long bytes) => bytes / (1024.0 * 1024.0);

    private static string StripPrefix(string s, string prefix) =>
        s.StartsWith(prefix, StringComparison.Ordinal) ? s[prefix.Length..] : s.TrimStart('/');

    private sealed class SnapView
    {
        public int Batch { get; set; }

        public long Ticks { get; set; }

        public double StepMs { get; set; }

        public double OutMs { get; set; }

        public double SaveMs { get; set; }

        public long Tick { get; init; }

        public string Dir { get; init; } = "";

        public List<SnapFile> Files { get; init; } = new();

        public long TotalBytes => Files.Sum(f => f.Length);
    }

    private sealed record SnapFile(string RelPath, int OpIndex, string Kind, long Length, string Hash);

    private sealed record Spec(
        List<string> Program,
        List<InputBinding> Inputs,
        List<OutputBinding> Output_Bindings);

    private sealed record InputBinding(string Table, string Uri, string Mode);

    private sealed record OutputBinding(string View, string Uri, string Mode);
}
