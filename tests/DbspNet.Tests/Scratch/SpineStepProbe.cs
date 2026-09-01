// The flat-vs-spine bulk-step re-measurement that `comparison-feldera-decisions.md` §9 row 3 asks
// for. `decision-trace-family.md` §1 measured spine at +14% on the bulk batch and concluded "stop
// growing spine" — but that compared LSM-with-in-step-compaction against a dictionary, while Feldera
// merges on background threads. Before moving our compaction off the step thread, this measures the
// thing that decides whether doing so could matter: how much of the spine step IS compaction.
//
// Times ingest and step separately (the §1 numbers are step-only, and ingest is identical work in
// both families, so folding them together would dilute the ratio). With
// DBSPNET_SPINE_COMPACTION_PROFILE=1 it also reports the merge/build split inside the spine step.
//
// Env: IVM_DATA_ROOT / IVM_SPEC / IVM_TRACE_FAMILY=flat|spine / IVM_TICKS (cap, default all)
//      IVM_SPINE_STAGING (memtable capacity, default 0 = one batch per delta)
using System.Diagnostics;
using System.Globalization;
using System.Text.Json;
using DbspNet.Arrow;
using DbspNet.Connectors.Abstractions;
using DbspNet.Connectors.EngineeredWood;
using DbspNet.Core.Circuit;
using DbspNet.Core.Operators.Stateful.Spine;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.TypeSystem;
using Xunit;
using Xunit.Abstractions;

namespace DbspNet.Tests.Scratch;

public class SpineStepProbe
{
    private static readonly JsonSerializerOptions JsonOpts = new() { PropertyNameCaseInsensitive = true };

    private readonly ITestOutputHelper _out;

    public SpineStepProbe(ITestOutputHelper output) => _out = output;

    [Fact]
    public async Task MeasureBulkStep()
    {
        var dataRoot = Environment.GetEnvironmentVariable("IVM_DATA_ROOT");
        var specPath = Environment.GetEnvironmentVariable("IVM_SPEC");
        if (string.IsNullOrEmpty(dataRoot) || string.IsNullOrEmpty(specPath))
        {
            _out.WriteLine("IVM_DATA_ROOT / IVM_SPEC not set — skipping.");
            return;
        }

        var family = (Environment.GetEnvironmentVariable("IVM_TRACE_FAMILY") ?? "flat")
            .Trim().ToLowerInvariant();
        var traceFamily = family == "spine" ? TraceFamily.Spine : TraceFamily.Flat;
        var staging = Env("IVM_SPINE_STAGING", 0);
        var tickCap = Env("IVM_TICKS", int.MaxValue);

        var spec = JsonSerializer.Deserialize<Spec>(File.ReadAllText(specPath), JsonOpts)!;
        var outputViews = spec.Output_Bindings.Select(o => o.View).ToHashSet(StringComparer.Ordinal);
        var options = new CompileOptions { TraceFamily = traceFamily, SpineStagingCapacity = staging };
        var program = SqlProgram.Compile(
            spec.Program, outputViews, options: options,
            numericStringCoercion: true, nullCollation: NullCollation.Low);

        var connectors = spec.Inputs
            .Select(i => new DeltaInputConnector(i.Table, Path.Combine(dataRoot, StripPrefix(i.Uri, "/data/raw/delta/"))))
            .ToList();
        var cursors = new Dictionary<string, IConnectorOffset>(StringComparer.Ordinal);
        foreach (var c in connectors)
        {
            await c.ResolveSchemaAsync(program.Inputs[c.Name].Schema, default);
            cursors[c.Name] = c.InitialOffset;
        }

        SpineCompactionProfile.Reset();
        var alloc0 = GC.GetTotalAllocatedBytes(precise: false);
        double ingestMs = 0, stepMs = 0;
        long ticks = 0;
        var wall = Stopwatch.StartNew();

        bool progressed;
        do
        {
            progressed = false;
            foreach (var c in connectors)
            {
                if (ticks >= tickCap)
                {
                    break;
                }

                var cursor = cursors[c.Name];
                var latest = await c.LatestOffsetAsync(default);
                if (latest is null || latest.CompareTo(cursor) <= 0)
                {
                    continue;
                }

                var swIn = Stopwatch.StartNew();
                var input = await c.NextAsync(cursor, default);
                if (input is null)
                {
                    continue;
                }

                await foreach (var vb in input.Content)
                {
                    program.Inputs[c.Name].PushArrow(vb.Batch, vb.Weights);
                }

                swIn.Stop();
                ingestMs += swIn.Elapsed.TotalMilliseconds;

                var swStep = Stopwatch.StartNew();
                program.Step();
                swStep.Stop();
                stepMs += swStep.Elapsed.TotalMilliseconds;

                ticks++;
                cursors[c.Name] = input.Offset;
                progressed = true;
            }
        }
        while (progressed);

        wall.Stop();
        var allocGiB = (GC.GetTotalAllocatedBytes(precise: false) - alloc0) / (1024.0 * 1024.0 * 1024.0);

        _out.WriteLine(FormattableString.Invariant(
            $"family {traceFamily}, staging {staging}, {ticks} ticks"));
        _out.WriteLine(FormattableString.Invariant(
            $"  STEP    {stepMs,10:F0} ms   <- the decision-trace-family §1 number"));
        _out.WriteLine(FormattableString.Invariant(
            $"  ingest  {ingestMs,10:F0} ms"));
        _out.WriteLine(FormattableString.Invariant(
            $"  wall    {wall.Elapsed.TotalMilliseconds,10:F0} ms, {allocGiB:F2} GiB allocated"));

        // A step timing is worthless if the families disagree on the answer. Process-independent
        // (StableHash, not GetHashCode — §11.1), so the digests can be compared across runs.
        long rows = 0, digest = 0;
        foreach (var (name, output) in program.Outputs.OrderBy(kv => kv.Key, StringComparer.Ordinal))
        {
            foreach (var (row, weight) in output.CurrentView)
            {
                rows += weight.Value;
                var h = 17L;
                for (var i = 0; i < output.Schema.Count; i++)
                {
                    h = unchecked((h * 486187739L) + StableCell(row[i]));
                }

                digest = unchecked(digest + (h * weight.Value));
            }
        }

        _out.WriteLine(FormattableString.Invariant(
            $"  OUTPUT  {rows:N0} rows over {program.Outputs.Count} views, digest {digest}"));

        if (SpineCompactionProfile.Enabled && traceFamily == TraceFamily.Spine)
        {
            var merge = SpineCompactionProfile.MergeMs;
            var build = SpineCompactionProfile.BuildMs;
            _out.WriteLine("");
            _out.WriteLine("  -- what the spine step spends on its LSM bookkeeping --");
            _out.WriteLine(FormattableString.Invariant(
                $"  merge (movable off-thread) {merge,10:F0} ms  {merge / stepMs * 100,5:F1}% of step   {SpineCompactionProfile.Merges:N0} merges, {SpineCompactionProfile.BatchesMerged:N0} batches, {SpineCompactionProfile.EntriesMerged:N0} entries"));
            _out.WriteLine(FormattableString.Invariant(
                $"  build (stays on-thread)    {build,10:F0} ms  {build / stepMs * 100,5:F1}% of step   {SpineCompactionProfile.Builds:N0} batch builds"));
            _out.WriteLine(FormattableString.Invariant(
                $"  => a PERFECT background merger removes at most {merge / stepMs * 100:F1}% of the spine step"));
        }
    }

    private static long StableCell(object? value) => value switch
    {
        null => 0,
        Utf8String u => StableHash.Of(u.Span),
        long l => StableHash.Of(l),
        int i => StableHash.Of(i),
        bool b => b ? 1 : 2,
        double d => StableHash.Of(BitConverter.DoubleToInt64Bits(d)),
        float f => StableHash.Of(BitConverter.SingleToInt32Bits(f)),
        Date32 dt => StableHash.Of(dt.Days),
        Timestamp ts => StableHash.Of(ts.Microseconds),
        Time64 t => StableHash.Of(t.Microseconds),
        _ => StableHash.Of(Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty),
    };

    private static int Env(string name, int fallback) =>
        int.TryParse(Environment.GetEnvironmentVariable(name), NumberStyles.Integer,
            CultureInfo.InvariantCulture, out var v) ? v : fallback;

    private static string StripPrefix(string s, string prefix) =>
        s.StartsWith(prefix, StringComparison.Ordinal) ? s[prefix.Length..] : s.TrimStart('/');

    private sealed record Spec(
        List<string> Program,
        List<InputBinding> Inputs,
        List<OutputBinding> Output_Bindings);

    private sealed record InputBinding(string Table, string Uri, string Mode);

    private sealed record OutputBinding(string View, string Uri, string Mode);
}
