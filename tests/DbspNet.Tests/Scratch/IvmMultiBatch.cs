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
        if (Directory.Exists(outRoot))
        {
            Directory.Delete(outRoot, recursive: true);
        }

        RewindPendingCommits(stagingRoot);

        var spec = JsonSerializer.Deserialize<Spec>(File.ReadAllText(specPath), JsonOpts)!;
        var outputViews = IvmSpecViews.ToCompile(spec.Outputs, spec.Output_Bindings.Select(o => o.View));
        var workers = int.TryParse(Environment.GetEnvironmentVariable("IVM_WORKERS"), out var w) ? w : 1;
        ParallelCompiledProgram? parallel = null;
        if (workers > 1)
        {
            Assert.True(
                SqlProgram.TryCompileParallel(
                    spec.Program, outputViews, workers, out parallel,
                    numericStringCoercion: true, nullCollation: NullCollation.Low),
                "parallel compile refused the program");
        }

        string InUri(string uri)
        {
            var rel = uri.StartsWith("/data/raw/delta/", StringComparison.Ordinal) ? uri["/data/raw/delta/".Length..] : uri.TrimStart('/');
            return rel.StartsWith("staging/", StringComparison.Ordinal)
                ? Path.Combine(stagingRoot, rel["staging/".Length..])
                : Path.Combine(dataRoot, rel);
        }

        var inputs = spec.Inputs
            .Select(i => (IInputConnector)new DeltaInputConnector(i.Table, InUri(i.Uri)))
            .ToList();
        var outputs = spec.Output_Bindings
            .Select(o => (IOutputConnector)new DeltaOutputConnector(o.View, Path.Combine(outRoot, o.View), mode))
            .ToList();
        // IVM_GC_EVENTS=1: every GC pause during batch 1 (suspend → restart), by generation.
        using var gcEvents = Environment.GetEnvironmentVariable("IVM_GC_EVENTS") == "1" ? new GcPauseListener() : null;

        var runner = parallel is not null
            ? await ProgramRunner.CreateAsync(parallel, inputs, outputs)
            : await ProgramRunner.CreateAsync(
                CompileSerial(),
                inputs, outputs);

        // IVM_TYPE_VIEWS=1 (serial only): each view tries the typed fast path, structural
        // fallback elsewhere (design-row-representation §23). That path disables single-use
        // inlining, so compare it against DBSPNET_INLINE_SINGLE_USE=off, not the default.
        CompiledProgram CompileSerial()
        {
            var typeViews = Environment.GetEnvironmentVariable("IVM_TYPE_VIEWS") == "1";
            var compiled = SqlProgram.Compile(
                spec.Program, outputViews, options: new CompileOptions { TypeEligibleProgramViews = typeViews },
                numericStringCoercion: true, nullCollation: NullCollation.Low);
            if (typeViews)
            {
                var (typed, fellBack) = PlanToCircuit.LastProgramTypedTally;
                _out.WriteLine($"TYPED {typed.Count} views typed, {fellBack.Count} fell back; typed: " + string.Join(", ", typed));
            }

            return compiled;
        }

        _out.WriteLine($"output mode: {mode}, workers: {workers}");
        for (var batch = 1; batch <= 3; batch++)
        {
            if (batch > 1)
            {
                PromotePendingCommits(stagingRoot, batch - 1);
            }

            var sw = Stopwatch.StartNew();
            long ticks;
            double drainMs, outputMs;
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
                await runner.WriteOutputsAsync();
                outputMs = sw.Elapsed.TotalMilliseconds - drainMs;
            }

            if (batch == 1 && Environment.GetEnvironmentVariable("IVM_PAUSE_DIR") is { Length: > 0 } pauseDir)
            {
                File.WriteAllText(Path.Combine(pauseDir, "pid"), Environment.ProcessId.ToString(CultureInfo.InvariantCulture));
                while (!File.Exists(Path.Combine(pauseDir, "go")))
                {
                    await Task.Delay(500);
                }
            }

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

            if (batch == 1 && gcEvents is not null)
            {
                await Task.Delay(2000); // runtime events arrive asynchronously
                _out.WriteLine("GCPAUSE " + gcEvents.Report());
            }

            // What the server's /wait reports after every batch.
            var c0 = Stopwatch.GetTimestamp();
            var viewRows = runner.ViewRowCounts().Sum(kv => kv.Value);
            var countsMs = Stopwatch.GetElapsedTime(c0).TotalMilliseconds;
            var bytes = Directory.EnumerateFiles(outRoot, "*.parquet", SearchOption.AllDirectories).Sum(f => new FileInfo(f).Length);
            _out.WriteLine(string.Create(
                CultureInfo.InvariantCulture,
                $"RESULT batch={batch} ticks={ticks} drain_ms={drainMs:F0} output_ms={outputMs:F0} counts_ms={countsMs:F0} view_rows={viewRows} parquet_mb={bytes / 1e6:F1}"));
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
    private sealed class GcPauseListener : System.Diagnostics.Tracing.EventListener
    {
        private readonly object _gate = new();
        private readonly Dictionary<string, (int Count, double Ms)> _byKind = new(StringComparer.Ordinal);
        private DateTime _suspend;
        private string? _kind;

        protected override void OnEventSourceCreated(System.Diagnostics.Tracing.EventSource source)
        {
            if (source.Name == "Microsoft-Windows-DotNETRuntime")
            {
                EnableEvents(source, System.Diagnostics.Tracing.EventLevel.Informational, (System.Diagnostics.Tracing.EventKeywords)0x1);
            }
        }

        protected override void OnEventWritten(System.Diagnostics.Tracing.EventWrittenEventArgs e)
        {
            lock (_gate)
            {
                switch (e.EventName)
                {
                    case "GCSuspendEEBegin_V1":
                        _suspend = e.TimeStamp;
                        break;
                    case "GCStart_V2":
                        var depth = Convert.ToInt32(e.Payload![e.PayloadNames!.IndexOf("Depth")], CultureInfo.InvariantCulture);
                        var type = Convert.ToInt32(e.Payload![e.PayloadNames!.IndexOf("Type")], CultureInfo.InvariantCulture);
                        _kind = "gen" + depth + (type == 1 ? " background" : type == 2 ? " foreground" : " blocking");
                        break;
                    case "GCRestartEEEnd_V1":
                        if (_suspend != default)
                        {
                            var kind = _kind ?? "no GC (suspension only)";
                            var (n, ms) = _byKind.GetValueOrDefault(kind);
                            _byKind[kind] = (n + 1, ms + (e.TimeStamp - _suspend).TotalMilliseconds);
                        }

                        _suspend = default;
                        _kind = null;
                        break;
                }
            }
        }

        public string Report()
        {
            lock (_gate)
            {
                return string.Join("; ", _byKind.OrderBy(kv => kv.Key, StringComparer.Ordinal).Select(kv =>
                    string.Create(CultureInfo.InvariantCulture, $"{kv.Key}: {kv.Value.Count} GCs {kv.Value.Ms:F0} ms")))
                    + string.Create(CultureInfo.InvariantCulture, $"; total {_byKind.Values.Sum(v => v.Ms):F0} ms");
            }
        }
    }
}
