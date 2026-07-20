// Local (Docker-free) reproduction of the ivm-bench SF=3 batch-1 run, for the
// optimization inner loop. Reads the copied-out Delta input tables and the deploy
// spec, compiles the exact program the DbspNet server deploys, and runs one batch
// with DBSPNET_PROFILE on — the same phase/operator profile you get in Docker, but
// rebuild-and-rerun in seconds instead of a full image rebuild + harness cycle.
//
// Gated on env vars so it is a no-op in CI / normal runs:
//   IVM_DATA_ROOT  local dir mirroring /data/raw/delta (copy mount/raw/3/delta here)
//   IVM_SPEC       path to the deploy spec JSON (dbt_to_program.py output)
//   IVM_OUT_ROOT   (optional) local dir for the 16 output Delta tables; default = temp
//
// Run just this test, e.g.:
//   IVM_DATA_ROOT=D:/ivm/raw/3/delta IVM_SPEC=D:/ivm/ivm_spec.json \
//     dotnet test --filter FullyQualifiedName~IvmBatchProfile
using System.Text.Json;
using DbspNet.Connectors.Abstractions;
using DbspNet.Connectors.EngineeredWood;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.TypeSystem;
using Xunit;
using Xunit.Abstractions;

namespace DbspNet.Tests.Scratch;

public class IvmBatchProfile
{
    private static readonly JsonSerializerOptions JsonOpts = new() { PropertyNameCaseInsensitive = true };

    private readonly ITestOutputHelper _out;

    public IvmBatchProfile(ITestOutputHelper output) => _out = output;

    [Fact]
    public async Task RunBatch1_Local()
    {
        var dataRoot = Environment.GetEnvironmentVariable("IVM_DATA_ROOT");
        var specPath = Environment.GetEnvironmentVariable("IVM_SPEC");
        if (string.IsNullOrEmpty(dataRoot) || string.IsNullOrEmpty(specPath))
        {
            _out.WriteLine("IVM_DATA_ROOT / IVM_SPEC not set — skipping local batch-1 repro.");
            return; // no-op unless explicitly driven
        }

        var outRoot = Environment.GetEnvironmentVariable("IVM_OUT_ROOT")
                      ?? Path.Combine(Path.GetTempPath(), "ivm-local-out");
        var profileFile = Environment.GetEnvironmentVariable("IVM_PROFILE_FILE")
                          ?? Path.Combine(Path.GetTempPath(), "ivm-local-profile.txt");

        // Turn the batch profiler on before ProgramRunner is first touched (its
        // enable flag is read once at type init).
        Environment.SetEnvironmentVariable("DBSPNET_PROFILE", "1");
        Environment.SetEnvironmentVariable("DBSPNET_PROFILE_FILE", profileFile);
        if (File.Exists(profileFile))
        {
            File.Delete(profileFile);
        }

        var spec = JsonSerializer.Deserialize<Spec>(File.ReadAllText(specPath), JsonOpts)!;

        // Remap container URIs to the local copies.
        string InUri(string uri) => Path.Combine(dataRoot, StripPrefix(uri, "/data/raw/delta/"));
        string OutUri(string view) => Path.Combine(outRoot, view);

        var outputViews = spec.Output_Bindings.Select(o => o.View).ToHashSet(StringComparer.Ordinal);

        // Measurement gate: IVM_TYPE_VIEWS=1 flips the program path to try the typed
        // fast path per view (structural fallback elsewhere). Off = the shipping
        // 100%-structural program. Everything else matches DbspNetEngine.DeployAsync.
        var typeViews = Environment.GetEnvironmentVariable("IVM_TYPE_VIEWS") is "1" or "true" or "TRUE";
        var options = typeViews
            ? new CompileOptions { TypeEligibleProgramViews = true }
            : CompileOptions.Default;
        var program = SqlProgram.Compile(
            spec.Program, outputViews, options: options,
            numericStringCoercion: true, nullCollation: NullCollation.Low);

        if (typeViews)
        {
            var (typed, fellBack) = PlanToCircuit.LastProgramTypedTally;
            _out.WriteLine($"TYPED program views: {typed.Count} typed, {fellBack.Count} fell back");
            _out.WriteLine("  typed:    " + string.Join(", ", typed));
            _out.WriteLine("  fellBack: " + string.Join(", ", fellBack));
        }

        var inputs = spec.Inputs
            .Select(i => (IInputConnector)new DeltaInputConnector(i.Table, InUri(i.Uri)))
            .ToList();
        var outputs = spec.Output_Bindings
            .Select(o => (IOutputConnector)new DeltaOutputConnector(o.View, OutUri(o.View), OutputMode.Truncate))
            .ToList();

        var runner = await ProgramRunner.CreateAsync(program, inputs, outputs);
        var ticks = await runner.RunBatchAsync();

        _out.WriteLine($"batch-1 done: {ticks} ticks. Profile → {profileFile}");
        if (File.Exists(profileFile))
        {
            _out.WriteLine(File.ReadAllText(profileFile));
        }

        Assert.True(ticks > 0);
    }

    private static string StripPrefix(string s, string prefix) =>
        s.StartsWith(prefix, StringComparison.Ordinal) ? s[prefix.Length..] : s.TrimStart('/');

    private sealed record Spec(
        List<string> Program,
        List<InputBinding> Inputs,
        List<OutputBinding> Output_Bindings);

    private sealed record InputBinding(string Table, string Uri, string Mode);

    private sealed record OutputBinding(string View, string Uri, string Mode);
}
