// Profiles WAL replay against the equivalent live ingest, for
// docs/design-incremental-persistence.md §7.3.
//
// §7.3 recorded replay as ~70x slower than the connector path on real SF=3 state (52.2 s vs
// 0.74 s for the same 9 ticks). This probe was written to reproduce that WITHOUT the 4 GiB
// state so the cause could be isolated in seconds rather than 4-minute runs.
//
// WHAT IT FOUND: the per-tick replay overhead is real but TINY — sub-millisecond to ~1.5 ms
// per tick, and it does not scale with table count or tick count. Most of what a naive
// measurement attributes to "replay" is actually fixed CreateAsync setup: reading and
// rewriting the manifest, subscribing to inputs, and opening a fresh segment file per table.
// So the columns below separate them:
//
//   setup   — CreateAsync over an EMPTY log: everything charged to every CreateAsync
//   replay  — CreateAsync over a recorded log
//   replay - setup — the actual cost of replaying the ticks
//   direct  — PushArrow + Step, i.e. what ProgramRunner.DrainAsync does
//
// At 9 ticks that net cost is on the order of ten milliseconds, which cannot account for
// §7.3's 52 seconds — the evidence that sent the investigation back to how §7.3 was measured
// rather than to the replay code.
//
// Gated: set IVM_WALPROF=1. Runs in seconds; no external data needed.
using System.Diagnostics;
using System.Globalization;
using DbspNet.Persistence;
using DbspNet.Persistence.IO.Local;
using DbspNet.Sql.Compiler;
using Xunit;
using Xunit.Abstractions;

namespace DbspNet.Tests.Scratch;

public class WalReplayProfile
{
    private readonly ITestOutputHelper _out;

    public WalReplayProfile(ITestOutputHelper output) => _out = output;

    [Fact]
    public async Task ProfileReplayVsDirect()
    {
        if (Environment.GetEnvironmentVariable("IVM_WALPROF") is not ("1" or "true" or "TRUE"))
        {
            _out.WriteLine("IVM_WALPROF not set — skipping.");
            return;
        }

        // `setup` is CreateAsync over an EMPTY log: manifest read/write, subscribing to inputs,
        // and opening a fresh segment file per table. Whatever that costs is charged to every
        // CreateAsync, replay or not — so the real per-tick replay cost is (replay - setup).
        _out.WriteLine("  tables  ticks    record ms   setup ms   replay ms   replay-setup   direct ms   per-tick replay   per-tick direct");
        foreach (var (tables, ticks) in new[]
                 {
                     (1, 20), (5, 20), (10, 20), (20, 20), (20, 40), (40, 20),
                 })
        {
            var dir = Path.Combine(Path.GetTempPath(), "dbspnet-walprof-" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(dir);
            try
            {
                var recordMs = await RecordAsync(dir, tables, ticks);
                var replayMs = await ReplayAsync(dir, tables);
                var directMs = Direct(tables, ticks);

                var emptyDir = dir + "-empty";
                Directory.CreateDirectory(emptyDir);
                double setupMs;
                try
                {
                    setupMs = await ReplayAsync(emptyDir, tables);
                }
                finally
                {
                    Directory.Delete(emptyDir, recursive: true);
                }

                var netReplay = replayMs - setupMs;
                _out.WriteLine(FormattableString.Invariant(
                    $"  {tables,6}  {ticks,5}  {recordMs,11:F0} {setupMs,10:F1} {replayMs,11:F1} {netReplay,14:F1} {directMs,11:F1} {netReplay / ticks,17:F2} {directMs / ticks,17:F2}"));
            }
            finally
            {
                Directory.Delete(dir, recursive: true);
            }
        }
    }

    // One source table per index plus one aggregate view each — enough state to be a real
    // circuit, small enough that state size cannot be the explanation.
    private static string[] Ddl(int tables)
    {
        var stmts = new List<string>();
        for (var t = 0; t < tables; t++)
        {
            stmts.Add($"CREATE TABLE t{t} (k INT NOT NULL, v BIGINT NOT NULL)");
            stmts.Add($"CREATE VIEW v{t} AS SELECT k, SUM(v) AS s FROM t{t} GROUP BY k");
        }

        return stmts.ToArray();
    }

    private static CompiledProgram Compile(int tables)
    {
        var outputs = new HashSet<string>(StringComparer.Ordinal);
        for (var t = 0; t < tables; t++)
        {
            outputs.Add($"v{t}");
        }

        return SqlProgram.Compile(Ddl(tables), outputs, ArrowSqlSnapshotCodecs.Instance);
    }

    // Tick i touches exactly ONE table (i % tables) with a couple of rows — the shape of an
    // ivm-bench incremental batch, where one source version arrives at a time.
    private static void PushTick(CompiledProgram p, int tables, int tick)
    {
        var table = $"t{tick % tables}";
        p.Table(table).Insert(tick, (long)tick * 10);
        p.Table(table).Insert(tick + 1, (long)tick * 20);
    }

    private static async Task<double> RecordAsync(string dir, int tables, int ticks)
    {
        var p = Compile(tables);
        var sw = Stopwatch.StartNew();
        await using (var wal = await WalRecorder.CreateAsync(p, new LocalTableFileSystem(dir)))
        {
            for (var i = 0; i < ticks; i++)
            {
                PushTick(p, tables, i);
                await wal.StepAsync();
            }
        }

        sw.Stop();
        return sw.Elapsed.TotalMilliseconds;
    }

    private static async Task<double> ReplayAsync(string dir, int tables)
    {
        var p = Compile(tables);
        var sw = Stopwatch.StartNew();
        await using (await WalRecorder.CreateAsync(p, new LocalTableFileSystem(dir)))
        {
            sw.Stop();
        }

        return sw.Elapsed.TotalMilliseconds;
    }

    private static double Direct(int tables, int ticks)
    {
        var p = Compile(tables);
        var sw = Stopwatch.StartNew();
        for (var i = 0; i < ticks; i++)
        {
            PushTick(p, tables, i);
            p.Step();
        }

        sw.Stop();
        return sw.Elapsed.TotalMilliseconds;
    }
}
