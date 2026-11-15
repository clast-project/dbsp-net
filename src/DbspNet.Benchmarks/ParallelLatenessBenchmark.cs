// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
// Prices the parallel LATENESS frontier (docs/design-parallel-frontier.md §2 Q2, §5): the same
// grouped aggregate at W workers on fine ticks, with and without a LATENESS declaration, plus the
// step-boundary global-max reduction timed on its own against the real replicas' operators.
//   dotnet run --project src/DbspNet.Benchmarks -c Release -- parlat [workers] [ticks] [rowsPerTick] [runs]
using System.Diagnostics;
using DbspNet.Core.Circuit;
using DbspNet.Sql.Compiler;

namespace DbspNet.Benchmarks;

internal static class ParallelLatenessBenchmark
{
    public static void Run(int workers, int ticks, int rowsPerTick, int runs)
    {
        Console.WriteLine(
            $"=== parallel LATENESS: W={workers}, ticks={ticks:N0}, rows/tick={rowsPerTick}, runs={runs} " +
            $"(ServerGC={System.Runtime.GCSettings.IsServerGC}) ===");
        Console.WriteLine($"  {"arm",-10} {"run",4} {"wall ms",10} {"us/tick",10} {"B/ev",10} {"retained",10}");

        var results = new Dictionary<string, List<(double Ms, double BPerEv)>>(StringComparer.Ordinal)
        {
            ["none"] = [],
            ["lateness"] = [],
        };

        // Warm-up: one short run per arm, so the first measured run is not paying the JIT.
        RunOnce("none", workers, Math.Min(ticks, 500), rowsPerTick);
        RunOnce("lateness", workers, Math.Min(ticks, 500), rowsPerTick);

        for (var r = 0; r < runs; r++)
        {
            // Alternate the order so drift (thermal, background) does not favor one arm.
            foreach (var arm in r % 2 == 0 ? new[] { "none", "lateness" } : ["lateness", "none"])
            {
                var (ms, bPerEv, retained) = RunOnce(arm, workers, ticks, rowsPerTick);
                results[arm].Add((ms, bPerEv));
                Console.WriteLine(
                    $"  {arm,-10} {r,4} {ms,10:F0} {ms * 1000 / ticks,10:F1} {bPerEv,10:F1} {retained,10:N0}");
            }
        }

        Console.WriteLine();
        foreach (var (arm, rs) in results)
        {
            var ms = rs.Select(x => x.Ms).Order().ToList();
            Console.WriteLine(
                $"  {arm,-10} median {ms[ms.Count / 2],8:F0} ms  min {ms[0],8:F0}  max {ms[^1],8:F0}  " +
                $"median us/tick {ms[ms.Count / 2] * 1000 / ticks,7:F1}  B/ev {rs.Select(x => x.BPerEv).Order().ElementAt(rs.Count / 2),8:F1}");
        }

        PriceReduction(workers);
    }

    private static string[] Sql(bool lateness) =>
    [
        $"CREATE TABLE e (ts BIGINT NOT NULL{(lateness ? " LATENESS 50" : string.Empty)}, g INT NOT NULL, v INT NOT NULL)",
        "CREATE VIEW per_ts AS SELECT ts, COUNT(*) AS c, SUM(v) AS s FROM e GROUP BY ts",
    ];

    private static (double Ms, double BPerEv, long Retained) RunOnce(string arm, int workers, int ticks, int rowsPerTick)
    {
        var outputs = new HashSet<string>(["per_ts"], StringComparer.Ordinal);
        if (!SqlProgram.TryCompileParallel(Sql(arm == "lateness"), outputs, workers, out var p) || p is null)
        {
            throw new InvalidOperationException("parallel compile refused");
        }

        using (p)
        {
            var rng = new Random(42);
            var input = p.Table("e");
            GC.Collect();
            var b0 = GC.GetTotalAllocatedBytes(precise: true);
            var sw = Stopwatch.StartNew();
            for (var t = 0; t < ticks; t++)
            {
                for (var i = 0; i < rowsPerTick; i++)
                {
                    // Near-monotone, with ~2% late by more than the bound.
                    var ts = (10L * t) + rng.Next(0, 40) - (rng.Next(50) == 0 ? 200 : 0);
                    input.Insert(ts, rng.Next(0, 16), rng.Next(1, 100));
                }

                p.Step();
            }

            sw.Stop();
            var bytes = GC.GetTotalAllocatedBytes(precise: true) - b0;
            var retained = p.Circuit.Replicas.Sum(r => r.CollectStats().Where(s => s.RetainedRows > 0).Sum(s => s.RetainedRows));
            return (sw.Elapsed.TotalMilliseconds, (double)bytes / ((long)ticks * rowsPerTick), retained);
        }
    }

    // The reduction alone: the W replicas' real LatenessOperators, folded as ParallelCircuit.Step does
    // after every step. Run on the driver thread while every worker is parked, so its cost adds to
    // the step's critical path one-for-one.
    private static void PriceReduction(int workers)
    {
        var outputs = new HashSet<string>(["per_ts"], StringComparer.Ordinal);
        if (!SqlProgram.TryCompileParallel(Sql(lateness: true), outputs, workers, out var p) || p is null)
        {
            throw new InvalidOperationException("parallel compile refused");
        }

        using (p)
        {
            p.Table("e").Insert(1000L, 0, 1);
            p.Step();
            var reduction = new GlobalMaxReduction(workers);
            for (var w = 0; w < workers; w++)
            {
                reduction.Join(w, p.Circuit.Replicas[w].Operators.OfType<IGlobalMaxParticipant>().Single());
            }

            const int N = 5_000_000;
            for (var i = 0; i < 100_000; i++)
            {
                reduction.Reduce();
            }

            var b0 = GC.GetAllocatedBytesForCurrentThread();
            var sw = Stopwatch.StartNew();
            for (var i = 0; i < N; i++)
            {
                reduction.Reduce();
            }

            sw.Stop();
            var bytes = GC.GetAllocatedBytesForCurrentThread() - b0;
            Console.WriteLine();
            Console.WriteLine(
                $"  global-max reduction alone, W={workers}: {sw.Elapsed.TotalNanoseconds / N:F1} ns per step per LATENESS column, " +
                $"{(double)bytes / N:F2} B allocated per step");
        }
    }
}
