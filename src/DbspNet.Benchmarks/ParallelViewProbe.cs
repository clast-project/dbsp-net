// SCRATCH probe — prices ParallelProgramOutput.Accumulate() against the serial
// path's in-circuit IntegrateOp. Same program, same data, same materialised view;
// only the integrate differs:
//   serial   : IntegrateOp.Step  -> _view.Integrate(delta)   O(|delta|), in place
//   parallel : Accumulate()      -> _view += delta -> Plus() O(|view|), full copy
// If the second is O(view) per tick, total work is quadratic in tick count.
using System.Diagnostics;
using DbspNet.Sql.Compiler;

namespace DbspNet.Benchmarks;

internal static class ParallelViewProbe
{
    public static void Run(int ticks, int rowsPerTick)
    {
        string[] sql =
        [
            "CREATE TABLE t (k INT NOT NULL, v BIGINT NOT NULL)",
            "CREATE VIEW all_rows AS SELECT k, v FROM t",
        ];
        var outputs = new HashSet<string>(["all_rows"], StringComparer.Ordinal);

        Console.WriteLine();
        Console.WriteLine($"=== output-view integrate: serial vs parallel (ticks={ticks}, rows/tick={rowsPerTick}) ===");
        Console.WriteLine($"    final view size = {ticks * rowsPerTick:N0} rows");
        Console.WriteLine();
        Console.WriteLine($"  {"path",-16} {"wall ms",10} {"alloc MiB",12} {"view rows",12}");

        // --- serial: in-circuit IntegrateOp ---
        {
            var p = SqlProgram.Compile(sql, outputs);
            var b0 = GC.GetAllocatedBytesForCurrentThread();
            var sw = Stopwatch.StartNew();
            for (var t = 0; t < ticks; t++)
            {
                for (var i = 0; i < rowsPerTick; i++) p.Table("t").Insert(t * rowsPerTick + i, (long)i);
                p.Step();
            }

            sw.Stop();
            var mb = (GC.GetAllocatedBytesForCurrentThread() - b0) / 1024.0 / 1024.0;
            Console.WriteLine($"  {"serial",-16} {sw.Elapsed.TotalMilliseconds,10:F0} {mb,12:F1} {p.Outputs["all_rows"].CurrentView.Count,12:N0}");
        }

        // --- parallel W=1: driver-side Accumulate ---
        {
            if (!SqlProgram.TryCompileParallel(sql, outputs, 1, out var p) || p is null)
            {
                Console.WriteLine("  parallel compile refused");
                return;
            }

            using (p)
            {
                var b0 = GC.GetAllocatedBytesForCurrentThread();
                var sw = Stopwatch.StartNew();
                for (var t = 0; t < ticks; t++)
                {
                    for (var i = 0; i < rowsPerTick; i++) p.Table("t").Insert(t * rowsPerTick + i, (long)i);
                    p.Step();
                }

                sw.Stop();
                var mb = (GC.GetAllocatedBytesForCurrentThread() - b0) / 1024.0 / 1024.0;
                Console.WriteLine($"  {"parallel W=1",-16} {sw.Elapsed.TotalMilliseconds,10:F0} {mb,12:F1} {p.Outputs["all_rows"].CurrentView.Count,12:N0}");
            }
        }
    }
}
