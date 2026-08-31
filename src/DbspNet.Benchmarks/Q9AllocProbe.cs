// SCRATCH probe (uncommitted) — chases the q9 bimodal-allocation finding
// (docs/design-row-representation.md §25.3). Runs q9 N times inside ONE process
// with per-operator allocation attribution, and prints the process's string-hash
// seed fingerprint so a mode can be correlated with it.
//
// Two questions:
//   (1) Is the mode PROCESS-scoped (same every iteration in one process) or does
//       it flip within a process? Process-scoped is consistent with randomized
//       string hashing; within-process flipping is not.
//   (2) WHICH operator carries the 381 B/ev difference?
using System.Text;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Optimizer;
using DbspNet.Sql.Plan;
using DbspNet.Benchmarks.Nexmark;
using static DbspNet.Benchmarks.Nexmark.NexmarkGenerator;

namespace DbspNet.Benchmarks;

internal static class Q9AllocProbe
{
    /// <summary>
    /// A/Bs MonomorphizeTopKOrderKey on the same stream and asserts the two output
    /// Z-sets are identical row-for-row and weight-for-weight — the §26 soundness gate.
    /// </summary>
    public static void Verify(string queryId, int totalEvents, int batchSize)
    {
        Console.WriteLine();
        Console.WriteLine($"=== {queryId} monomorphized-TOP-K equivalence gate (events={totalEvents:N0}) ===");
        var query = NexmarkQueries.All.First(q => q.Id == queryId);
        var consumed = query.Tables.ToHashSet();
        var events = Generate(totalEvents);

        var boxed = RunOnce(query.Sql, events, consumed, batchSize, monomorphize: false);
        var unboxed = RunOnce(query.Sql, events, consumed, batchSize, monomorphize: true);

        Console.WriteLine($"  boxed   : {boxed.Count:N0} distinct output rows");
        Console.WriteLine($"  unboxed : {unboxed.Count:N0} distinct output rows");

        var mismatches = 0;
        foreach (var (row, w) in boxed)
        {
            if (!unboxed.TryGetValue(row, out var w2) || w2 != w)
            {
                if (mismatches++ < 5) Console.WriteLine($"    MISMATCH boxed[{row}]={w} unboxed={(unboxed.TryGetValue(row, out var v) ? v.ToString() : "<absent>")}");
            }
        }

        foreach (var (row, _) in unboxed)
        {
            if (!boxed.ContainsKey(row) && mismatches++ < 5) Console.WriteLine($"    MISMATCH extra row in unboxed: {row}");
        }

        Console.WriteLine(mismatches == 0
            ? "  RESULT: IDENTICAL — every row and weight matches."
            : $"  RESULT: {mismatches} MISMATCHES.");
    }

    private static Dictionary<string, long> RunOnce(
        string sql, List<Event> events, HashSet<NexmarkTable> consumed, int batchSize, bool monomorphize)
    {
        var q = Compile(sql, monomorphize);
        Feed(q, events, consumed, batchSize);
        var rows = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach (var (k, w) in q.Current)
        {
            if (w.Value == 0) continue;
            var sb = new StringBuilder();
            for (var i = 0; i < k.Count; i++) sb.Append(k[i]?.ToString() ?? "<null>").Append('\u001f');
            rows[sb.ToString()] = w.Value;
        }

        return rows;
    }

    public static void Run(string queryId, int totalEvents, int batchSize, int iterations)
    {
        Console.WriteLine();
        Console.WriteLine($"=== {queryId} alloc-mode probe (events={totalEvents:N0}, batch={batchSize:N0}, iters={iterations}) ===");

        // Process fingerprint: string.GetHashCode is randomized per process (Marvin
        // with a per-process seed), so these values identify this process's seed.
        Console.WriteLine($"  string-hash seed fingerprint: \"bid\"={"bid".GetHashCode()}  \"item\"={"item".GetHashCode()}");

        var query = NexmarkQueries.All.First(q => q.Id == queryId);
        var consumed = query.Tables.ToHashSet();
        var events = Generate(totalEvents);
        Console.WriteLine($"  stream generated: {events.Count:N0} events");

        for (var iter = 0; iter < iterations; iter++)
        {
            var q = Compile(query.Sql);
            q.Circuit.ProfileOperators = true;

            var b0 = GC.GetAllocatedBytesForCurrentThread();
            Feed(q, events, consumed, batchSize);
            var total = GC.GetAllocatedBytesForCurrentThread() - b0;

            string rowType = "(none)";
            foreach (var (k, _) in q.Current) { rowType = k.GetType().Name; break; }

            long outRows = 0;
            foreach (var (_, w) in q.Current)
            {
                if (w.Value != 0) outRows++;
            }

            Console.WriteLine();
            Console.WriteLine($"  --- iter {iter}: {total / (double)events.Count,8:F0} B/ev   out={outRows:N0}  sinkRowType={rowType} ---");
            foreach (var p in q.Circuit.CollectOperatorProfile().OrderByDescending(p => p.AllocBytes))
            {
                if (p.AllocBytes <= 0) continue;
                Console.WriteLine(
                    $"      op{p.Index,-3} {p.Name,-34} {p.AllocBytes / (double)events.Count,8:F1} B/ev" +
                    $"  retained={p.RetainedRows,-9} lastOut={p.LastOutputRows}");
            }
        }
    }

    private static void Feed(CompiledQuery q, List<Event> events, HashSet<NexmarkTable> consumed, int batchSize)
    {
        var buffers = consumed.ToDictionary(t => t, _ => new List<(object?[], long)>(batchSize));
        var since = 0;
        foreach (var e in events)
        {
            if (consumed.Contains(e.Table)) buffers[e.Table].Add((e.Row, 1L));
            if (++since >= batchSize) { Flush(q, buffers); q.Step(); since = 0; }
        }

        if (since > 0) { Flush(q, buffers); q.Step(); }
    }

    private static void Flush(CompiledQuery q, Dictionary<NexmarkTable, List<(object?[], long)>> buffers)
    {
        foreach (var (table, list) in buffers)
        {
            if (list.Count == 0) continue;
            q.Table(table switch
            {
                NexmarkTable.Person => "person",
                NexmarkTable.Auction => "auction",
                _ => "bid",
            }).Push(list);
            list.Clear();
        }
    }

    private static CompiledQuery Compile(string sql, bool monomorphizeTopK = true)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in NexmarkQueries.Ddl) resolver.Resolve(Parser.ParseStatement(s));
        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(sql))).Query;
        return PlanToCircuit.Compile(
            PlanOptimizer.Optimize(plan), null,
            new CompileOptions { MonomorphizeTopKOrderKey = monomorphizeTopK });
    }
}
