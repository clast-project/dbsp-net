// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using CsCheck;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

namespace DbspNet.Tests.EndToEnd;

/// <summary>
/// LATENESS phase 4e — the PBT oracle dimension. For a random near-monotone
/// stream and a random lateness bound, the incremental query (with input
/// late-drop + frontier-driven trace GC) must agree with the batch evaluation
/// over the <em>admitted</em> rows — i.e. the input with late rows removed.
/// <list type="number">
/// <item>compile the LATENESS query, push each tick, accumulate output deltas
/// (the "incremental-with-GC" answer);</item>
/// <item>independently replay the same ticks through a frontier simulation that
/// drops late rows by the identical start-of-tick rule, and run
/// <see cref="BatchPlanEvaluator"/> over the net admitted table states (the
/// "batch-over-non-late-input" answer);</item>
/// <item>assert the two Z-sets are equal.</item>
/// </list>
/// This is sound for an arbitrary stream because the late-drop transforms the
/// input and the trace GC is — by construction (phases 1–4c) — observationally
/// invisible: it reclaims state strictly below the frontier, which no future
/// admitted row can ever touch. So any divergence is a GC-that-changed-output
/// bug or a late-drop/frontier mismatch. CsCheck shrinks to a minimal
/// reproducer; set <c>CsCheck_Seed</c> to replay.
/// </summary>
public class LatenessPbtTests
{
    private static readonly Gen<int> GenLateness = Gen.Int[0, 12];

    // All shapes are monotone on the LATENESS column (ts = column 0): GROUP BY
    // ts, equi-join on ts, UNION on ts — exactly the operators that GC (phases
    // 3, 4a, 4b, 4c), plus a WHERE to exercise frontier propagation.
    private static readonly Gen<string> GenShape = Gen.OneOfConst(
        "SELECT ts, COUNT(*) AS c FROM a GROUP BY ts",
        "SELECT ts, SUM(v) AS s FROM a GROUP BY ts",
        "SELECT ts, COUNT(*) AS c FROM a WHERE v > 0 GROUP BY ts",
        "SELECT a.v, b.v FROM a JOIN b ON a.ts = b.ts",
        "SELECT a.v, b.v FROM a LEFT JOIN b ON a.ts = b.ts",
        "SELECT a.v, b.v FROM a RIGHT JOIN b ON a.ts = b.ts",
        "SELECT ts FROM a UNION SELECT ts FROM b");

    // ts is the BIGINT monotone column (range chosen so the moving frontier both
    // admits and drops across a run); v is a small INT with some ≤ 0 for WHERE.
    private static readonly Gen<InputEvent> GenEventA =
        Gen.Select(Gen.Int[0, 30], Gen.Int[-2, 5])
            .Select(p => new InputEvent("a", [(long)p.Item1, p.Item2], 1L));

    private static readonly Gen<InputEvent> GenEventB =
        Gen.Select(Gen.Int[0, 30], Gen.Int[-2, 5])
            .Select(p => new InputEvent("b", [(long)p.Item1, p.Item2], 1L));

    private static readonly Gen<InputEvent> GenEvent = Gen.OneOf(GenEventA, GenEventB);

    private static readonly Gen<IReadOnlyList<IReadOnlyList<InputEvent>>> GenTicks =
        GenEvent.Array[0, 5]
            .Select(arr => (IReadOnlyList<InputEvent>)arr)
            .Array[1, 8]
            .Select(arr => (IReadOnlyList<IReadOnlyList<InputEvent>>)arr);

    [Fact]
    public void LatenessIncrementalEqualsBatchOverAdmitted_Flat()
    {
        Gen.Select(GenLateness, GenShape, GenTicks)
            .Sample((d, sql, ticks) => CheckOne(d, sql, ticks, compileOptions: null), iter: 2000);
    }

    [Fact]
    public void LatenessIncrementalEqualsBatchOverAdmitted_Spine()
    {
        Gen.Select(GenLateness, GenShape, GenTicks)
            .Sample(
                (d, sql, ticks) => CheckOne(
                    d, sql, ticks, new CompileOptions { TraceFamily = TraceFamily.Spine }),
                iter: 2000);
    }

    private static bool CheckOne(
        int lateness,
        string sql,
        IReadOnlyList<IReadOnlyList<InputEvent>> ticks,
        CompileOptions? compileOptions)
    {
        string[] ddl =
        [
            $"CREATE TABLE a (ts BIGINT NOT NULL LATENESS {lateness}, v INT NOT NULL)",
            $"CREATE TABLE b (ts BIGINT NOT NULL LATENESS {lateness}, v INT NOT NULL)",
        ];

        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in ddl)
        {
            resolver.Resolve(Parser.ParseStatement(s));
        }

        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(sql))).Query;
        var compiled = PlanToCircuit.Compile(plan, snapshotCodecs: null, compileOptions);

        // Push only events for referenced tables (single-table shapes ignore b).
        var filtered = ticks
            .Select(tick => (IReadOnlyList<InputEvent>)tick
                .Where(e => compiled.Inputs.ContainsKey(e.Table))
                .ToList())
            .ToList();

        var accumulated = IncrementalOracle.RunAndAccumulate(compiled, filtered);

        // Oracle: admit rows by the same start-of-tick frontier rule the
        // LatenessOperator uses, then batch-evaluate over the net admitted state.
        // BatchPlanEvaluator ignores ColumnLateness, so it computes the plain
        // relational answer over whatever rows it is given.
        var admitted = SimulateAdmit(filtered, lateness);
        var tableStates = new Dictionary<string, ZSet<StructuralRow, Z64>>(StringComparer.Ordinal)
        {
            ["a"] = IncrementalOracle.NetTable(admitted, "a"),
            ["b"] = IncrementalOracle.NetTable(admitted, "b"),
        };
        var ctx = new BatchEvalContext(tableStates, new Dictionary<CteRef, ZSet<StructuralRow, Z64>>());
        var batch = BatchPlanEvaluator.Evaluate(plan, ctx);

        if (!accumulated.Equals(batch))
        {
            var family = compileOptions?.TraceFamily ?? TraceFamily.Flat;
            Console.Error.WriteLine($"SQL: {sql}  LATENESS {lateness}  trace={family}");
            Console.Error.WriteLine("ticks:     " + DescribeTicks(filtered));
            Console.Error.WriteLine("admitted:  [" + string.Join(", ", admitted.Select(e => e.ToString())) + "]");
            Console.Error.WriteLine("accumulated (circuit): " + accumulated);
            Console.Error.WriteLine("batch       (oracle):  " + batch);
            return false;
        }

        return true;
    }

    /// <summary>
    /// Replays the ticks through the same late-drop the input-side
    /// <c>LatenessOperator</c> applies: a row is dropped iff its monotone value
    /// (ts = column 0) is strictly below the frontier as it stood at the
    /// <em>start</em> of the tick (the prior ticks' <c>maxSeen − lateness</c>);
    /// <c>maxSeen</c> then advances on the tick's admitted rows. Per-table, since
    /// each scanned table has its own frontier source.
    /// </summary>
    private static List<InputEvent> SimulateAdmit(
        IReadOnlyList<IReadOnlyList<InputEvent>> ticks, long lateness)
    {
        var admitted = new List<InputEvent>();
        var maxByTable = new Dictionary<string, long>(StringComparer.Ordinal); // committed from prior ticks
        foreach (var tick in ticks)
        {
            var tickMax = new Dictionary<string, long>(StringComparer.Ordinal);
            foreach (var ev in tick)
            {
                var ts = (long)ev.Row[0]!; // ts is always a boxed long (BIGINT column)
                if (maxByTable.TryGetValue(ev.Table, out var preMax) && ts < preMax - lateness)
                {
                    continue; // late → dropped at the input
                }

                admitted.Add(ev);
                if (!tickMax.TryGetValue(ev.Table, out var m) || ts > m)
                {
                    tickMax[ev.Table] = ts;
                }
            }

            // Commit this tick's admitted max — it advances the frontier the
            // NEXT tick drops against.
            foreach (var (table, m) in tickMax)
            {
                maxByTable[table] = maxByTable.TryGetValue(table, out var cur) ? Math.Max(cur, m) : m;
            }
        }

        return admitted;
    }

    private static string DescribeTicks(IReadOnlyList<IReadOnlyList<InputEvent>> ticks)
    {
        var parts = ticks.Select(t =>
            t.Count == 0 ? "[]" : "[" + string.Join(", ", t.Select(e => e.ToString())) + "]");
        return "[" + string.Join(", ", parts) + "]";
    }
}
