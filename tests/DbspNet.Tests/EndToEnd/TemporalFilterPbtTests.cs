// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using CsCheck;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Tests.EndToEnd;

/// <summary>
/// The temporal-filter correctness oracle (the redefined incremental-equals-batch
/// law for time-dependent output). For a random insert-only timestamped stream
/// and a random monotone per-tick logical clock, drive the circuit advancing the
/// clock each tick and accumulate the output deltas; then assert the result
/// equals <see cref="BatchPlanEvaluator"/> over the net input with <c>NOW</c>
/// fixed to the run's final clock.
/// <para>
/// This is sound because a temporal filter at logical time <c>t</c> is exactly
/// the relational filter with <c>NOW = t</c>, and the operator's recompute-and-
/// diff emits deltas that telescope to the valid set at the final clock — so the
/// accumulated circuit output must equal <c>validAt(finalClock)</c>, which is
/// what the batch computes. Insert-only events keep net weights non-negative, so
/// the operator's positive-weight valid set lines up with the batch's filtered
/// Z-set. CsCheck shrinks failures; set <c>CsCheck_Seed</c> to replay.
/// </para>
/// </summary>
public class TemporalFilterPbtTests
{
    private const long Sec = 1_000_000L; // microseconds per second

    private static readonly Gen<string> GenShape = Gen.OneOfConst(
        "SELECT ts, v FROM a WHERE ts <= NOW()",
        "SELECT ts, v FROM a WHERE ts > NOW() - INTERVAL '3' SECOND",
        "SELECT ts, v FROM a WHERE ts <= NOW() AND ts > NOW() - INTERVAL '3' SECOND",
        "SELECT ts, v FROM a WHERE ts BETWEEN NOW() - INTERVAL '3' SECOND AND NOW()",
        "SELECT ts, v FROM a WHERE ts <= NOW() AND v > 0",
        "SELECT ts, COUNT(*) AS c FROM a WHERE ts > NOW() - INTERVAL '4' SECOND GROUP BY ts");

    // One tick: insert-only events (ts in [0,20]s, v in [-2,5]) plus a
    // non-negative clock advance (seconds) applied before the tick steps.
    private static readonly Gen<(InputEvent[] Events, int ClockDeltaSec)> GenTick =
        Gen.Select(
            Gen.Select(Gen.Int[0, 20], Gen.Int[-2, 5])
                .Select(p => new InputEvent("a", [new Timestamp(p.Item1 * Sec), p.Item2], 1L))
                .Array[0, 4],
            Gen.Int[0, 6]);

    private static readonly Gen<(InputEvent[] Events, int ClockDeltaSec)[]> GenRun = GenTick.Array[1, 10];

    [Fact]
    public void TemporalFilterIncrementalEqualsBatchAtFinalClock()
    {
        Gen.Select(GenShape, GenRun).Sample((sql, run) => CheckOne(sql, run), iter: 2000);
    }

    private static bool CheckOne(string sql, (InputEvent[] Events, int ClockDeltaSec)[] run)
    {
        string[] ddl = ["CREATE TABLE a (ts TIMESTAMP NOT NULL, v INT NOT NULL)"];
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in ddl)
        {
            resolver.Resolve(Parser.ParseStatement(s));
        }

        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(sql))).Query;
        var compiled = PlanToCircuit.Compile(plan);

        var accumulated = ZSet<StructuralRow, Z64>.Empty;
        var allEvents = new List<InputEvent>();
        long clock = 0;
        foreach (var (events, deltaSec) in run)
        {
            clock += deltaSec * Sec;
            compiled.AdvanceClock(clock);
            foreach (var ev in events)
            {
                compiled.Table("a").Insert(ev.Row);
                allEvents.Add(ev);
            }

            compiled.Step();
            accumulated += compiled.Current;
        }

        var finalClock = clock;
        var tableStates = new Dictionary<string, ZSet<StructuralRow, Z64>>(StringComparer.Ordinal)
        {
            ["a"] = IncrementalOracle.NetTable(allEvents, "a"),
        };
        var ctx = new BatchEvalContext(
            tableStates, new Dictionary<CteRef, ZSet<StructuralRow, Z64>>(), now: finalClock);
        var batch = BatchPlanEvaluator.Evaluate(plan, ctx);

        if (!accumulated.Equals(batch))
        {
            Console.Error.WriteLine($"SQL: {sql}  finalClock={finalClock}");
            Console.Error.WriteLine("events: [" + string.Join(", ", allEvents.Select(e => e.ToString())) + "]");
            Console.Error.WriteLine("accumulated (circuit): " + accumulated);
            Console.Error.WriteLine("batch @NOW=final (oracle): " + batch);
            return false;
        }

        return true;
    }
}
