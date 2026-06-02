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
    private const long Hour = 3_600_000_000L;
    private const long Day = 86_400_000_000L;

    private static readonly Gen<string> GenShape = Gen.OneOfConst(
        "SELECT ts, v FROM a WHERE ts <= NOW()",
        "SELECT ts, v FROM a WHERE ts > NOW() - INTERVAL '3' SECOND",
        "SELECT ts, v FROM a WHERE ts <= NOW() AND ts > NOW() - INTERVAL '3' SECOND",
        "SELECT ts, v FROM a WHERE ts BETWEEN NOW() - INTERVAL '3' SECOND AND NOW()",
        "SELECT ts, v FROM a WHERE ts <= NOW() AND v > 0",
        "SELECT ts, COUNT(*) AS c FROM a WHERE ts > NOW() - INTERVAL '4' SECOND GROUP BY ts");

    // One tick: insert-only events (ts in [0,20]s, v in [-2,5]) plus a
    // non-negative clock advance (seconds) applied before the tick steps.
    private static readonly Gen<(InputEvent[] Events, long ClockDelta)> GenTick =
        Gen.Select(
            Gen.Select(Gen.Int[0, 20], Gen.Int[-2, 5])
                .Select(p => new InputEvent("a", [new Timestamp(p.Item1 * Sec), p.Item2], 1L))
                .Array[0, 4],
            Gen.Int[0, 6].Select(s => s * Sec));

    private static readonly Gen<(InputEvent[] Events, long ClockDelta)[]> GenRun = GenTick.Array[1, 10];

    [Fact]
    public void TemporalFilterIncrementalEqualsBatchAtFinalClock()
    {
        Gen.Select(GenShape, GenRun).Sample(
            (sql, run) => CheckOne(sql, "CREATE TABLE a (ts TIMESTAMP NOT NULL, v INT NOT NULL)", "a", run),
            iter: 2000);
    }

    // CURRENT_DATE shapes over a DATE key. The clock advances in *hours*, so a
    // tick can stay inside one calendar day or cross several day boundaries —
    // exercising the day-truncation of the clock that makes CURRENT_DATE linear
    // in floor(now/day), not in now.
    private static readonly Gen<string> GenDateShape = Gen.OneOfConst(
        "SELECT d, v FROM a WHERE d <= CURRENT_DATE",
        "SELECT d, v FROM a WHERE d > CURRENT_DATE - INTERVAL '3' DAY",
        "SELECT d, v FROM a WHERE d <= CURRENT_DATE AND d > CURRENT_DATE - INTERVAL '3' DAY",
        "SELECT d, v FROM a WHERE d BETWEEN CURRENT_DATE - INTERVAL '3' DAY AND CURRENT_DATE",
        "SELECT d, v FROM a WHERE d <= CURRENT_DATE AND v > 0",
        "SELECT d, COUNT(*) AS c FROM a WHERE d > CURRENT_DATE - INTERVAL '4' DAY GROUP BY d");

    private static readonly Gen<(InputEvent[] Events, long ClockDelta)> GenDateTick =
        Gen.Select(
            Gen.Select(Gen.Int[0, 20], Gen.Int[-2, 5])
                .Select(p => new InputEvent("a", [new Date32(p.Item1), p.Item2], 1L))
                .Array[0, 4],
            Gen.Int[0, 50].Select(h => h * Hour));

    private static readonly Gen<(InputEvent[] Events, long ClockDelta)[]> GenDateRun = GenDateTick.Array[1, 10];

    [Fact]
    public void CurrentDateTemporalFilterIncrementalEqualsBatchAtFinalClock()
    {
        // Start the clock part-way into a day (Hour/2) so the day boundaries the
        // run crosses don't all coincide with tick boundaries.
        Gen.Select(GenDateShape, GenDateRun).Sample(
            (sql, run) => CheckOne(sql, "CREATE TABLE a (d DATE NOT NULL, v INT NOT NULL)", "a", run, start: Hour / 2),
            iter: 2000);
    }

    // CURRENT_DATE over CAST(ts AS DATE) of a TIMESTAMP column — the natural
    // "filter timestamped events by calendar date" pattern. The GROUP BY shapes
    // exercise the downstream-GC frontier: if the source-column frontier were
    // unsound (dropped a still-live row), the accumulated incremental output
    // would diverge from the batch oracle and fail here.
    private static readonly Gen<string> GenCastDateShape = Gen.OneOfConst(
        "SELECT CAST(ts AS DATE) AS d, v FROM a WHERE CAST(ts AS DATE) <= CURRENT_DATE",
        "SELECT CAST(ts AS DATE) AS d, v FROM a WHERE CAST(ts AS DATE) > CURRENT_DATE - INTERVAL '3' DAY",
        // Derived table + GROUP BY on the projected date alias (GROUP BY takes only
        // bare columns), exercising the projected-CAST monotone GC frontier.
        "SELECT d, COUNT(*) AS c FROM "
            + "(SELECT CAST(ts AS DATE) AS d FROM a WHERE CAST(ts AS DATE) > CURRENT_DATE - INTERVAL '4' DAY) s "
            + "GROUP BY d",
        // GROUP BY the raw ts column, exercising the source-column (midnight-µs) frontier.
        "SELECT ts, COUNT(*) AS c FROM a "
            + "WHERE CAST(ts AS DATE) > CURRENT_DATE - INTERVAL '4' DAY GROUP BY ts");

    private static readonly Gen<(InputEvent[] Events, long ClockDelta)> GenCastDateTick =
        Gen.Select(
            Gen.Select(Gen.Int[0, 20], Gen.Int[0, 23], Gen.Int[-2, 5])
                .Select(t => new InputEvent("a", [new Timestamp(t.Item1 * Day + t.Item2 * Hour), t.Item3], 1L))
                .Array[0, 4],
            Gen.Int[0, 50].Select(h => h * Hour));

    private static readonly Gen<(InputEvent[] Events, long ClockDelta)[]> GenCastDateRun = GenCastDateTick.Array[1, 10];

    [Fact]
    public void CastTimestampToDateTemporalFilterIncrementalEqualsBatchAtFinalClock()
    {
        Gen.Select(GenCastDateShape, GenCastDateRun).Sample(
            (sql, run) => CheckOne(sql, "CREATE TABLE a (ts TIMESTAMP NOT NULL, v INT NOT NULL)", "a", run, start: Hour / 2),
            iter: 2000);
    }

    private static bool CheckOne(
        string sql, string ddl, string table, (InputEvent[] Events, long ClockDelta)[] run, long start = 0)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        resolver.Resolve(Parser.ParseStatement(ddl));

        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(sql))).Query;
        var compiled = PlanToCircuit.Compile(plan);

        var accumulated = ZSet<StructuralRow, Z64>.Empty;
        var allEvents = new List<InputEvent>();
        long clock = start;
        foreach (var (events, delta) in run)
        {
            clock += delta;
            compiled.AdvanceClock(clock);
            foreach (var ev in events)
            {
                compiled.Table(table).Insert(ev.Row);
                allEvents.Add(ev);
            }

            compiled.Step();
            accumulated += compiled.Current;
        }

        var finalClock = clock;
        var tableStates = new Dictionary<string, ZSet<StructuralRow, Z64>>(StringComparer.Ordinal)
        {
            [table] = IncrementalOracle.NetTable(allEvents, table),
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
