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
        // GROUP BY the date expression directly — exercises the expression-key
        // GROUP BY path picking up the monotone day-floor GC frontier.
        "SELECT CAST(ts AS DATE) AS d, COUNT(*) AS c FROM a "
            + "WHERE CAST(ts AS DATE) > CURRENT_DATE - INTERVAL '4' DAY GROUP BY CAST(ts AS DATE)",
        // Derived table + GROUP BY on the projected date alias, exercising the
        // projected-CAST monotone GC frontier through a ProjectPlan.
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

    // Parallel arms (docs/design-parallel-frontier.md §2 Q5): the single-query parallel compile shards a
    // temporal filter, so it must equal the SERIAL compile tick by tick at W = 2, 3, 4 — including
    // the GROUP BY shapes, whose GC frontier is the shared clock.
    [Fact]
    public void TemporalFilterParallelEqualsSerial()
    {
        Gen.Select(GenShape, GenRun).Sample(
            (sql, run) => CheckParallel(sql, "CREATE TABLE a (ts TIMESTAMP NOT NULL, v INT NOT NULL)", "a", run),
            iter: 300);
    }

    [Fact]
    public void CastTimestampToDateTemporalFilterParallelEqualsSerial()
    {
        Gen.Select(GenCastDateShape, GenCastDateRun).Sample(
            (sql, run) => CheckParallel(sql, "CREATE TABLE a (ts TIMESTAMP NOT NULL, v INT NOT NULL)", "a", run, start: Hour / 2),
            iter: 300);
    }

    // Program arm (docs/design-program-temporal.md): the same windows split across views. Each shape
    // is the program's views (the last one is checked), and the single query it must equal with the
    // views written inline. Intermediate views are randomly outputs (own stream, seeded verdicts) or
    // single-use (inlined into the consumer). The two-window union is the frontier-identity case;
    // the filter over an aggregate view is the elided-exchange case on the parallel path.
    private static readonly (string[] Views, string Single)[] ProgramShapes =
    [
        (["SELECT ts, v FROM a WHERE ts > NOW() - INTERVAL '3' SECOND",
          "SELECT ts, COUNT(*) AS c FROM v0 GROUP BY ts"],
         "SELECT ts, COUNT(*) AS c FROM (SELECT ts, v FROM a WHERE ts > NOW() - INTERVAL '3' SECOND) v0 GROUP BY ts"),
        (["SELECT ts, v FROM a WHERE ts > NOW() - INTERVAL '2' SECOND",
          "SELECT ts, v FROM a WHERE ts > NOW() - INTERVAL '5' SECOND",
          "SELECT ts, COUNT(*) AS c FROM (SELECT ts FROM v0 UNION ALL SELECT ts FROM v1) u GROUP BY ts"],
         "SELECT ts, COUNT(*) AS c FROM (SELECT ts FROM a WHERE ts > NOW() - INTERVAL '2' SECOND "
            + "UNION ALL SELECT ts FROM a WHERE ts > NOW() - INTERVAL '5' SECOND) u GROUP BY ts"),
        (["SELECT ts, SUM(v) AS s FROM a GROUP BY ts",
          "SELECT ts, s FROM v0 WHERE ts > NOW() - INTERVAL '3' SECOND",
          "SELECT ts, MAX(s) AS m FROM v1 GROUP BY ts"],
         "SELECT ts, MAX(s) AS m FROM (SELECT ts, s FROM (SELECT ts, SUM(v) AS s FROM a GROUP BY ts) v0 "
            + "WHERE ts > NOW() - INTERVAL '3' SECOND) v1 GROUP BY ts"),
        (["SELECT ts, v FROM a WHERE ts BETWEEN NOW() - INTERVAL '4' SECOND AND NOW()",
          "SELECT DISTINCT ts FROM v0"],
         "SELECT DISTINCT ts FROM (SELECT ts, v FROM a WHERE ts BETWEEN NOW() - INTERVAL '4' SECOND AND NOW()) v0"),
        (["SELECT ts, v FROM a WHERE ts > NOW() - INTERVAL '3' SECOND AND v > 0",
          "SELECT x.ts, x.v, y.v AS w FROM v0 x JOIN v0 y ON x.ts = y.ts"],
         "SELECT x.ts, x.v, y.v AS w FROM (SELECT ts, v FROM a WHERE ts > NOW() - INTERVAL '3' SECOND AND v > 0) x "
            + "JOIN (SELECT ts, v FROM a WHERE ts > NOW() - INTERVAL '3' SECOND AND v > 0) y ON x.ts = y.ts"),
        (["SELECT ts, v FROM a WHERE ts <= NOW()",
          "SELECT v, COUNT(*) AS c FROM v0 GROUP BY v"],
         "SELECT v, COUNT(*) AS c FROM (SELECT ts, v FROM a WHERE ts <= NOW()) v0 GROUP BY v"),
        // One view: the filter sits on the inner aggregate's output and passes its partition through,
        // so at W>1 the outer aggregate elides its exchange.
        (["SELECT ts, MAX(s) AS m, COUNT(*) AS c FROM (SELECT ts, s FROM (SELECT ts, SUM(v) AS s FROM a GROUP BY ts) x "
            + "WHERE ts > NOW() - INTERVAL '3' SECOND) y GROUP BY ts"],
         "SELECT ts, MAX(s) AS m, COUNT(*) AS c FROM (SELECT ts, s FROM (SELECT ts, SUM(v) AS s FROM a GROUP BY ts) x "
            + "WHERE ts > NOW() - INTERVAL '3' SECOND) y GROUP BY ts"),
    ];

    private const string ProgramDdl = "CREATE TABLE a (ts TIMESTAMP NOT NULL, v INT NOT NULL)";

    private static (string[] Sql, HashSet<string> Outputs, string Last) ProgramOf(string[] views, bool intermediatesAreOutputs)
    {
        var sql = new List<string> { ProgramDdl };
        var outputs = new HashSet<string>(StringComparer.Ordinal);
        for (var i = 0; i < views.Length; i++)
        {
            sql.Add($"CREATE VIEW v{i} AS {views[i]}");
            if (i == views.Length - 1 || intermediatesAreOutputs)
            {
                outputs.Add($"v{i}");
            }
        }

        return ([.. sql], outputs, $"v{views.Length - 1}");
    }

    [Fact]
    public void ProgramTemporalFilterEqualsSingleQueryAndBatch()
    {
        Gen.Select(Gen.Int[0, ProgramShapes.Length - 1], Gen.Bool, GenRun).Sample(
            (shape, outs, run) => CheckProgram(ProgramShapes[shape], outs, run),
            iter: 1000);
    }

    // Parallel program arm: every shape at W = 2, 3, 4, flat and spine, equals the SERIAL program tick
    // by tick (memory parallel-view-in-circuit: parallel-vs-batch can pass while wrong). Deletes are
    // mixed in, so a retraction must meet its insert on one worker.
    [Fact]
    public void ProgramTemporalFilterParallelEqualsSerial()
    {
        Gen.Select(Gen.Int[0, ProgramShapes.Length - 1], Gen.Bool, Gen.Bool, GenRun).Sample(
            (shape, outs, spine, run) => CheckProgramParallel(ProgramShapes[shape], outs, spine, run),
            iter: 150);
    }

    private static bool CheckProgramParallel(
        (string[] Views, string Single) shape, bool intermediatesAreOutputs, bool spine, (InputEvent[] Events, long ClockDelta)[] run)
    {
        var (sql, outputs, _) = ProgramOf(shape.Views, intermediatesAreOutputs);
        var options = new CompileOptions { TraceFamily = spine ? TraceFamily.Spine : TraceFamily.Flat };
        var serial = SqlProgram.Compile(sql, outputs, options: options);
        var parallels = new List<ParallelCompiledProgram>();
        try
        {
            foreach (var w in new[] { 2, 3, 4 })
            {
                Assert.True(SqlProgram.TryCompileParallel(sql, outputs, w, out var p, options: options), $"parallel compile refused: {string.Join("; ", sql)}");
                parallels.Add(p!);
            }

            // Each tick also deletes the first event of the tick two back, if there was one.
            long clock = 0;
            for (var t = 0; t < run.Length; t++)
            {
                clock += run[t].ClockDelta;
                serial.AdvanceClock(clock);
                parallels.ForEach(p => p.AdvanceClock(clock));
                foreach (var ev in run[t].Events)
                {
                    serial.Table("a").Insert(ev.Row);
                    parallels.ForEach(p => p.Table("a").Insert(ev.Row));
                }

                if (t >= 2 && run[t - 2].Events.Length > 0)
                {
                    var row = run[t - 2].Events[0].Row;
                    serial.Table("a").Delete(row);
                    parallels.ForEach(p => p.Table("a").Delete(row));
                }

                serial.Step();
                foreach (var p in parallels)
                {
                    p.Step();
                    foreach (var view in outputs)
                    {
                        if (!p.Outputs[view].CurrentView.Equals(serial.Outputs[view].CurrentView))
                        {
                            Console.Error.WriteLine($"program: {string.Join("; ", sql)}  view {view}  W={p.Workers}  spine={spine}  tick {t}  clock={clock}");
                            Console.Error.WriteLine("serial:   " + serial.Outputs[view].CurrentView);
                            Console.Error.WriteLine("parallel: " + p.Outputs[view].CurrentView);
                            return false;
                        }
                    }
                }
            }

            return true;
        }
        finally
        {
            parallels.ForEach(p => p.Dispose());
        }
    }

    private static bool CheckProgram((string[] Views, string Single) shape, bool intermediatesAreOutputs, (InputEvent[] Events, long ClockDelta)[] run)
    {
        var (sql, outputs, last) = ProgramOf(shape.Views, intermediatesAreOutputs);
        var program = SqlProgram.Compile(sql, outputs);

        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        resolver.Resolve(Parser.ParseStatement(ProgramDdl));
        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(shape.Single))).Query;
        var single = PlanToCircuit.Compile(plan);

        var accumulated = ZSet<StructuralRow, Z64>.Empty;
        var allEvents = new List<InputEvent>();
        long clock = 0;
        for (var t = 0; t < run.Length; t++)
        {
            clock += run[t].ClockDelta;
            program.AdvanceClock(clock);
            single.AdvanceClock(clock);
            foreach (var ev in run[t].Events)
            {
                program.Table("a").Insert(ev.Row);
                single.Table("a").Insert(ev.Row);
                allEvents.Add(ev);
            }

            program.Step();
            single.Step();
            accumulated += single.Current;
            if (!program.Outputs[last].CurrentView.Equals(accumulated))
            {
                Console.Error.WriteLine($"program: {string.Join("; ", sql)}  outputs={string.Join(",", outputs)}  tick {t}  clock={clock}");
                Console.Error.WriteLine("single:  " + accumulated);
                Console.Error.WriteLine("program: " + program.Outputs[last].CurrentView);
                return false;
            }
        }

        var ctx = new BatchEvalContext(
            new Dictionary<string, ZSet<StructuralRow, Z64>>(StringComparer.Ordinal) { ["a"] = IncrementalOracle.NetTable(allEvents, "a") },
            new Dictionary<CteRef, ZSet<StructuralRow, Z64>>(),
            now: clock);
        var batch = BatchPlanEvaluator.Evaluate(plan, ctx);
        if (!accumulated.Equals(batch))
        {
            Console.Error.WriteLine($"single vs batch: {shape.Single}  finalClock={clock}");
            Console.Error.WriteLine("single: " + accumulated);
            Console.Error.WriteLine("batch:  " + batch);
            return false;
        }

        return true;
    }

    private static bool CheckParallel(
        string sql, string ddl, string table, (InputEvent[] Events, long ClockDelta)[] run, long start = 0)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        resolver.Resolve(Parser.ParseStatement(ddl));
        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(sql))).Query;
        var serial = PlanToCircuit.Compile(plan);
        var parallels = new List<ParallelStructuralCompiledQuery>();
        try
        {
            foreach (var w in new[] { 2, 3, 4 })
            {
                Assert.True(PlanToCircuit.TryCompileParallel(plan, w, out var p), $"parallel compile refused: {sql}");
                parallels.Add(p!);
            }

            long clock = start;
            for (var t = 0; t < run.Length; t++)
            {
                clock += run[t].ClockDelta;
                serial.AdvanceClock(clock);
                foreach (var p in parallels)
                {
                    p.AdvanceClock(clock);
                }

                foreach (var ev in run[t].Events)
                {
                    serial.Table(table).Insert(ev.Row);
                    foreach (var p in parallels)
                    {
                        p.Table(table).Insert(ev.Row);
                    }
                }

                serial.Step();
                foreach (var p in parallels)
                {
                    p.Step();
                    if (!p.Current.Equals(serial.Current))
                    {
                        Console.Error.WriteLine($"SQL: {sql}  W={p.Workers}  tick {t}  clock={clock}");
                        Console.Error.WriteLine("serial:   " + serial.Current);
                        Console.Error.WriteLine("parallel: " + p.Current);
                        return false;
                    }
                }
            }

            return true;
        }
        finally
        {
            foreach (var p in parallels)
            {
                p.Dispose();
            }
        }
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
