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
/// Incremental≡batch oracle for event-time tumbling windows (<c>GROUP BY
/// TUMBLE</c>). Two complementary dimensions:
/// <list type="number">
/// <item><b>Monotone + GC</b> (flat &amp; spine): a near-monotone TIMESTAMP stream
/// with a random LATENESS bound. The window-start group key is GC'd through the
/// <c>tumble_start</c> bucket-floor frontier transform; the accumulated incremental
/// output must equal the batch answer over the admitted rows. Any divergence is an
/// unsound window-GC (a window collected before its end) or a late-drop mismatch.</item>
/// <item><b>±1 retractions</b> (no LATENESS): an arbitrary signed-weight stream,
/// so the window assignment + incremental aggregate must agree with the batch
/// recomputation under deletes — with no GC in play.</item>
/// </list>
/// CsCheck shrinks to a minimal reproducer; set <c>CsCheck_Seed</c> to replay.
/// </summary>
public class TumbleWindowPbtTests
{
    private const long Sec = 1_000_000L;

    // Window size is a fixed 5s; tumble_start floors ts to a 5s bucket.
    private static readonly Gen<string> GenShape = Gen.OneOfConst(
        "SELECT TUMBLE_START(ts, INTERVAL '5' SECOND) AS ws, COUNT(*) AS c FROM a GROUP BY TUMBLE(ts, INTERVAL '5' SECOND)",
        "SELECT TUMBLE_START(ts, INTERVAL '5' SECOND) AS ws, SUM(v) AS s FROM a GROUP BY TUMBLE(ts, INTERVAL '5' SECOND)",
        "SELECT TUMBLE_START(ts, INTERVAL '5' SECOND) AS ws, MAX(v) AS m FROM a GROUP BY TUMBLE(ts, INTERVAL '5' SECOND)",
        "SELECT TUMBLE_START(ts, INTERVAL '5' SECOND) AS ws, COUNT(*) AS c FROM a WHERE v > 0 GROUP BY TUMBLE(ts, INTERVAL '5' SECOND)",
        // Project both window bounds (TUMBLE_END = window start + size) and group by a
        // second key — the q12 shape (per-key per-window counts).
        "SELECT v, TUMBLE_START(ts, INTERVAL '5' SECOND) AS ws, TUMBLE_END(ts, INTERVAL '5' SECOND) AS we, COUNT(*) AS c FROM a GROUP BY v, TUMBLE(ts, INTERVAL '5' SECOND)");

    // ts at whole-second granularity in [0, 30]s; v a small INT (some ≤ 0 for WHERE).
    private static readonly Gen<InputEvent> GenEvent =
        Gen.Select(Gen.Int[0, 30], Gen.Int[-2, 5])
            .Select(p => new InputEvent("a", [new Timestamp((long)p.Item1 * Sec), p.Item2], 1L));

    private static readonly Gen<InputEvent> GenSignedEvent =
        Gen.Select(Gen.Int[0, 30], Gen.Int[-2, 5], Gen.OneOfConst(1L, -1L))
            .Select(p => new InputEvent("a", [new Timestamp((long)p.Item1 * Sec), p.Item2], p.Item3));

    private static Gen<IReadOnlyList<IReadOnlyList<InputEvent>>> Ticks(Gen<InputEvent> ev) =>
        ev.Array[0, 5]
            .Select(arr => (IReadOnlyList<InputEvent>)arr)
            .Array[1, 8]
            .Select(arr => (IReadOnlyList<IReadOnlyList<InputEvent>>)arr);

    [Fact]
    public void TumbleIncrementalEqualsBatchOverAdmitted_Flat()
    {
        Gen.Select(Gen.Int[0, 12], GenShape, Ticks(GenEvent))
            .Sample((sec, sql, ticks) => CheckMonotone(sec, sql, ticks, compileOptions: null), iter: 2000);
    }

    [Fact]
    public void TumbleIncrementalEqualsBatchOverAdmitted_Spine()
    {
        Gen.Select(Gen.Int[0, 12], GenShape, Ticks(GenEvent))
            .Sample(
                (sec, sql, ticks) => CheckMonotone(
                    sec, sql, ticks, new CompileOptions { TraceFamily = TraceFamily.Spine }),
                iter: 2000);
    }

    [Fact]
    public void TumbleIncrementalEqualsBatch_UnderRetractions_NoLateness()
    {
        // No LATENESS: every (possibly negative-weight) event is admitted, so the
        // incremental windowed aggregate must equal the batch over the net Z-set.
        Gen.Select(GenShape, Ticks(GenSignedEvent))
            .Sample(
                (sql, ticks) =>
                {
                    var compiled = IncrementalOracle.CompileQuery(
                        ["CREATE TABLE a (ts TIMESTAMP NOT NULL, v INT NOT NULL)"], sql);
                    var accumulated = IncrementalOracle.RunAndAccumulate(compiled, ticks);

                    var plan = ResolvePlan(["CREATE TABLE a (ts TIMESTAMP NOT NULL, v INT NOT NULL)"], sql);
                    var ctx = new BatchEvalContext(
                        new Dictionary<string, ZSet<StructuralRow, Z64>>(StringComparer.Ordinal)
                        {
                            ["a"] = IncrementalOracle.NetTable(ticks.SelectMany(t => t), "a"),
                        },
                        new Dictionary<CteRef, ZSet<StructuralRow, Z64>>());
                    var batch = BatchPlanEvaluator.Evaluate(plan, ctx);

                    return accumulated.Equals(batch);
                },
                iter: 2000);
    }

    private static bool CheckMonotone(
        int latenessSeconds,
        string sql,
        IReadOnlyList<IReadOnlyList<InputEvent>> ticks,
        CompileOptions? compileOptions)
    {
        var latenessMicros = latenessSeconds * Sec;
        string[] ddl = [$"CREATE TABLE a (ts TIMESTAMP NOT NULL LATENESS {latenessMicros}, v INT NOT NULL)"];

        var plan = ResolvePlan(ddl, sql);
        var compiled = PlanToCircuit.Compile(plan, snapshotCodecs: null, compileOptions);

        var accumulated = IncrementalOracle.RunAndAccumulate(compiled, ticks);

        var admitted = SimulateAdmit(ticks, latenessMicros);
        var ctx = new BatchEvalContext(
            new Dictionary<string, ZSet<StructuralRow, Z64>>(StringComparer.Ordinal)
            {
                ["a"] = IncrementalOracle.NetTable(admitted, "a"),
            },
            new Dictionary<CteRef, ZSet<StructuralRow, Z64>>());
        var batch = BatchPlanEvaluator.Evaluate(plan, ctx);

        if (!accumulated.Equals(batch))
        {
            var family = compileOptions?.TraceFamily ?? TraceFamily.Flat;
            Console.Error.WriteLine($"SQL: {sql}  LATENESS {latenessMicros}  trace={family}");
            Console.Error.WriteLine("accumulated (circuit): " + accumulated);
            Console.Error.WriteLine("batch       (oracle):  " + batch);
            return false;
        }

        return true;
    }

    private static LogicalPlan ResolvePlan(string[] ddl, string sql)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in ddl)
        {
            resolver.Resolve(Parser.ParseStatement(s));
        }

        return ((SelectPlan)resolver.Resolve(Parser.ParseStatement(sql))).Query;
    }

    /// <summary>Start-of-tick late-drop on the TIMESTAMP key (column 0), mirroring
    /// the input-side <c>LatenessOperator</c>: drop a row whose ts is strictly below
    /// the prior ticks' <c>maxSeen − lateness</c>; <c>maxSeen</c> advances on the
    /// tick's admitted rows.</summary>
    private static List<InputEvent> SimulateAdmit(
        IReadOnlyList<IReadOnlyList<InputEvent>> ticks, long latenessMicros)
    {
        var admitted = new List<InputEvent>();
        long? maxSeen = null;
        foreach (var tick in ticks)
        {
            long? tickMax = null;
            foreach (var ev in tick)
            {
                var ts = ((Timestamp)ev.Row[0]!).Microseconds;
                if (maxSeen is { } m && ts < m - latenessMicros)
                {
                    continue; // late → dropped at the input
                }

                admitted.Add(ev);
                tickMax = tickMax is { } tm ? Math.Max(tm, ts) : ts;
            }

            if (tickMax is { } committed)
            {
                maxSeen = maxSeen is { } cur ? Math.Max(cur, committed) : committed;
            }
        }

        return admitted;
    }
}
