// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections.Generic;
using System.Linq;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Persistence;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Tests.Sql;

/// <summary>
/// Phase-2 end-to-end tests for the time-driven temporal filter: the operator
/// emits rows aging into the validity window and retracts them aging out as the
/// host-driven logical clock advances — including ticks with no new input — and
/// round-trips its state through a snapshot.
/// </summary>
public class TemporalFilterCompilerTests : IDisposable
{
    private const long Hour = 3_600_000_000L;
    private const long Day = 86_400_000_000L;
    private readonly string _snapshotDir =
        Path.Combine(Path.GetTempPath(), "dbspnet-tf-snap-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        if (Directory.Exists(_snapshotDir))
        {
            Directory.Delete(_snapshotDir, recursive: true);
        }

        GC.SuppressFinalize(this);
    }

    private static CompiledQuery Compile(string query)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        resolver.Resolve(Parser.ParseStatement(
            "CREATE TABLE events (id INT NOT NULL, ts TIMESTAMP NOT NULL)"));
        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(query))).Query;
        return PlanToCircuit.Compile(plan, ArrowSqlSnapshotCodecs.Instance);
    }

    private static void Accumulate(CompiledQuery q, Dictionary<(int Id, long Ts), long> acc)
    {
        foreach (var (row, w) in q.Current)
        {
            var key = ((int)row[0]!, ((Timestamp)row[1]!).Microseconds);
            acc.TryGetValue(key, out var cur);
            var next = cur + w.Value;
            if (next == 0)
            {
                acc.Remove(key);
            }
            else
            {
                acc[key] = next;
            }
        }
    }

    private static HashSet<(int, long)> Live(Dictionary<(int Id, long Ts), long> acc) =>
        acc.Where(kv => kv.Value > 0).Select(kv => kv.Key).ToHashSet();

    [Fact]
    public void ForcesStructuralCompile_WithTemporalFilterOp()
    {
        var q = Compile("SELECT id, ts FROM events WHERE ts <= NOW()");
        Assert.Single(q.Circuit.Operators.OfType<TemporalFilterOp<StructuralRow>>());
    }

    [Fact]
    public void DisappearBound_BoundsDownstreamAggregateState()
    {
        // A temporal filter with an upper bound advertises a clock-driven
        // frontier (clock − 10s) on its time-key, so a GROUP BY ts above it GCs
        // groups that have aged out — state stays bounded to the trailing window
        // rather than growing with every distinct timestamp seen.
        var q = Compile(
            "SELECT ts, COUNT(*) AS c FROM events WHERE ts > NOW() - INTERVAL '10' SECOND GROUP BY ts");

        for (long i = 0; i <= 100; i++)
        {
            q.AdvanceClock(i * 1_000_000L);          // clock tracks the newest event
            q.Table("events").Insert((int)i, new Timestamp(i * 1_000_000L));
            q.Step();
        }

        var agg = q.Circuit.Operators
            .OfType<IncrementalAggregateOp<StructuralRow, StructuralRow, StructuralRow>>()
            .Single();

        // 101 distinct group keys streamed, but the clock frontier (now − 10s)
        // keeps only the trailing ~10-second window — bounded, far below 101.
        Assert.True(agg.RetainedGroupCount <= 12, $"retained {agg.RetainedGroupCount}");
        Assert.True(agg.RetainedGroupCount >= 10, $"retained {agg.RetainedGroupCount}");
    }

    [Fact]
    public void AppearBound_RowsEnterAsClockReachesTimestamp()
    {
        // ts <= NOW(): a row becomes valid once the clock reaches its timestamp.
        var q = Compile("SELECT id, ts FROM events WHERE ts <= NOW()");
        var acc = new Dictionary<(int, long), long>();

        q.AdvanceClock(100);
        q.Table("events").Insert(1, new Timestamp(50));   // already in the past
        q.Table("events").Insert(2, new Timestamp(200));  // still in the future
        q.Step();
        Accumulate(q, acc);
        Assert.Equal(new HashSet<(int, long)> { (1, 50) }, Live(acc));

        // No new input — only the clock advances past row 2's timestamp.
        q.AdvanceClock(250);
        q.Step();
        Accumulate(q, acc);
        Assert.Equal(new HashSet<(int, long)> { (1, 50), (2, 200) }, Live(acc));
    }

    [Fact]
    public void DisappearBound_RowsRetractedAsClockPasses()
    {
        // ts > NOW() - INTERVAL '1' HOUR  <=>  valid while NOW() < ts + 1h.
        var q = Compile("SELECT id, ts FROM events WHERE ts > NOW() - INTERVAL '1' HOUR");
        var acc = new Dictionary<(int, long), long>();

        q.AdvanceClock(0);
        q.Table("events").Insert(1, new Timestamp(0));
        q.Step();
        Accumulate(q, acc);
        Assert.Equal(new HashSet<(int, long)> { (1, 0) }, Live(acc)); // valid immediately

        // Clock reaches ts + 1h with no new input: the row ages out (retraction).
        q.AdvanceClock(Hour);
        q.Step();
        Accumulate(q, acc);
        Assert.Empty(Live(acc));
    }

    [Fact]
    public void Window_RowValidOnlyInsideTheInterval()
    {
        // The classic sliding window: valid while ts <= NOW() < ts + 1h.
        var q = Compile(
            "SELECT id, ts FROM events WHERE ts <= NOW() AND ts > NOW() - INTERVAL '1' HOUR");
        var acc = new Dictionary<(int, long), long>();

        // Clock before ts: not yet appeared.
        q.AdvanceClock(500);
        q.Table("events").Insert(1, new Timestamp(1000));
        q.Step();
        Accumulate(q, acc);
        Assert.Empty(Live(acc));

        // Clock inside [ts, ts+1h): valid.
        q.AdvanceClock(1000);
        q.Step();
        Accumulate(q, acc);
        Assert.Equal(new HashSet<(int, long)> { (1, 1000) }, Live(acc));

        // Clock at ts+1h (exclusive upper bound): aged out.
        q.AdvanceClock(1000 + Hour);
        q.Step();
        Accumulate(q, acc);
        Assert.Empty(Live(acc));
    }

    // ---------- CURRENT_DATE (day-truncated clock) ----------

    private static CompiledQuery CompileDate(string query)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        resolver.Resolve(Parser.ParseStatement(
            "CREATE TABLE evd (id INT NOT NULL, d DATE NOT NULL)"));
        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(query))).Query;
        return PlanToCircuit.Compile(plan, ArrowSqlSnapshotCodecs.Instance);
    }

    private static void AccumulateDate(CompiledQuery q, Dictionary<(int Id, int Day), long> acc)
    {
        foreach (var (row, w) in q.Current)
        {
            var key = ((int)row[0]!, ((Date32)row[1]!).Days);
            acc.TryGetValue(key, out var cur);
            var next = cur + w.Value;
            if (next == 0)
            {
                acc.Remove(key);
            }
            else
            {
                acc[key] = next;
            }
        }
    }

    private static HashSet<(int, int)> LiveDate(Dictionary<(int Id, int Day), long> acc) =>
        acc.Where(kv => kv.Value > 0).Select(kv => kv.Key).ToHashSet();

    [Fact]
    public void CurrentDate_AppearBound_RowEntersAtDayBoundary()
    {
        // d <= CURRENT_DATE: the row for day 10 becomes valid exactly when the
        // logical clock crosses into day 10. Sub-day clock movement inside day 9
        // doesn't admit it; movement inside day 10 doesn't churn it — the clock
        // is truncated to its day before the comparison.
        var q = CompileDate("SELECT id, d FROM evd WHERE d <= CURRENT_DATE");
        var acc = new Dictionary<(int, int), long>();

        // Late in day 9: CURRENT_DATE = 9, row (day 10) not yet valid.
        q.AdvanceClock(9 * Day + 23 * Hour);
        q.Table("evd").Insert(1, new Date32(10));
        q.Step();
        AccumulateDate(q, acc);
        Assert.Empty(LiveDate(acc));

        // Clock crosses into day 10 (midnight) with no new input: now valid.
        q.AdvanceClock(10 * Day);
        q.Step();
        AccumulateDate(q, acc);
        Assert.Equal(new HashSet<(int, int)> { (1, 10) }, LiveDate(acc));

        // Sub-day movement inside day 10: still valid, no spurious delta.
        q.AdvanceClock(10 * Day + 5 * Hour);
        q.Step();
        Assert.True(q.Current.IsEmpty, "no delta for sub-day clock movement within the same day");
    }

    [Fact]
    public void CurrentDate_DisappearBound_RowAgesOutByWholeDays()
    {
        // d > CURRENT_DATE - INTERVAL '2' DAY  <=>  valid while CURRENT_DATE < d + 2.
        var q = CompileDate("SELECT id, d FROM evd WHERE d > CURRENT_DATE - INTERVAL '2' DAY");
        var acc = new Dictionary<(int, int), long>();

        q.AdvanceClock(10 * Day);
        q.Table("evd").Insert(1, new Date32(10)); // CURRENT_DATE 10 < 12: valid
        q.Step();
        AccumulateDate(q, acc);
        Assert.Equal(new HashSet<(int, int)> { (1, 10) }, LiveDate(acc));

        // CURRENT_DATE reaches day 11 (still < 12): stays valid.
        q.AdvanceClock(11 * Day + 12 * Hour);
        q.Step();
        AccumulateDate(q, acc);
        Assert.Equal(new HashSet<(int, int)> { (1, 10) }, LiveDate(acc));

        // CURRENT_DATE reaches day 12 (exclusive upper bound): aged out.
        q.AdvanceClock(12 * Day);
        q.Step();
        AccumulateDate(q, acc);
        Assert.Empty(LiveDate(acc));
    }

    [Fact]
    public void CurrentDate_DisappearBound_BoundsDownstreamAggregateState()
    {
        // The DATE clock advertises a day-space GC frontier (CURRENT_DATE − 10
        // days) on its time-key, so a GROUP BY d above it keeps only the trailing
        // ~10-day window even as 100 distinct days stream through.
        var q = CompileDate(
            "SELECT d, COUNT(*) AS c FROM evd WHERE d > CURRENT_DATE - INTERVAL '10' DAY GROUP BY d");

        for (var i = 0; i <= 100; i++)
        {
            q.AdvanceClock(i * Day);                 // clock tracks the newest day
            q.Table("evd").Insert(i, new Date32(i));
            q.Step();
        }

        var agg = q.Circuit.Operators
            .OfType<IncrementalAggregateOp<StructuralRow, StructuralRow, StructuralRow>>()
            .Single();
        Assert.True(agg.RetainedGroupCount <= 12, $"retained {agg.RetainedGroupCount}");
        Assert.True(agg.RetainedGroupCount >= 10, $"retained {agg.RetainedGroupCount}");
    }

    [Fact]
    public void CastTimestampToDate_ProjectedDateGroupBy_BoundsAggregateState()
    {
        // The headline CURRENT_DATE-over-a-TIMESTAMP-column pattern: filter events
        // by calendar date, project CAST(ts AS DATE), then aggregate per day.
        // GROUP BY accepts only bare columns, so the date is projected in a derived
        // table and grouped by its alias. The filter advertises a day-space GC
        // frontier on the underlying `ts` column; the projected column d —
        // recognised as a monotone function of ts — carries the day-floor transform
        // so the GROUP BY d GCs to the trailing ~5-day window.
        var q = Compile(
            "SELECT d, COUNT(*) AS c FROM "
            + "(SELECT CAST(ts AS DATE) AS d FROM events "
            + " WHERE CAST(ts AS DATE) > CURRENT_DATE - INTERVAL '5' DAY) sub GROUP BY d");

        for (var i = 0; i <= 100; i++)
        {
            q.AdvanceClock(i * Day + 12 * Hour);            // midday of day i
            q.Table("events").Insert(i, new Timestamp(i * Day + 9 * Hour)); // 09:00 on day i
            q.Step();
        }

        var agg = q.Circuit.Operators
            .OfType<IncrementalAggregateOp<StructuralRow, StructuralRow, StructuralRow>>()
            .Single();
        // 101 distinct days streamed; the clock frontier (CURRENT_DATE − 5 days)
        // keeps only the trailing window.
        Assert.True(agg.RetainedGroupCount <= 7, $"retained {agg.RetainedGroupCount}");
        Assert.True(agg.RetainedGroupCount >= 5, $"retained {agg.RetainedGroupCount}");
    }

    [Fact]
    public void CastTimestampToDate_FilterBoundsRawTimestampGroupBy()
    {
        // Same filter, but grouping on the raw `ts` column: the source-column
        // frontier is midnight-µs of the frontier day, so a GROUP BY ts also GCs
        // (one group per distinct ts, bounded to the trailing ~10 days).
        var q = Compile(
            "SELECT ts, COUNT(*) AS c FROM events " +
            "WHERE CAST(ts AS DATE) > CURRENT_DATE - INTERVAL '10' DAY GROUP BY ts");

        for (var i = 0; i <= 100; i++)
        {
            q.AdvanceClock(i * Day + 12 * Hour);
            q.Table("events").Insert(i, new Timestamp(i * Day + 9 * Hour));
            q.Step();
        }

        var agg = q.Circuit.Operators
            .OfType<IncrementalAggregateOp<StructuralRow, StructuralRow, StructuralRow>>()
            .Single();
        Assert.True(agg.RetainedGroupCount <= 12, $"retained {agg.RetainedGroupCount}");
        Assert.True(agg.RetainedGroupCount >= 10, $"retained {agg.RetainedGroupCount}");
    }

    [Fact]
    public void SnapshotRestore_ContinuesIdenticallyToNonRestartRun()
    {
        // Drive a window filter through a scripted sequence; compare a run that
        // snapshots + restarts midway against a straight-through reference. Each
        // step is (clock, optional insert).
        var script = new (long Clock, int? Id, long Ts)[]
        {
            (0, 1, 0),
            (1000, 2, Hour),
            (Hour, null, 0),          // row 1 ages out, row 2 appears region
            (Hour + 2000, 3, 2 * Hour),
            (2 * Hour, null, 0),      // row 2 ages out
            (2 * Hour + 5000, null, 0),
        };

        var reference = Run(script, snapshotAfter: -1);
        var restarted = Run(script, snapshotAfter: 2);
        Assert.Equal(reference, restarted);
    }

    private Dictionary<(int Id, long Ts), long> Run(
        (long Clock, int? Id, long Ts)[] script, int snapshotAfter)
    {
        const string Query =
            "SELECT id, ts FROM events WHERE ts <= NOW() AND ts > NOW() - INTERVAL '1' HOUR";
        var q = Compile(Query);
        var acc = new Dictionary<(int, long), long>();

        for (var i = 0; i < script.Length; i++)
        {
            var (clock, id, ts) = script[i];
            q.AdvanceClock(clock);
            if (id is { } realId)
            {
                q.Table("events").Insert(realId, new Timestamp(ts));
            }

            q.Step();
            Accumulate(q, acc);

            if (i == snapshotAfter)
            {
                Snapshot.WriteAsync(q.Circuit, _snapshotDir).AsTask().GetAwaiter().GetResult();
                q = Compile(Query);
                Snapshot.ReadAsync(q.Circuit, _snapshotDir).AsTask().GetAwaiter().GetResult();
            }
        }

        return acc;
    }
}
