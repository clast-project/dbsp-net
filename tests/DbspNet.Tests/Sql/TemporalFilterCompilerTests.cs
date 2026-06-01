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
