// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Linq;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Persistence;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

namespace DbspNet.Tests.Persistence;

/// <summary>
/// LATENESS phase 4d: the input-side <c>LatenessOperator</c>'s <c>maxSeen</c>
/// round-trips through the SQL <see cref="Snapshot.WriteAsync(global::DbspNet.Core.Circuit.RootCircuit, string, int, System.Threading.CancellationToken)"/>
/// path, so on restore the frontier is re-advanced to <c>maxSeen − lateness</c>
/// — consistent with the GC'd downstream traces restored alongside it. The
/// crucial property: a late row arriving after restore is still dropped (it
/// cannot resurrect a group/key the producer already collected), and a restored
/// circuit continues bit-for-bit like one that never snapshotted.
/// </summary>
public class LatenessSnapshotTests : IDisposable
{
    private readonly string _snapshotDir;

    public LatenessSnapshotTests()
    {
        _snapshotDir = Path.Combine(
            Path.GetTempPath(), "dbspnet-lateness-" + Guid.NewGuid().ToString("N"));
    }

    public void Dispose()
    {
        if (Directory.Exists(_snapshotDir))
        {
            Directory.Delete(_snapshotDir, recursive: true);
        }

        GC.SuppressFinalize(this);
    }

    private static CompiledQuery Compile(string[] ddl, string query)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in ddl)
        {
            resolver.Resolve(Parser.ParseStatement(s));
        }

        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(query))).Query;
        return PlanToCircuit.Compile(plan, ArrowSqlSnapshotCodecs.Instance);
    }

    private static int AggRetained(CompiledQuery q) =>
        q.Circuit.Operators
            .OfType<IncrementalAggregateOp<StructuralRow, StructuralRow, StructuralRow>>()
            .Single()
            .RetainedGroupCount;

    private static int JoinRetained(CompiledQuery q) =>
        q.Circuit.Operators
            .OfType<IncrementalJoinOp<StructuralRow, StructuralRow, StructuralRow, StructuralRow, Z64>>()
            .Single()
            .RetainedKeyCount;

    private static int DistinctRetained(CompiledQuery q) =>
        q.Circuit.Operators
            .OfType<DistinctOp<StructuralRow, Z64>>()
            .Single()
            .RetainedKeyCount;

    private static long Dropped(CompiledQuery q) =>
        q.Circuit.Operators
            .OfType<LatenessOperator<StructuralRow>>()
            .Sum(op => op.DroppedCount);

    private static readonly string[] EventsDdl =
        ["CREATE TABLE events (ts BIGINT NOT NULL LATENESS 10, v INT NOT NULL)"];

    [Fact]
    public async Task AggregateMaxSeen_RoundTrips_LateRowStillDroppedAfterRestore()
    {
        // Producer runs to ts=30: frontier = 20, aggregate GC'd to groups [20, 30].
        var producer = Compile(EventsDdl, "SELECT ts, COUNT(*) FROM events GROUP BY ts");
        for (long t = 0; t <= 30; t++)
        {
            producer.Table("events").Insert(t, 1);
            producer.Step();
        }

        Assert.Equal(11, AggRetained(producer));
        await Snapshot.WriteAsync(producer.Circuit, _snapshotDir);

        var consumer = Compile(EventsDdl, "SELECT ts, COUNT(*) FROM events GROUP BY ts");
        await Snapshot.ReadAsync(consumer.Circuit, _snapshotDir);

        // The GC'd trace restored bounded.
        Assert.Equal(11, AggRetained(consumer));

        // A late row (ts = 5 ≪ frontier 20) must be dropped — which only happens
        // if maxSeen round-tripped and the frontier was re-advanced to 20 on load.
        // Had maxSeen NOT persisted, the frontier would be MinValue, ts=5 admitted,
        // and the already-collected group 5 resurrected.
        consumer.Table("events").Insert(5L, 1);
        consumer.Step();
        Assert.True(consumer.Current.IsEmpty);   // dropped at input → no output
        Assert.Equal(1, Dropped(consumer));      // and counted as late
        Assert.Equal(11, AggRetained(consumer)); // group 5 not resurrected

        // An in-window row continues normally (and extends the window).
        consumer.Table("events").Insert(31L, 1);
        consumer.Step();
        Assert.Equal(1, consumer.WeightOf(31L, 1L).Value);
    }

    [Fact]
    public async Task AggregateGcState_AfterRestore_MatchesNeverSnapshottedTwin()
    {
        // Twin runs straight through 0..40.
        var twin = Compile(EventsDdl, "SELECT ts, COUNT(*) FROM events GROUP BY ts");
        for (long t = 0; t <= 40; t++)
        {
            twin.Table("events").Insert(t, 1);
            twin.Step();
        }

        // Producer snapshots at the midpoint (tick 20), consumer restores and
        // resumes — its remaining-tick output and bounded state must match the twin.
        var producer = Compile(EventsDdl, "SELECT ts, COUNT(*) FROM events GROUP BY ts");
        for (long t = 0; t <= 20; t++)
        {
            producer.Table("events").Insert(t, 1);
            producer.Step();
        }

        await Snapshot.WriteAsync(producer.Circuit, _snapshotDir);

        var consumer = Compile(EventsDdl, "SELECT ts, COUNT(*) FROM events GROUP BY ts");
        await Snapshot.ReadAsync(consumer.Circuit, _snapshotDir);
        for (long t = 21; t <= 40; t++)
        {
            consumer.Table("events").Insert(t, 1);
            consumer.Step();
        }

        // Identical final-tick delta and identical bounded state.
        Assert.Equal(twin.Current, consumer.Current);
        Assert.Equal(AggRetained(twin), AggRetained(consumer));
        Assert.Equal(11, AggRetained(consumer));
    }

    private static readonly string[] JoinDdl =
    [
        "CREATE TABLE a (ts BIGINT NOT NULL LATENESS 10, x INT NOT NULL)",
        "CREATE TABLE b (ts BIGINT NOT NULL LATENESS 10, y INT NOT NULL)",
    ];

    [Fact]
    public async Task JoinGcState_AfterRestore_MatchesNeverSnapshottedTwin()
    {
        // Two LATENESS sources → two LatenessOperators, each persisting its own
        // maxSeen; the join GCs against the min of their frontiers. Both must
        // restore for the bounded join state to stay consistent.
        const string Query = "SELECT a.x, b.y FROM a JOIN b ON a.ts = b.ts";

        var twin = Compile(JoinDdl, Query);
        for (long t = 0; t <= 40; t++)
        {
            twin.Table("a").Insert(t, (int)t);
            twin.Table("b").Insert(t, (int)t);
            twin.Step();
        }

        var producer = Compile(JoinDdl, Query);
        for (long t = 0; t <= 20; t++)
        {
            producer.Table("a").Insert(t, (int)t);
            producer.Table("b").Insert(t, (int)t);
            producer.Step();
        }

        await Snapshot.WriteAsync(producer.Circuit, _snapshotDir);

        var consumer = Compile(JoinDdl, Query);
        await Snapshot.ReadAsync(consumer.Circuit, _snapshotDir);
        for (long t = 21; t <= 40; t++)
        {
            consumer.Table("a").Insert(t, (int)t);
            consumer.Table("b").Insert(t, (int)t);
            consumer.Step();
        }

        Assert.Equal(twin.Current, consumer.Current);
        Assert.Equal(JoinRetained(twin), JoinRetained(consumer));
        Assert.Equal(22, JoinRetained(consumer));
    }

    private static readonly string[] UnionDdl =
    [
        "CREATE TABLE a (ts BIGINT NOT NULL LATENESS 10)",
        "CREATE TABLE b (ts BIGINT NOT NULL LATENESS 10)",
    ];

    [Fact]
    public async Task DistinctGcState_AfterRestore_MatchesNeverSnapshottedTwin()
    {
        const string Query = "SELECT ts FROM a UNION SELECT ts FROM b";

        var twin = Compile(UnionDdl, Query);
        for (long t = 0; t <= 40; t++)
        {
            twin.Table("a").Insert(t);
            twin.Table("b").Insert(t);
            twin.Step();
        }

        var producer = Compile(UnionDdl, Query);
        for (long t = 0; t <= 20; t++)
        {
            producer.Table("a").Insert(t);
            producer.Table("b").Insert(t);
            producer.Step();
        }

        await Snapshot.WriteAsync(producer.Circuit, _snapshotDir);

        var consumer = Compile(UnionDdl, Query);
        await Snapshot.ReadAsync(consumer.Circuit, _snapshotDir);
        for (long t = 21; t <= 40; t++)
        {
            consumer.Table("a").Insert(t);
            consumer.Table("b").Insert(t);
            consumer.Step();
        }

        Assert.Equal(twin.Current, consumer.Current);
        Assert.Equal(DistinctRetained(twin), DistinctRetained(consumer));
        Assert.Equal(11, DistinctRetained(consumer));
    }
}
