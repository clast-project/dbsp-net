// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Persistence;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

namespace DbspNet.Tests.Persistence;

/// <summary>
/// End-to-end snapshot of the recursive CTE's nested fixpoint circuit
/// (<c>FixpointOperator</c>): every imported base table's integrated trace
/// plus the prior-tick fixpoint round-trip through Arrow IPC. After restore,
/// the consumer's next-tick output is the delta — exactly what the producer
/// would have emitted continuing from the same point. Covers chain extension
/// and the retraction path.
/// </summary>
public class RecursiveCteSnapshotTests : IDisposable
{
    private readonly string _snapshotDir;

    public RecursiveCteSnapshotTests()
    {
        _snapshotDir = Path.Combine(
            Path.GetTempPath(), "dbspnet-rcte-" + Guid.NewGuid().ToString("N"));
    }

    public void Dispose()
    {
        if (Directory.Exists(_snapshotDir))
        {
            Directory.Delete(_snapshotDir, recursive: true);
        }

        GC.SuppressFinalize(this);
    }

    private const string ReachQuery =
        "WITH RECURSIVE reach AS ( " +
        "    SELECT src, dst FROM edges " +
        "    UNION ALL " +
        "    SELECT r.src, e.dst FROM reach r JOIN edges e ON r.dst = e.src) " +
        "SELECT src, dst FROM reach";

    private static readonly string[] EdgesDdl =
    {
        "CREATE TABLE edges (src INT NOT NULL, dst INT NOT NULL)",
    };

    private static CompiledQuery Compile()
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in EdgesDdl)
        {
            resolver.Resolve(Parser.ParseStatement(s));
        }

        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(ReachQuery))).Query;
        return PlanToCircuit.Compile(plan, ArrowSqlSnapshotCodecs.Instance);
    }

    private static long WeightOf(ZSet<StructuralRow, Z64> z, params object?[] row) =>
        z.WeightOf(new StructuralRow(row)).Value;

    [Fact]
    public async Task RestoredState_ExtendingChain_EmitsOnlyNewlyReachablePairs()
    {
        // Producer: chain 1 -> 2 -> 3, full closure already computed.
        var producer = Compile();
        producer.Table("edges").Insert(1, 2);
        producer.Table("edges").Insert(2, 3);
        producer.Step();   // reach = {(1,2),(2,3),(1,3)}

        await Snapshot.WriteAsync(producer.Circuit, _snapshotDir);

        // Consumer: fresh circuit, restore _r + external trace, then
        // extend the chain. The semi-naïve path must use the restored R
        // to find newly-derivable pairs.
        var consumer = Compile();
        await Snapshot.ReadAsync(consumer.Circuit, _snapshotDir);

        consumer.Table("edges").Insert(3, 4);
        consumer.Step();

        // Newly-emitted pairs only: (3,4), (2,4), (1,4).
        Assert.Equal(1, WeightOf(consumer.Current, 3, 4));
        Assert.Equal(1, WeightOf(consumer.Current, 2, 4));
        Assert.Equal(1, WeightOf(consumer.Current, 1, 4));
        // The pre-existing pairs must NOT re-emit — that's what _previousResult
        // restoration guarantees.
        Assert.Equal(0, WeightOf(consumer.Current, 1, 2));
        Assert.Equal(0, WeightOf(consumer.Current, 2, 3));
        Assert.Equal(0, WeightOf(consumer.Current, 1, 3));
        Assert.Equal(3, consumer.Current.Count);
    }

    [Fact]
    public async Task RestoredState_RetractionTriggersFullRecompute()
    {
        // Producer: chain 1 -> 2 -> 3, snapshot.
        var producer = Compile();
        producer.Table("edges").Insert(1, 2);
        producer.Table("edges").Insert(2, 3);
        producer.Step();

        await Snapshot.WriteAsync(producer.Circuit, _snapshotDir);

        // Consumer: restore, then retract the bridge edge. Retraction
        // forces FullRecompute, which uses the *integrated* external
        // traces — those must be restored from the snapshot.
        var consumer = Compile();
        await Snapshot.ReadAsync(consumer.Circuit, _snapshotDir);

        consumer.Table("edges").Delete(2, 3);
        consumer.Step();

        // (2,3) and (1,3) are no longer reachable.
        Assert.Equal(-1, WeightOf(consumer.Current, 2, 3));
        Assert.Equal(-1, WeightOf(consumer.Current, 1, 3));
        Assert.Equal(0, WeightOf(consumer.Current, 1, 2));
    }

    [Fact]
    public async Task RestoredState_BranchingExtension_ProducesCorrectClosure()
    {
        // Producer: chain 1 -> 2 -> 3.
        var producer = Compile();
        producer.Table("edges").Insert(1, 2);
        producer.Table("edges").Insert(2, 3);
        producer.Step();

        await Snapshot.WriteAsync(producer.Circuit, _snapshotDir);

        // Consumer: add a branch from 2 -> 5. The new pairs are (2,5)
        // direct + (1,5) transitively — both require the restored R to
        // include (1,2).
        var consumer = Compile();
        await Snapshot.ReadAsync(consumer.Circuit, _snapshotDir);

        consumer.Table("edges").Insert(2, 5);
        consumer.Step();

        Assert.Equal(1, WeightOf(consumer.Current, 2, 5));
        Assert.Equal(1, WeightOf(consumer.Current, 1, 5));
        Assert.Equal(2, consumer.Current.Count);
    }

    [Fact]
    public async Task RestoredState_NoChange_ProducesEmptyDelta()
    {
        // After restore, a Step with no input deltas should emit nothing —
        // _previousResult equals _r, so the difference is empty.
        var producer = Compile();
        producer.Table("edges").Insert(1, 2);
        producer.Table("edges").Insert(2, 3);
        producer.Step();

        await Snapshot.WriteAsync(producer.Circuit, _snapshotDir);

        var consumer = Compile();
        await Snapshot.ReadAsync(consumer.Circuit, _snapshotDir);

        consumer.Step();

        Assert.True(consumer.Current.IsEmpty);
    }

    [Fact]
    public async Task SnapshotBeforeAnyStep_RoundTripsEmpty()
    {
        // A snapshot taken from a freshly-built circuit (no Step yet)
        // should round-trip cleanly: empty external traces, empty _r,
        // empty _previousResult.
        var producer = Compile();
        await Snapshot.WriteAsync(producer.Circuit, _snapshotDir);

        var consumer = Compile();
        await Snapshot.ReadAsync(consumer.Circuit, _snapshotDir);

        consumer.Table("edges").Insert(1, 2);
        consumer.Step();

        // Behaves like a fresh circuit: emits the direct edge.
        Assert.Equal(1, WeightOf(consumer.Current, 1, 2));
        Assert.Equal(1, consumer.Current.Count);
    }

    [Fact]
    public async Task NoCodec_RecursiveCte_ThrowsOnSnapshot()
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in EdgesDdl)
        {
            resolver.Resolve(Parser.ParseStatement(s));
        }

        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(ReachQuery))).Query;
        var query = PlanToCircuit.Compile(plan);  // no codecs

        query.Table("edges").Insert(1, 2);
        query.Step();

        await Assert.ThrowsAsync<NotSupportedException>(async () =>
            await Snapshot.WriteAsync(query.Circuit, _snapshotDir));
    }
}
