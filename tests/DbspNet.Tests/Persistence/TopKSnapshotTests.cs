// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Persistence;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

namespace DbspNet.Tests.Persistence;

/// <summary>
/// End-to-end snapshot of a <c>TopKOp</c> (<c>ORDER BY … LIMIT</c>). The
/// operator persists its full integrated input (not just the emitted window),
/// because a retraction of a windowed row must promote the next row — which the
/// restored consumer can only do if it knows about rows below the window.
/// These tests snapshot a producer that has seen more rows than the limit,
/// rebuild a fresh circuit from the same plan, restore, and verify subsequent
/// ticks behave as if the consumer had seen all the producer's history.
/// </summary>
public class TopKSnapshotTests : IDisposable
{
    private const string Ddl = "CREATE TABLE t (a INT NOT NULL)";
    private const string Query = "SELECT a FROM t ORDER BY a LIMIT 2";

    private readonly string _snapshotDir;

    public TopKSnapshotTests()
    {
        _snapshotDir = Path.Combine(
            Path.GetTempPath(), "dbspnet-topk-" + Guid.NewGuid().ToString("N"));
    }

    public void Dispose()
    {
        if (Directory.Exists(_snapshotDir))
        {
            Directory.Delete(_snapshotDir, recursive: true);
        }

        GC.SuppressFinalize(this);
    }

    private static CompiledQuery Compile() =>
        Compile(ArrowSqlSnapshotCodecs.Instance);

    private static CompiledQuery Compile(ISqlSnapshotCodecs? codecs)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        resolver.Resolve(Parser.ParseStatement(Ddl));
        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(Query))).Query;
        return codecs is null
            ? PlanToCircuit.Compile(plan)
            : PlanToCircuit.Compile(plan, codecs);
    }

    [Fact]
    public async Task RestoredState_RetractionPromotesNextRow()
    {
        // Producer sees four rows; the window is {1, 2} but the accumulation
        // retains 3 and 4 below it.
        var producer = Compile();
        producer.Table("t").Insert(1);
        producer.Table("t").Insert(2);
        producer.Table("t").Insert(3);
        producer.Table("t").Insert(4);
        producer.Step();
        Assert.Equal(2, producer.Current.Count);

        await Snapshot.WriteAsync(producer.Circuit, _snapshotDir);

        var consumer = Compile();
        await Snapshot.ReadAsync(consumer.Circuit, _snapshotDir);

        // Retract the current top row. This is the proof the FULL accumulation
        // round-tripped (not just the emitted window): 1 leaves and 3 — which
        // the consumer never saw inserted directly — is promoted in. The
        // already-windowed row 2 must not be re-emitted.
        consumer.Table("t").Delete(1);
        consumer.Step();

        Assert.Equal(2, consumer.Current.Count);
        Assert.Equal(-1, consumer.WeightOf(1).Value);
        Assert.Equal(1, consumer.WeightOf(3).Value);
        Assert.Equal(0, consumer.WeightOf(2).Value);
    }

    [Fact]
    public async Task RestoredState_NewSmallestRowEntersWindow()
    {
        var producer = Compile();
        producer.Table("t").Insert(10);
        producer.Table("t").Insert(20);
        producer.Table("t").Insert(30);
        producer.Step(); // window {10, 20}

        await Snapshot.WriteAsync(producer.Circuit, _snapshotDir);

        var consumer = Compile();
        await Snapshot.ReadAsync(consumer.Circuit, _snapshotDir);

        // A new smallest row enters; the prior window's largest (20) is evicted.
        // The restored window state is what tells the consumer 20 was emitted.
        consumer.Table("t").Insert(5);
        consumer.Step();

        Assert.Equal(2, consumer.Current.Count);
        Assert.Equal(1, consumer.WeightOf(5).Value);
        Assert.Equal(-1, consumer.WeightOf(20).Value);
        Assert.Equal(0, consumer.WeightOf(10).Value);
    }

    [Fact]
    public async Task RestoredState_NoChange_EmitsNothing()
    {
        // After restore, an insert that lands below the window (outside it)
        // changes nothing observable — proving the window was restored, not
        // recomputed-and-re-emitted.
        var producer = Compile();
        producer.Table("t").Insert(1);
        producer.Table("t").Insert(2);
        producer.Step();

        await Snapshot.WriteAsync(producer.Circuit, _snapshotDir);

        var consumer = Compile();
        await Snapshot.ReadAsync(consumer.Circuit, _snapshotDir);

        consumer.Table("t").Insert(9); // far outside the LIMIT 2 window
        consumer.Step();

        Assert.True(consumer.Current.IsEmpty);
    }

    [Fact]
    public async Task NoCodec_ThrowsOnSnapshot()
    {
        var query = Compile(codecs: null); // no codec registry — TopKOp has no codec
        query.Table("t").Insert(1);
        query.Step();

        await Assert.ThrowsAsync<NotSupportedException>(async () =>
            await Snapshot.WriteAsync(query.Circuit, _snapshotDir));
    }
}
