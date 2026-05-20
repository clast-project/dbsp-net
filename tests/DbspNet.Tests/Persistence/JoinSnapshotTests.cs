// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Persistence;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

namespace DbspNet.Tests.Persistence;

/// <summary>
/// End-to-end snapshot of <c>IncrementalJoinOp</c> and
/// <c>IncrementalLeftJoinOp</c>: the per-side integrated traces
/// round-trip through Arrow IPC under <c>left.arrows</c> /
/// <c>right.arrows</c>. After restore, the consumer's next-tick join
/// output matches what the producer would have emitted, including the
/// dl⋈R, L⋈dr, dl⋈dr cross-tick interactions.
/// </summary>
public class JoinSnapshotTests : IDisposable
{
    private readonly string _snapshotDir;

    public JoinSnapshotTests()
    {
        _snapshotDir = Path.Combine(
            Path.GetTempPath(), "dbspnet-join-" + Guid.NewGuid().ToString("N"));
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

    private static readonly string[] CustomersDdl =
    {
        "CREATE TABLE customers (cid INT NOT NULL, name VARCHAR(16) NOT NULL)",
        "CREATE TABLE orders (oid INT NOT NULL, cid INT NOT NULL, amt BIGINT NOT NULL)",
    };

    [Fact]
    public async Task InnerJoin_RestoredState_MatchesNewLeftRowAgainstOldRight()
    {
        // Producer: load right side (orders) only. After restore, insert a
        // new left row (customer); the consumer must find existing right
        // rows in its restored _rightTrace.
        var producer = Compile(
            CustomersDdl,
            "SELECT customers.name, orders.amt FROM customers JOIN orders ON customers.cid = orders.cid");
        producer.Table("orders").Insert(100, 1, 50L);
        producer.Table("orders").Insert(101, 1, 75L);
        producer.Step();   // no joins yet — left side empty

        await Snapshot.WriteAsync(producer.Circuit, _snapshotDir);

        var consumer = Compile(
            CustomersDdl,
            "SELECT customers.name, orders.amt FROM customers JOIN orders ON customers.cid = orders.cid");
        await Snapshot.ReadAsync(consumer.Circuit, _snapshotDir);

        // Insert the matching customer; the operator's L⋈dr formula needs
        // L_{t-1} (empty) but the new dl ⋈ R_{t-1} requires the right
        // trace to be populated — that's what we just restored.
        consumer.Table("customers").Insert(1, "alice");
        consumer.Step();

        Assert.Equal(2, consumer.Current.Count);
        Assert.Equal(1, consumer.WeightOf("alice", 50L).Value);
        Assert.Equal(1, consumer.WeightOf("alice", 75L).Value);
    }

    [Fact]
    public async Task InnerJoin_RestoredState_MatchesNewRightRowAgainstOldLeft()
    {
        // Mirror: snapshot with left side populated, then insert a new
        // right row in the consumer.
        var producer = Compile(
            CustomersDdl,
            "SELECT customers.name, orders.amt FROM customers JOIN orders ON customers.cid = orders.cid");
        producer.Table("customers").Insert(1, "alice");
        producer.Table("customers").Insert(2, "bob");
        producer.Step();

        await Snapshot.WriteAsync(producer.Circuit, _snapshotDir);

        var consumer = Compile(
            CustomersDdl,
            "SELECT customers.name, orders.amt FROM customers JOIN orders ON customers.cid = orders.cid");
        await Snapshot.ReadAsync(consumer.Circuit, _snapshotDir);

        consumer.Table("orders").Insert(100, 2, 75L);
        consumer.Step();

        Assert.Equal(1, consumer.Current.Count);
        Assert.Equal(1, consumer.WeightOf("bob", 75L).Value);
    }

    [Fact]
    public async Task InnerJoin_RestoredState_RetractionFlowsThroughBothTraces()
    {
        // Producer: left and right both populated and matched.
        var producer = Compile(
            CustomersDdl,
            "SELECT customers.name, orders.amt FROM customers JOIN orders ON customers.cid = orders.cid");
        producer.Table("customers").Insert(1, "alice");
        producer.Table("orders").Insert(100, 1, 50L);
        producer.Step();   // emits ('alice', 50): +1

        await Snapshot.WriteAsync(producer.Circuit, _snapshotDir);

        var consumer = Compile(
            CustomersDdl,
            "SELECT customers.name, orders.amt FROM customers JOIN orders ON customers.cid = orders.cid");
        await Snapshot.ReadAsync(consumer.Circuit, _snapshotDir);

        // Retract the order on the consumer side: dr = -1 for the right
        // row; L_{t-1} ⋈ dr should produce ('alice', 50): -1.
        consumer.Table("orders").Delete(100, 1, 50L);
        consumer.Step();

        Assert.Equal(1, consumer.Current.Count);
        Assert.Equal(-1, consumer.WeightOf("alice", 50L).Value);
    }

    [Fact]
    public async Task LeftJoin_RestoredState_GainedMatchEmitsRetractAndJoinedRow()
    {
        // Producer: only the left side populated. LEFT JOIN currently
        // emits NULL-padded rows for unmatched lefts.
        var producer = Compile(
            CustomersDdl,
            "SELECT customers.name, orders.amt FROM customers LEFT JOIN orders ON customers.cid = orders.cid");
        producer.Table("customers").Insert(1, "alice");
        producer.Step();   // emits ('alice', NULL): +1

        await Snapshot.WriteAsync(producer.Circuit, _snapshotDir);

        var consumer = Compile(
            CustomersDdl,
            "SELECT customers.name, orders.amt FROM customers LEFT JOIN orders ON customers.cid = orders.cid");
        await Snapshot.ReadAsync(consumer.Circuit, _snapshotDir);

        // Insert an order that matches: gained-match transition →
        // retract NULL-padded, emit joined.
        consumer.Table("orders").Insert(100, 1, 50L);
        consumer.Step();

        Assert.Equal(2, consumer.Current.Count);
        Assert.Equal(-1, consumer.WeightOf("alice", null).Value);
        Assert.Equal(1, consumer.WeightOf("alice", 50L).Value);
    }

    [Fact]
    public async Task LeftJoin_RestoredState_LostMatchRetractsJoinAndEmitsNullPadded()
    {
        // Producer: matched.
        var producer = Compile(
            CustomersDdl,
            "SELECT customers.name, orders.amt FROM customers LEFT JOIN orders ON customers.cid = orders.cid");
        producer.Table("customers").Insert(1, "alice");
        producer.Table("orders").Insert(100, 1, 50L);
        producer.Step();   // emits ('alice', 50): +1

        await Snapshot.WriteAsync(producer.Circuit, _snapshotDir);

        var consumer = Compile(
            CustomersDdl,
            "SELECT customers.name, orders.amt FROM customers LEFT JOIN orders ON customers.cid = orders.cid");
        await Snapshot.ReadAsync(consumer.Circuit, _snapshotDir);

        // Retract the order: lost-match transition → retract joined,
        // emit NULL-padded.
        consumer.Table("orders").Delete(100, 1, 50L);
        consumer.Step();

        Assert.Equal(2, consumer.Current.Count);
        Assert.Equal(-1, consumer.WeightOf("alice", 50L).Value);
        Assert.Equal(1, consumer.WeightOf("alice", null).Value);
    }

    [Fact]
    public async Task RightJoin_RestoredState_PreservedSideUnmatchedAfterRestore()
    {
        // RIGHT JOIN compiles as LEFT JOIN with sides swapped. Snapshot
        // happens with right (preserved) populated and no left matches;
        // consumer adds a left match.
        var producer = Compile(
            CustomersDdl,
            "SELECT customers.name, orders.amt FROM customers RIGHT JOIN orders ON customers.cid = orders.cid");
        producer.Table("orders").Insert(100, 1, 50L);
        producer.Step();   // emits (NULL, 50): +1

        await Snapshot.WriteAsync(producer.Circuit, _snapshotDir);

        var consumer = Compile(
            CustomersDdl,
            "SELECT customers.name, orders.amt FROM customers RIGHT JOIN orders ON customers.cid = orders.cid");
        await Snapshot.ReadAsync(consumer.Circuit, _snapshotDir);

        consumer.Table("customers").Insert(1, "alice");
        consumer.Step();

        Assert.Equal(2, consumer.Current.Count);
        Assert.Equal(-1, consumer.WeightOf(null, 50L).Value);
        Assert.Equal(1, consumer.WeightOf("alice", 50L).Value);
    }

    [Fact]
    public async Task NoCodec_InnerJoin_ThrowsOnSnapshot()
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in CustomersDdl)
        {
            resolver.Resolve(Parser.ParseStatement(s));
        }

        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(
            "SELECT customers.name, orders.amt FROM customers JOIN orders ON customers.cid = orders.cid"))).Query;
        var query = PlanToCircuit.Compile(plan);  // no codecs

        query.Table("customers").Insert(1, "alice");
        query.Step();

        await Assert.ThrowsAsync<NotSupportedException>(async () =>
            await Snapshot.WriteAsync(query.Circuit, _snapshotDir));
    }
}
