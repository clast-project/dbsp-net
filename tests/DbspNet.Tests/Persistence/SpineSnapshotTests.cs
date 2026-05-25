// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Persistence;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

namespace DbspNet.Tests.Persistence;

/// <summary>
/// Snapshot/restore coverage for circuits compiled onto the spine
/// (LSM-style) trace family via <see cref="CompileOptions"/>. The spine
/// operators serialise their traces per-batch (rather than as one
/// consolidated blob), but through the same Arrow IPC codec contract — so
/// the SQL-level <c>Snapshot.WriteAsync</c>/<c>ReadAsync</c> path should
/// round-trip them exactly as it does the flat family. These mirror the
/// flat <see cref="MultiOpSnapshotTests"/> compositions to confirm every
/// spine operator (distinct, aggregate, inner/left join) survives a
/// snapshot boundary. The closing tests assert flat↔spine snapshots are
/// not cross-loadable — the operator-type plan fingerprint differs.
/// </summary>
public class SpineSnapshotTests : IDisposable
{
    private readonly string _snapshotDir;

    public SpineSnapshotTests()
    {
        _snapshotDir = Path.Combine(
            Path.GetTempPath(), "dbspnet-spine-snap-" + Guid.NewGuid().ToString("N"));
    }

    public void Dispose()
    {
        if (Directory.Exists(_snapshotDir))
        {
            Directory.Delete(_snapshotDir, recursive: true);
        }

        GC.SuppressFinalize(this);
    }

    private static CompiledQuery Compile(string[] ddl, string query, TraceFamily family = TraceFamily.Spine)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in ddl)
        {
            resolver.Resolve(Parser.ParseStatement(s));
        }

        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(query))).Query;
        return PlanToCircuit.Compile(plan, ArrowSqlSnapshotCodecs.Instance, new CompileOptions { TraceFamily = family });
    }

    private static void AssertSpineEngaged(CompiledQuery q) =>
        Assert.Contains(
            q.Circuit.Operators,
            op => op.GetType().Name.StartsWith("Spine", StringComparison.Ordinal));

    [Fact]
    public async Task InnerJoinPlusGroupBy_StateComposesAcrossSnapshot()
    {
        // Spine ops: SpineIncrementalJoinOp + SpineIncrementalAggregateOp.
        string[] ddl =
        {
            "CREATE TABLE products (id INT NOT NULL, category VARCHAR(16) NOT NULL)",
            "CREATE TABLE orders (oid INT NOT NULL, pid INT NOT NULL, amt BIGINT NOT NULL)",
        };
        const string Query =
            "SELECT products.category, SUM(orders.amt) " +
            "FROM orders JOIN products ON orders.pid = products.id " +
            "GROUP BY products.category";

        var producer = Compile(ddl, Query);
        AssertSpineEngaged(producer);
        producer.Table("products").Insert(1, "electronics");
        producer.Table("products").Insert(2, "books");
        producer.Table("orders").Insert(101, 1, 100L);
        producer.Table("orders").Insert(102, 1, 50L);
        producer.Table("orders").Insert(103, 2, 30L);
        producer.Step();
        // electronics=150, books=30.

        await Snapshot.WriteAsync(producer.Circuit, _snapshotDir);

        var consumer = Compile(ddl, Query);
        await Snapshot.ReadAsync(consumer.Circuit, _snapshotDir);

        // New order on an existing product traverses the restored join trace
        // and the restored aggregate state.
        consumer.Table("orders").Insert(104, 1, 25L);
        consumer.Step();

        Assert.Equal(2, consumer.Current.Count);
        Assert.Equal(-1, consumer.WeightOf("electronics", 150L).Value);
        Assert.Equal(1, consumer.WeightOf("electronics", 175L).Value);
    }

    [Fact]
    public async Task UnionPlusGroupBy_DistinctAndAggregateBothRestore()
    {
        // UNION (set semantics) → SpineDistinctOp; GROUP BY →
        // SpineIncrementalAggregateOp.
        string[] ddl =
        {
            "CREATE TABLE t1 (cat VARCHAR(8) NOT NULL, amt BIGINT NOT NULL)",
            "CREATE TABLE t2 (cat VARCHAR(8) NOT NULL, amt BIGINT NOT NULL)",
        };
        const string Query =
            "WITH u AS (SELECT cat, amt FROM t1 UNION SELECT cat, amt FROM t2) " +
            "SELECT cat, SUM(amt) FROM u GROUP BY cat";

        var producer = Compile(ddl, Query);
        AssertSpineEngaged(producer);
        producer.Table("t1").Insert("a", 10L);
        producer.Table("t1").Insert("a", 20L);
        producer.Table("t2").Insert("a", 10L);   // dedup'd against t1's (a,10)
        producer.Table("t2").Insert("b", 5L);
        producer.Step();
        // GROUP BY over distinct rows: {(a, 30), (b, 5)}.

        await Snapshot.WriteAsync(producer.Circuit, _snapshotDir);

        var consumer = Compile(ddl, Query);
        await Snapshot.ReadAsync(consumer.Circuit, _snapshotDir);

        // (a,20) already present in the distinct trace at +1 → cumulative 2,
        // still positive → no distinct delta → empty output.
        consumer.Table("t2").Insert("a", 20L);
        consumer.Step();
        Assert.True(consumer.Current.IsEmpty);

        // Brand-new (a,100) → distinct emits it → group a's sum 30 → 130.
        consumer.Table("t1").Insert("a", 100L);
        consumer.Step();
        Assert.Equal(2, consumer.Current.Count);
        Assert.Equal(-1, consumer.WeightOf("a", 30L).Value);
        Assert.Equal(1, consumer.WeightOf("a", 130L).Value);
    }

    [Fact]
    public async Task LeftJoinPlusGroupBy_PreservesUnmatchedSide()
    {
        // Spine ops: SpineIncrementalLeftJoinOp + SpineIncrementalAggregateOp.
        string[] ddl =
        {
            "CREATE TABLE customers (cid INT NOT NULL, name VARCHAR(16) NOT NULL)",
            "CREATE TABLE orders (oid INT NOT NULL, cid INT NOT NULL)",
        };
        const string Query =
            "SELECT customers.name, COUNT(orders.oid) " +
            "FROM customers LEFT JOIN orders ON customers.cid = orders.cid " +
            "GROUP BY customers.name";

        var producer = Compile(ddl, Query);
        AssertSpineEngaged(producer);
        producer.Table("customers").Insert(1, "alice");
        producer.Table("customers").Insert(2, "bob");
        producer.Table("orders").Insert(100, 1);
        producer.Table("orders").Insert(101, 1);
        producer.Step();
        // ('alice', 2), ('bob', 0).

        await Snapshot.WriteAsync(producer.Circuit, _snapshotDir);

        var consumer = Compile(ddl, Query);
        await Snapshot.ReadAsync(consumer.Circuit, _snapshotDir);

        // Bob's first order: gained-match in the restored LEFT JOIN trace
        // retracts ('bob', 0) and the aggregate re-emits ('bob', 1).
        consumer.Table("orders").Insert(102, 2);
        consumer.Step();

        Assert.Equal(2, consumer.Current.Count);
        Assert.Equal(-1, consumer.WeightOf("bob", 0L).Value);
        Assert.Equal(1, consumer.WeightOf("bob", 1L).Value);
    }

    [Fact]
    public async Task FlatSnapshot_RejectedBySpineConsumer()
    {
        // A snapshot written by the flat operator family must not load into a
        // spine circuit: the operator-type plan fingerprint differs.
        string[] ddl = { "CREATE TABLE sales (cat VARCHAR(8) NOT NULL, amt BIGINT NOT NULL)" };
        const string Query = "SELECT cat, SUM(amt) FROM sales GROUP BY cat";

        var producer = Compile(ddl, Query, TraceFamily.Flat);
        producer.Table("sales").Insert("a", 10L);
        producer.Step();
        await Snapshot.WriteAsync(producer.Circuit, _snapshotDir);

        var consumer = Compile(ddl, Query, TraceFamily.Spine);
        var ex = await Assert.ThrowsAsync<InvalidDataException>(async () =>
            await Snapshot.ReadAsync(consumer.Circuit, _snapshotDir));
        Assert.Contains("plan fingerprint mismatch", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task SpineSnapshot_RejectedByFlatConsumer()
    {
        string[] ddl = { "CREATE TABLE sales (cat VARCHAR(8) NOT NULL, amt BIGINT NOT NULL)" };
        const string Query = "SELECT cat, SUM(amt) FROM sales GROUP BY cat";

        var producer = Compile(ddl, Query, TraceFamily.Spine);
        producer.Table("sales").Insert("a", 10L);
        producer.Step();
        await Snapshot.WriteAsync(producer.Circuit, _snapshotDir);

        var consumer = Compile(ddl, Query, TraceFamily.Flat);
        var ex = await Assert.ThrowsAsync<InvalidDataException>(async () =>
            await Snapshot.ReadAsync(consumer.Circuit, _snapshotDir));
        Assert.Contains("plan fingerprint mismatch", ex.Message, StringComparison.OrdinalIgnoreCase);
    }
}
