using DbspNet.Persistence;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

namespace DbspNet.Tests.Persistence;

/// <summary>
/// Snapshot/restore coverage for queries that compose multiple stateful
/// operators in one circuit. Each prior persistence test exercised one
/// op type in isolation; these check that positional <c>op-{i}</c>
/// indexing, manifest <c>SnapshottedIndices</c> ordering, and per-op
/// state composition all hold up when JOIN + GROUP BY (etc.) coexist in
/// the same plan.
/// </summary>
public class MultiOpSnapshotTests : IDisposable
{
    private readonly string _snapshotDir;

    public MultiOpSnapshotTests()
    {
        _snapshotDir = Path.Combine(
            Path.GetTempPath(), "dbspnet-multi-" + Guid.NewGuid().ToString("N"));
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

    [Fact]
    public void InnerJoinPlusGroupBy_StateComposesAcrossSnapshot()
    {
        // Classic analytics shape: two stateful ops in series.
        // Stateful ops produced by this plan: 1 IncrementalJoinOp + 1
        // IncrementalAggregateOp.
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
        producer.Table("products").Insert(1, "electronics");
        producer.Table("products").Insert(2, "books");
        producer.Table("orders").Insert(101, 1, 100L);
        producer.Table("orders").Insert(102, 1, 50L);
        producer.Table("orders").Insert(103, 2, 30L);
        producer.Step();
        // Producer state: electronics=150, books=30.

        Snapshot.Write(producer.Circuit, _snapshotDir);

        var consumer = Compile(ddl, Query);
        Snapshot.Read(consumer.Circuit, _snapshotDir);

        // New order on an existing product: must traverse the restored
        // join trace (to find the matching product) AND the restored
        // agg cache (to retract the prior sum and emit the new one).
        consumer.Table("orders").Insert(104, 1, 25L);
        consumer.Step();

        Assert.Equal(2, consumer.Current.Count);
        Assert.Equal(-1, consumer.WeightOf("electronics", 150L).Value);
        Assert.Equal(1, consumer.WeightOf("electronics", 175L).Value);
    }

    [Fact]
    public void ThreeWayJoin_BothJoinTracesRestoreIndependently()
    {
        // Stateful ops: 2 IncrementalJoinOps. Each holds its own
        // (left, right) trace pair under op-{i}/left.arrows /
        // op-{i}/right.arrows — verifies positional op subdir layout
        // doesn't collide.
        string[] ddl =
        {
            "CREATE TABLE a (k INT NOT NULL, x INT NOT NULL)",
            "CREATE TABLE b (k INT NOT NULL, m INT NOT NULL)",
            "CREATE TABLE c (m INT NOT NULL, z INT NOT NULL)",
        };
        const string Query =
            "SELECT a.x, c.z FROM a JOIN b ON a.k = b.k JOIN c ON b.m = c.m";

        var producer = Compile(ddl, Query);
        producer.Table("a").Insert(1, 100);
        producer.Table("a").Insert(2, 200);
        producer.Table("b").Insert(1, 10);
        producer.Table("b").Insert(2, 20);
        producer.Table("c").Insert(10, 999);
        producer.Table("c").Insert(20, 888);
        producer.Step();
        // Output: (100, 999), (200, 888).

        Snapshot.Write(producer.Circuit, _snapshotDir);

        var consumer = Compile(ddl, Query);
        Snapshot.Read(consumer.Circuit, _snapshotDir);

        // New row in a + new row in b that creates a third complete chain
        // a→b→c using the restored b/c traces.
        consumer.Table("a").Insert(3, 300);
        consumer.Table("b").Insert(3, 10);   // b.m=10 matches existing c.m=10
        consumer.Step();

        // The new chain produces (300, 999).
        Assert.Equal(1, consumer.WeightOf(300, 999).Value);
        Assert.Equal(1, consumer.Current.Count);
    }

    [Fact]
    public void UnionPlusGroupBy_DistinctAndAggregateBothRestore()
    {
        // UNION (without ALL) compiles as UnionAll + DistinctOp.
        // GROUP BY adds the IncrementalAggregateOp. So this plan has
        // both a Distinct and an Aggregate as snapshottable operators.
        string[] ddl =
        {
            "CREATE TABLE t1 (cat VARCHAR(8) NOT NULL, amt BIGINT NOT NULL)",
            "CREATE TABLE t2 (cat VARCHAR(8) NOT NULL, amt BIGINT NOT NULL)",
        };
        const string Query =
            "WITH u AS (SELECT cat, amt FROM t1 UNION SELECT cat, amt FROM t2) " +
            "SELECT cat, SUM(amt) FROM u GROUP BY cat";

        var producer = Compile(ddl, Query);
        producer.Table("t1").Insert("a", 10L);
        producer.Table("t1").Insert("a", 20L);   // distinct from (a,10)
        producer.Table("t2").Insert("a", 10L);   // dedup'd against t1's (a,10)
        producer.Table("t2").Insert("b", 5L);
        producer.Step();
        // After UNION dedup: {(a,10), (a,20), (b,5)}.
        // After GROUP BY: {(a, 30), (b, 5)}.

        Snapshot.Write(producer.Circuit, _snapshotDir);

        var consumer = Compile(ddl, Query);
        Snapshot.Read(consumer.Circuit, _snapshotDir);

        // Insert (a,20) into t2 — the Distinct trace already has (a,20) at +1
        // (from t1), so adding +1 from t2 makes the cumulative weight 2,
        // still positive → Distinct emits no delta. The Aggregate sees no
        // input delta, so the final output is empty.
        consumer.Table("t2").Insert("a", 20L);
        consumer.Step();
        Assert.True(consumer.Current.IsEmpty);

        // Insert a brand-new (a,100) into t1 — Distinct emits (a,100):+1.
        // Aggregate folds it into a's group: SUM was 30, now 130.
        consumer.Table("t1").Insert("a", 100L);
        consumer.Step();
        Assert.Equal(2, consumer.Current.Count);
        Assert.Equal(-1, consumer.WeightOf("a", 30L).Value);
        Assert.Equal(1, consumer.WeightOf("a", 130L).Value);
    }

    [Fact]
    public void LeftJoinPlusGroupBy_PreservesUnmatchedSide()
    {
        // LEFT JOIN keeps unmatched left rows (NULL-padded right). With
        // GROUP BY downstream, an unmatched customer should still appear
        // in the output with COUNT(orders.oid) = 0.
        // Stateful ops: 1 IncrementalLeftJoinOp + 1 IncrementalAggregateOp.
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
        producer.Table("customers").Insert(1, "alice");
        producer.Table("customers").Insert(2, "bob");
        producer.Table("orders").Insert(100, 1);
        producer.Table("orders").Insert(101, 1);
        producer.Step();
        // Output: ('alice', 2), ('bob', 0).

        Snapshot.Write(producer.Circuit, _snapshotDir);

        var consumer = Compile(ddl, Query);
        Snapshot.Read(consumer.Circuit, _snapshotDir);

        // Bob's first order: gained-match in the LEFT JOIN, so the
        // restored join state retracts ('bob', NULL) and emits ('bob', 102).
        // Then the aggregate sees a delta on 'bob' and updates count 0 → 1.
        consumer.Table("orders").Insert(102, 2);
        consumer.Step();

        Assert.Equal(2, consumer.Current.Count);
        Assert.Equal(-1, consumer.WeightOf("bob", 0L).Value);
        Assert.Equal(1, consumer.WeightOf("bob", 1L).Value);
    }

    [Fact]
    public void RecursiveCtePlusGroupBy_BothOpsCompose()
    {
        // Stateful ops: 1 RecursiveCteOp (driving fixed-point) + 1
        // IncrementalAggregateOp (counting reachable nodes per source).
        string[] ddl =
        {
            "CREATE TABLE edges (src INT NOT NULL, dst INT NOT NULL)",
        };
        const string Query =
            "WITH RECURSIVE reach AS ( " +
            "    SELECT src, dst FROM edges " +
            "    UNION ALL " +
            "    SELECT r.src, e.dst FROM reach r JOIN edges e ON r.dst = e.src) " +
            "SELECT src, COUNT(dst) FROM reach GROUP BY src";

        var producer = Compile(ddl, Query);
        producer.Table("edges").Insert(1, 2);
        producer.Table("edges").Insert(2, 3);
        producer.Step();
        // reach: {(1,2),(2,3),(1,3)}. Counts: src=1 → 2 (reaches 2,3),
        // src=2 → 1 (reaches 3).

        Snapshot.Write(producer.Circuit, _snapshotDir);

        var consumer = Compile(ddl, Query);
        Snapshot.Read(consumer.Circuit, _snapshotDir);

        // Extend the chain. The recursive CTE's restored R lets the
        // semi-naïve path discover (3,4),(2,4),(1,4) as new derivations.
        // Aggregate counts: src=1 gains 1 (now reaches 2,3,4), src=2 gains
        // 1 (now reaches 3,4), src=3 gains 1 (new key, count=1).
        consumer.Table("edges").Insert(3, 4);
        consumer.Step();

        Assert.Equal(-1, consumer.WeightOf(1, 2L).Value);
        Assert.Equal(1, consumer.WeightOf(1, 3L).Value);
        Assert.Equal(-1, consumer.WeightOf(2, 1L).Value);
        Assert.Equal(1, consumer.WeightOf(2, 2L).Value);
        Assert.Equal(1, consumer.WeightOf(3, 1L).Value);
    }

    [Fact]
    public void Manifest_RecordsAllStatefulOpPositions()
    {
        // Verify the snapshot manifest's SnapshottedIndices captures
        // every ISnapshotable operator (not just the first), and the
        // indices are strictly increasing (i.e. positional).
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
        producer.Table("products").Insert(1, "x");
        producer.Table("orders").Insert(101, 1, 1L);
        producer.Step();

        Snapshot.Write(producer.Circuit, _snapshotDir);
        var snapDir = Path.Combine(_snapshotDir, "snap-" + producer.Circuit.TickCount);
        var manifest = SnapshotManifest.Read(Path.Combine(snapDir, "manifest.json"));

        // JOIN + GROUP BY contributes 2 stateful ops.
        Assert.Equal(2, manifest.SnapshottedIndices.Count);

        // Indices must be strictly increasing — that's how the loader
        // pairs them with the rebuilt circuit's operator list.
        for (var i = 1; i < manifest.SnapshottedIndices.Count; i++)
        {
            Assert.True(manifest.SnapshottedIndices[i] > manifest.SnapshottedIndices[i - 1]);
        }

        // Each index resolves to a per-op subdirectory on disk.
        foreach (var i in manifest.SnapshottedIndices)
        {
            Assert.True(Directory.Exists(Path.Combine(snapDir, $"op-{i}")));
        }
    }
}
