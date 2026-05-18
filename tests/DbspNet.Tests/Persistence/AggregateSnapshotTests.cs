using DbspNet.Core.Algebra;
using DbspNet.Persistence;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

namespace DbspNet.Tests.Persistence;

/// <summary>
/// End-to-end snapshot of <c>IncrementalAggregateOp</c>: the trace
/// (per-group multiset) round-trips through Arrow IPC; the operator
/// rebuilds its per-group aggregate cache and per-aggregator scratch
/// state from the loaded trace on Load. After restore, the consumer
/// emits only deltas — exactly the values that would have been emitted
/// by the producer continuing from the same point.
/// </summary>
public class AggregateSnapshotTests : IDisposable
{
    private readonly string _snapshotDir;

    public AggregateSnapshotTests()
    {
        _snapshotDir = Path.Combine(
            Path.GetTempPath(), "dbspnet-aggregate-" + Guid.NewGuid().ToString("N"));
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
    public void Sum_RestoredState_EmitsOnlyDeltaOnNextTick()
    {
        // Producer: per-category running sums. Two ticks of inserts so
        // the producer's _aggCache holds non-trivial values to rebuild.
        var producer = Compile(
            ["CREATE TABLE sales (cat VARCHAR(8) NOT NULL, amt BIGINT NOT NULL)"],
            "SELECT cat, SUM(amt) FROM sales GROUP BY cat");
        producer.Table("sales").Insert("a", 10L);
        producer.Table("sales").Insert("b", 20L);
        producer.Step();   // emits (a,10):+1, (b,20):+1

        producer.Table("sales").Insert("a", 5L);
        producer.Step();   // emits (a,10):-1, (a,15):+1

        Snapshot.Write(producer.Circuit, _snapshotDir);

        // Consumer: fresh circuit from the same plan; restore state.
        var consumer = Compile(
            ["CREATE TABLE sales (cat VARCHAR(8) NOT NULL, amt BIGINT NOT NULL)"],
            "SELECT cat, SUM(amt) FROM sales GROUP BY cat");
        Snapshot.Read(consumer.Circuit, _snapshotDir);

        // Insert another 'a' value: the operator must know prev sum was 15
        // to retract (a,15) and emit (a,18).
        consumer.Table("sales").Insert("a", 3L);
        consumer.Step();

        Assert.Equal(2, consumer.Current.Count);
        Assert.Equal(-1, consumer.WeightOf("a", 15L).Value);
        Assert.Equal(1, consumer.WeightOf("a", 18L).Value);
    }

    [Fact]
    public void Sum_RestoredState_HandlesRetractionToZero()
    {
        // Producer: insert 'a':10 and 'a':5 — running sum = 15. Retract 5.
        var producer = Compile(
            ["CREATE TABLE sales (cat VARCHAR(8) NOT NULL, amt BIGINT NOT NULL)"],
            "SELECT cat, SUM(amt) FROM sales GROUP BY cat");
        producer.Table("sales").Insert("a", 10L);
        producer.Table("sales").Insert("a", 5L);
        producer.Step();

        Snapshot.Write(producer.Circuit, _snapshotDir);

        var consumer = Compile(
            ["CREATE TABLE sales (cat VARCHAR(8) NOT NULL, amt BIGINT NOT NULL)"],
            "SELECT cat, SUM(amt) FROM sales GROUP BY cat");
        Snapshot.Read(consumer.Circuit, _snapshotDir);

        // Retract one of the 'a' rows: prev sum 15 → new sum 10.
        consumer.Table("sales").Delete("a", 5L);
        consumer.Step();

        Assert.Equal(2, consumer.Current.Count);
        Assert.Equal(-1, consumer.WeightOf("a", 15L).Value);
        Assert.Equal(1, consumer.WeightOf("a", 10L).Value);
    }

    [Fact]
    public void MinMax_RestoredState_EmitsCorrectDelta()
    {
        // Restoring MIN/MAX exercises the Counts dictionary + Active sorted
        // set state — both must be rebuilt from the trace for the next
        // tick to produce a correct retract/emit pair.
        var producer = Compile(
            ["CREATE TABLE sales (cat VARCHAR(8) NOT NULL, amt BIGINT NOT NULL)"],
            "SELECT cat, MIN(amt), MAX(amt) FROM sales GROUP BY cat");
        producer.Table("sales").Insert("a", 10L);
        producer.Table("sales").Insert("a", 5L);
        producer.Table("sales").Insert("a", 20L);
        producer.Step();

        Snapshot.Write(producer.Circuit, _snapshotDir);

        var consumer = Compile(
            ["CREATE TABLE sales (cat VARCHAR(8) NOT NULL, amt BIGINT NOT NULL)"],
            "SELECT cat, MIN(amt), MAX(amt) FROM sales GROUP BY cat");
        Snapshot.Read(consumer.Circuit, _snapshotDir);

        // New min: prev (5,20) → new (3,20).
        consumer.Table("sales").Insert("a", 3L);
        consumer.Step();

        Assert.Equal(2, consumer.Current.Count);
        Assert.Equal(-1, consumer.WeightOf("a", 5L, 20L).Value);
        Assert.Equal(1, consumer.WeightOf("a", 3L, 20L).Value);
    }

    [Fact]
    public void Avg_RestoredState_AccumulatesAcrossSnapshot()
    {
        var producer = Compile(
            ["CREATE TABLE sales (cat VARCHAR(8) NOT NULL, amt BIGINT NOT NULL)"],
            "SELECT cat, AVG(amt) FROM sales GROUP BY cat");
        producer.Table("sales").Insert("a", 10L);
        producer.Table("sales").Insert("a", 20L);
        producer.Step();   // avg = 15.0

        Snapshot.Write(producer.Circuit, _snapshotDir);

        var consumer = Compile(
            ["CREATE TABLE sales (cat VARCHAR(8) NOT NULL, amt BIGINT NOT NULL)"],
            "SELECT cat, AVG(amt) FROM sales GROUP BY cat");
        Snapshot.Read(consumer.Circuit, _snapshotDir);

        // After restore, avg should still know NonNullCount=2, Sum=30.
        // Adding 30 → avg becomes 60/3 = 20.0.
        consumer.Table("sales").Insert("a", 30L);
        consumer.Step();

        Assert.Equal(2, consumer.Current.Count);
        Assert.Equal(-1, consumer.WeightOf("a", 15.0).Value);
        Assert.Equal(1, consumer.WeightOf("a", 20.0).Value);
    }

    [Fact]
    public void NoCodec_OperatorThrowsOnSnapshot()
    {
        // Compile WITHOUT a codec registry — IncrementalAggregateOp has no codec.
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        resolver.Resolve(Parser.ParseStatement(
            "CREATE TABLE sales (cat VARCHAR(8) NOT NULL, amt BIGINT NOT NULL)"));
        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(
            "SELECT cat, SUM(amt) FROM sales GROUP BY cat"))).Query;
        var query = PlanToCircuit.Compile(plan);  // no codecs

        query.Table("sales").Insert("a", 10L);
        query.Step();

        Assert.Throws<NotSupportedException>(() =>
            Snapshot.Write(query.Circuit, _snapshotDir));
    }

    [Fact]
    public void EmptyGroup_AfterSnapshot_NewKeyEmitsAtPlusOne()
    {
        // Producer has only group 'a'. Consumer inserts a brand-new 'b'.
        // No retraction expected for 'b' (it didn't exist before).
        var producer = Compile(
            ["CREATE TABLE sales (cat VARCHAR(8) NOT NULL, amt BIGINT NOT NULL)"],
            "SELECT cat, SUM(amt) FROM sales GROUP BY cat");
        producer.Table("sales").Insert("a", 10L);
        producer.Step();

        Snapshot.Write(producer.Circuit, _snapshotDir);

        var consumer = Compile(
            ["CREATE TABLE sales (cat VARCHAR(8) NOT NULL, amt BIGINT NOT NULL)"],
            "SELECT cat, SUM(amt) FROM sales GROUP BY cat");
        Snapshot.Read(consumer.Circuit, _snapshotDir);

        consumer.Table("sales").Insert("b", 7L);
        consumer.Step();

        Assert.Equal(1, consumer.Current.Count);
        Assert.Equal(1, consumer.WeightOf("b", 7L).Value);
    }
}
