using DbspNet.Persistence;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

namespace DbspNet.Tests.Persistence;

/// <summary>
/// First end-to-end snapshot of a real stateful operator: <c>SELECT DISTINCT</c>
/// builds a <c>DistinctOp</c> whose internal <c>ZSetTrace</c> holds the
/// dedup history. Snapshot the trace, rebuild a fresh circuit from the
/// same plan with the same codec, restore — verify subsequent ticks
/// behave as if the consumer had seen all the producer's history.
/// </summary>
public class DistinctSnapshotTests : IDisposable
{
    private readonly string _snapshotDir;

    public DistinctSnapshotTests()
    {
        _snapshotDir = Path.Combine(
            Path.GetTempPath(), "dbspnet-distinct-" + Guid.NewGuid().ToString("N"));
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
    public void SnapshotAndRestore_DeduplicatesAcrossSessions()
    {
        // Producer: insert duplicates across two ticks; SELECT DISTINCT
        // emits the new row +1 the first time, ignores subsequent
        // duplicate inserts.
        var producer = Compile(
            ["CREATE TABLE t (id INT NOT NULL)"],
            "SELECT id FROM t UNION SELECT id FROM t");
        producer.Table("t").Insert(1);
        producer.Table("t").Insert(2);
        producer.Step();           // delta: {1: +1, 2: +1}

        producer.Table("t").Insert(1);  // duplicate
        producer.Table("t").Insert(3);
        producer.Step();           // delta: {3: +1} — id=1 was already present

        // Snapshot.
        Snapshot.Write(producer.Circuit, _snapshotDir);

        // Consumer: fresh circuit from the same plan. Restore the
        // DistinctOp's trace from the snapshot.
        var consumer = Compile(
            ["CREATE TABLE t (id INT NOT NULL)"],
            "SELECT id FROM t UNION SELECT id FROM t");
        Snapshot.Read(consumer.Circuit, _snapshotDir);

        // After restore, the DistinctOp knows ids 1, 2, 3 are already
        // present. Inserting another duplicate of id=1 should produce
        // an empty delta.
        consumer.Table("t").Insert(1);
        consumer.Step();
        Assert.True(consumer.Current.IsEmpty);

        // Inserting a new value should produce +1 for that value only.
        consumer.Table("t").Insert(4);
        consumer.Step();
        Assert.Equal(1, consumer.Current.Count);
        Assert.Equal(1, consumer.WeightOf(4).Value);
    }

    [Fact]
    public void RestoredState_HandlesRetraction()
    {
        // Producer: insert 1 and 2, snapshot, retract 1.
        var producer = Compile(
            ["CREATE TABLE t (id INT NOT NULL)"],
            "SELECT id FROM t UNION SELECT id FROM t");
        producer.Table("t").Insert(1);
        producer.Table("t").Insert(2);
        producer.Step();
        Snapshot.Write(producer.Circuit, _snapshotDir);

        // Consumer: restore, retract 1.
        var consumer = Compile(
            ["CREATE TABLE t (id INT NOT NULL)"],
            "SELECT id FROM t UNION SELECT id FROM t");
        Snapshot.Read(consumer.Circuit, _snapshotDir);

        consumer.Table("t").Delete(1);
        consumer.Step();

        // The trace's accumulated weight for id=1 was +1 before the
        // delete; the delete makes it 0 → the dedup output emits −1.
        Assert.Equal(1, consumer.Current.Count);
        Assert.Equal(-1, consumer.WeightOf(1).Value);
    }

    [Fact]
    public void NoCodec_OperatorsThrowOnSnapshot()
    {
        // Compile WITHOUT a codec registry — DistinctOp has no codec.
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        resolver.Resolve(Parser.ParseStatement("CREATE TABLE t (id INT NOT NULL)"));
        var plan = ((SelectPlan)resolver.Resolve(
            Parser.ParseStatement("SELECT id FROM t UNION SELECT id FROM t"))).Query;
        var query = PlanToCircuit.Compile(plan);  // no codecs

        query.Table("t").Insert(1);
        query.Step();

        Assert.Throws<NotSupportedException>(() =>
            Snapshot.Write(query.Circuit, _snapshotDir));
    }
}
