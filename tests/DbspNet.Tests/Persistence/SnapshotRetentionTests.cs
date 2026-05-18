using System.Text;
using DbspNet.Core.Circuit;
using DbspNet.Persistence;

namespace DbspNet.Tests.Persistence;

/// <summary>
/// Snapshot retention: <c>Snapshot.Write</c> takes a
/// <c>retainCount</c> that controls how many of the most-recent
/// snapshots survive on disk; older ones get pruned. The current.txt
/// pointer always names the just-written snapshot, regardless of how
/// many older ones are retained. <see cref="Snapshot.ListSnapshots"/>
/// returns retained ticks in ascending order.
/// </summary>
public class SnapshotRetentionTests : IDisposable
{
    private readonly string _snapshotDir;

    public SnapshotRetentionTests()
    {
        _snapshotDir = Path.Combine(
            Path.GetTempPath(), "dbspnet-retention-" + Guid.NewGuid().ToString("N"));
    }

    public void Dispose()
    {
        if (Directory.Exists(_snapshotDir))
        {
            Directory.Delete(_snapshotDir, recursive: true);
        }

        GC.SuppressFinalize(this);
    }

    private sealed class CounterOp : IOperator, ISnapshotable
    {
        public int Value { get; set; }

        public void Step() => Value++;

        public void Save(ISnapshotWriter writer)
        {
            using var stream = writer.OpenWrite("value.txt");
            using var sw = new StreamWriter(stream, Encoding.UTF8);
            sw.Write(Value);
        }

        public void Load(ISnapshotReader reader)
        {
            using var stream = reader.OpenRead("value.txt");
            using var sr = new StreamReader(stream, Encoding.UTF8);
            Value = int.Parse(sr.ReadToEnd(), System.Globalization.CultureInfo.InvariantCulture);
        }

        public string SchemaFingerprint => "counter";
    }

    private static RootCircuit Build(IOperator op)
    {
        return RootCircuit.Build(builder => builder.AddRawOperator(op));
    }

    [Fact]
    public void RetainCount1_KeepsOnlyLatest()
    {
        var op = new CounterOp { Value = 0 };
        var circuit = Build(op);

        for (var i = 0; i < 5; i++)
        {
            circuit.Step();
            op.Value = (int)circuit.TickCount;
            Snapshot.Write(circuit, _snapshotDir, retainCount: 1);
        }

        var ticks = Snapshot.ListSnapshots(_snapshotDir);
        Assert.Single(ticks);
        Assert.Equal(5L, ticks[0]);
    }

    [Fact]
    public void RetainCount3_KeepsLastThree()
    {
        var op = new CounterOp { Value = 0 };
        var circuit = Build(op);

        for (var i = 0; i < 5; i++)
        {
            circuit.Step();
            op.Value = (int)circuit.TickCount;
            Snapshot.Write(circuit, _snapshotDir, retainCount: 3);
        }

        var ticks = Snapshot.ListSnapshots(_snapshotDir);
        Assert.Equal(new long[] { 3, 4, 5 }, ticks);
    }

    [Fact]
    public void RetainCount_LargerThanWritten_KeepsAll()
    {
        var op = new CounterOp { Value = 0 };
        var circuit = Build(op);

        for (var i = 0; i < 3; i++)
        {
            circuit.Step();
            op.Value = (int)circuit.TickCount;
            Snapshot.Write(circuit, _snapshotDir, retainCount: 10);
        }

        var ticks = Snapshot.ListSnapshots(_snapshotDir);
        Assert.Equal(new long[] { 1, 2, 3 }, ticks);
    }

    [Fact]
    public void Read_AlwaysLoadsLatest_RegardlessOfRetainedHistory()
    {
        var op = new CounterOp();
        var circuit = Build(op);

        circuit.Step();   // tick 1
        op.Value = 100;
        Snapshot.Write(circuit, _snapshotDir, retainCount: 5);

        circuit.Step();   // tick 2
        op.Value = 200;
        Snapshot.Write(circuit, _snapshotDir, retainCount: 5);

        circuit.Step();   // tick 3
        op.Value = 300;
        Snapshot.Write(circuit, _snapshotDir, retainCount: 5);

        // Latest is tick 3 with Value=300.
        var consumerOp = new CounterOp();
        var consumer = Build(consumerOp);
        Snapshot.Read(consumer, _snapshotDir);
        Assert.Equal(300, consumerOp.Value);
    }

    [Fact]
    public void RetainCount_ShrinksAcrossWrites_PrunesExtras()
    {
        var op = new CounterOp { Value = 0 };
        var circuit = Build(op);

        // Establish 5 snapshots with retainCount=5.
        for (var i = 0; i < 5; i++)
        {
            circuit.Step();
            Snapshot.Write(circuit, _snapshotDir, retainCount: 5);
        }

        Assert.Equal(5, Snapshot.ListSnapshots(_snapshotDir).Count);

        // Tighten to retainCount=2 on the next write — the prune happens
        // at write-time so older retained snapshots get cleaned.
        circuit.Step();
        Snapshot.Write(circuit, _snapshotDir, retainCount: 2);

        var ticks = Snapshot.ListSnapshots(_snapshotDir);
        Assert.Equal(new long[] { 5, 6 }, ticks);
    }

    [Fact]
    public void RetainCount_ZeroOrNegative_Throws()
    {
        var op = new CounterOp();
        var circuit = Build(op);

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            Snapshot.Write(circuit, _snapshotDir, retainCount: 0));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            Snapshot.Write(circuit, _snapshotDir, retainCount: -1));
    }

    [Fact]
    public void ListSnapshots_OnEmptyDir_ReturnsEmpty()
    {
        Assert.Empty(Snapshot.ListSnapshots(_snapshotDir));
    }

    [Fact]
    public void ListSnapshots_IgnoresUnrelatedDirectoryEntries()
    {
        // Drop a non-snap-T directory and a malformed snap-* name —
        // ListSnapshots should ignore both.
        Directory.CreateDirectory(Path.Combine(_snapshotDir, "snap-bad"));
        Directory.CreateDirectory(Path.Combine(_snapshotDir, "random-dir"));
        File.WriteAllText(Path.Combine(_snapshotDir, "current.txt"), "snap-bad");

        var op = new CounterOp();
        var circuit = Build(op);
        circuit.Step();
        Snapshot.Write(circuit, _snapshotDir);

        var ticks = Snapshot.ListSnapshots(_snapshotDir);
        Assert.Equal(new long[] { 1 }, ticks);
    }

    [Fact]
    public void RetainCount_AcrossWalRecorderWriteSnapshot()
    {
        // WalRecorder.WriteSnapshot accepts a snapshotRetainCount and
        // forwards it to Snapshot.Write — verifies the WAL layer
        // doesn't break the retention guarantee.
        var walDir = _snapshotDir + "-wal";
        try
        {
            var producer = SqlSnapshotTestSupport.Compile();
            using var wal = new WalRecorder(producer, walDir, _snapshotDir);

            for (var i = 0; i < 4; i++)
            {
                producer.Table("sales").Insert("a", (long)(i + 1));
                wal.Step();
                wal.WriteSnapshot(snapshotRetainCount: 2);
            }

            var ticks = Snapshot.ListSnapshots(_snapshotDir);
            Assert.Equal(new long[] { 3, 4 }, ticks);
        }
        finally
        {
            if (Directory.Exists(walDir))
            {
                Directory.Delete(walDir, recursive: true);
            }
        }
    }
}

/// <summary>
/// Shared compile helper for retention tests that reach into the SQL
/// layer (via WalRecorder). Kept inline to avoid pulling in the full
/// HybridSnapshotWalTests scaffolding.
/// </summary>
internal static class SqlSnapshotTestSupport
{
    public static DbspNet.Sql.Compiler.CompiledQuery Compile()
    {
        var catalog = new DbspNet.Sql.Plan.Catalog();
        var resolver = new DbspNet.Sql.Plan.Resolver(catalog);
        resolver.Resolve(DbspNet.Sql.Parser.Parser.ParseStatement(
            "CREATE TABLE sales (cat VARCHAR(8) NOT NULL, amt BIGINT NOT NULL)"));
        var plan = ((DbspNet.Sql.Plan.SelectPlan)resolver.Resolve(
            DbspNet.Sql.Parser.Parser.ParseStatement(
                "SELECT cat, SUM(amt) FROM sales GROUP BY cat"))).Query;
        return DbspNet.Sql.Compiler.PlanToCircuit.Compile(
            plan, ArrowSqlSnapshotCodecs.Instance);
    }
}
