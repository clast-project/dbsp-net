using System.Text;
using DbspNet.Core.Circuit;
using DbspNet.Persistence;

namespace DbspNet.Tests.Persistence;

/// <summary>
/// High-level integration tests that drive
/// <see cref="Snapshot"/>/<see cref="WalRecorder"/> through an
/// explicit <see cref="IBlobStore"/> reference (no string-path
/// convenience). Proves the abstraction is the load-bearing surface,
/// and that the persistence layer's behaviour is identical regardless
/// of the backing impl. Per-impl contract conformance lives in
/// <see cref="BlobStoreContractTests"/>.
/// </summary>
public class BlobStoreTests : IDisposable
{
    private readonly string _root;

    public BlobStoreTests()
    {
        _root = Path.Combine(
            Path.GetTempPath(), "dbspnet-blobstore-" + Guid.NewGuid().ToString("N"));
    }

    public void Dispose()
    {
        if (Directory.Exists(_root))
        {
            Directory.Delete(_root, recursive: true);
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

    private static RootCircuit Build(IOperator op) =>
        RootCircuit.Build(builder => builder.AddRawOperator(op));

    [Fact]
    public void Snapshot_RoundTripsThroughLocalFileBlobStore()
    {
        var store = new LocalFileBlobStore(_root);

        var producerOp = new CounterOp { Value = 42 };
        var producer = Build(producerOp);
        Snapshot.Write(producer, store);

        Assert.True(Snapshot.Exists(store));

        var consumerOp = new CounterOp();
        var consumer = Build(consumerOp);
        Snapshot.Read(consumer, store);
        Assert.Equal(42, consumerOp.Value);
    }

    [Fact]
    public void Snapshot_RoundTripsThroughInMemoryBlobStore()
    {
        // Identical scenario to the local case, but backed by
        // InMemoryBlobStore — proves Snapshot doesn't accidentally rely
        // on filesystem semantics. This is the cloud-parity smoke
        // test: any cloud impl that satisfies BlobStoreContractTests
        // will also pass this.
        var store = new InMemoryBlobStore();

        var producerOp = new CounterOp { Value = 42 };
        var producer = Build(producerOp);
        Snapshot.Write(producer, store);

        Assert.True(Snapshot.Exists(store));

        var consumerOp = new CounterOp();
        var consumer = Build(consumerOp);
        Snapshot.Read(consumer, store);
        Assert.Equal(42, consumerOp.Value);
    }

    [Fact]
    public void Snapshot_ListSnapshots_ThroughIBlobStore()
    {
        var store = new InMemoryBlobStore();

        var op = new CounterOp();
        var circuit = Build(op);
        for (var i = 0; i < 3; i++)
        {
            circuit.Step();
            Snapshot.Write(circuit, store, retainCount: 5);
        }

        var ticks = Snapshot.ListSnapshots(store);
        Assert.Equal(new long[] { 1, 2, 3 }, ticks);
    }

    [Fact]
    public void WalRecorder_AcceptsIBlobStoreDirectly()
    {
        // WalRecorder has long-lived OpenWrite streams (ArrowDeltaWriter
        // wraps them across many ticks). Verifies the abstraction
        // supports that pattern — cloud impls back this with multipart
        // upload, in-memory impl with a buffered MemoryStream.
#pragma warning disable CA1859
        IBlobStore walStore = new InMemoryBlobStore();
        IBlobStore snapshotStore = new InMemoryBlobStore();
#pragma warning restore CA1859

        var query = SqlSnapshotTestSupport.Compile();
        using (var wal = new WalRecorder(query, walStore, snapshotStore))
        {
            query.Table("sales").Insert("a", 10L);
            wal.Step();
            wal.WriteSnapshot();
        }

        Assert.True(Snapshot.Exists(snapshotStore));
        Assert.NotEmpty(walStore.ListKeys(""));
    }

    [Fact]
    public void HybridReplay_WorksAcrossInMemoryRestart()
    {
        // Restart pattern: open recorder, push and step, snapshot, dispose;
        // open a fresh recorder against the same in-memory stores; the
        // restored state should match what the producer had at snapshot
        // time. Proves the cloud-parity story end-to-end: snapshot +
        // WAL replay through a non-filesystem backend.
#pragma warning disable CA1859
        IBlobStore walStore = new InMemoryBlobStore();
        IBlobStore snapshotStore = new InMemoryBlobStore();
#pragma warning restore CA1859

        long producerTick;
        {
            var producer = SqlSnapshotTestSupport.Compile();
            using var wal = new WalRecorder(producer, walStore, snapshotStore);
            producer.Table("sales").Insert("a", 10L);
            producer.Table("sales").Insert("b", 20L);
            wal.Step();
            wal.WriteSnapshot();
            producer.Table("sales").Insert("a", 5L);
            wal.Step();
            producerTick = producer.Circuit.TickCount;
        }

        var consumer = SqlSnapshotTestSupport.Compile();
        using (var wal = new WalRecorder(consumer, walStore, snapshotStore))
        {
            Assert.Equal(producerTick, consumer.Circuit.TickCount);
        }
    }
}
