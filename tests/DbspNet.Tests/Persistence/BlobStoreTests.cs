// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Buffers;
using System.Globalization;
using System.Text;
using DbspNet.Core.Circuit;
using DbspNet.Core.IO;
using DbspNet.Persistence;
using DbspNet.Persistence.IO;
using DbspNet.Persistence.IO.Local;

namespace DbspNet.Tests.Persistence;

/// <summary>
/// High-level integration tests that drive
/// <see cref="Snapshot"/>/<see cref="WalRecorder"/> through an
/// explicit <see cref="ITableFileSystem"/> reference (no string-path
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

        public async ValueTask SaveAsync(ISnapshotWriter writer, CancellationToken cancellationToken = default)
        {
            await using var file = await writer.CreateAsync("value.txt", cancellationToken).ConfigureAwait(false);
            var bytes = Encoding.UTF8.GetBytes(Value.ToString(CultureInfo.InvariantCulture));
            await file.WriteAsync(bytes, cancellationToken).ConfigureAwait(false);
        }

        public async ValueTask LoadAsync(ISnapshotReader reader, CancellationToken cancellationToken = default)
        {
            await using var file = await reader.OpenReadAsync("value.txt", cancellationToken).ConfigureAwait(false);
            var length = await file.GetLengthAsync(cancellationToken).ConfigureAwait(false);
            using var owner = await file.ReadAsync(new FileRange(0, length), cancellationToken).ConfigureAwait(false);
            var text = Encoding.UTF8.GetString(owner.Memory.Span[..(int)length]);
            Value = int.Parse(text, CultureInfo.InvariantCulture);
        }

        public string SchemaFingerprint => "counter";
    }

    private static RootCircuit Build(IOperator op) =>
        RootCircuit.Build(builder => builder.AddRawOperator(op));

    [Fact]
    public async Task Snapshot_RoundTripsThroughLocalFileBlobStore()
    {
        var fs = new LocalTableFileSystem(_root);

        var producerOp = new CounterOp { Value = 42 };
        var producer = Build(producerOp);
        await Snapshot.WriteAsync(producer, fs);

        Assert.True(await Snapshot.ExistsAsync(fs));

        var consumerOp = new CounterOp();
        var consumer = Build(consumerOp);
        await Snapshot.ReadAsync(consumer, fs);
        Assert.Equal(42, consumerOp.Value);
    }

    [Fact]
    public async Task Snapshot_RoundTripsThroughInMemoryBlobStore()
    {
        // Identical scenario to the local case, but backed by
        // InMemoryTableFileSystem — proves Snapshot doesn't accidentally rely
        // on filesystem semantics. This is the cloud-parity smoke
        // test: any cloud impl that satisfies BlobStoreContractTests
        // will also pass this.
        var fs = new InMemoryTableFileSystem();

        var producerOp = new CounterOp { Value = 42 };
        var producer = Build(producerOp);
        await Snapshot.WriteAsync(producer, fs);

        Assert.True(await Snapshot.ExistsAsync(fs));

        var consumerOp = new CounterOp();
        var consumer = Build(consumerOp);
        await Snapshot.ReadAsync(consumer, fs);
        Assert.Equal(42, consumerOp.Value);
    }

    [Fact]
    public async Task Snapshot_ListSnapshots_ThroughIBlobStore()
    {
        var fs = new InMemoryTableFileSystem();

        var op = new CounterOp();
        var circuit = Build(op);
        for (var i = 0; i < 3; i++)
        {
            circuit.Step();
            await Snapshot.WriteAsync(circuit, fs, retainCount: 5);
        }

        var ticks = await Snapshot.ListSnapshotsAsync(fs);
        Assert.Equal(new long[] { 1, 2, 3 }, ticks);
    }

    [Fact]
    public async Task WalRecorder_AcceptsIBlobStoreDirectly()
    {
        // WalRecorder has long-lived OpenWrite streams (ArrowDeltaWriter
        // wraps them across many ticks). Verifies the abstraction
        // supports that pattern — cloud impls back this with multipart
        // upload, in-memory impl with a buffered MemoryStream.
#pragma warning disable CA1859
        ITableFileSystem walFs = new InMemoryTableFileSystem();
        ITableFileSystem snapshotFs = new InMemoryTableFileSystem();
#pragma warning restore CA1859

        var query = SqlSnapshotTestSupport.Compile();
        await using (var wal = await WalRecorder.CreateAsync(query, walFs, snapshotFs))
        {
            query.Table("sales").Insert("a", 10L);
            await wal.StepAsync();
            await wal.WriteSnapshotAsync();
        }

        Assert.True(await Snapshot.ExistsAsync(snapshotFs));
        var walEntries = new List<string>();
        await foreach (var entry in walFs.ListAsync(""))
        {
            walEntries.Add(entry.Path);
        }

        Assert.NotEmpty(walEntries);
    }

    [Fact]
    public async Task HybridReplay_WorksAcrossInMemoryRestart()
    {
        // Restart pattern: open recorder, push and step, snapshot, dispose;
        // open a fresh recorder against the same in-memory stores; the
        // restored state should match what the producer had at snapshot
        // time. Proves the cloud-parity story end-to-end: snapshot +
        // WAL replay through a non-filesystem backend.
#pragma warning disable CA1859
        ITableFileSystem walFs = new InMemoryTableFileSystem();
        ITableFileSystem snapshotFs = new InMemoryTableFileSystem();
#pragma warning restore CA1859

        long producerTick;
        {
            var producer = SqlSnapshotTestSupport.Compile();
            await using var wal = await WalRecorder.CreateAsync(producer, walFs, snapshotFs);
            producer.Table("sales").Insert("a", 10L);
            producer.Table("sales").Insert("b", 20L);
            await wal.StepAsync();
            await wal.WriteSnapshotAsync();
            producer.Table("sales").Insert("a", 5L);
            await wal.StepAsync();
            producerTick = producer.Circuit.TickCount;
        }

        var consumer = SqlSnapshotTestSupport.Compile();
        await using (var wal = await WalRecorder.CreateAsync(consumer, walFs, snapshotFs))
        {
            Assert.Equal(producerTick, consumer.Circuit.TickCount);
        }
    }
}
