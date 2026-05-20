// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Buffers;
using System.Globalization;
using System.Text;
using DbspNet.Core.Circuit;
using DbspNet.Core.IO;
using DbspNet.Persistence;

namespace DbspNet.Tests.Persistence;

/// <summary>
/// Verifies the snapshot foundation (manifest, plan fingerprint, write /
/// read orchestration) using a mock <see cref="ISnapshotable"/> op so
/// the test doesn't depend on any specific stateful operator's
/// serialisation. Real operators get round-tripped in chunk 3+.
/// </summary>
public class SnapshotFoundationTests : IDisposable
{
    private readonly string _snapshotDir;

    public SnapshotFoundationTests()
    {
        _snapshotDir = Path.Combine(
            Path.GetTempPath(), "dbspnet-snapshot-" + Guid.NewGuid().ToString("N"));
    }

    public void Dispose()
    {
        if (Directory.Exists(_snapshotDir))
        {
            Directory.Delete(_snapshotDir, recursive: true);
        }

        GC.SuppressFinalize(this);
    }

    /// <summary>Test-only stateful op that holds an int and round-trips it.</summary>
    private sealed class CounterOp : IOperator, ISnapshotable
    {
        public int Value { get; set; }

        public void Step()
        {
            Value++;
        }

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

        public string SchemaFingerprint => "counter-op";
    }

    /// <summary>Non-snapshottable op for sanity (skipped during save/load).</summary>
    private sealed class PassiveOp : IOperator
    {
        public void Step() { }
    }

    private static RootCircuit Build(params IOperator[] ops)
    {
        return RootCircuit.Build(builder =>
        {
            foreach (var op in ops)
            {
                builder.AddRawOperator(op);
            }
        });
    }

    [Fact]
    public async Task Write_CreatesSnapDirWithManifestAndPerOperatorSubdirs()
    {
        var c = new CounterOp { Value = 7 };
        var p = new PassiveOp();
        var circuit = Build(c, p);

        var snapshotted = await Snapshot.WriteAsync(circuit, _snapshotDir);

        // Layout: {snapshotDir}/snap-{tick}/{manifest.json, op-0/, ...}
        // plus {snapshotDir}/current.txt naming the latest snap-T.
        Assert.Equal(1, snapshotted);  // PassiveOp skipped
        var snapDir = Path.Combine(_snapshotDir, "snap-0");
        Assert.True(File.Exists(Path.Combine(_snapshotDir, "current.txt")));
        Assert.True(File.Exists(Path.Combine(snapDir, "manifest.json")));
        Assert.True(File.Exists(Path.Combine(snapDir, "op-0", "value.txt")));
        Assert.False(Directory.Exists(Path.Combine(snapDir, "op-1")));

        var manifest = await SnapshotManifest.ReadAsync(Path.Combine(snapDir, "manifest.json"));
        Assert.Equal(2, manifest.OperatorCount);
        Assert.Equal(new[] { 0 }, manifest.SnapshottedIndices);
    }

    [Fact]
    public async Task Read_RestoresStateIntoFreshCircuit()
    {
        // Producer: build, mutate state, snapshot.
        var producerCounter = new CounterOp { Value = 42 };
        var producer = Build(producerCounter, new PassiveOp());
        await Snapshot.WriteAsync(producer, _snapshotDir);

        // Consumer: same plan shape, fresh state, load snapshot.
        var consumerCounter = new CounterOp { Value = 0 };
        var consumer = Build(consumerCounter, new PassiveOp());
        var restored = await Snapshot.ReadAsync(consumer, _snapshotDir);

        Assert.Equal(1, restored);
        Assert.Equal(42, consumerCounter.Value);
    }

    [Fact]
    public async Task Read_WrongOperatorCount_Throws()
    {
        var producer = Build(new CounterOp { Value = 1 }, new PassiveOp());
        await Snapshot.WriteAsync(producer, _snapshotDir);

        // Consumer has fewer operators — fingerprint hashes the type
        // sequence so this trips both the explicit count check and the
        // fingerprint check.
        var consumer = Build(new CounterOp { Value = 0 });

        await Assert.ThrowsAsync<InvalidDataException>(async () => await Snapshot.ReadAsync(consumer, _snapshotDir));
    }

    [Fact]
    public async Task Read_ReorderedOperators_ThrowsFingerprintMismatch()
    {
        var producer = Build(new CounterOp { Value = 1 }, new PassiveOp());
        await Snapshot.WriteAsync(producer, _snapshotDir);

        // Same operator types but different order — fingerprint includes
        // the position, so this should fail.
        var consumer = Build(new PassiveOp(), new CounterOp { Value = 0 });

        var ex = await Assert.ThrowsAsync<InvalidDataException>(
            async () => await Snapshot.ReadAsync(consumer, _snapshotDir));
        Assert.Contains("fingerprint mismatch", ex.Message,
            StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Manifest_TickAndSchemaVersion_Recorded()
    {
        var circuit = Build(new CounterOp { Value = 0 });
        circuit.Step();
        circuit.Step();
        circuit.Step();

        await Snapshot.WriteAsync(circuit, _snapshotDir);
        var snapDir = Path.Combine(_snapshotDir, "snap-3");
        var manifest = await SnapshotManifest.ReadAsync(Path.Combine(snapDir, "manifest.json"));

        Assert.Equal(SnapshotManifest.CurrentSchemaVersion, manifest.SchemaVersion);
        Assert.Equal(3, manifest.Tick);
    }

    [Fact]
    public async Task Read_MissingManifest_Throws()
    {
        var consumer = Build(new CounterOp { Value = 0 });
        Directory.CreateDirectory(_snapshotDir);  // empty dir, no manifest

        await Assert.ThrowsAsync<FileNotFoundException>(
            async () => await Snapshot.ReadAsync(consumer, _snapshotDir));
    }
}
