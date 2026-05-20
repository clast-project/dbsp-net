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
/// Snapshot retention: <c>Snapshot.WriteAsync</c> takes a
/// <c>retainCount</c> that controls how many of the most-recent
/// snapshots survive on disk; older ones get pruned. The current.txt
/// pointer always names the just-written snapshot, regardless of how
/// many older ones are retained. <see cref="Snapshot.ListSnapshotsAsync(string, CancellationToken)"/>
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

    private static RootCircuit Build(IOperator op)
    {
        return RootCircuit.Build(builder => builder.AddRawOperator(op));
    }

    [Fact]
    public async Task RetainCount1_KeepsOnlyLatest()
    {
        var op = new CounterOp { Value = 0 };
        var circuit = Build(op);

        for (var i = 0; i < 5; i++)
        {
            circuit.Step();
            op.Value = (int)circuit.TickCount;
            await Snapshot.WriteAsync(circuit, _snapshotDir, retainCount: 1);
        }

        var ticks = await Snapshot.ListSnapshotsAsync(_snapshotDir);
        Assert.Single(ticks);
        Assert.Equal(5L, ticks[0]);
    }

    [Fact]
    public async Task RetainCount3_KeepsLastThree()
    {
        var op = new CounterOp { Value = 0 };
        var circuit = Build(op);

        for (var i = 0; i < 5; i++)
        {
            circuit.Step();
            op.Value = (int)circuit.TickCount;
            await Snapshot.WriteAsync(circuit, _snapshotDir, retainCount: 3);
        }

        var ticks = await Snapshot.ListSnapshotsAsync(_snapshotDir);
        Assert.Equal(new long[] { 3, 4, 5 }, ticks);
    }

    [Fact]
    public async Task RetainCount_LargerThanWritten_KeepsAll()
    {
        var op = new CounterOp { Value = 0 };
        var circuit = Build(op);

        for (var i = 0; i < 3; i++)
        {
            circuit.Step();
            op.Value = (int)circuit.TickCount;
            await Snapshot.WriteAsync(circuit, _snapshotDir, retainCount: 10);
        }

        var ticks = await Snapshot.ListSnapshotsAsync(_snapshotDir);
        Assert.Equal(new long[] { 1, 2, 3 }, ticks);
    }

    [Fact]
    public async Task Read_AlwaysLoadsLatest_RegardlessOfRetainedHistory()
    {
        var op = new CounterOp();
        var circuit = Build(op);

        circuit.Step();   // tick 1
        op.Value = 100;
        await Snapshot.WriteAsync(circuit, _snapshotDir, retainCount: 5);

        circuit.Step();   // tick 2
        op.Value = 200;
        await Snapshot.WriteAsync(circuit, _snapshotDir, retainCount: 5);

        circuit.Step();   // tick 3
        op.Value = 300;
        await Snapshot.WriteAsync(circuit, _snapshotDir, retainCount: 5);

        // Latest is tick 3 with Value=300.
        var consumerOp = new CounterOp();
        var consumer = Build(consumerOp);
        await Snapshot.ReadAsync(consumer, _snapshotDir);
        Assert.Equal(300, consumerOp.Value);
    }

    [Fact]
    public async Task RetainCount_ShrinksAcrossWrites_PrunesExtras()
    {
        var op = new CounterOp { Value = 0 };
        var circuit = Build(op);

        // Establish 5 snapshots with retainCount=5.
        for (var i = 0; i < 5; i++)
        {
            circuit.Step();
            await Snapshot.WriteAsync(circuit, _snapshotDir, retainCount: 5);
        }

        Assert.Equal(5, (await Snapshot.ListSnapshotsAsync(_snapshotDir)).Count);

        // Tighten to retainCount=2 on the next write — the prune happens
        // at write-time so older retained snapshots get cleaned.
        circuit.Step();
        await Snapshot.WriteAsync(circuit, _snapshotDir, retainCount: 2);

        var ticks = await Snapshot.ListSnapshotsAsync(_snapshotDir);
        Assert.Equal(new long[] { 5, 6 }, ticks);
    }

    [Fact]
    public async Task RetainCount_ZeroOrNegative_Throws()
    {
        var op = new CounterOp();
        var circuit = Build(op);

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
            await Snapshot.WriteAsync(circuit, _snapshotDir, retainCount: 0));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
            await Snapshot.WriteAsync(circuit, _snapshotDir, retainCount: -1));
    }

    [Fact]
    public async Task ListSnapshots_OnEmptyDir_ReturnsEmpty()
    {
        Assert.Empty(await Snapshot.ListSnapshotsAsync(_snapshotDir));
    }

    [Fact]
    public async Task ListSnapshots_IgnoresUnrelatedDirectoryEntries()
    {
        // Drop a non-snap-T directory and a malformed snap-* name —
        // ListSnapshots should ignore both.
        Directory.CreateDirectory(Path.Combine(_snapshotDir, "snap-bad"));
        Directory.CreateDirectory(Path.Combine(_snapshotDir, "random-dir"));
        File.WriteAllText(Path.Combine(_snapshotDir, "current.txt"), "snap-bad");

        var op = new CounterOp();
        var circuit = Build(op);
        circuit.Step();
        await Snapshot.WriteAsync(circuit, _snapshotDir);

        var ticks = await Snapshot.ListSnapshotsAsync(_snapshotDir);
        Assert.Equal(new long[] { 1 }, ticks);
    }

    [Fact]
    public async Task RetainCount_AcrossWalRecorderWriteSnapshot()
    {
        // WalRecorder.WriteSnapshot accepts a snapshotRetainCount and
        // forwards it to Snapshot.Write — verifies the WAL layer
        // doesn't break the retention guarantee.
        var walDir = _snapshotDir + "-wal";
        try
        {
            var producer = SqlSnapshotTestSupport.Compile();
            await using var wal = await WalRecorder.CreateAsync(producer, walDir, _snapshotDir);

            for (var i = 0; i < 4; i++)
            {
                producer.Table("sales").Insert("a", (long)(i + 1));
                await wal.StepAsync();
                await wal.WriteSnapshotAsync(snapshotRetainCount: 2);
            }

            var ticks = await Snapshot.ListSnapshotsAsync(_snapshotDir);
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
