// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Persistence;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

namespace DbspNet.Tests.Persistence;

/// <summary>
/// End-to-end snapshot + WAL hybrid: <c>WalRecorder.WriteSnapshotAsync</c>
/// captures end-of-tick operator state and prunes the WAL prefix; reopen
/// loads the snapshot then replays only the WAL tail. Together these are
/// approach (C) from <c>docs/persistence.md</c>.
/// </summary>
public class HybridSnapshotWalTests : IDisposable
{
    private readonly string _walDir;
    private readonly string _snapshotDir;

    public HybridSnapshotWalTests()
    {
        var stem = Guid.NewGuid().ToString("N");
        _walDir = Path.Combine(Path.GetTempPath(), "dbspnet-hybrid-wal-" + stem);
        _snapshotDir = Path.Combine(Path.GetTempPath(), "dbspnet-hybrid-snap-" + stem);
    }

    public void Dispose()
    {
        if (Directory.Exists(_walDir))
        {
            Directory.Delete(_walDir, recursive: true);
        }

        if (Directory.Exists(_snapshotDir))
        {
            Directory.Delete(_snapshotDir, recursive: true);
        }

        GC.SuppressFinalize(this);
    }

    private static CompiledQuery Compile()
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        resolver.Resolve(Parser.ParseStatement(
            "CREATE TABLE sales (cat VARCHAR(8) NOT NULL, amt BIGINT NOT NULL)"));
        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(
            "SELECT cat, SUM(amt) FROM sales GROUP BY cat"))).Query;
        return PlanToCircuit.Compile(plan, ArrowSqlSnapshotCodecs.Instance);
    }

    [Fact]
    public async Task Reopen_WithSnapshotAndPostSnapshotWal_RestoresFullState()
    {
        // Producer: record some ticks, snapshot, record more.
        long producerTick;
        {
            var producer = Compile();
            await using var wal = await WalRecorder.CreateAsync(producer, _walDir, _snapshotDir);
            producer.Table("sales").Insert("a", 10L);
            producer.Table("sales").Insert("b", 20L);
            await wal.StepAsync();   // tick 1: a=10, b=20

            producer.Table("sales").Insert("a", 5L);
            await wal.StepAsync();   // tick 2: a=15, b=20

            await wal.WriteSnapshotAsync();   // snapshot at tick 2

            producer.Table("sales").Insert("b", 7L);
            await wal.StepAsync();   // tick 3: a=15, b=27

            producerTick = producer.Circuit.TickCount;
        }

        // Consumer: fresh circuit, reopen the same dirs. Snapshot brings
        // state to tick 2 quickly; WAL replays only tick 3.
        var consumer = Compile();
        await using (var wal = await WalRecorder.CreateAsync(consumer, _walDir, _snapshotDir))
        {
            // Verify consumer reached the same absolute tick as producer.
            Assert.Equal(producerTick, consumer.Circuit.TickCount);

            // After replay, push a delta that exercises restored state:
            // adding to 'a' should retract the prior sum and emit a new one.
            consumer.Table("sales").Insert("a", 100L);
            await wal.StepAsync();
        }

        // The most recent delta retracts a=15, emits a=115.
        Assert.Equal(2, consumer.Current.Count);
        Assert.Equal(-1, consumer.WeightOf("a", 15L).Value);
        Assert.Equal(1, consumer.WeightOf("a", 115L).Value);
    }

    [Fact]
    public async Task WriteSnapshot_PrunesPreSnapshotSegmentFiles()
    {
        var producer = Compile();
        await using (var wal = await WalRecorder.CreateAsync(producer, _walDir, _snapshotDir))
        {
            producer.Table("sales").Insert("a", 10L);
            await wal.StepAsync();
            await wal.WriteSnapshotAsync();
        }

        // The pre-snapshot segment file should have been deleted; only
        // the post-snapshot segment file (segment 1) survives.
        Assert.False(File.Exists(Path.Combine(_walDir, "sales.0.arrows")));
        Assert.True(File.Exists(Path.Combine(_walDir, "sales.1.arrows")));

        // Snapshot directory exists with current.txt + the latest snap-T.
        Assert.True(File.Exists(Path.Combine(_snapshotDir, "current.txt")));
        Assert.True(await Snapshot.ExistsAsync(_snapshotDir));
    }

    [Fact]
    public async Task WriteSnapshot_TwoConsecutiveSnapshots_PruneEachOther()
    {
        var producer = Compile();
        await using var wal = await WalRecorder.CreateAsync(producer, _walDir, _snapshotDir);

        producer.Table("sales").Insert("a", 10L);
        await wal.StepAsync();
        await wal.WriteSnapshotAsync();   // snapshot 1: tick 1, prunes segment 0

        producer.Table("sales").Insert("a", 5L);
        await wal.StepAsync();
        await wal.WriteSnapshotAsync();   // snapshot 2: tick 2, prunes segment 1

        // Only segment 2 (still in flight, no ticks yet) and segment ids
        // pre-snapshot-2 should be gone.
        Assert.False(File.Exists(Path.Combine(_walDir, "sales.0.arrows")));
        Assert.False(File.Exists(Path.Combine(_walDir, "sales.1.arrows")));
    }

    [Fact]
    public async Task Reopen_SnapshotOnly_NoWalEntries_LoadsSnapshot()
    {
        // Producer: snapshot but no post-snapshot ticks.
        {
            var producer = Compile();
            await using var wal = await WalRecorder.CreateAsync(producer, _walDir, _snapshotDir);
            producer.Table("sales").Insert("a", 10L);
            await wal.StepAsync();
            await wal.WriteSnapshotAsync();
            // Dispose without further ticks.
        }

        // Consumer: reopen, push a new delta, verify state was restored.
        var consumer = Compile();
        await using (var wal = await WalRecorder.CreateAsync(consumer, _walDir, _snapshotDir))
        {
            consumer.Table("sales").Insert("a", 5L);
            await wal.StepAsync();
        }

        Assert.Equal(2, consumer.Current.Count);
        Assert.Equal(-1, consumer.WeightOf("a", 10L).Value);
        Assert.Equal(1, consumer.WeightOf("a", 15L).Value);
    }

    [Fact]
    public async Task Reopen_NoSnapshotDir_BackwardCompatWithWalOnly()
    {
        // Snapshot dir omitted entirely — pure WAL replay path.
        {
            var producer = Compile();
            await using var wal = await WalRecorder.CreateAsync(producer, _walDir);
            producer.Table("sales").Insert("a", 10L);
            await wal.StepAsync();
        }

        var consumer = Compile();
        await using (var wal = await WalRecorder.CreateAsync(consumer, _walDir))
        {
            consumer.Table("sales").Insert("a", 5L);
            await wal.StepAsync();
        }

        Assert.Equal(2, consumer.Current.Count);
        Assert.Equal(-1, consumer.WeightOf("a", 10L).Value);
        Assert.Equal(1, consumer.WeightOf("a", 15L).Value);
    }

    [Fact]
    public async Task WriteSnapshot_WithoutSnapshotDir_Throws()
    {
        var query = Compile();
        await using var wal = await WalRecorder.CreateAsync(query, _walDir);   // no snapshotDir

        query.Table("sales").Insert("a", 10L);
        await wal.StepAsync();

        await Assert.ThrowsAsync<InvalidOperationException>(async () => await wal.WriteSnapshotAsync());
    }

    [Fact]
    public async Task WriteSnapshot_WithPendingInput_Throws()
    {
        // Pushing without a Step leaves the per-tick buffer non-empty;
        // WriteSnapshot must reject that to keep WAL/snapshot in sync.
        var query = Compile();
        await using var wal = await WalRecorder.CreateAsync(query, _walDir, _snapshotDir);

        query.Table("sales").Insert("a", 10L);
        // Note: no Step.

        await Assert.ThrowsAsync<InvalidOperationException>(async () => await wal.WriteSnapshotAsync());
    }

    [Fact]
    public async Task TickCount_RestoredAcrossSnapshotReopen()
    {
        // The hybrid path needs absolute tick numbers to align — verify
        // the consumer's circuit.TickCount jumps to the snapshot's tick.
        long producerTick;
        {
            var producer = Compile();
            await using var wal = await WalRecorder.CreateAsync(producer, _walDir, _snapshotDir);
            producer.Table("sales").Insert("a", 10L);
            await wal.StepAsync();
            producer.Table("sales").Insert("a", 5L);
            await wal.StepAsync();
            await wal.WriteSnapshotAsync();
            producerTick = producer.Circuit.TickCount;
        }

        var consumer = Compile();
        await using var consumerWal = await WalRecorder.CreateAsync(consumer, _walDir, _snapshotDir);
        Assert.Equal(producerTick, consumer.Circuit.TickCount);
    }

    [Fact]
    public async Task SnapshotInPath_DoesNotExist_TreatedAsAbsent()
    {
        // Pass a snapshotDir path that has never been written to. The
        // recorder should fall back to the pure-WAL path (no snapshot,
        // no replay-skip).
        {
            var producer = Compile();
            await using var wal = await WalRecorder.CreateAsync(producer, _walDir);
            producer.Table("sales").Insert("a", 10L);
            await wal.StepAsync();
        }

        var consumer = Compile();
        var nonexistentSnapshotDir = Path.Combine(Path.GetTempPath(), "dbspnet-hybrid-missing-" + Guid.NewGuid().ToString("N"));
        try
        {
            await using var wal = await WalRecorder.CreateAsync(consumer, _walDir, nonexistentSnapshotDir);
            Assert.Equal(1L, consumer.Circuit.TickCount);   // pure replay; one tick from WAL
        }
        finally
        {
            if (Directory.Exists(nonexistentSnapshotDir))
            {
                Directory.Delete(nonexistentSnapshotDir, recursive: true);
            }
        }
    }
}
