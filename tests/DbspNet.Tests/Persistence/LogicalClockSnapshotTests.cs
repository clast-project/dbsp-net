// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Circuit;
using DbspNet.Persistence;

namespace DbspNet.Tests.Persistence;

/// <summary>
/// The temporal-filter logical clock (<see cref="RootCircuit.LogicalTime"/>)
/// must survive a snapshot/restore so a restarted circuit reproduces the same
/// <c>NOW()</c> rather than restarting "unset". The value rides in the snapshot
/// manifest alongside the tick count.
/// </summary>
public class LogicalClockSnapshotTests : IDisposable
{
    private readonly string _snapshotDir;

    public LogicalClockSnapshotTests()
    {
        _snapshotDir = Path.Combine(
            Path.GetTempPath(), "dbspnet-clock-snap-" + Guid.NewGuid().ToString("N"));
    }

    public void Dispose()
    {
        if (Directory.Exists(_snapshotDir))
        {
            Directory.Delete(_snapshotDir, recursive: true);
        }

        GC.SuppressFinalize(this);
    }

    [Fact]
    public async Task SnapshotRestore_RestoresLogicalTime()
    {
        var c = RootCircuit.Build(_ => { });
        c.AdvanceTime(1_700_000_000_000_000); // microseconds since epoch
        c.Step();

        await Snapshot.WriteAsync(c, _snapshotDir);

        // A fresh circuit built from the same (empty) plan starts unset…
        var restored = RootCircuit.Build(_ => { });
        Assert.Equal(long.MinValue, restored.LogicalTime);

        // …and picks up the persisted clock on load.
        await Snapshot.ReadAsync(restored, _snapshotDir);
        Assert.Equal(1_700_000_000_000_000, restored.LogicalTime);
    }

    [Fact]
    public async Task SnapshotRestore_NoTemporalFilter_RoundTripsUnsetSentinel()
    {
        // A circuit that never advanced the clock records the unset sentinel
        // and restores it unchanged — temporal-filter-free queries are
        // unaffected by the new manifest field.
        var c = RootCircuit.Build(_ => { });
        c.Step();
        await Snapshot.WriteAsync(c, _snapshotDir);

        var restored = RootCircuit.Build(_ => { });
        await Snapshot.ReadAsync(restored, _snapshotDir);
        Assert.Equal(long.MinValue, restored.LogicalTime);
    }
}
