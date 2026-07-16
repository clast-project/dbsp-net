// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System;
using System.Threading;
using System.Threading.Tasks;
using DbspNet.Connectors.Abstractions;
using DbspNet.Persistence;
using DbspNet.Persistence.IO;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;
using SqlSchema = DbspNet.Sql.Plan.Schema;

namespace DbspNet.Tests.Connectors;

/// <summary>
/// G3: the <see cref="SnapshotCheckpointStore"/> persists source offsets inside the
/// snapshot manifest, so they commit atomically with engine state (no separate sidecar
/// that could tear from the tick). See docs/design-connectors.md.
/// </summary>
public class CheckpointStoreTests
{
    private static CompiledQuery Compile()
    {
        var catalog = new Catalog();
        catalog.Register("t", new SqlSchema(
        [
            new SchemaColumn("a", new SqlIntegerType(false)),
            new SchemaColumn("b", new SqlIntegerType(false)),
        ]));
        var resolver = new Resolver(catalog);
        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement("SELECT a, b FROM t"))).Query;
        return PlanToCircuit.Compile(plan, ArrowSqlSnapshotCodecs.Instance, new CompileOptions { StoredOutput = true });
    }

    [Fact]
    public async Task Offsets_RideInManifest_NoSidecar_AndRoundTrip()
    {
        var producer = Compile();
        producer.Table("t").Insert(1, 2);
        producer.Step();

        var fs = new InMemoryTableFileSystem();
        var store = new SnapshotCheckpointStore(fs);
        var offsets = new[] { new SourceCheckpoint("t", "5") };
        await store.SaveAsync(producer.Circuit, offsets, CancellationToken.None);

        // No sidecar file — the offsets live in the manifest, which commits atomically
        // with the snapshot pointer.
        Assert.False(await fs.ExistsAsync("offsets.json", CancellationToken.None));

        var metadata = await Snapshot.ReadMetadataAsync(fs, CancellationToken.None);
        Assert.NotNull(metadata);
        Assert.True(metadata!.ContainsKey(SnapshotCheckpointStore.OffsetsMetadataKey));

        // Restore into a fresh circuit: engine tick and offsets come from the one manifest.
        var consumer = Compile();
        var state = await store.TryRestoreAsync(consumer.Circuit, CancellationToken.None);

        Assert.NotNull(state);
        Assert.Equal(producer.Circuit.TickCount, state!.Tick);
        Assert.Equal(producer.Circuit.TickCount, consumer.Circuit.TickCount); // engine tick restored
        Assert.Single(state.Offsets);
        Assert.Equal("t", state.Offsets[0].SourceName);
        Assert.Equal("5", state.Offsets[0].Offset);
    }

    [Fact]
    public async Task TryRestore_NoCheckpoint_ReturnsNull()
    {
        var store = new SnapshotCheckpointStore(new InMemoryTableFileSystem());
        var state = await store.TryRestoreAsync(Compile().Circuit, CancellationToken.None);
        Assert.Null(state);
    }
}
