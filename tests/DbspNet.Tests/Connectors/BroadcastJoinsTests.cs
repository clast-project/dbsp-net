// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using DbspNet.Arrow;
using DbspNet.Connectors.Abstractions;
using DbspNet.Server;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;
using SqlSchema = DbspNet.Sql.Plan.Schema;

namespace DbspNet.Tests.Connectors;

/// <summary>
/// Broadcast joins for small dimensions in a parallel deploy (<see cref="BroadcastJoins"/>,
/// docs/design-tpcdi-batch1.md §4a): sizes come from Delta-log row counts on a fresh start, and a
/// persistent deploy records the choice so a restore rebuilds the same plan even after the tables grow.
/// </summary>
public sealed class BroadcastJoinsTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "dbspnet-bcast-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        if (Directory.Exists(_root))
        {
            Directory.Delete(_root, recursive: true);
        }
    }

    // dim is a leaf relation on the join's right side, so it is a broadcast candidate.
    private static readonly string[] Sql =
    [
        "CREATE TABLE fact (fact_id INT NOT NULL, code INT NOT NULL, amount INT NOT NULL)",
        "CREATE TABLE dim (code INT NOT NULL, label INT NOT NULL)",
        "CREATE VIEW totals AS SELECT d.label, SUM(f.amount) AS total FROM fact f JOIN dim d ON f.code = d.code GROUP BY d.label",
    ];

    private static SqlSchema FactSchema() => new(
    [
        new SchemaColumn("fact_id", new SqlIntegerType(false)),
        new SchemaColumn("code", new SqlIntegerType(false)),
        new SchemaColumn("amount", new SqlIntegerType(false)),
    ]);

    private static SqlSchema DimSchema() => new(
    [
        new SchemaColumn("code", new SqlIntegerType(false)),
        new SchemaColumn("label", new SqlIntegerType(false)),
    ]);

    private readonly List<object?[]> _fact = [];
    private readonly List<object?[]> _dim = [];

    [Theory]
    [InlineData(null, BroadcastJoins.DefaultMaxRows)]
    [InlineData("", BroadcastJoins.DefaultMaxRows)]
    [InlineData("0", 0L)]
    [InlineData("off", 0L)]
    [InlineData("OFF", 0L)]
    [InlineData(" 250 ", 250L)]
    public void ConfiguredMaxRows_ParsesTheSetting(string? setting, long expected) =>
        Assert.Equal(expected, BroadcastJoins.ConfiguredMaxRows(setting));

    [Theory]
    [InlineData("-1")]
    [InlineData("lots")]
    public void ConfiguredMaxRows_RefusesNonsense(string setting) =>
        Assert.Throws<FormatException>(() => BroadcastJoins.ConfiguredMaxRows(setting));

    [Fact]
    public async Task FreshParallelDeploy_BroadcastsInputsWithinTheThreshold_AndMatchesBatch()
    {
        await Seed(dimRows: 3, factRows: 6);
        var engine = new DbspNetEngine(workers: 4, broadcastMaxRows: 4);
        var deploy = await engine.DeployAsync(Spec(snapshotDir: null));
        Assert.Equal(4, deploy.Workers);
        Assert.Equal(4, deploy.BroadcastMaxRows);
        Assert.Equal(["dim"], deploy.BroadcastInputs);
        Assert.Equal(1, BroadcastExchanges(engine));

        await RunBatch(engine);
        await AssertTotals();
    }

    [Fact]
    public async Task EmptyAtDeploy_CountsAsLarge()
    {
        await Seed(dimRows: 0, factRows: 6);
        var engine = new DbspNetEngine(workers: 4, broadcastMaxRows: 1000);
        var deploy = await engine.DeployAsync(Spec(snapshotDir: null));
        Assert.DoesNotContain("dim", deploy.BroadcastInputs ?? []);
        Assert.Equal(0, BroadcastExchanges(engine));

        await AppendDim(3);
        await RunBatch(engine);
        await AssertTotals();
    }

    [Fact]
    public async Task SerialDeploy_DoesNotBroadcast()
    {
        await Seed(dimRows: 3, factRows: 6);
        var deploy = await new DbspNetEngine(workers: 1, broadcastMaxRows: 1000).DeployAsync(Spec(snapshotDir: null));
        Assert.Equal(0, deploy.BroadcastMaxRows);
        Assert.Null(deploy.BroadcastInputs);
    }

    /// <summary>
    /// The checkpoint records the sizes the plan was compiled from. After the dimension grows past the
    /// threshold, fresh counts would compile a hash join, so the redeploy must use the recorded choice
    /// (at the same W and re-sharded to another) to match the snapshot, and still be correct.
    /// </summary>
    [Theory]
    [InlineData(4)]
    [InlineData(2)]
    public async Task Restore_UsesTheRecordedChoice_AfterTheDimensionGrows(int redeployWorkers)
    {
        var snapshotDir = Path.Combine(_root, "snap");
        await Seed(dimRows: 3, factRows: 6);
        var first = new DbspNetEngine(workers: 4, CheckpointPolicy.EveryBatch, broadcastMaxRows: 4);
        Assert.Equal(["dim"], (await first.DeployAsync(Spec(snapshotDir))).BroadcastInputs);
        await RunBatch(first);

        var recorded = await RecordedChoice(snapshotDir);
        Assert.NotNull(recorded);
        Assert.Equal(4, recorded!.MaxRows);
        Assert.Equal(3, recorded.RowCounts["dim"]);
        Assert.Equal(6, recorded.RowCounts["fact"]);

        await AppendDim(7);
        await AppendFact(4);
        var second = new DbspNetEngine(redeployWorkers, CheckpointPolicy.EveryBatch, broadcastMaxRows: 4);
        var redeploy = await second.DeployAsync(Spec(snapshotDir));
        Assert.True(redeploy.RestoredTick > 0);
        Assert.Equal(redeployWorkers, redeploy.Workers);
        Assert.Equal(["dim"], redeploy.BroadcastInputs);
        Assert.Equal(1, BroadcastExchanges(second));
        await RunBatch(second);
        await AssertTotals();

        // The next checkpoint carries the same choice forward.
        Assert.Equal(3, (await RecordedChoice(snapshotDir))!.RowCounts["dim"]);
    }

    /// <summary>A checkpoint written with broadcast off restores with it off, whatever the setting now.</summary>
    [Fact]
    public async Task Restore_OfASnapshotWithoutTheKey_DoesNotBroadcast()
    {
        var snapshotDir = Path.Combine(_root, "snap0");
        await Seed(dimRows: 3, factRows: 6);
        var first = new DbspNetEngine(workers: 4, CheckpointPolicy.EveryBatch, broadcastMaxRows: 0);
        Assert.Equal(0, (await first.DeployAsync(Spec(snapshotDir))).BroadcastMaxRows);
        await RunBatch(first);
        Assert.Null(await RecordedChoice(snapshotDir));

        await AppendFact(2);
        var second = new DbspNetEngine(workers: 4, CheckpointPolicy.EveryBatch, broadcastMaxRows: 1000);
        var redeploy = await second.DeployAsync(Spec(snapshotDir));
        Assert.True(redeploy.RestoredTick > 0);
        Assert.Equal(0, redeploy.BroadcastMaxRows);
        Assert.Equal(0, BroadcastExchanges(second));
        await RunBatch(second);
        await AssertTotals();
    }

    private ProgramSpec Spec(string? snapshotDir) => new(
        Sql,
        [new InputSpec("fact", Path.Combine(_root, "fact")), new InputSpec("dim", Path.Combine(_root, "dim"))],
        [new OutputSpec("totals", Path.Combine(_root, "out"))],
        SnapshotDir: snapshotDir);

    private async Task Seed(int dimRows, int factRows)
    {
        Directory.CreateDirectory(Path.Combine(_root, "out"));
        foreach (var (name, schema) in new[] { ("fact", FactSchema()), ("dim", DimSchema()) })
        {
            var dir = Path.Combine(_root, name);
            Directory.CreateDirectory(dir);
            using var table = await DeltaTable.CreateAsync(
                new LocalTableFileSystem(dir), ArrowSchemaBridge.ToArrow(schema), cancellationToken: CancellationToken.None);
        }

        await AppendDim(dimRows);
        await AppendFact(factRows);
    }

    // Dimension codes 0..n-1 (continuing), labels code % 2; fact rows spread over codes 0..9.
    private Task AppendDim(int n) => Append("dim", DimSchema(), _dim,
        Enumerable.Range(_dim.Count, n).Select(c => new object?[] { c, c % 2 }).ToArray());

    private Task AppendFact(int n) => Append("fact", FactSchema(), _fact,
        Enumerable.Range(_fact.Count, n).Select(i => new object?[] { i, i % 10, i + 1 }).ToArray());

    private async Task Append(string name, SqlSchema schema, List<object?[]> net, object?[][] rows)
    {
        if (rows.Length == 0)
        {
            return;
        }

        using var table = await DeltaTable.OpenAsync(
            new LocalTableFileSystem(Path.Combine(_root, name)), cancellationToken: CancellationToken.None);
        await table.WriteAsync([ArrowTestData.Batch(schema, rows)], DeltaWriteMode.Append, CancellationToken.None);
        net.AddRange(rows);
    }

    private static async Task RunBatch(DbspNetEngine engine)
    {
        engine.Resume();
        var wait = await engine.WaitAsync();
        Assert.All(wait.Outputs, o => Assert.Equal("success", o.Status));
    }

    private async Task AssertTotals()
    {
        var labelOf = _dim.ToDictionary(r => (int)r[0]!, r => (int)r[1]!);
        var expected = _fact
            .Where(f => labelOf.ContainsKey((int)f[1]!))
            .GroupBy(f => labelOf[(int)f[1]!])
            .ToDictionary(g => g.Key, g => g.Sum(f => (long)(int)f[2]!));
        Assert.NotEmpty(expected);

        var actual = new Dictionary<int, long>();
        using var table = await DeltaTable.OpenAsync(
            new LocalTableFileSystem(Path.Combine(_root, "out")), cancellationToken: CancellationToken.None);
        await foreach (var batch in table.ReadAllAsync(null, CancellationToken.None))
        {
            var label = (Int32Array)batch.Column(0);
            var total = (Int64Array)batch.Column(1);
            for (var i = 0; i < batch.Length; i++)
            {
                Assert.True(actual.TryAdd(label.GetValue(i)!.Value, total.GetValue(i)!.Value));
            }
        }

        Assert.Equal(expected.OrderBy(kv => kv.Key), actual.OrderBy(kv => kv.Key));
    }

    // Broadcast exchanges in the deployed circuit (one replica's count; every replica has the same).
    private static int BroadcastExchanges(DbspNetEngine engine) =>
        engine.DeployedParallel!.Circuit.Replicas[0].Operators.Count(o => o.GetType().Name.StartsWith("BroadcastExchangeOp", StringComparison.Ordinal));

    private static async Task<BroadcastJoins?> RecordedChoice(string snapshotDir)
    {
        var metadata = await new SnapshotCheckpointStore(new DbspNet.Persistence.IO.Local.LocalTableFileSystem(snapshotDir)).RecordedParallelMetadataAsync();
        Assert.NotNull(metadata);
        return metadata!.TryGetValue(BroadcastJoins.MetadataKey, out var json) ? BroadcastJoins.Parse(json) : null;
    }
}
