// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Persistence;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

namespace DbspNet.Tests.Persistence;

/// <summary>
/// End-to-end snapshot of a <c>PartitionedWindowAggregateOp</c> (bounded RANGE
/// window aggregate). The operator persists its full per-partition integrated
/// input, so a row inserted into a restored circuit can correctly re-aggregate
/// the frames of rows the consumer never saw inserted directly.
/// </summary>
public class WindowAggregateSnapshotTests : IDisposable
{
    private const string Ddl = "CREATE TABLE w (g INT NOT NULL, ts INT NOT NULL, v INT NOT NULL)";

    private const string Query =
        "SELECT g, ts, v, SUM(v) OVER (PARTITION BY g ORDER BY ts " +
        "RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS roll FROM w";

    private readonly string _snapshotDir =
        Path.Combine(Path.GetTempPath(), "dbspnet-wagg-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
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
        resolver.Resolve(Parser.ParseStatement(Ddl));
        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(Query))).Query;
        return PlanToCircuit.Compile(plan, ArrowSqlSnapshotCodecs.Instance);
    }

    [Fact]
    public async Task RestoredState_NewRowReaggregatesNeighborFrame()
    {
        // Producer: g=1 has ts=1 (v=10) and ts=3 (v=30). Frames (1 PRECEDING):
        // ts1 = {10} = 10; ts3 = {30} = 30.
        var producer = Compile();
        producer.Table("w").Insert(1, 1, 10);
        producer.Table("w").Insert(1, 3, 30);
        producer.Step();

        await Snapshot.WriteAsync(producer.Circuit, _snapshotDir);

        var consumer = Compile();
        await Snapshot.ReadAsync(consumer.Circuit, _snapshotDir);

        // Insert ts=2 (v=5). Its own frame [1,2] = {10,5} = 15; and it falls into
        // ts3's frame [2,3] = {5,30} = 35, which the restored state must recompute
        // from 30 → 35. ts1's frame [0,1] is untouched.
        consumer.Table("w").Insert(1, 2, 5);
        consumer.Step();

        Assert.Equal(1, consumer.WeightOf(1, 2, 5, 15L).Value);   // new row
        Assert.Equal(-1, consumer.WeightOf(1, 3, 30, 30L).Value); // old ts3 value retracted
        Assert.Equal(1, consumer.WeightOf(1, 3, 30, 35L).Value);  // ts3 re-aggregated
        Assert.Equal(0, consumer.WeightOf(1, 1, 10, 10L).Value);  // ts1 undisturbed
    }
}
