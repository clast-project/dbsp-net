// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Persistence;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

namespace DbspNet.Tests.Persistence;

/// <summary>
/// End-to-end snapshot of a <c>PartitionedTopKOp</c> (windowed ROW_NUMBER). Like
/// <see cref="TopKSnapshotTests"/>, the operator persists its full per-partition
/// integrated input (not just the emitted windows), so a retraction of a
/// windowed row in a restored circuit can promote the next row in that same
/// partition — a row the consumer never saw inserted directly.
/// </summary>
public class PartitionedTopKSnapshotTests : IDisposable
{
    private const string Ddl = "CREATE TABLE emp (dept INT NOT NULL, sal INT NOT NULL)";

    private const string Query =
        "SELECT dept, sal FROM (SELECT dept, sal, ROW_NUMBER() OVER " +
        "(PARTITION BY dept ORDER BY sal DESC) AS rn FROM emp) s WHERE rn <= 2";

    private readonly string _snapshotDir;

    public PartitionedTopKSnapshotTests()
    {
        _snapshotDir = Path.Combine(
            Path.GetTempPath(), "dbspnet-ptopk-" + Guid.NewGuid().ToString("N"));
    }

    public void Dispose()
    {
        if (Directory.Exists(_snapshotDir))
        {
            Directory.Delete(_snapshotDir, recursive: true);
        }

        GC.SuppressFinalize(this);
    }

    private static CompiledQuery Compile() => Compile(ArrowSqlSnapshotCodecs.Instance);

    private static CompiledQuery Compile(ISqlSnapshotCodecs? codecs)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        resolver.Resolve(Parser.ParseStatement(Ddl));
        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(Query))).Query;
        return codecs is null
            ? PlanToCircuit.Compile(plan)
            : PlanToCircuit.Compile(plan, codecs);
    }

    [Fact]
    public async Task RestoredState_RetractionPromotesNextRowInPartition()
    {
        // dept 1 sees three rows; its window is {100, 90} with 80 retained below.
        var producer = Compile();
        producer.Table("emp").Insert(1, 100);
        producer.Table("emp").Insert(1, 90);
        producer.Table("emp").Insert(1, 80);
        producer.Table("emp").Insert(2, 50);
        producer.Step();
        Assert.Equal(3, producer.Current.Count);

        await Snapshot.WriteAsync(producer.Circuit, _snapshotDir);

        var consumer = Compile();
        await Snapshot.ReadAsync(consumer.Circuit, _snapshotDir);

        // Retract dept-1's top salary. Proof the full per-partition accumulation
        // round-tripped: 80 — never seen inserted by the consumer — is promoted
        // into dept 1's window, and dept 2 is undisturbed.
        consumer.Table("emp").Delete(1, 100);
        consumer.Step();

        Assert.Equal(-1, consumer.WeightOf(1, 100).Value);
        Assert.Equal(1, consumer.WeightOf(1, 80).Value);
        Assert.Equal(0, consumer.WeightOf(1, 90).Value);
        Assert.Equal(0, consumer.WeightOf(2, 50).Value);
    }

    [Fact]
    public async Task NoCodec_ThrowsOnSnapshot()
    {
        var query = Compile(codecs: null);
        query.Table("emp").Insert(1, 10);
        query.Step();

        await Assert.ThrowsAsync<NotSupportedException>(async () =>
            await Snapshot.WriteAsync(query.Circuit, _snapshotDir));
    }
}
