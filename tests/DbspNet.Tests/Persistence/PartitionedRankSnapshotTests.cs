// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Persistence;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

namespace DbspNet.Tests.Persistence;

/// <summary>
/// End-to-end snapshot of a <c>PartitionedRankOp</c> (rank-in-output). Like
/// <see cref="PartitionedTopKSnapshotTests"/>, the operator persists its full
/// per-partition integrated input (not just the emitted widened rows), so a
/// retraction in a restored circuit re-ranks the surviving rows — including a row
/// the consumer never saw inserted directly.
/// </summary>
public class PartitionedRankSnapshotTests : IDisposable
{
    private const string Ddl = "CREATE TABLE emp (dept INT NOT NULL, sal INT NOT NULL)";

    private const string Query =
        "SELECT dept, sal, RANK() OVER (PARTITION BY dept ORDER BY sal DESC) AS r FROM emp";

    private readonly string _snapshotDir;

    public PartitionedRankSnapshotTests()
    {
        _snapshotDir = Path.Combine(
            Path.GetTempPath(), "dbspnet-prank-" + Guid.NewGuid().ToString("N"));
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
    public async Task RestoredState_RetractionReranksPartition()
    {
        // dept 1: three rows ranked 1 (100), 2 (90), 3 (80). dept 2 untouched.
        var producer = Compile();
        producer.Table("emp").Insert(1, 100);
        producer.Table("emp").Insert(1, 90);
        producer.Table("emp").Insert(1, 80);
        producer.Table("emp").Insert(2, 50);
        producer.Step();
        Assert.Equal(4, producer.Current.Count);

        await Snapshot.WriteAsync(producer.Circuit, _snapshotDir);

        var consumer = Compile();
        await Snapshot.ReadAsync(consumer.Circuit, _snapshotDir);

        // Retract dept-1's top salary. Proof the full per-partition accumulation
        // round-tripped: 90 (never seen inserted by the consumer) shifts to rank 1
        // and 80 to rank 2; dept 2 is undisturbed.
        consumer.Table("emp").Delete(1, 100);
        consumer.Step();

        Assert.Equal(-1, consumer.WeightOf(1, 100, 1L).Value); // old rank-1 row gone
        Assert.Equal(-1, consumer.WeightOf(1, 90, 2L).Value);  // 90 leaves rank 2 …
        Assert.Equal(1, consumer.WeightOf(1, 90, 1L).Value);   // … arrives at rank 1
        Assert.Equal(-1, consumer.WeightOf(1, 80, 3L).Value);  // 80 leaves rank 3 …
        Assert.Equal(1, consumer.WeightOf(1, 80, 2L).Value);   // … arrives at rank 2
        Assert.Equal(0, consumer.WeightOf(2, 50, 1L).Value);   // other partition quiet
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
