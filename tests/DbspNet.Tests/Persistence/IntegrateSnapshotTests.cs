// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Collections;
using DbspNet.Persistence;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

namespace DbspNet.Tests.Persistence;

/// <summary>
/// The stored / integrated output (<c>IntegrateOp</c>) persists the full
/// materialized view as part of the circuit snapshot — the analogue of Feldera's
/// <c>+stored</c> materialized view being retained across a restart. A restored
/// circuit continues from the saved view (a retraction after restore touches rows
/// the consumer never inserted directly).
/// </summary>
public class IntegrateSnapshotTests : IDisposable
{
    private const string Ddl = "CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)";
    private const string Query = "SELECT a, b FROM t WHERE b > 0";

    private static readonly CompileOptions Stored = new() { StoredOutput = true };

    private readonly string _snapshotDir;

    public IntegrateSnapshotTests()
    {
        _snapshotDir = Path.Combine(
            Path.GetTempPath(), "dbspnet-integrate-" + Guid.NewGuid().ToString("N"));
    }

    public void Dispose()
    {
        if (Directory.Exists(_snapshotDir))
        {
            Directory.Delete(_snapshotDir, recursive: true);
        }

        GC.SuppressFinalize(this);
    }

    private static CompiledQuery Compile(ISqlSnapshotCodecs? codecs)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        resolver.Resolve(Parser.ParseStatement(Ddl));
        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(Query))).Query;
        return PlanToCircuit.Compile(plan, codecs, Stored);
    }

    [Fact]
    public async Task RestoredView_ContinuesFromPersistedState()
    {
        // Producer builds a view of three rows, then snapshots.
        var producer = Compile(ArrowSqlSnapshotCodecs.Instance);
        producer.Table("t").Insert(1, 10);
        producer.Table("t").Insert(2, 20);
        producer.Table("t").Insert(3, 30);
        producer.Step();
        Assert.Equal(3, producer.CurrentView.Count);

        await Snapshot.WriteAsync(producer.Circuit, _snapshotDir);

        var consumer = Compile(ArrowSqlSnapshotCodecs.Instance);
        await Snapshot.ReadAsync(consumer.Circuit, _snapshotDir);

        // Delete a row the consumer never inserted, and add a new one. Proof the
        // full view round-tripped: the delete lands, leaving 2 rows + 1 new = 3.
        consumer.Table("t").Delete(2, 20);
        consumer.Table("t").Insert(4, 40);
        consumer.Step();

        Assert.Equal(3, consumer.CurrentView.Count);
        Assert.Equal(1, consumer.CurrentView.WeightOf(new StructuralRow(new object?[] { 1, 10 })).Value);
        Assert.Equal(0, consumer.CurrentView.WeightOf(new StructuralRow(new object?[] { 2, 20 })).Value);
        Assert.Equal(1, consumer.CurrentView.WeightOf(new StructuralRow(new object?[] { 4, 40 })).Value);

        // Cross-check against an uninterrupted run of the same total sequence.
        var control = Compile(ArrowSqlSnapshotCodecs.Instance);
        control.Table("t").Insert(1, 10);
        control.Table("t").Insert(2, 20);
        control.Table("t").Insert(3, 30);
        control.Step();
        control.Table("t").Delete(2, 20);
        control.Table("t").Insert(4, 40);
        control.Step();
        Assert.True(consumer.CurrentView.Equals(control.CurrentView));
    }

    [Fact]
    public async Task NoCodec_ThrowsOnSnapshot()
    {
        var query = Compile(codecs: null); // StoredOutput but no snapshot codec
        query.Table("t").Insert(1, 10);
        query.Step();

        await Assert.ThrowsAsync<NotSupportedException>(async () =>
            await Snapshot.WriteAsync(query.Circuit, _snapshotDir));
    }
}
