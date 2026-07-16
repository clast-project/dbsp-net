// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using DbspNet.Arrow;
using DbspNet.Connectors.Abstractions;
using DbspNet.Connectors.EngineeredWood;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;
using SqlSchema = DbspNet.Sql.Plan.Schema;

namespace DbspNet.Tests.Connectors;

/// <summary>
/// End-to-end round-trip of the engineered-wood Delta connectors: build a local Delta
/// table across versions (append + overwrite), read it through
/// <see cref="DeltaInputConnector"/> + <see cref="PipelineRunner"/> (CDF → signed
/// weights, one tick per version), and write the view back through
/// <see cref="DeltaOutputConnector"/> (truncate). The engine view must equal the source
/// table's current contents, and the sink table must equal the engine view.
/// </summary>
public sealed class DeltaConnectorTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "dbspnet-delta-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        if (Directory.Exists(_root))
        {
            Directory.Delete(_root, recursive: true);
        }
    }

    private static SqlSchema Schema() => new(
    [
        new SchemaColumn("id", new SqlIntegerType(false)),
        new SchemaColumn("val", new SqlIntegerType(false)),
    ]);

    private static async Task WriteVersion(DeltaTable table, IReadOnlyList<object?[]> rows, DeltaWriteMode mode)
    {
        var batch = ArrowTestData.Batch(Schema(), rows);
        await table.WriteAsync(new[] { batch }, mode, CancellationToken.None);
    }

    private static async Task<ZSet<StructuralRow, Z64>> ReadZSet(DeltaTable table)
    {
        var b = new ZSetBuilder<StructuralRow, Z64>();
        await foreach (var batch in table.ReadAllAsync(null, CancellationToken.None))
        {
            var id = (Int32Array)batch.Column(0);
            var val = (Int32Array)batch.Column(1);
            for (var i = 0; i < batch.Length; i++)
            {
                b.Add(new StructuralRow(new object?[] { id.GetValue(i), val.GetValue(i) }), Z64.One);
            }
        }

        return b.Build();
    }

    private static ZSet<StructuralRow, Z64> ExpectedOf(params int[][] rows)
    {
        var b = new ZSetBuilder<StructuralRow, Z64>();
        foreach (var r in rows)
        {
            b.Add(new StructuralRow(new object?[] { r[0], r[1] }), Z64.One);
        }

        return b.Build();
    }

    [Fact]
    public async Task Delta_RoundTrip_AppendsOverwritesAndTruncateSink()
    {
        var srcDir = Path.Combine(_root, "source");
        var sinkDir = Path.Combine(_root, "sink");
        Directory.CreateDirectory(srcDir);
        Directory.CreateDirectory(sinkDir);

        // Build the source table: create (v0) → append (v1, v2) → overwrite (v3). The
        // overwrite produces delete+insert CDF, exercising delete inference and Z-set
        // cancellation of the row that survives.
        var srcFs = new LocalTableFileSystem(srcDir);
        var src = await DeltaTable.CreateAsync(srcFs, ArrowSchemaBridge.ToArrow(Schema()), cancellationToken: CancellationToken.None);
        await WriteVersion(src, new object?[][] { new object?[] { 1, 10 }, new object?[] { 2, 20 } }, DeltaWriteMode.Append);
        await WriteVersion(src, new object?[][] { new object?[] { 3, 30 } }, DeltaWriteMode.Append);
        await WriteVersion(src, new object?[][] { new object?[] { 1, 10 }, new object?[] { 4, 40 } }, DeltaWriteMode.Overwrite);
        src.Dispose();

        // Read source via the connector → engine (StoredOutput view) → truncate sink.
        var catalog = new Catalog();
        var input = new DeltaInputConnector("src", srcDir);          // infer schema
        var output = new DeltaOutputConnector("v", sinkDir, OutputMode.Truncate);
        var runner = await PipelineRunner.CreateAsync(
            catalog,
            new (IInputConnector, SqlSchema?)[] { (input, null) },
            cat =>
            {
                var resolver = new Resolver(cat);
                var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement("SELECT id, val FROM src"))).Query;
                return PlanToCircuit.Compile(plan, snapshotCodecs: null, new CompileOptions { StoredOutput = true });
            },
            new IOutputConnector[] { output });

        var ticks = await runner.DrainAsync();
        Assert.Equal(4, ticks);                       // versions 0..3, one tick each

        // The engine view equals the source's current contents: {(1,10),(4,40)}.
        var expected = ExpectedOf(new[] { 1, 10 }, new[] { 4, 40 });
        Assert.True(runner.Query.CurrentView.Equals(expected), $"view={runner.Query.CurrentView}");

        // The truncate sink equals the engine view.
        var sink = await DeltaTable.OpenAsync(new LocalTableFileSystem(sinkDir), cancellationToken: CancellationToken.None);
        var sinkRows = await ReadZSet(sink);
        sink.Dispose();
        Assert.True(sinkRows.Equals(expected), $"sink={sinkRows}");

        await input.DisposeAsync();
        await output.DisposeAsync();
    }

    [Fact]
    public async Task Delta_Incremental_ResumesFromCursorAcrossDrains()
    {
        var srcDir = Path.Combine(_root, "inc");
        Directory.CreateDirectory(srcDir);
        var srcFs = new LocalTableFileSystem(srcDir);
        var src = await DeltaTable.CreateAsync(srcFs, ArrowSchemaBridge.ToArrow(Schema()), cancellationToken: CancellationToken.None);
        await WriteVersion(src, new object?[][] { new object?[] { 1, 10 } }, DeltaWriteMode.Append);

        var catalog = new Catalog();
        var input = new DeltaInputConnector("src", srcDir);
        var runner = await PipelineRunner.CreateAsync(
            catalog,
            new (IInputConnector, SqlSchema?)[] { (input, null) },
            cat =>
            {
                var resolver = new Resolver(cat);
                var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement("SELECT id, val FROM src"))).Query;
                return PlanToCircuit.Compile(plan, snapshotCodecs: null, new CompileOptions { StoredOutput = true });
            },
            System.Array.Empty<IOutputConnector>());

        // First drain: versions 0 (empty) and 1.
        Assert.Equal(2, await runner.DrainAsync());
        Assert.True(runner.Query.CurrentView.Equals(ExpectedOf(new[] { 1, 10 })));

        // A new version arrives; a second drain picks up only that version.
        await WriteVersion(src, new object?[][] { new object?[] { 2, 20 } }, DeltaWriteMode.Append);
        Assert.Equal(1, await runner.DrainAsync());
        Assert.True(runner.Query.CurrentView.Equals(ExpectedOf(new[] { 1, 10 }, new[] { 2, 20 })));

        src.Dispose();
        await input.DisposeAsync();
    }
}
