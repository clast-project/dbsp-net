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
/// A miniature TPC-DI-shaped end-to-end over the engineered-wood Delta connectors: two
/// source tables — an SCD2 dimension (<c>company</c>, several rows per key over time)
/// and a fact table (<c>trade</c>) — feed a view that takes each company's current
/// record (rank-in-output <c>ROW_NUMBER … = 1</c> in a CASE, then filtered), joins the
/// facts to it, and aggregates. Both sources are read one Delta version per tick and the
/// full view is truncate-written to a Delta sink. The engine view and the sink must equal
/// the batch re-computation over the net inputs — the shape (SCD2 current-record join +
/// aggregate) that gated ivm-bench, now driven purely by connectors.
/// </summary>
public sealed class MiniPipelineE2ETests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "dbspnet-mini-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        if (Directory.Exists(_root))
        {
            Directory.Delete(_root, recursive: true);
        }
    }

    private static SqlSchema CompanySchema() => new(
    [
        new SchemaColumn("company_id", new SqlIntegerType(false)),
        new SchemaColumn("name_code", new SqlIntegerType(false)),
        new SchemaColumn("effective_ts", new SqlIntegerType(false)),
    ]);

    private static SqlSchema TradeSchema() => new(
    [
        new SchemaColumn("trade_id", new SqlIntegerType(false)),
        new SchemaColumn("company_id", new SqlIntegerType(false)),
        new SchemaColumn("amount", new SqlIntegerType(false)),
    ]);

    private const string View =
        "SELECT c.name_code, SUM(t.amount) AS total " +
        "FROM trade t JOIN (" +
        "  SELECT company_id, name_code FROM (" +
        "    SELECT company_id, name_code, " +
        "      CASE WHEN ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY effective_ts DESC) = 1 THEN 1 ELSE 0 END AS is_current" +
        "    FROM company" +
        "  ) s WHERE is_current = 1" +
        ") c ON t.company_id = c.company_id " +
        "GROUP BY c.name_code";

    private static async Task<DeltaTable> CreateTable(string dir, SqlSchema schema)
    {
        Directory.CreateDirectory(dir);
        return await DeltaTable.CreateAsync(
            new LocalTableFileSystem(dir), ArrowSchemaBridge.ToArrow(schema), cancellationToken: CancellationToken.None);
    }

    private static async Task Append(DeltaTable table, SqlSchema schema, List<object?[]> net, params object?[][] rows)
    {
        await table.WriteAsync(new[] { ArrowTestData.Batch(schema, rows) }, DeltaWriteMode.Append, CancellationToken.None);
        net.AddRange(rows);
    }

    private static ZSet<StructuralRow, Z64> NetOf(List<object?[]> rows)
    {
        var b = new ZSetBuilder<StructuralRow, Z64>();
        foreach (var r in rows)
        {
            b.Add(new StructuralRow(r), Z64.One);
        }

        return b.Build();
    }

    private static async Task<ZSet<StructuralRow, Z64>> ReadSink(string dir, int cols)
    {
        var table = await DeltaTable.OpenAsync(new LocalTableFileSystem(dir), cancellationToken: CancellationToken.None);
        var b = new ZSetBuilder<StructuralRow, Z64>();
        await foreach (var batch in table.ReadAllAsync(null, CancellationToken.None))
        {
            var arrays = new IArrowArray[cols];
            for (var c = 0; c < cols; c++)
            {
                arrays[c] = batch.Column(c);
            }

            for (var i = 0; i < batch.Length; i++)
            {
                var row = new object?[cols];
                for (var c = 0; c < cols; c++)
                {
                    row[c] = arrays[c] switch
                    {
                        Int32Array a => (object?)a.GetValue(i),
                        Int64Array a => a.GetValue(i),
                        _ => throw new NotSupportedException($"unexpected sink column type {arrays[c].GetType().Name}"),
                    };
                }

                b.Add(new StructuralRow(row), Z64.One);
            }
        }

        table.Dispose();
        return b.Build();
    }

    [Fact]
    public async Task Scd2CurrentJoinAggregate_OverTwoDeltaSources_MatchesBatch()
    {
        var companyDir = Path.Combine(_root, "company");
        var tradeDir = Path.Combine(_root, "trade");
        var sinkDir = Path.Combine(_root, "sink");
        Directory.CreateDirectory(sinkDir);

        var companyNet = new List<object?[]>();
        var tradeNet = new List<object?[]>();

        // SCD2 company feed: initial rows, then later-timestamped updates per key.
        var company = await CreateTable(companyDir, CompanySchema());
        await Append(company, CompanySchema(), companyNet,
            new object?[] { 1, 100, 1 }, new object?[] { 2, 200, 1 });
        await Append(company, CompanySchema(), companyNet, new object?[] { 1, 101, 2 }); // company 1 → 101
        await Append(company, CompanySchema(), companyNet, new object?[] { 2, 201, 3 }); // company 2 → 201
        company.Dispose();

        // Fact feed.
        var trade = await CreateTable(tradeDir, TradeSchema());
        await Append(trade, TradeSchema(), tradeNet,
            new object?[] { 10, 1, 50 }, new object?[] { 11, 2, 30 });
        await Append(trade, TradeSchema(), tradeNet, new object?[] { 12, 1, 20 });
        trade.Dispose();

        // Drive both sources through the pipeline into a truncate Delta sink.
        var catalog = new Catalog();
        var companyIn = new DeltaInputConnector("company", companyDir);
        var tradeIn = new DeltaInputConnector("trade", tradeDir);
        var sinkOut = new DeltaOutputConnector("v", sinkDir, OutputMode.Truncate);
        LogicalPlan? plan = null;
        var runner = await PipelineRunner.CreateAsync(
            catalog,
            new (IInputConnector, SqlSchema?)[] { (companyIn, null), (tradeIn, null) },
            cat =>
            {
                var resolver = new Resolver(cat);
                plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(View))).Query;
                return PlanToCircuit.Compile(plan, snapshotCodecs: null, new CompileOptions { StoredOutput = true });
            },
            new IOutputConnector[] { sinkOut });

        await runner.DrainAsync();

        // Batch oracle over the net inputs.
        var ctx = new BatchEvalContext(
            new Dictionary<string, ZSet<StructuralRow, Z64>>(StringComparer.Ordinal)
            {
                ["company"] = NetOf(companyNet),
                ["trade"] = NetOf(tradeNet),
            },
            new Dictionary<CteRef, ZSet<StructuralRow, Z64>>());
        var batch = BatchPlanEvaluator.Evaluate(plan!, ctx);

        Assert.True(
            runner.Query.CurrentView.Equals(batch),
            $"view≠batch\n  view={runner.Query.CurrentView}\n  batch={batch}");

        // Sanity: current company 1 = name_code 101 (trades 50+20=70), company 2 = 201 (30).
        Assert.Equal(2, runner.Query.CurrentView.Count);
        Assert.Equal(1, runner.Query.CurrentView.WeightOf(new StructuralRow(new object?[] { 101, 70L })).Value);
        Assert.Equal(1, runner.Query.CurrentView.WeightOf(new StructuralRow(new object?[] { 201, 30L })).Value);

        // The truncate sink equals the engine view.
        var sink = await ReadSink(sinkDir, cols: 2);
        Assert.True(sink.Equals(batch), $"sink≠batch\n  sink={sink}");

        await companyIn.DisposeAsync();
        await tradeIn.DisposeAsync();
        await sinkOut.DisposeAsync();
    }
}
