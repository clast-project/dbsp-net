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
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;
using SqlSchema = DbspNet.Sql.Plan.Schema;

namespace DbspNet.Tests.Connectors;

/// <summary>
/// The whole ivm-bench engine shape, in miniature: a multi-view PROGRAM (DAG of
/// CREATE TABLE sources + CREATE VIEW definitions, one shared circuit, multiple outputs)
/// driven end-to-end over local Delta tables via <see cref="ProgramRunner"/> — N Delta
/// inputs, per-batch drain, M truncate Delta outputs. Each output sink must equal a
/// dependency-order batch re-computation, across batches (full load then append).
/// </summary>
public sealed class DeltaProgramE2ETests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "dbspnet-prog-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        if (Directory.Exists(_root))
        {
            Directory.Delete(_root, recursive: true);
        }
    }

    private static readonly string[] ProgramSql =
    [
        "CREATE TABLE company (company_id INT NOT NULL, name_code INT NOT NULL, effective_ts INT NOT NULL)",
        "CREATE TABLE trade (trade_id INT NOT NULL, company_id INT NOT NULL, amount INT NOT NULL)",
        "CREATE VIEW company_current AS " +
        "  SELECT company_id, name_code FROM (" +
        "    SELECT company_id, name_code, " +
        "      CASE WHEN ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY effective_ts DESC) = 1 THEN 1 ELSE 0 END AS is_current " +
        "    FROM company) s WHERE is_current = 1",
        "CREATE VIEW trade_enriched AS " +
        "  SELECT t.trade_id, t.amount, c.name_code FROM trade t JOIN company_current c ON t.company_id = c.company_id",
        "CREATE VIEW company_totals AS " +
        "  SELECT c.name_code, SUM(t.amount) AS total FROM trade t JOIN company_current c ON t.company_id = c.company_id " +
        "  GROUP BY c.name_code",
    ];

    private static readonly HashSet<string> OutputViews = new(StringComparer.Ordinal) { "trade_enriched", "company_totals" };

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

    // Batch oracle: evaluate every view in dependency order, feeding each result in as a
    // table so downstream views resolve.
    private static Dictionary<string, ZSet<StructuralRow, Z64>> BatchAll(
        ResolvedProgram program, ZSet<StructuralRow, Z64> company, ZSet<StructuralRow, Z64> trade)
    {
        var tables = new Dictionary<string, ZSet<StructuralRow, Z64>>(StringComparer.Ordinal)
        {
            ["company"] = company,
            ["trade"] = trade,
        };
        var results = new Dictionary<string, ZSet<StructuralRow, Z64>>(StringComparer.Ordinal);
        foreach (var v in program.Views)
        {
            var ctx = new BatchEvalContext(tables, new Dictionary<CteRef, ZSet<StructuralRow, Z64>>());
            var r = BatchPlanEvaluator.Evaluate(v.Query, ctx);
            tables[v.ViewName] = r;
            results[v.ViewName] = r;
        }

        return results;
    }

    private static async Task<ZSet<StructuralRow, Z64>> ReadSink(string dir, int cols)
    {
        var table = await DeltaTable.OpenAsync(new LocalTableFileSystem(dir), cancellationToken: CancellationToken.None);
        var b = new ZSetBuilder<StructuralRow, Z64>();
        await foreach (var batch in table.ReadAllAsync(null, CancellationToken.None))
        {
            for (var i = 0; i < batch.Length; i++)
            {
                var row = new object?[cols];
                for (var c = 0; c < cols; c++)
                {
                    row[c] = batch.Column(c) switch
                    {
                        Int32Array a => (object?)a.GetValue(i),
                        Int64Array a => a.GetValue(i),
                        _ => throw new NotSupportedException($"col {c} type {batch.Column(c).GetType().Name}"),
                    };
                }

                b.Add(new StructuralRow(row), Z64.One);
            }
        }

        table.Dispose();
        return b.Build();
    }

    [Fact]
    public async Task Program_TwoSources_TwoOutputs_OverDelta_MatchesBatchAcrossBatches()
    {
        var companyDir = Path.Combine(_root, "company");
        var tradeDir = Path.Combine(_root, "trade");
        var enrichedDir = Path.Combine(_root, "out_enriched");
        var totalsDir = Path.Combine(_root, "out_totals");
        Directory.CreateDirectory(enrichedDir);
        Directory.CreateDirectory(totalsDir);

        var companyNet = new List<object?[]>();
        var tradeNet = new List<object?[]>();

        // Batch 1 (full load).
        var company = await CreateTable(companyDir, CompanySchema());
        await Append(company, CompanySchema(), companyNet, new object?[] { 1, 100, 1 }, new object?[] { 2, 200, 1 });
        var trade = await CreateTable(tradeDir, TradeSchema());
        await Append(trade, TradeSchema(), tradeNet, new object?[] { 10, 1, 50 }, new object?[] { 11, 2, 30 });

        var resolved = SqlProgram.Resolve(ProgramSql, OutputViews);
        var program = PlanToCircuit.CompileProgram(resolved.Tables, resolved.Views);

        var companyIn = new DeltaInputConnector("company", companyDir);
        var tradeIn = new DeltaInputConnector("trade", tradeDir);
        var enrichedOut = new DeltaOutputConnector("trade_enriched", enrichedDir, OutputMode.Truncate);
        var totalsOut = new DeltaOutputConnector("company_totals", totalsDir, OutputMode.Truncate);

        var runner = await ProgramRunner.CreateAsync(
            program,
            new IInputConnector[] { companyIn, tradeIn },
            new IOutputConnector[] { enrichedOut, totalsOut });

        await runner.RunBatchAsync();
        await AssertOutputsMatch(resolved, companyNet, tradeNet, enrichedDir, totalsDir);

        // Batch 2 (append): SCD2 update to company 1 + new trades. The shared current-record
        // view re-ranks and flows into both outputs.
        await Append(company, CompanySchema(), companyNet, new object?[] { 1, 101, 2 });
        await Append(trade, TradeSchema(), tradeNet, new object?[] { 12, 1, 20 }, new object?[] { 13, 2, 40 });

        await runner.RunBatchAsync();
        await AssertOutputsMatch(resolved, companyNet, tradeNet, enrichedDir, totalsDir);

        company.Dispose();
        trade.Dispose();
        await companyIn.DisposeAsync();
        await tradeIn.DisposeAsync();
        await enrichedOut.DisposeAsync();
        await totalsOut.DisposeAsync();
    }

    private static async Task AssertOutputsMatch(
        ResolvedProgram resolved, List<object?[]> companyNet, List<object?[]> tradeNet, string enrichedDir, string totalsDir)
    {
        var batch = BatchAll(resolved, NetOf(companyNet), NetOf(tradeNet));

        var enriched = await ReadSink(enrichedDir, cols: 3);
        Assert.True(enriched.Equals(batch["trade_enriched"]),
            $"trade_enriched\n  sink={enriched}\n  batch={batch["trade_enriched"]}");

        var totals = await ReadSink(totalsDir, cols: 2);
        Assert.True(totals.Equals(batch["company_totals"]),
            $"company_totals\n  sink={totals}\n  batch={batch["company_totals"]}");
    }
}
