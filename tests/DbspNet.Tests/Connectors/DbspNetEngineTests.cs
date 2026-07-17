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
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Server;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;
using SqlSchema = DbspNet.Sql.Plan.Schema;

namespace DbspNet.Tests.Connectors;

/// <summary>
/// The <see cref="DbspNetEngine"/> control service (DbspNet's pipeline-manager analogue),
/// driven through its deploy → resume → wait batch API over real local Delta tables. Each
/// batch's output sinks must equal a dependency-order batch re-computation, and the
/// per-batch <see cref="WaitResult"/> must report the outputs (all success). This is the
/// engine surface the ivm-bench dbt-server client will call.
/// </summary>
public sealed class DbspNetEngineTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "dbspnet-engine-" + Guid.NewGuid().ToString("N"));

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
        "CREATE VIEW company_current AS SELECT company_id, name_code FROM (" +
        "  SELECT company_id, name_code, " +
        "    CASE WHEN ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY effective_ts DESC) = 1 THEN 1 ELSE 0 END AS is_current " +
        "  FROM company) s WHERE is_current = 1",
        "CREATE VIEW company_totals AS SELECT c.name_code, SUM(t.amount) AS total " +
        "  FROM trade t JOIN company_current c ON t.company_id = c.company_id GROUP BY c.name_code",
    ];

    private static readonly HashSet<string> OutputViews = new(StringComparer.Ordinal) { "company_totals" };

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

    private static ZSet<StructuralRow, Z64> BatchTotals(
        ResolvedProgram program, ZSet<StructuralRow, Z64> company, ZSet<StructuralRow, Z64> trade)
    {
        var tables = new Dictionary<string, ZSet<StructuralRow, Z64>>(StringComparer.Ordinal)
        {
            ["company"] = company,
            ["trade"] = trade,
        };
        ZSet<StructuralRow, Z64> totals = ZSet<StructuralRow, Z64>.Empty;
        foreach (var v in program.Views)
        {
            var ctx = new BatchEvalContext(tables, new Dictionary<CteRef, ZSet<StructuralRow, Z64>>());
            var r = BatchPlanEvaluator.Evaluate(v.Query, ctx);
            tables[v.ViewName] = r;
            if (v.ViewName == "company_totals")
            {
                totals = r;
            }
        }

        return totals;
    }

    private static async Task<ZSet<StructuralRow, Z64>> ReadTotals(string dir)
    {
        var table = await DeltaTable.OpenAsync(new LocalTableFileSystem(dir), cancellationToken: CancellationToken.None);
        var b = new ZSetBuilder<StructuralRow, Z64>();
        await foreach (var batch in table.ReadAllAsync(null, CancellationToken.None))
        {
            var nameCode = (Int32Array)batch.Column(0);
            var total = (Int64Array)batch.Column(1);
            for (var i = 0; i < batch.Length; i++)
            {
                b.Add(new StructuralRow(new object?[] { nameCode.GetValue(i), total.GetValue(i) }), Z64.One);
            }
        }

        table.Dispose();
        return b.Build();
    }

    [Fact]
    public async Task Deploy_Resume_Wait_AcrossBatches_MatchesBatch()
    {
        var companyDir = Path.Combine(_root, "company");
        var tradeDir = Path.Combine(_root, "trade");
        var totalsDir = Path.Combine(_root, "out_totals");
        Directory.CreateDirectory(totalsDir);

        var companyNet = new List<object?[]>();
        var tradeNet = new List<object?[]>();

        var company = await CreateTable(companyDir, CompanySchema());
        await Append(company, CompanySchema(), companyNet, new object?[] { 1, 100, 1 }, new object?[] { 2, 200, 1 });
        var trade = await CreateTable(tradeDir, TradeSchema());
        await Append(trade, TradeSchema(), tradeNet, new object?[] { 10, 1, 50 }, new object?[] { 11, 2, 30 });

        var resolved = SqlProgram.Resolve(ProgramSql, OutputViews);

        var engine = new DbspNetEngine();
        var spec = new ProgramSpec(
            ProgramSql,
            new[] { new InputSpec("company", companyDir), new InputSpec("trade", tradeDir) },
            new[] { new OutputSpec("company_totals", totalsDir) });

        var deploy = await engine.DeployAsync(spec);
        Assert.Equal(2, deploy.InputCount);
        Assert.Equal(1, deploy.OutputCount);

        // Batch 1.
        var resume1 = engine.Resume();
        Assert.True(resume1.ResumedAtEpochS > 0);
        var wait1 = await engine.WaitAsync();
        Assert.Single(wait1.Outputs);
        Assert.Equal("company_totals", wait1.Outputs[0].View);
        Assert.All(wait1.Outputs, o => Assert.Equal("success", o.Status));
        Assert.True((await ReadTotals(totalsDir)).Equals(BatchTotals(resolved, NetOf(companyNet), NetOf(tradeNet))));

        // Batch 2 (append): SCD2 update to company 1 + new trades.
        await Append(company, CompanySchema(), companyNet, new object?[] { 1, 101, 2 });
        await Append(trade, TradeSchema(), tradeNet, new object?[] { 12, 1, 20 }, new object?[] { 13, 2, 40 });

        engine.Resume();
        var wait2 = await engine.WaitAsync();
        Assert.True(wait2.Ticks >= 1);
        Assert.True((await ReadTotals(totalsDir)).Equals(BatchTotals(resolved, NetOf(companyNet), NetOf(tradeNet))));

        company.Dispose();
        trade.Dispose();
    }
}
