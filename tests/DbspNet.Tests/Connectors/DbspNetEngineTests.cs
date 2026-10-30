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
        var deploy = await RunTwoBatches(new DbspNetEngine(), ProgramSql, "serial");
        Assert.Equal(1, deploy.Workers);
        Assert.Null(deploy.SerialFallback);
    }

    /// <summary>With workers > 1 and a program whose every view shards, the engine runs the
    /// data-parallel program — Delta in, sharded push, gathered incremental Delta out — and the
    /// sink still equals the batch re-computation after every batch.</summary>
    [Fact]
    public async Task Workers4_ShardableProgram_RunsParallel_AndMatchesBatch()
    {
        var deploy = await RunTwoBatches(new DbspNetEngine(workers: 4), ProgramSql, "parallel");
        Assert.Equal(4, deploy.Workers);
        Assert.Null(deploy.SerialFallback);
    }

    /// <summary>A program with a view the parallel compile refuses (EXCEPT) falls back to the
    /// serial program, says which view and construct blocked it, and stays correct.</summary>
    [Fact]
    public async Task Workers4_UnshardableProgram_FallsBackToSerial_AndMatchesBatch()
    {
        var sql = ProgramSql.Append(
            "CREATE VIEW traded_only AS SELECT company_id FROM trade EXCEPT SELECT company_id FROM company").ToArray();
        var deploy = await RunTwoBatches(
            new DbspNetEngine(workers: 4), sql, "fallback", storedViews: new[] { "traded_only" });
        Assert.Equal(1, deploy.Workers);
        Assert.NotNull(deploy.SerialFallback);
        Assert.Contains("traded_only", deploy.SerialFallback, StringComparison.Ordinal);
        Assert.Contains("EXCEPT", deploy.SerialFallback, StringComparison.Ordinal);
    }

    /// <summary>
    /// Checkpointing with workers > 1 runs the parallel program, and a redeploy resumes it. The
    /// redeploy asks for a different worker count and gets the checkpoint's (a checkpoint pins W),
    /// and after one more batch its sink equals a from-scratch run over the same sources.
    /// </summary>
    [Fact]
    public async Task Workers4_WithSnapshotDir_RunsParallel_AndARedeployResumesAtTheRecordedWorkerCount()
    {
        var snapshotDir = Path.Combine(_root, "snap");
        var deploy = await RunTwoBatches(new DbspNetEngine(workers: 4), ProgramSql, "persistent", snapshotDir: snapshotDir);
        Assert.True(deploy.Persistent);
        Assert.Equal(4, deploy.Workers);
        Assert.Null(deploy.SerialFallback);

        var resumed = await AppendThenRedeploy(new DbspNetEngine(workers: 2), "persistent", snapshotDir);
        Assert.Equal(4, resumed.Workers);
        Assert.True(resumed.RestoredTick > 0);
        await AssertMatchesFromScratch("persistent");
    }

    /// <summary>A serial checkpoint stays serial when the redeploy asks for workers.</summary>
    [Fact]
    public async Task SerialCheckpoint_RedeployWithWorkers_StaysSerial()
    {
        var snapshotDir = Path.Combine(_root, "snap1");
        var deploy = await RunTwoBatches(new DbspNetEngine(), ProgramSql, "persistent1", snapshotDir: snapshotDir);
        Assert.Equal(1, deploy.Workers);

        var resumed = await AppendThenRedeploy(new DbspNetEngine(workers: 4), "persistent1", snapshotDir);
        Assert.Equal(1, resumed.Workers);
        Assert.True(resumed.RestoredTick > 0);
        await AssertMatchesFromScratch("persistent1");
    }

    private ProgramSpec SpecFor(string prefix, string totalsDir, string? snapshotDir) => new(
        ProgramSql,
        new[]
        {
            new InputSpec("company", Path.Combine(_root, prefix, "company")),
            new InputSpec("trade", Path.Combine(_root, prefix, "trade")),
        },
        new[] { new OutputSpec("company_totals", totalsDir) },
        SnapshotDir: snapshotDir);

    // One more version on each source, then a new engine deployed over the same sources, sink and
    // checkpoint, and one batch run on it.
    private async Task<DeployResult> AppendThenRedeploy(DbspNetEngine engine, string prefix, string snapshotDir)
    {
        foreach (var (name, schema, row) in new[]
                 {
                     ("company", CompanySchema(), new object?[] { 4, 400, 1 }),
                     ("trade", TradeSchema(), new object?[] { 16, 1, 9 }),
                 })
        {
            using var table = await DeltaTable.OpenAsync(
                new LocalTableFileSystem(Path.Combine(_root, prefix, name)), cancellationToken: CancellationToken.None);
            await Append(table, schema, new List<object?[]>(), row);
        }

        var deploy = await engine.DeployAsync(SpecFor(prefix, Path.Combine(_root, prefix, "out_totals"), snapshotDir));
        engine.Resume();
        await engine.WaitAsync();
        return deploy;
    }

    private async Task AssertMatchesFromScratch(string prefix)
    {
        var scratchDir = Path.Combine(_root, prefix, "out_scratch");
        Directory.CreateDirectory(scratchDir);
        var scratch = new DbspNetEngine();
        await scratch.DeployAsync(SpecFor(prefix, scratchDir, snapshotDir: null));
        scratch.Resume();
        await scratch.WaitAsync();

        var expected = await ReadTotals(scratchDir);
        Assert.True(expected.Count > 0);
        Assert.True((await ReadTotals(Path.Combine(_root, prefix, "out_totals"))).Equals(expected));
    }

    private async Task<DeployResult> RunTwoBatches(
        DbspNetEngine engine, string[] sql, string prefix, string[]? storedViews = null, string? snapshotDir = null)
    {
        var companyDir = Path.Combine(_root, prefix, "company");
        var tradeDir = Path.Combine(_root, prefix, "trade");
        var totalsDir = Path.Combine(_root, prefix, "out_totals");
        Directory.CreateDirectory(totalsDir);

        var companyNet = new List<object?[]>();
        var tradeNet = new List<object?[]>();

        var company = await CreateTable(companyDir, CompanySchema());
        await Append(company, CompanySchema(), companyNet, new object?[] { 1, 100, 1 }, new object?[] { 2, 200, 1 });
        var trade = await CreateTable(tradeDir, TradeSchema());
        await Append(trade, TradeSchema(), tradeNet, new object?[] { 10, 1, 50 }, new object?[] { 11, 2, 30 });

        var resolved = SqlProgram.Resolve(sql, OutputViews);

        var spec = new ProgramSpec(
            sql,
            new[] { new InputSpec("company", companyDir), new InputSpec("trade", tradeDir) },
            new[] { new OutputSpec("company_totals", totalsDir) },
            SnapshotDir: snapshotDir,
            StoredViews: storedViews);

        var deploy = await engine.DeployAsync(spec);
        Assert.Equal(2, deploy.InputCount);
        Assert.Equal(1, deploy.OutputCount);

        // Batch 1.
        var resume1 = engine.Resume();
        Assert.True(resume1.ResumedAtEpochS > 0);
        var wait1 = await engine.WaitAsync();
        Assert.Contains(wait1.Outputs, o => o.View == "company_totals");
        Assert.All(wait1.Outputs, o => Assert.Equal("success", o.Status));
        Assert.True((await ReadTotals(totalsDir)).Equals(BatchTotals(resolved, NetOf(companyNet), NetOf(tradeNet))));

        // Batch 2 (append): SCD2 update to company 1 + new trades.
        await Append(company, CompanySchema(), companyNet, new object?[] { 1, 101, 2 });
        await Append(trade, TradeSchema(), tradeNet, new object?[] { 12, 1, 20 }, new object?[] { 13, 2, 40 });

        engine.Resume();
        var wait2 = await engine.WaitAsync();
        Assert.True(wait2.Ticks >= 1);
        Assert.True((await ReadTotals(totalsDir)).Equals(BatchTotals(resolved, NetOf(companyNet), NetOf(tradeNet))));

        // Batch 3 (append): another company version, and a trade for a company with none.
        await Append(company, CompanySchema(), companyNet, new object?[] { 2, 201, 3 }, new object?[] { 3, 300, 1 });
        await Append(trade, TradeSchema(), tradeNet, new object?[] { 14, 3, 5 }, new object?[] { 15, 4, 7 });

        engine.Resume();
        await engine.WaitAsync();
        Assert.True((await ReadTotals(totalsDir)).Equals(BatchTotals(resolved, NetOf(companyNet), NetOf(tradeNet))));

        company.Dispose();
        trade.Dispose();
        return deploy;
    }

    /// <summary>
    /// A view declared `+stored: true` with no output connector must still be COMPUTED. ivm-bench
    /// has two (fact_market_history, daily_market_pulse): Feldera computes them and the benchmark
    /// measures that compute, only the Delta write is skipped. Because DbspNet prunes any view that
    /// is not an output — together with its whole upstream chain — leaving them out silently
    /// deleted work the benchmark was measuring (docs/comparison-feldera-decisions.md §6.3).
    /// </summary>
    [Fact]
    public async Task StoredViewsAreComputedThoughNotWritten()
    {
        var companyDir = Path.Combine(_root, "s_company");
        var tradeDir = Path.Combine(_root, "s_trade");
        var totalsDir = Path.Combine(_root, "s_out");
        Directory.CreateDirectory(totalsDir);

        var company = await CreateTable(companyDir, CompanySchema());
        await Append(company, CompanySchema(), new List<object?[]>(), new object?[] { 1, 100, 1 });
        var trade = await CreateTable(tradeDir, TradeSchema());
        await Append(trade, TradeSchema(), new List<object?[]>(), new object?[] { 10, 1, 50 });

        // company_current is read by company_totals, so it survives the prune either way; use a
        // view nothing else reads, which is the shape of fact_market_history / daily_market_pulse.
        var sql = ProgramSql.Append(
            "CREATE VIEW unbound_tally AS SELECT company_id, COUNT(*) AS n FROM company GROUP BY company_id").ToArray();
        var inputs = new[] { new InputSpec("company", companyDir), new InputSpec("trade", tradeDir) };
        var outputs = new[] { new OutputSpec("company_totals", totalsDir) };

        // The property is about the COMPILE SET, which DeployAsync fixes — no batch needed.
        var withoutStored = new DbspNetEngine();
        await withoutStored.DeployAsync(new ProgramSpec(sql, inputs, outputs));
        Assert.DoesNotContain(
            "unbound_tally", withoutStored.GetStats().Outputs.Select(o => o.View));

        var withStored = new DbspNetEngine();
        var deploy = await withStored.DeployAsync(new ProgramSpec(
            sql, inputs, outputs, SnapshotDir: null, StoredViews: new[] { "unbound_tally" }));

        // One Delta sink either way — the stored view is computed, not written.
        Assert.Equal(1, deploy.OutputCount);

        var views = withStored.GetStats().Outputs.Select(o => o.View).ToList();
        Assert.Contains("company_totals", views);
        Assert.Contains("unbound_tally", views);
    }

}
