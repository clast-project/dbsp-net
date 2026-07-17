// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Apache.Arrow.Types;
using DbspNet.Arrow;
using DbspNet.Connectors.Abstractions;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Persistence;
using DbspNet.Persistence.IO;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;
using SqlSchema = DbspNet.Sql.Plan.Schema;

namespace DbspNet.Tests.Connectors;

/// <summary>
/// Phase-1 coverage for the connector framework (docs/design-connectors.md): the
/// <see cref="PipelineRunner"/> driving a query from a scripted source to a spy sink,
/// one tick per version, with the integrated view checked against a batch oracle; the
/// schema infer-unless-declared handshake; and checkpoint/recovery (restore to tick T
/// + resume from the committed offset).
/// </summary>
public class PipelineRunnerTests
{
    private static SqlSchema TwoInts(string a = "a", string b = "b") =>
        new([new SchemaColumn(a, new SqlIntegerType(false)), new SchemaColumn(b, new SqlIntegerType(false))]);

    private static CompiledQuery CompileFor(Catalog cat, string sql, out LogicalPlan plan, bool stored, bool codecs)
    {
        var resolver = new Resolver(cat);
        plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(sql))).Query;
        return PlanToCircuit.Compile(
            plan,
            codecs ? ArrowSqlSnapshotCodecs.Instance : null,
            new CompileOptions { StoredOutput = stored });
    }

    private static ZSet<StructuralRow, Z64> Net(IReadOnlyList<IReadOnlyList<(object?[] Row, long Weight)>> versions)
    {
        var b = new ZSetBuilder<StructuralRow, Z64>();
        foreach (var v in versions)
        {
            foreach (var (row, w) in v)
            {
                b.Add(new StructuralRow(row), new Z64(w));
            }
        }

        return b.Build();
    }

    // Random insert/delete versions over a small domain; deletes only remove a
    // currently-present row so aggregates stay well-defined.
    private static List<IReadOnlyList<(object?[] Row, long Weight)>> RandomVersions(int seed, int count = 12)
    {
        var rng = new Random(seed);
        var present = new List<object?[]>();
        var versions = new List<IReadOnlyList<(object?[], long)>>();
        for (var t = 0; t < count; t++)
        {
            var version = new List<(object?[], long)>();
            var ops = rng.Next(1, 4);
            for (var o = 0; o < ops; o++)
            {
                if (present.Count > 0 && rng.NextDouble() < 0.35)
                {
                    var idx = rng.Next(present.Count);
                    version.Add((present[idx], -1L));
                    present.RemoveAt(idx);
                }
                else
                {
                    var row = new object?[] { rng.Next(0, 3), rng.Next(0, 4) };
                    present.Add(row);
                    version.Add((row, 1L));
                }
            }

            versions.Add(version);
        }

        return versions;
    }

    // ---- Differential: integrated view == batch, one tick per version ----------

    [Theory]
    [InlineData("SELECT a, b FROM t WHERE b > 1")]
    [InlineData("SELECT a, SUM(b) AS s FROM t GROUP BY a")]
    [InlineData("SELECT a, COUNT(*) AS c FROM t GROUP BY a")]
    [InlineData("SELECT DISTINCT a FROM t")]
    [InlineData("SELECT a, b, RANK() OVER (PARTITION BY a ORDER BY b DESC) AS r FROM t")]
    public async Task ViewEqualsBatch_OneTickPerVersion(string sql)
    {
        for (var seed = 0; seed < 8; seed++)
        {
            var versions = RandomVersions(seed);
            var catalog = new Catalog();
            LogicalPlan? plan = null;
            var input = new FakeInputConnector("t", TwoInts(), versions);
            var output = new FakeOutputConnector("v", OutputMode.Truncate);

            var runner = await PipelineRunner.CreateAsync(
                catalog,
                new (IInputConnector, SqlSchema?)[] { (input, null) },
                cat => CompileFor(cat, sql, out plan!, stored: true, codecs: false),
                new IOutputConnector[] { output });

            var ticks = await runner.DrainAsync();

            // One engine tick per source version.
            Assert.Equal(versions.Count, ticks);
            Assert.Equal(versions.Count, runner.Query.Circuit.TickCount);

            // The integrated view equals the batch re-computation over the net input.
            var ctx = new BatchEvalContext(
                new Dictionary<string, ZSet<StructuralRow, Z64>>(StringComparer.Ordinal) { ["t"] = Net(versions) },
                new Dictionary<CteRef, ZSet<StructuralRow, Z64>>());
            var batch = BatchPlanEvaluator.Evaluate(plan!, ctx);
            Assert.True(
                runner.Query.CurrentView.Equals(batch),
                $"seed={seed} sql={sql}\n  view={runner.Query.CurrentView}\n  batch={batch}");

            // The sink received one write per tick; the last write's row count equals the
            // view's total multiplicity (ToArrowView expands each row by its weight, so a
            // duplicate-bearing filter view writes more physical rows than distinct ones).
            Assert.Equal(versions.Count, output.Writes.Count);
            long totalMultiplicity = 0;
            foreach (var (_, w) in runner.Query.CurrentView)
            {
                totalMultiplicity += w.Value;
            }

            Assert.Equal(totalMultiplicity, output.Writes[^1].RowCount);
            Assert.NotNull(output.BoundSchema);
        }
    }

    // ---- Pipelined (read-ahead) runner ----------------------------------------

    [Theory]
    [InlineData("SELECT a, b FROM t WHERE b > 1")]
    [InlineData("SELECT a, SUM(b) AS s FROM t GROUP BY a")]
    [InlineData("SELECT DISTINCT a FROM t")]
    [InlineData("SELECT a, b, RANK() OVER (PARTITION BY a ORDER BY b DESC) AS r FROM t")]
    public async Task Pipelined_MatchesSequentialAndBatch(string sql)
    {
        for (var seed = 0; seed < 8; seed++)
        {
            var versions = RandomVersions(seed);

            // Sequential reference.
            LogicalPlan? seqPlan = null;
            var seqRunner = await PipelineRunner.CreateAsync(
                new Catalog(),
                new (IInputConnector, SqlSchema?)[] { (new FakeInputConnector("t", TwoInts(), versions), null) },
                cat => CompileFor(cat, sql, out seqPlan!, stored: true, codecs: false),
                System.Array.Empty<IOutputConnector>());
            await seqRunner.DrainAsync();

            // Pipelined.
            LogicalPlan? pipePlan = null;
            var pipeRunner = await PipelineRunner.CreateAsync(
                new Catalog(),
                new (IInputConnector, SqlSchema?)[] { (new FakeInputConnector("t", TwoInts(), versions), null) },
                cat => CompileFor(cat, sql, out pipePlan!, stored: true, codecs: false),
                System.Array.Empty<IOutputConnector>());
            var ticks = await pipeRunner.DrainPipelinedAsync(prefetch: 3);

            Assert.Equal(versions.Count, ticks);
            Assert.True(
                pipeRunner.Query.CurrentView.Equals(seqRunner.Query.CurrentView),
                $"pipelined≠sequential seed={seed} sql={sql}");

            var ctx = new BatchEvalContext(
                new Dictionary<string, ZSet<StructuralRow, Z64>>(StringComparer.Ordinal) { ["t"] = Net(versions) },
                new Dictionary<CteRef, ZSet<StructuralRow, Z64>>());
            var batch = BatchPlanEvaluator.Evaluate(pipePlan!, ctx);
            Assert.True(pipeRunner.Query.CurrentView.Equals(batch), $"pipelined≠batch seed={seed} sql={sql}");
        }
    }

    [Fact]
    public async Task Pipelined_ReadsAheadWhileEngineBlocked()
    {
        var versions = RandomVersions(seed: 2, count: 6);
        var input = new FakeInputConnector("t", TwoInts(), versions);
        using var gate = new System.Threading.SemaphoreSlim(0);
        var output = new FakeOutputConnector("v", OutputMode.Truncate) { WriteGate = gate };

        var runner = await PipelineRunner.CreateAsync(
            new Catalog(),
            new (IInputConnector, SqlSchema?)[] { (input, null) },
            cat => CompileFor(cat, "SELECT a, b FROM t", out _, stored: true, codecs: false),
            new IOutputConnector[] { output });

        // The engine driver Steps the first version, then blocks on the gated write; the
        // reader keeps prefetching ahead into the bounded channel meanwhile.
        var drain = runner.DrainPipelinedAsync(prefetch: 3).AsTask();

        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (input.NextCount < 2 && DateTime.UtcNow < deadline)
        {
            await Task.Delay(5);
        }

        Assert.True(input.NextCount >= 2, $"reader did not run ahead (NextCount={input.NextCount})");
        Assert.Empty(output.Writes); // driver still blocked on the first (gated) write

        gate.Release(int.MaxValue);
        await drain;
        Assert.Equal(versions.Count, output.Writes.Count);
    }

    [Fact]
    public async Task Changelog_Sink_ReceivesPerTickDeltas()
    {
        var versions = RandomVersions(3);
        var catalog = new Catalog();
        var input = new FakeInputConnector("t", TwoInts(), versions);
        var output = new FakeOutputConnector("v", OutputMode.Changelog);

        // Changelog output does not require StoredOutput.
        var runner = await PipelineRunner.CreateAsync(
            catalog,
            new (IInputConnector, SqlSchema?)[] { (input, null) },
            cat => CompileFor(cat, "SELECT a, b FROM t", out _, stored: false, codecs: false),
            new IOutputConnector[] { output });

        var ticks = await runner.DrainAsync();
        Assert.Equal(versions.Count, ticks);
        Assert.Equal(versions.Count, output.Writes.Count);
        Assert.Equal(Enumerable.Range(1, versions.Count).Select(i => (long)i), output.Writes.Select(w => w.Tick));
    }

    [Fact]
    public async Task TruncateOutput_WithoutStoredOutput_Throws()
    {
        var catalog = new Catalog();
        var input = new FakeInputConnector("t", TwoInts(), RandomVersions(1));
        var output = new FakeOutputConnector("v", OutputMode.Truncate);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await PipelineRunner.CreateAsync(
                catalog,
                new (IInputConnector, SqlSchema?)[] { (input, null) },
                cat => CompileFor(cat, "SELECT a, b FROM t", out _, stored: false, codecs: false),
                new IOutputConnector[] { output }));
    }

    // ---- Schema: infer-unless-declared ----------------------------------------

    [Fact]
    public void Infer_RoundTripsSupportedTypes()
    {
        var declared = new SqlSchema(
        [
            new SchemaColumn("i", new SqlIntegerType(false)),
            new SchemaColumn("l", new SqlBigintType(true)),
            new SchemaColumn("s", new SqlVarcharType(null, true)),   // Arrow StringType carries no length
            new SchemaColumn("m", new SqlDecimalType(10, 2, false)),
            new SchemaColumn("d", new SqlDateType(true)),
            new SchemaColumn("ts", new SqlTimestampType(false)),
            new SchemaColumn("b", new SqlBooleanType(true)),
            new SchemaColumn("f", new SqlDoubleType(false)),
        ]);

        var inferred = ArrowSchemaMapper.Instance.Infer(ArrowSchemaBridge.ToArrow(declared));

        Assert.Equal(declared.Count, inferred.Count);
        for (var i = 0; i < declared.Count; i++)
        {
            Assert.Equal(declared[i].Name, inferred[i].Name);
            Assert.True(declared[i].Type.Equals(inferred[i].Type), $"col {declared[i].Name}");
        }
    }

    [Fact]
    public void Bind_MatchesByName_IgnoresUnusedAndReorders()
    {
        var declared = TwoInts("a", "b");
        var source = ArrowSchemaBridge.ToArrow(new SqlSchema(
        [
            new SchemaColumn("b", new SqlIntegerType(false)),
            new SchemaColumn("c", new SqlIntegerType(false)),   // unused
            new SchemaColumn("a", new SqlIntegerType(false)),
        ]));

        var binding = ArrowSchemaMapper.Instance.Bind(declared, source);

        Assert.Equal(2, binding.SourceIndexByDeclaredColumn.Count);
        Assert.Equal(2, binding.SourceIndexByDeclaredColumn[0]); // declared "a" ← source idx 2
        Assert.Equal(0, binding.SourceIndexByDeclaredColumn[1]); // declared "b" ← source idx 0
    }

    [Fact]
    public void Bind_MissingColumn_Throws()
    {
        var declared = TwoInts("a", "b");
        var source = ArrowSchemaBridge.ToArrow(new SqlSchema([new SchemaColumn("a", new SqlIntegerType(false))]));
        Assert.Throws<InvalidOperationException>(() => ArrowSchemaMapper.Instance.Bind(declared, source));
    }

    [Fact]
    public void Bind_IncompatibleType_Throws()
    {
        var declared = TwoInts("a", "b");
        var source = ArrowSchemaBridge.ToArrow(new SqlSchema(
        [
            new SchemaColumn("a", new SqlIntegerType(false)),
            new SchemaColumn("b", new SqlVarcharType(null, false)),  // declared INT vs source VARCHAR
        ]));
        Assert.Throws<InvalidOperationException>(() => ArrowSchemaMapper.Instance.Bind(declared, source));
    }

    [Fact]
    public void FromArrowType_RejectsNested()
    {
        var nested = new StructType(new[] { new Apache.Arrow.Field("x", Int32Type.Default, true) });
        Assert.Throws<NotSupportedException>(() => ArrowSchemaBridge.FromArrowType(nested));
    }

    // ---- Checkpoint / recovery -------------------------------------------------

    [Fact]
    public async Task Recovery_RestoresTick_ResumesFromOffset_MatchesUninterrupted()
    {
        const string sql = "SELECT a, SUM(b) AS s FROM t GROUP BY a";
        var versions = RandomVersions(7, count: 12);
        var k = versions.Count / 2;
        var fs = new InMemoryTableFileSystem();
        var checkpoint = new SnapshotCheckpointStore(fs);

        // Phase A: process the first k versions, checkpointing each tick.
        var runnerA = await PipelineRunner.CreateAsync(
            new Catalog(),
            new (IInputConnector, SqlSchema?)[] { (new FakeInputConnector("t", TwoInts(), versions.Take(k).ToList()), null) },
            cat => CompileFor(cat, sql, out _, stored: true, codecs: true),
            Array.Empty<IOutputConnector>(),
            checkpoint,
            checkpointEveryTicks: 1);
        Assert.Equal(k, await runnerA.DrainAsync());

        // "Crash": a fresh circuit + a full-script source; restore, then resume.
        var runnerB = await PipelineRunner.CreateAsync(
            new Catalog(),
            new (IInputConnector, SqlSchema?)[] { (new FakeInputConnector("t", TwoInts(), versions), null) },
            cat => CompileFor(cat, sql, out _, stored: true, codecs: true),
            Array.Empty<IOutputConnector>(),
            checkpoint,
            checkpointEveryTicks: 1);
        await runnerB.RestoreAsync();
        Assert.Equal(k, runnerB.Query.Circuit.TickCount);          // restored to tick k
        var ticksB = await runnerB.DrainAsync();
        Assert.Equal(versions.Count - k, ticksB);                  // only k..N reprocessed
        Assert.Equal(versions.Count, runnerB.Query.Circuit.TickCount);

        // Control: one uninterrupted run over all versions.
        var runnerC = await PipelineRunner.CreateAsync(
            new Catalog(),
            new (IInputConnector, SqlSchema?)[] { (new FakeInputConnector("t", TwoInts(), versions), null) },
            cat => CompileFor(cat, sql, out _, stored: true, codecs: false),
            Array.Empty<IOutputConnector>());
        await runnerC.DrainAsync();

        Assert.True(
            runnerB.Query.CurrentView.Equals(runnerC.Query.CurrentView),
            $"recovered≠uninterrupted\n  recovered={runnerB.Query.CurrentView}\n  control={runnerC.Query.CurrentView}");
    }
}
