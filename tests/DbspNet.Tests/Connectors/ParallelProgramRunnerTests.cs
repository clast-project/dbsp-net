// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
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
using Xunit;
using SqlSchema = DbspNet.Sql.Plan.Schema;

namespace DbspNet.Tests.Connectors;

/// <summary>
/// <see cref="ProgramRunner"/> driving a <see cref="ParallelCompiledProgram"/> must be
/// indistinguishable from it driving the serial <see cref="CompiledProgram"/>: the same
/// multi-batch insert/delete stream through both, each writing its own Delta outputs, and
/// after every batch the two engines' views agree and each Delta table equals its engine's
/// view. Incremental mode is the sharp case — it writes the <b>gathered</b> per-tick delta,
/// summed between writes — so every view here has rows that more than one worker produces
/// or retracts (a projection of a whole-row-sharded table, a union, a join output re-keyed
/// by an aggregate).
/// </summary>
public sealed class ParallelProgramRunnerTests : IDisposable
{
    private const int Workers = 4;

    private static readonly string[] ProgramSql =
    [
        "CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)",
        "CREATE TABLE u (a INT NOT NULL, c INT NOT NULL)",
        "CREATE VIEW totals AS SELECT a, SUM(b) AS s, COUNT(*) AS n FROM t GROUP BY a",
        "CREATE VIEW joined AS SELECT t.a, t.b, u.c FROM t JOIN u ON t.a = u.a",
        // Join, then re-key by the right side's column: an exchange between the two.
        "CREATE VIEW by_c AS SELECT c, SUM(b) AS s FROM joined GROUP BY c",
        "CREATE VIEW dist AS SELECT DISTINCT a FROM t",
        // Not grouped: one output row (a) comes from several workers' shards of t and u.
        "CREATE VIEW bag AS SELECT a FROM t WHERE b > 0 UNION ALL SELECT a FROM u WHERE c < 10",
        "CREATE VIEW top2 AS SELECT a, b FROM (" +
        "  SELECT a, b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY b DESC) AS rn FROM t) s WHERE rn <= 2",
        "CREATE VIEW ranked AS SELECT a, b, RANK() OVER (PARTITION BY a ORDER BY b) AS r FROM t",
        "CREATE VIEW running AS SELECT a, b, SUM(b) OVER (PARTITION BY a ORDER BY b) AS rs FROM t",
    ];

    private static readonly string[] Views = ["totals", "joined", "by_c", "dist", "bag", "top2", "ranked", "running"];

    private readonly string _root = Path.Combine(Path.GetTempPath(), "dbspnet-parrunner-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        if (Directory.Exists(_root))
        {
            Directory.Delete(_root, recursive: true);
        }
    }

    [Fact]
    public void TheProgramShards()
    {
        Assert.Empty(SqlProgram.ParallelBlockers(ProgramSql, ViewSet()));
        Assert.True(SqlProgram.TryCompileParallel(ProgramSql, ViewSet(), Workers, out var p));
        p!.Dispose();
    }

    /// <summary>The batch profile's operator section for a parallel program: one row per
    /// operator, summed over the replicas — so a stateful operator's retained rows are the
    /// whole state, not one shard's.</summary>
    [Fact]
    public void ParallelOperatorProfile_SumsTheReplicas()
    {
        Assert.True(SqlProgram.TryCompileParallel(ProgramSql, ViewSet(), Workers, out var compiled));
        using var parallel = compiled!;
        parallel.Circuit.ProfileOperators = true;
        var t = parallel.Table("t");
        for (var a = 0; a < 40; a++)
        {
            t.Insert(a, a);
        }

        parallel.Step();
        var profile = parallel.Circuit.CollectOperatorProfile();
        Assert.NotEmpty(profile);

        // DISTINCT a over 40 distinct values: its integrated state is 40 rows in total,
        // spread across the replicas.
        Assert.Contains(profile, op => op.Label == "dist" && op.RetainedRows == 40);
    }

    [Theory]
    [InlineData(OutputMode.Incremental, 1)]
    [InlineData(OutputMode.Incremental, 2)]
    [InlineData(OutputMode.Incremental, 3)]
    [InlineData(OutputMode.Truncate, 4)]
    [InlineData(OutputMode.Truncate, 5)]
    public async Task ParallelRunner_EqualsSerialRunner_AndEachSinkEqualsItsView_AfterEveryBatch(OutputMode mode, int seed)
    {
        var rng = new Random(seed);
        var tVersions = new List<IReadOnlyList<(object?[] Row, long Weight)>>();
        var uVersions = new List<IReadOnlyList<(object?[] Row, long Weight)>>();
        var tLive = new List<object?[]>();
        var uLive = new List<object?[]>();

        var serial = SqlProgram.Compile(ProgramSql, ViewSet());
        Assert.True(SqlProgram.TryCompileParallel(ProgramSql, ViewSet(), Workers, out var compiled));
        using var parallel = compiled!;

        var serialOut = Outputs("serial", mode);
        var parallelOut = Outputs("parallel", mode);
        var serialRunner = await ProgramRunner.CreateAsync(serial, Inputs(tVersions, uVersions), serialOut);
        var parallelRunner = await ProgramRunner.CreateAsync(parallel, Inputs(tVersions, uVersions), parallelOut);
        Assert.Equal(Workers, parallelRunner.Workers);

        for (var batch = 0; batch < 8; batch++)
        {
            // Several versions per batch on each table (some batches touch only one table),
            // so a batch spans several ticks and incremental mode sums deltas across them.
            for (var v = rng.Next(0, 4); v > 0; v--)
            {
                tVersions.Add(NextVersion(rng, tLive));
            }

            for (var v = rng.Next(0, 3); v > 0; v--)
            {
                uVersions.Add(NextVersion(rng, uLive));
            }

            var serialTicks = await serialRunner.RunBatchAsync();
            var parallelTicks = await parallelRunner.RunBatchAsync();
            Assert.Equal(serialTicks, parallelTicks);

            foreach (var view in Views)
            {
                var s = Materialize(serial.Outputs[view].CurrentView, serial.Outputs[view].Schema.Count);
                var p = Materialize(parallel.Outputs[view].CurrentView, parallel.Outputs[view].Schema.Count);
                Assert.True(Same(s, p), $"batch {batch} view {view}: parallel {Show(p)}\nserial {Show(s)}");

                var serialSink = await ReadSink("serial", view);
                Assert.True(Same(s, serialSink), $"batch {batch} view {view} (serial sink): {Show(serialSink)}\nview {Show(s)}");
                var parallelSink = await ReadSink("parallel", view);
                Assert.True(Same(p, parallelSink), $"batch {batch} view {view} (parallel sink): {Show(parallelSink)}\nview {Show(p)}");
            }
        }

        await DisposeAll(serialOut);
        await DisposeAll(parallelOut);
    }

    private static HashSet<string> ViewSet() => new(Views, StringComparer.Ordinal);

    private static List<IInputConnector> Inputs(
        List<IReadOnlyList<(object?[] Row, long Weight)>> t, List<IReadOnlyList<(object?[] Row, long Weight)>> u) =>
        [new FakeInputConnector("t", Schema("a", "b"), t), new FakeInputConnector("u", Schema("a", "c"), u)];

    private List<IOutputConnector> Outputs(string engine, OutputMode mode) =>
        Views.Select(v => (IOutputConnector)new DeltaOutputConnector(v, TableDir(engine, v), mode)).ToList();

    private static async Task DisposeAll(List<IOutputConnector> outputs)
    {
        foreach (var o in outputs)
        {
            await o.DisposeAsync();
        }
    }

    private string TableDir(string engine, string view)
    {
        var dir = Path.Combine(_root, engine, view);
        Directory.CreateDirectory(dir);
        return dir;
    }

    private static SqlSchema Schema(string first, string second) =>
        new([
            new SchemaColumn(first, new SqlIntegerType(false)),
            new SchemaColumn(second, new SqlIntegerType(false)),
        ]);

    // Inserts (some duplicates, so multiplicities exceed 1) and deletes of live rows only.
    private static List<(object?[] Row, long Weight)> NextVersion(Random rng, List<object?[]> live)
    {
        var version = new List<(object?[] Row, long Weight)>();
        for (var i = rng.Next(1, 10); i > 0; i--)
        {
            if (live.Count > 0 && rng.Next(3) == 0)
            {
                var idx = rng.Next(live.Count);
                version.Add((live[idx], -1));
                live.RemoveAt(idx);
            }
            else
            {
                var row = live.Count > 0 && rng.Next(5) == 0
                    ? live[rng.Next(live.Count)]
                    : new object?[] { rng.Next(0, 5), rng.Next(-5, 15) };
                version.Add((row, 1));
                live.Add(row);
            }
        }

        return version;
    }

    private async Task<Dictionary<string, long>> ReadSink(string engine, string view)
    {
        var table = await DeltaTable.OpenAsync(new LocalTableFileSystem(TableDir(engine, view)), cancellationToken: CancellationToken.None);
        var map = new Dictionary<string, long>(StringComparer.Ordinal);
        await foreach (var batch in table.ReadAllAsync(null, CancellationToken.None))
        {
            for (var i = 0; i < batch.Length; i++)
            {
                var cells = new string[batch.ColumnCount];
                for (var c = 0; c < batch.ColumnCount; c++)
                {
                    cells[c] = batch.Column(c) switch
                    {
                        Int32Array a => a.GetValue(i)?.ToString(System.Globalization.CultureInfo.InvariantCulture) ?? "<null>",
                        Int64Array a => a.GetValue(i)?.ToString(System.Globalization.CultureInfo.InvariantCulture) ?? "<null>",
                        var other => throw new InvalidOperationException($"unexpected column type {other.GetType().Name}"),
                    };
                }

                var key = string.Join("|", cells);
                map[key] = map.GetValueOrDefault(key) + 1;
            }
        }

        table.Dispose();
        return map;
    }

    private static Dictionary<string, long> Materialize(ZSet<StructuralRow, Z64> view, int arity)
    {
        var map = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach (var kv in view)
        {
            var cells = new string[arity];
            for (var i = 0; i < cells.Length; i++)
            {
                cells[i] = kv.Key[i]?.ToString() ?? "<null>";
            }

            var key = string.Join("|", cells);
            map[key] = map.GetValueOrDefault(key) + kv.Value.Value;
            if (map[key] == 0)
            {
                map.Remove(key);
            }
        }

        return map;
    }

    private static bool Same(Dictionary<string, long> a, Dictionary<string, long> b) =>
        a.Count == b.Count && a.All(kv => b.TryGetValue(kv.Key, out var v) && v == kv.Value);

    private static string Show(Dictionary<string, long> m) =>
        string.Join(", ", m.OrderBy(e => e.Key, StringComparer.Ordinal).Select(e => $"{e.Key}:{e.Value}"));
}
