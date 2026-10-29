// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using Apache.Arrow;
using DbspNet.Arrow;
using DbspNet.Connectors.Abstractions;
using DbspNet.Connectors.EngineeredWood;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;
using Xunit;
using SqlSchema = DbspNet.Sql.Plan.Schema;

namespace DbspNet.Tests.Connectors;

/// <summary>
/// <see cref="OutputMode.Incremental"/> on <see cref="DeltaOutputConnector"/>: the Delta table
/// must equal the engine's view after every batch, while each batch writes only its net change
/// (deletion vectors over retracted rows plus one file of inserted rows) instead of
/// rewriting the view as <see cref="OutputMode.Truncate"/> does.
/// </summary>
public sealed class IncrementalDeltaOutputTests : IDisposable
{
    private static readonly string[] ProgramSql =
    [
        "CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)",
        // Updates: a group's row is retracted and re-inserted when its sums change.
        "CREATE VIEW totals AS SELECT a, SUM(b) AS s, COUNT(*) AS c FROM t GROUP BY a",
        // A bag: a row in both branches has multiplicity 2.
        "CREATE VIEW bag AS SELECT a FROM t WHERE b > 0 UNION ALL SELECT a FROM t WHERE b < 10",
        // Duplicate input rows give multiplicity > 1.
        "CREATE VIEW rows_ AS SELECT a, b FROM t",
    ];

    private static readonly string[] Views = ["totals", "bag", "rows_"];

    private readonly string _root = Path.Combine(Path.GetTempPath(), "dbspnet-incdelta-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        if (Directory.Exists(_root))
        {
            Directory.Delete(_root, recursive: true);
        }
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public async Task SinkEqualsView_AfterEveryBatch_AndEachBatchAddsAtMostOneFile(int seed)
    {
        var rng = new Random(seed);
        var versions = new List<IReadOnlyList<(object?[] Row, long Weight)>>();
        var live = new List<object?[]>();
        var program = SqlProgram.Compile(ProgramSql, new HashSet<string>(Views, StringComparer.Ordinal));
        var outputs = Outputs();
        var runner = await ProgramRunner.CreateAsync(
            program, [new FakeInputConnector("t", TwoInts(), versions)], outputs);

        for (var batch = 0; batch < 8; batch++)
        {
            for (var v = rng.Next(1, 4); v > 0; v--)
            {
                versions.Add(NextVersion(rng, live));
            }

            await runner.RunBatchAsync();
            foreach (var view in Views)
            {
                var sink = await ReadSink(view);
                Assert.True(
                    Same(Materialize(program.Outputs[view]), sink),
                    $"batch {batch} view {view}: sink {Show(sink)}\nview {Show(Materialize(program.Outputs[view]))}");
                Assert.True(DataFiles(view) <= batch + 1, $"view {view} has {DataFiles(view)} files after {batch + 1} batches");
            }
        }

        await DisposeAll(outputs);
    }

    [Fact]
    public async Task Restart_TruncatesTheExistingTable_ThenContinuesIncrementally()
    {
        var rng = new Random(7);
        var versions = new List<IReadOnlyList<(object?[] Row, long Weight)>>();
        var live = new List<object?[]>();
        for (var i = 0; i < 3; i++)
        {
            versions.Add(NextVersion(rng, live));
        }

        var first = SqlProgram.Compile(ProgramSql, new HashSet<string>(Views, StringComparer.Ordinal));
        var outputs1 = Outputs();
        var r1 = await ProgramRunner.CreateAsync(first, [new FakeInputConnector("t", TwoInts(), versions)], outputs1);
        await r1.RunBatchAsync();
        await DisposeAll(outputs1);

        // A new process over the same tables, replaying every version (no checkpoint): its
        // first write truncates what the old process left, then later batches are incremental.
        for (var i = 0; i < 2; i++)
        {
            versions.Add(NextVersion(rng, live));
        }

        var second = SqlProgram.Compile(ProgramSql, new HashSet<string>(Views, StringComparer.Ordinal));
        var outputs2 = Outputs();
        var r2 = await ProgramRunner.CreateAsync(second, [new FakeInputConnector("t", TwoInts(), versions)], outputs2);
        await r2.RunBatchAsync();
        versions.Add(NextVersion(rng, live));
        await r2.RunBatchAsync();

        foreach (var view in Views)
        {
            Assert.True(Same(Materialize(second.Outputs[view]), await ReadSink(view)), $"view {view}");
        }

        await DisposeAll(outputs2);
    }

    [Fact]
    public async Task ExistingTableWithoutDeletionVectors_IsRefused()
    {
        var dir = Path.Combine(_root, "plain");
        Directory.CreateDirectory(dir);
        var plain = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(dir), ArrowSchemaBridge.ToArrow(TwoInts()), cancellationToken: CancellationToken.None);
        plain.Dispose();

        var connector = new DeltaOutputConnector("v", dir, OutputMode.Incremental);
        var error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => connector.BindSchemaAsync(TwoInts(), CancellationToken.None).AsTask());
        Assert.Contains("deletion vectors", error.Message, StringComparison.Ordinal);
    }

    private static async Task DisposeAll(List<IOutputConnector> outputs)
    {
        foreach (var o in outputs)
        {
            await o.DisposeAsync();
        }
    }

    private List<IOutputConnector> Outputs() =>
        Views.Select(v => (IOutputConnector)new DeltaOutputConnector(v, TableDir(v), OutputMode.Incremental)).ToList();

    private string TableDir(string view)
    {
        var dir = Path.Combine(_root, view);
        Directory.CreateDirectory(dir);
        return dir;
    }

    private int DataFiles(string view) => Directory.GetFiles(TableDir(view), "*.parquet").Length;

    private static SqlSchema TwoInts() =>
        new([
            new SchemaColumn("a", new SqlIntegerType(false)),
            new SchemaColumn("b", new SqlIntegerType(false)),
        ]);

    // Inserts (some duplicates, so multiplicities exceed 1) and deletes of live rows only.
    private static List<(object?[] Row, long Weight)> NextVersion(Random rng, List<object?[]> live)
    {
        var version = new List<(object?[] Row, long Weight)>();
        for (var i = rng.Next(1, 8); i > 0; i--)
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
                    : new object?[] { rng.Next(0, 4), rng.Next(-5, 15) };
                version.Add((row, 1));
                live.Add(row);
            }
        }

        return version;
    }

    private async Task<Dictionary<string, long>> ReadSink(string view)
    {
        var table = await DeltaTable.OpenAsync(new LocalTableFileSystem(TableDir(view)), cancellationToken: CancellationToken.None);
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

    private static Dictionary<string, long> Materialize(ProgramOutput output)
    {
        var map = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach (var kv in output.CurrentView)
        {
            var cells = new string[output.Schema.Count];
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
