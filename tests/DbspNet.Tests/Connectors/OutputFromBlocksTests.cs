// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
// A full output write built from the view's blocks (docs/design-bounded-memory.md §3h): Arrow
// batches copied from the blocks' columns and the connector's row index gathered from the same
// entries, no row decoded. The oracle is the row path: the same program and input written by a
// runner with block writes off must leave an identical Delta table after every batch, and later
// incremental batches (deletes found through the gathered index) must keep it identical. Every
// column kind, nulls, multiplicities above one, serial and parallel, and tiny blocks so the views
// hold several runs and an unfrozen level when written.
using Apache.Arrow;
using DbspNet.Arrow;
using DbspNet.Connectors.Abstractions;
using DbspNet.Connectors.EngineeredWood;
using DbspNet.Core.Operators.Stateful.Blocks;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;
using Xunit;
using SqlSchema = DbspNet.Sql.Plan.Schema;

namespace DbspNet.Tests.Connectors;

public sealed class OutputFromBlocksTests : IDisposable
{
    private static readonly string[] Sql =
    [
        "CREATE TABLE r (k INT NOT NULL, b BIGINT, s VARCHAR)",
        "CREATE VIEW wide AS SELECT k, b, s, CAST(b AS DOUBLE) / 3 AS f, CAST(b AS REAL) AS r4, b > 3 AS flag, " +
        "CAST(b AS DECIMAL(12, 2)) / 4 AS x, CASE WHEN b > 5 THEN DATE '2024-01-02' ELSE DATE '2023-12-31' END AS d, " +
        "CASE WHEN b % 2 = 0 THEN TIMESTAMP '2024-01-02 03:04:05' ELSE NULL END AS ts FROM r",
        // A bag: rows in both branches, and repeated input rows, have multiplicity above one.
        "CREATE VIEW bag AS SELECT s, k FROM r UNION ALL SELECT s, k FROM r WHERE k > 1",
        "CREATE VIEW grp AS SELECT k, s, COUNT(*) AS c FROM r GROUP BY k, s",
    ];

    private static readonly string[] Views = ["wide", "bag", "grp"];

    private readonly string _root = Path.Combine(Path.GetTempPath(), "dbspnet-outblocks-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        if (Directory.Exists(_root))
        {
            Directory.Delete(_root, recursive: true);
        }
    }

    private static SqlSchema Input() =>
        new([
            new SchemaColumn("k", new SqlIntegerType(false)),
            new SchemaColumn("b", new SqlBigintType(true)),
            new SchemaColumn("s", new SqlVarcharType(null, true)),
        ]);

    private static readonly string[] Strings = ["", "a", "é", "日本語", "longer string value", "\U0001F600"];

    // Inserts (some repeated, so multiplicities exceed one; some with nulls) and deletes of live rows.
    private static List<(object?[] Row, long Weight)> NextVersion(Random rng, List<object?[]> live, int size)
    {
        var version = new List<(object?[] Row, long Weight)>();
        for (var i = 0; i < size; i++)
        {
            if (live.Count > 0 && rng.Next(4) == 0)
            {
                var at = rng.Next(live.Count);
                version.Add((live[at], -1));
                live.RemoveAt(at);
                continue;
            }

            var row = live.Count > 0 && rng.Next(6) == 0
                ? live[rng.Next(live.Count)]
                : new object?[]
                {
                    rng.Next(0, 6),
                    rng.Next(5) == 0 ? null : (long)rng.Next(-3, 12),
                    rng.Next(5) == 0 ? null : Strings[rng.Next(Strings.Length)] + rng.Next(0, 3),
                };
            version.Add((row, 1));
            live.Add(row);
        }

        return version;
    }

    private string Dir(string arm, string view)
    {
        var dir = Path.Combine(_root, arm, view);
        Directory.CreateDirectory(dir);
        return dir;
    }

    // native: the batches' buffers from Arrow's native allocator (as under a GC heap limit) or managed.
    [Theory]
    [InlineData(1, 1, false)]
    [InlineData(2, 1, true)]
    [InlineData(1, 3, true)]
    [InlineData(2, 3, false)]
    public async Task AFullWriteFromBlocks_LeavesTheSameTableAsTheRowPath_AndIncrementalBatchesCarryOn(int seed, int workers, bool native)
    {
        using var mode = TraceBlocksMode.Use(join: true, freezeEntries: 8, blockEntries: 4, view: true);
        using var buffers = DbspNet.Persistence.Blocks.ViewArrow.UseNative(native);
        var versions = new List<IReadOnlyList<(object?[] Row, long Weight)>>();
        var live = new List<object?[]>();
        var rng = new Random(seed);
        for (var v = 0; v < 6; v++)
        {
            versions.Add(NextVersion(rng, live, 40));
        }

        var fromBlocks = await Runner("blocks", versions, workers);
        var fromRows = await Runner("rows", versions, workers);
        fromRows.Runner.ViewBlocks = false;
        for (var batch = 0; batch < 5; batch++)
        {
            await fromBlocks.Runner.RunBatchAsync();
            await fromRows.Runner.RunBatchAsync();
            foreach (var view in Views)
            {
                var a = await ReadSink(Dir("blocks", view));
                var b = await ReadSink(Dir("rows", view));
                Assert.True(Same(a, b), $"batch {batch} view {view}:\nblocks {Show(a)}\nrows   {Show(b)}");
                Assert.NotEmpty(a);
            }

            // The first batch's write is the full one: every view from blocks, none by rows.
            Assert.Equal(Views.Length, fromBlocks.Runner.ViewBlockWrites);
            Assert.Equal(0, fromRows.Runner.ViewBlockWrites);
            versions.Add(NextVersion(rng, live, 15));
        }

        foreach (var o in fromBlocks.Outputs.Concat(fromRows.Outputs))
        {
            await o.DisposeAsync();
        }
    }

    private async Task<(ProgramRunner Runner, List<IOutputConnector> Outputs)> Runner(
        string arm, List<IReadOnlyList<(object?[] Row, long Weight)>> versions, int workers)
    {
        var outputs = Views.Select(v => (IOutputConnector)new DeltaOutputConnector(v, Dir(arm, v), OutputMode.Incremental)).ToList();
        IInputConnector[] inputs = [new FakeInputConnector("r", Input(), versions)];
        if (workers == 1)
        {
            var program = SqlProgram.Compile(Sql, new HashSet<string>(Views, StringComparer.Ordinal));
            return (await ProgramRunner.CreateAsync(program, inputs, outputs), outputs);
        }

        Assert.True(SqlProgram.TryCompileParallel(Sql, new HashSet<string>(Views, StringComparer.Ordinal), workers, out var parallel));
        return (await ProgramRunner.CreateAsync(parallel!, inputs, outputs), outputs);
    }

    // Each row of the table, every column formatted from its Arrow bytes, with its count.
    private static async Task<Dictionary<string, long>> ReadSink(string dir)
    {
        var table = await DeltaTable.OpenAsync(new LocalTableFileSystem(dir), cancellationToken: CancellationToken.None);
        var map = new Dictionary<string, long>(StringComparer.Ordinal);
        await foreach (var batch in table.ReadAllAsync(null, CancellationToken.None))
        {
            for (var i = 0; i < batch.Length; i++)
            {
                var cells = new string[batch.ColumnCount];
                for (var c = 0; c < batch.ColumnCount; c++)
                {
                    var column = batch.Column(c);
                    cells[c] = column.IsNull(i) ? "<null>" : column switch
                    {
                        StringArray a => "'" + a.GetString(i) + "'",
                        Int32Array a => a.GetValue(i)!.Value.ToString(System.Globalization.CultureInfo.InvariantCulture),
                        Int64Array a => a.GetValue(i)!.Value.ToString(System.Globalization.CultureInfo.InvariantCulture),
                        DoubleArray a => BitConverter.DoubleToInt64Bits(a.GetValue(i)!.Value).ToString("x", System.Globalization.CultureInfo.InvariantCulture),
                        FloatArray a => BitConverter.SingleToInt32Bits(a.GetValue(i)!.Value).ToString("x", System.Globalization.CultureInfo.InvariantCulture),
                        BooleanArray a => a.GetValue(i)!.Value ? "T" : "F",
                        Date32Array a => "d" + a.GetValue(i)!.Value.ToString(System.Globalization.CultureInfo.InvariantCulture),
                        TimestampArray a => "t" + a.GetValue(i)!.Value.ToString(System.Globalization.CultureInfo.InvariantCulture),
                        Decimal128Array a => "x" + Convert.ToHexString(a.ValueBuffer.Span.Slice(i * 16, 16)),
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

    private static bool Same(Dictionary<string, long> a, Dictionary<string, long> b) =>
        a.Count == b.Count && a.All(kv => b.TryGetValue(kv.Key, out var v) && v == kv.Value);

    private static string Show(Dictionary<string, long> m) =>
        string.Join(", ", m.OrderBy(e => e.Key, StringComparer.Ordinal).Take(40).Select(e => $"{e.Key}:{e.Value}"));
}
