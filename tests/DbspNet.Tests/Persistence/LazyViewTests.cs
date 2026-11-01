// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
// Lazy restore of Z-set traces — output views and DISTINCT — and outputs after restore
// (docs/design-durable-state.md §1f, increment 6). Required:
//
//   1. a restored view stays cold: its row count is exact without reading it, and the full write a
//      sink needs after restore streams the cold rows (EnumerateView) without reading the view into
//      memory — and equals the uninterrupted view;
//   2. a lazily restored state survives a checkpoint that rewrites it whole followed by the sweep:
//      the runs it still reads are pinned, so touching never-read rows afterwards still works.
using System.Reflection;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Persistence;
using DbspNet.Persistence.IO;
using DbspNet.Persistence.Runs;
using DbspNet.Sql.Compiler;

namespace DbspNet.Tests.Persistence;

public class LazyViewTests
{
    private static readonly string[] Sql =
    [
        "CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)",
        "CREATE TABLE u (a INT NOT NULL, s VARCHAR NOT NULL)",
        "CREATE VIEW j AS SELECT t.a, t.b, u.s FROM t JOIN u ON t.a = u.a",
        "CREATE VIEW d AS SELECT DISTINCT b FROM t",
        "CREATE VIEW lg AS SELECT a, b, LAG(b) OVER (PARTITION BY a ORDER BY b) AS p FROM t",
    ];

    private static readonly HashSet<string> Views = new(["j", "d", "lg"], StringComparer.Ordinal);

    // Above the 4,096-entry floor below which a chain is always extended, so a large change does
    // force whole rewrites.
    private const int Keys = 9000;

    private static CompiledProgram Compile() => SqlProgram.Compile(Sql, Views, ArrowSqlSnapshotCodecs.Instance);

    private static void Load(CompiledProgram p)
    {
        for (var a = 0; a < Keys; a++)
        {
            p.Table("t").Insert(a, a % 300);
            p.Table("t").Insert(a, (a * 7) % 300);
            p.Table("u").Insert(a, "s" + (a % 9));
        }

        p.Step();
    }

    private static void Touch(CompiledProgram p, int tick, int from, int to)
    {
        for (var a = from; a < to; a++)
        {
            p.Table("t").Insert(a, 1000 + tick);
            p.Table("t").Delete(a, a % 300);
        }

        p.Step();
    }

    private static string Show(IEnumerable<KeyValuePair<StructuralRow, Z64>> rows) =>
        string.Join("; ", rows.Select(e => e.Key.ToString() + "#" + e.Value.Value).Order(StringComparer.Ordinal));

    private static string Digest(CompiledProgram p) =>
        string.Join("\n", Views.Order(StringComparer.Ordinal).Select(v => v + ": " + Show(p.Outputs[v].CurrentView)));

    // The output views' IntegrateOps (IsCold is internal).
    private static List<object> ViewOps(CompiledProgram p) =>
        p.Circuit.Operators.Where(o => o.GetType().Name.StartsWith("IntegrateOp", StringComparison.Ordinal)).Cast<object>().ToList();

    private static bool IsCold(object op) =>
        (bool)op.GetType().GetProperty("IsCold", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(op)!;

    [Fact]
    public async Task ARestoredView_StaysCold_CountsExactly_AndStreamsItsFullWrite()
    {
        using var layout = StateLayout.Use(true);
        var fs = new InMemoryTableFileSystem();
        var producer = Compile();
        var twin = Compile();
        Load(producer);
        Load(twin);
        await Snapshot.WriteAsync(producer.Circuit, fs);

        var restored = Compile();
        await Snapshot.ReadAsync(restored.Circuit, fs);
        Assert.All(ViewOps(restored), op => Assert.True(IsCold(op), "a view was read whole at restore"));

        Touch(restored, 1, 0, 20);
        Touch(twin, 1, 0, 20);
        foreach (var v in Views)
        {
            // Counted and streamed without reading the view into memory.
            Assert.Equal(twin.Outputs[v].RowCount, restored.Outputs[v].RowCount);
            Assert.Equal(Show(twin.Outputs[v].CurrentView), Show(restored.Outputs[v].View.EnumerateCurrent()));
        }

        Assert.All(ViewOps(restored), op => Assert.True(IsCold(op), "counting or streaming read a view into memory"));

        // The chain carries on: a delta checkpoint, a second lazy restore, every row touched.
        await Snapshot.WriteAsync(restored.Circuit, fs);
        var again = Compile();
        await Snapshot.ReadAsync(again.Circuit, fs);
        Touch(again, 2, 0, Keys);
        Touch(twin, 2, 0, Keys);
        Assert.Equal(Digest(twin), Digest(again));
    }

    [Fact]
    public async Task ALazyState_RewrittenWholeAndSwept_StillReadsItsRuns()
    {
        using var layout = StateLayout.Use(true);
        var fs = new InMemoryTableFileSystem();
        var producer = Compile();
        var twin = Compile();
        Load(producer);
        Load(twin);
        await Snapshot.WriteAsync(producer.Circuit, fs, retainCount: 1);

        var restored = Compile();
        await Snapshot.ReadAsync(restored.Circuit, fs);

        // A change over half the state: every tracked state overflows and the next checkpoint
        // writes each whole — a new base, so the new snapshot no longer names the restored runs —
        // and retain 1 prunes the old snapshot, so the sweep would delete them.
        Touch(restored, 1, 0, 7000);
        Touch(twin, 1, 0, 7000);
        await Snapshot.WriteAsync(restored.Circuit, fs, retainCount: 1);
        await CheckpointCleanup.WaitAsync(fs);

        // Rows of keys 7000+ were never read: reading them now needs the restored runs.
        Touch(restored, 2, 7000, Keys);
        Touch(twin, 2, 7000, Keys);
        Assert.Equal(Digest(twin), Digest(restored));
    }
}
