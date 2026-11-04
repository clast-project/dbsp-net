// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
// Blocked partitioned TOP-K state (docs/design-bounded-memory.md §5.12) through checkpoints, on the
// multi-view program path (structural, as ivm-bench compiles): in the runs layout the store writes
// its own files, otherwise the whole state as trace.arrows. Either way a restore matches an
// uninterrupted twin and goes on matching as ticks retract and insert around the cut, with a zero
// write-back budget so the cache is dropped every step.
using DbspNet.Core.Operators.Stateful;
using DbspNet.Core.Operators.Stateful.Blocks;
using DbspNet.Persistence;
using DbspNet.Persistence.IO;
using DbspNet.Persistence.Runs;
using DbspNet.Sql.Compiler;
using Xunit;

namespace DbspNet.Tests.Persistence;

public class TopKBlockedCheckpointTests
{
    private static readonly string[] Sql =
    [
        "CREATE TABLE t (a INT NOT NULL, b INT NOT NULL, c VARCHAR)",
        "CREATE VIEW rn AS SELECT a, b, c FROM (SELECT a, b, c, ROW_NUMBER() OVER (PARTITION BY a ORDER BY b DESC, c) AS r FROM t) s WHERE r <= 2",
        "CREATE VIEW rk AS SELECT a, b, c FROM (SELECT a, b, c, RANK() OVER (PARTITION BY a ORDER BY b) AS r FROM t) s WHERE r <= 1",
        "CREATE VIEW dr AS SELECT a, b, c FROM (SELECT a, b, c, DENSE_RANK() OVER (PARTITION BY c ORDER BY b) AS r FROM t) s WHERE r <= 2",
    ];

    private static readonly HashSet<string> Views = new(["rn", "rk", "dr"], StringComparer.Ordinal);

    private static CompiledProgram Compile()
    {
        using var blocks = TraceBlocksMode.Use(join: true, freezeEntries: 8, blockEntries: 4, window: true);
        using var writeBack = SpillSettings.UseWindowWriteBack(0);
        return SqlProgram.Compile(Sql, Views, ArrowSqlSnapshotCodecs.Instance);
    }

    private static List<object> TopKOps(CompiledProgram p) =>
        [.. p.Circuit.Operators.Where(o => o.GetType().Name.StartsWith("PartitionedTopKCompactOp", StringComparison.Ordinal))];

    private static bool IsBlocked(object op) =>
        (bool)op.GetType().GetProperty("IsBlocked", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!.GetValue(op)!;

    private static void Tick(CompiledProgram p, int tick)
    {
        var t = p.Table("t");
        var rng = new Random(tick);
        for (var i = 0; i < (tick == 0 ? 300 : 30); i++)
        {
            t.Insert(rng.Next(0, 40), rng.Next(0, 9), "c" + rng.Next(0, 5));
        }

        if (tick > 0)
        {
            // Retract some of the previous tick's rows: windows move across the cut.
            var old = new Random(tick - 1);
            for (var i = 0; i < 12; i++)
            {
                t.Delete(old.Next(0, 40), old.Next(0, 9), "c" + old.Next(0, 5));
            }
        }

        p.Step();
    }

    private static string Digest(CompiledProgram p) =>
        string.Join("\n", Views.Order(StringComparer.Ordinal).Select(v => v + ": " + string.Join("; ",
            p.Outputs[v].CurrentView.Select(e => string.Join("|", Enumerable.Range(0, p.Outputs[v].Schema.Count).Select(i => FormattableString.Invariant($"{e.Key[i]}"))) + "#" + e.Value.Value)
                .Order(StringComparer.Ordinal))));

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Blocked_topk_restores_exactly_in_either_layout(bool runsLayout)
    {
        using var layout = StateLayout.Use(runsLayout);
        var p = Compile();
        var twin = Compile();
        Assert.Equal(3, TopKOps(p).Count);
        Assert.All(TopKOps(p), op => Assert.True(IsBlocked(op)));

        var fs = new InMemoryTableFileSystem();
        CompiledProgram? restored = null;
        for (var tick = 0; tick < 10; tick++)
        {
            Tick(p, tick);
            Tick(twin, tick);
            restored?.Let(r => Tick(r, tick));
            Assert.Equal(Digest(twin), Digest(p));
            if (restored is not null)
            {
                Assert.Equal(Digest(twin), Digest(restored));
            }

            if (tick is 3 or 6)
            {
                await Snapshot.WriteAsync(p.Circuit, fs, retainCount: 1);
                restored = Compile();
                await Snapshot.ReadAsync(restored.Circuit, fs);
                Assert.Equal(Digest(p), Digest(restored));
            }
        }
    }
}

internal static class LetExtension
{
    public static void Let<T>(this T value, Action<T> action) => action(value);
}
