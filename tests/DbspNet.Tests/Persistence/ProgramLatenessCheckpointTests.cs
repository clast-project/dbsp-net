// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Circuit;
using DbspNet.Core.IO;
using DbspNet.Persistence;
using DbspNet.Persistence.IO;
using DbspNet.Persistence.Runs;
using DbspNet.Sql.Compiler;

namespace DbspNet.Tests.Persistence;

/// <summary>
/// Checkpoint coverage for LATENESS on the multi-view program path (issue #3). A program's
/// input-side LatenessOperators must round-trip <c>maxSeen</c>, and the cross-view GC'd traces
/// must round-trip bounded, so that a restored program drops exactly the late rows the original
/// would and carries on step for step like a twin that never checkpointed. Without
/// <c>maxSeen</c>, the restored frontier would start unset, admit rows the original dropped, and
/// resurrect groups and keys its traces had already collected. The outputs would then differ
/// silently. Run with whole-snapshot and incremental (run-chain) layouts.
/// </summary>
public class ProgramLatenessCheckpointTests
{
    public ProgramLatenessCheckpointTests() => RunCheckpoint.Verify = true;

    public static TheoryData<string, string, bool, bool> Cases()
    {
        var data = new TheoryData<string, string, bool, bool>();
        (string Mid, string Out)[] shapes =
        [
            // GROUP BY over another view (forward-shifted key: identity frontier transform).
            ("SELECT ts + 2 AS t2, x FROM a WHERE x >= 0", "SELECT t2, COUNT(*) AS c FROM mid GROUP BY t2"),

            // An aggregate view joined to a table: GC on the join's two traces and the aggregate.
            ("SELECT ts, SUM(x) AS s FROM a GROUP BY ts", "SELECT m.ts, m.s, b.x FROM mid m JOIN b ON m.ts = b.ts"),

            // DISTINCT over a UNION view.
            ("SELECT ts FROM a UNION ALL SELECT ts FROM b", "SELECT DISTINCT ts FROM mid"),
        ];
        foreach (var (mid, @out) in shapes)
        {
            foreach (var midIsOutput in new[] { true, false })
            {
                foreach (var runs in new[] { false, true })
                {
                    data.Add(mid, @out, midIsOutput, runs);
                }
            }
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task RestoredProgram_DropsTheSameLateRows_AndMatchesItsTwin(
        string mid, string @out, bool midIsOutput, bool runs)
    {
        using var layout = StateLayout.Use(runs);
        string[] sql =
        [
            "CREATE TABLE a (ts BIGINT NOT NULL LATENESS 10, x INT NOT NULL)",
            "CREATE TABLE b (ts BIGINT NOT NULL LATENESS 10, x INT NOT NULL)",
            $"CREATE VIEW mid AS {mid}",
            $"CREATE VIEW out AS {@out}",
        ];
        var outputs = midIsOutput
            ? new HashSet<string>(["mid", "out"], StringComparer.Ordinal)
            : new HashSet<string>(["out"], StringComparer.Ordinal);
        CompiledProgram Compile() => SqlProgram.Compile(sql, outputs, ArrowSqlSnapshotCodecs.Instance);

        var fs = new InMemoryTableFileSystem();
        var p = Compile();
        long time = 0;
        var sawGc = false;
        for (var checkpoint = 0; checkpoint < 5; checkpoint++)
        {
            for (var i = 0; i < 8; i++, time++)
            {
                Step(p, time);
            }

            await Snapshot.WriteAsync(p.Circuit, fs);

            var restored = Compile();
            await Snapshot.ReadAsync(restored.Circuit, fs);
            var reference = Compile();
            for (var t = 0L; t < time; t++)
            {
                Step(reference, t);
            }

            // The restored program holds what the original held: outputs, GC frontiers, and the
            // bounded state behind them.
            Assert.Equal(Describe(p), Describe(restored));
            sawGc |= GcSites(restored).Any(s => s.GcDroppedTotal > 0 || s.GcFrontier > 0);

            // Late rows (more than 10 below the max seen), in-window rows, and new ones. Every step
            // must match the twin: the late rows must be dropped by the restored frontier, and
            // the in-window ones must land on state that survived the restore.
            for (var t = time - 15; t < time + 12; t++)
            {
                Step(restored, t);
                Step(reference, t);
                Assert.Equal(Describe(reference), Describe(restored));
            }

            Assert.Equal(Dropped(reference), Dropped(restored));
            Assert.True(Dropped(restored) > 0, "no late row was dropped after restore");
        }

        Assert.True(sawGc, "no GC frontier reached a cross-view stateful operator");
    }

    private static void Step(CompiledProgram p, long t)
    {
        p.Table("a").Insert(t, (int)(t % 7));
        p.Table("b").Insert(t, (int)(t % 5));
        p.Step();
    }

    // Each output view's full contents plus every GC site's frontier and retained size.
    private static string Describe(CompiledProgram p)
    {
        var views = p.Outputs.OrderBy(kv => kv.Key, StringComparer.Ordinal).Select(kv =>
            kv.Key + "=" + string.Join(";", kv.Value.CurrentView
                .Select(e => e.Key.ToString() + "#" + e.Value.Value)
                .Order(StringComparer.Ordinal)));
        var gc = GcSites(p).Select(s => $"{s.Name}:{s.GcFrontier}:{s.RetainedRows}");
        return string.Join(" | ", views) + " || " + string.Join(", ", gc);
    }

    private static List<OperatorStat> GcSites(CompiledProgram p) =>
        p.Circuit.CollectStats().Where(s => s.GcFrontier is not null && s.Name != "Lateness").ToList();

    private static long Dropped(CompiledProgram p) =>
        p.Circuit.Operators
            .OfType<DbspNet.Core.Operators.Stateful.LatenessOperator<DbspNet.Core.Collections.StructuralRow>>()
            .Sum(op => op.DroppedCount);
}
