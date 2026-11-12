// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
// D5 for TOP-K (docs/design-bounded-memory.md §5.47, as for rank in §5.43): with in-step spilling on,
// a blocked partitioned TOP-K steps a delta past the step chunk one slice of partitions at a time.
// Required: every tick's outputs equal an unsliced twin's (ROW_NUMBER, RANK and DENSE_RANK cuts,
// inserts, deletes and multiplicities); the sliced program really slices; a step runs only the
// slices its rows reach.
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Core.Operators.Stateful.Blocks;
using DbspNet.Persistence;
using DbspNet.Sql.Compiler;
using Xunit;

namespace DbspNet.Tests.Operators.Stateful;

public class TopKSliceTests
{
    private static readonly string[] Sql =
    [
        "CREATE TABLE t (a INT NOT NULL, b INT NOT NULL, c VARCHAR)",
        "CREATE VIEW rn AS SELECT a, b, c FROM (SELECT a, b, c, ROW_NUMBER() OVER (PARTITION BY a ORDER BY b, c) AS r FROM t) x WHERE r <= 2",
        "CREATE VIEW rk AS SELECT a, b FROM (SELECT a, b, RANK() OVER (PARTITION BY a ORDER BY b) AS r FROM t) x WHERE r <= 1",
        "CREATE VIEW dr AS SELECT a, b FROM (SELECT a, b, DENSE_RANK() OVER (PARTITION BY a % 37 ORDER BY b DESC) AS r FROM t) x WHERE r <= 3",
    ];

    private static readonly HashSet<string> Views = new(["rn", "rk", "dr"], StringComparer.Ordinal);

    private static CompiledProgram Compile()
    {
        using var blocks = TraceBlocksMode.Use(join: true, freezeEntries: 16, blockEntries: 4, window: true);
        using var writeBack = SpillSettings.UseWindowWriteBack(0);
        return SqlProgram.Compile(Sql, Views, ArrowSqlSnapshotCodecs.Instance);
    }

    private static List<PartitionedTopKCompactOp<StructuralRow, StructuralRow>> TopKs(CompiledProgram p) =>
        [.. p.Circuit.Operators.OfType<PartitionedTopKCompactOp<StructuralRow, StructuralRow>>()];

    private static string Digest(CompiledProgram p) =>
        string.Join("\n", Views.Order(StringComparer.Ordinal).Select(v => v + ": " + string.Join("; ",
            p.Outputs[v].CurrentView.Select(e => string.Join("|", Enumerable.Range(0, p.Outputs[v].Schema.Count).Select(i => FormattableString.Invariant($"{e.Key[i]}"))) + "#" + e.Value.Value)
                .Order(StringComparer.Ordinal))));

    // Tick 0 loads 600 partitions of 1–6 rows, some rows twice; later ticks insert into, delete from
    // and empty a spread of them, and open new ones.
    private static void Tick(CompiledProgram p, int tick)
    {
        var t = p.Table("t");
        if (tick == 0)
        {
            for (var a = 0; a < 600; a++)
            {
                for (var b = 0; b <= a % 6; b++)
                {
                    t.Insert(a, (a * 7 + b * 3) % 11, b % 3 == 0 ? null : "v" + b);
                    if ((a + b) % 9 == 0)
                    {
                        t.Insert(a, (a * 7 + b * 3) % 11, b % 3 == 0 ? null : "v" + b);
                    }
                }
            }
        }
        else
        {
            for (var a = tick % 5; a < 700; a += 3)
            {
                t.Insert(a, (tick * 5 + a) % 11, "n" + tick);
                if (a < 600 && a % 2 == tick % 2)
                {
                    t.Delete(a, (a * 7) % 11, null);
                }
            }
        }

        p.Step();
    }

    private static void Run(long? stepRows, int forcedChunk)
    {
        // The twin is pinned unsliced: a forced-spill environment would otherwise slice it too.
        CompiledProgram plain;
        using (SpillSettings.UseStepSpill(null))
        {
            plain = Compile();
        }

        CompiledProgram sliced;
        using (SpillSettings.UseStepSpill(stepRows, forcedChunk))
        {
            sliced = Compile();
        }

        Assert.NotEmpty(TopKs(sliced));
        Assert.All(TopKs(sliced), op => Assert.True(op.IsBlocked));
        for (var tick = 0; tick < 8; tick++)
        {
            using (SpillSettings.UseStepSpill(null))
            {
                Tick(plain, tick);
            }

            using (SpillSettings.UseStepSpill(stepRows, forcedChunk))
            {
                Tick(sliced, tick);
            }

            Assert.Equal(Digest(plain), Digest(sliced));
        }

        Assert.All(TopKs(sliced), op => Assert.True(op.SlicedSteps > 0 && op.SliceSteps > op.SlicedSteps, $"{op.SlicedSteps} sliced steps, {op.SliceSteps} slices"));
        Assert.All(TopKs(plain), op => Assert.Equal(0, op.SlicedSteps));
    }

    [Fact]
    public void Sliced_steps_match_an_unsliced_twin() => Run(stepRows: 64, forcedChunk: 4096);

    [Fact]
    public void Forced_step_spill_slices_and_matches() => Run(stepRows: 0, forcedChunk: 7);

    // Two partitions fill at most two slices of a sliced step.
    [Fact]
    public void A_step_runs_only_the_slices_its_rows_reach()
    {
        string[] sql =
        [
            "CREATE TABLE g (a INT NOT NULL, b INT NOT NULL)",
            "CREATE VIEW gr AS SELECT a, b FROM (SELECT a, b, ROW_NUMBER() OVER (PARTITION BY b ORDER BY a) AS r FROM g) x WHERE r <= 3",
        ];
        var views = new HashSet<string>(["gr"], StringComparer.Ordinal);
        CompiledProgram Make()
        {
            using var blocks = TraceBlocksMode.Use(join: true, freezeEntries: 16, blockEntries: 4, window: true);
            using var writeBack = SpillSettings.UseWindowWriteBack(0);
            return SqlProgram.Compile(sql, views, ArrowSqlSnapshotCodecs.Instance);
        }

        var plain = Make();
        CompiledProgram sliced;
        using (SpillSettings.UseStepSpill(100))
        {
            sliced = Make();
        }

        var topK = TopKs(sliced).Single();
        for (var tick = 0; tick < 3; tick++)
        {
            foreach (var (p, rows) in new[] { (plain, (long?)null), (sliced, 100L) })
            {
                using var _ = SpillSettings.UseStepSpill(rows);
                for (var a = 0; a < 2_000; a++)
                {
                    p.Table("g").Insert(a + (tick * 10_000), a % 2);
                }

                var (steps, slices) = (topK.SlicedSteps, topK.SliceSteps);
                p.Step();
                if (rows is not null)
                {
                    Assert.Equal(1, topK.SlicedSteps - steps);
                    Assert.InRange(topK.SliceSteps - slices, 1, 2);
                }
            }

            Assert.Equal(Digest2(plain), Digest2(sliced));
        }

        static string Digest2(CompiledProgram p) =>
            string.Join(";", p.Outputs["gr"].CurrentView.Select(e => $"{e.Key[0]}|{e.Key[1]}#{e.Value.Value}").Order(StringComparer.Ordinal));
    }
}
