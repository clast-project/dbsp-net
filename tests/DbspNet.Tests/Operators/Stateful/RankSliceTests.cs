// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
// D5 for rank (docs/design-bounded-memory.md §5.43): with in-step spilling on, a blocked rank steps a
// delta past the step chunk one slice of partitions at a time, and its write-back cache can drop
// between slices. Required: every tick's outputs equal an unsliced twin's (ROW_NUMBER, RANK and
// DENSE_RANK, inserts, deletes and multiplicities), and a huge step holds only a slice's partitions.
using System.Reflection;
using DbspNet.Core.Operators.Stateful.Blocks;
using DbspNet.Persistence;
using DbspNet.Sql.Compiler;
using Xunit;

namespace DbspNet.Tests.Operators.Stateful;

public class RankSliceTests
{
    private static readonly string[] Sql =
    [
        "CREATE TABLE t (a INT NOT NULL, b INT NOT NULL, c VARCHAR)",
        "CREATE VIEW rn AS SELECT a, b, c, ROW_NUMBER() OVER (PARTITION BY a ORDER BY b, c) AS r FROM t",
        "CREATE VIEW rk AS SELECT a, b, c, RANK() OVER (PARTITION BY a ORDER BY b) AS r FROM t",
        "CREATE VIEW dr AS SELECT a, b, DENSE_RANK() OVER (PARTITION BY a % 37 ORDER BY b DESC) AS r FROM t",
    ];

    private static readonly HashSet<string> Views = new(["rn", "rk", "dr"], StringComparer.Ordinal);

    private static CompiledProgram Compile(long budget)
    {
        using var blocks = TraceBlocksMode.Use(join: true, freezeEntries: 16, blockEntries: 4, window: true);
        using var writeBack = SpillSettings.UseWindowWriteBack(budget);
        return SqlProgram.Compile(Sql, Views, ArrowSqlSnapshotCodecs.Instance);
    }

    private static List<OrderedPartitionStore> Stores(CompiledProgram p) =>
        [.. p.Circuit.Operators
            .Where(o => o.GetType().Name.StartsWith("PartitionedRankOp", StringComparison.Ordinal))
            .Select(o => o.GetType().GetField("_store", BindingFlags.NonPublic | BindingFlags.Instance)?.GetValue(o) as OrderedPartitionStore)
            .OfType<OrderedPartitionStore>()];

    private static string Digest(CompiledProgram p) =>
        string.Join("\n", Views.Order(StringComparer.Ordinal).Select(v => v + ": " + string.Join("; ",
            p.Outputs[v].CurrentView.Select(e => string.Join("|", Enumerable.Range(0, p.Outputs[v].Schema.Count).Select(i => FormattableString.Invariant($"{e.Key[i]}"))) + "#" + e.Value.Value)
                .Order(StringComparer.Ordinal))));

    // Tick 0 loads 600 partitions of 1–6 rows, some rows twice (weight 2); later ticks insert into,
    // delete from and empty a spread of them, and open new ones.
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
                    // A tick-0 row (for b = 0, every partition has one).
                    t.Delete(a, (a * 7) % 11, null);
                    t.Insert(a, (a * 7) % 11, null);
                    t.Delete(a, (a * 7) % 11, null);
                }
            }
        }

        p.Step();
    }

    private static void Run(long? stepRows, int forcedChunk)
    {
        var plain = Compile(0);
        CompiledProgram sliced;
        using (SpillSettings.UseStepSpill(stepRows, forcedChunk))
        {
            sliced = Compile(0);
        }

        for (var tick = 0; tick < 8; tick++)
        {
            Tick(plain, tick);
            using (SpillSettings.UseStepSpill(stepRows, forcedChunk))
            {
                Tick(sliced, tick);
            }

            Assert.Equal(Digest(plain), Digest(sliced));
        }
    }

    [Fact]
    public void Sliced_steps_match_an_unsliced_twin() => Run(stepRows: 64, forcedChunk: 4096);

    [Fact]
    public void Forced_step_spill_slices_and_matches() => Run(stepRows: 0, forcedChunk: 7);

    [Fact]
    public void A_huge_step_holds_only_a_slices_partitions()
    {
        // Budget 0: a store drops its cache whenever a slice (or step) ends. Unsliced, the one step
        // caches every partition at once; sliced, at most about a slice's.
        const int Partitions = 20_000;
        var plain = Compile(0);
        CompiledProgram sliced;
        using (SpillSettings.UseStepSpill(1000))
        {
            sliced = Compile(0);
        }

        foreach (var (p, rows) in new[] { (plain, (long?)null), (sliced, 1000L) })
        {
            using var _ = SpillSettings.UseStepSpill(rows);
            var t = p.Table("t");
            for (var a = 0; a < Partitions; a++)
            {
                t.Insert(a, a % 13, "h" + a);
            }

            p.Step();
        }

        Assert.Equal(Digest(plain), Digest(sliced));
        var rn = Stores(plain).Max(s => s.PeakCachedPartitions);
        Assert.Equal(Partitions, rn);

        // 20 slices of ~1,000 rows: each slice's partitions, with room for the hash's unevenness.
        Assert.All(Stores(sliced).Where(s => s.PeakCachedPartitions > 37), s =>
            Assert.InRange(s.PeakCachedPartitions, 1, 1500));
        Assert.Contains(Stores(sliced), s => s.PeakCachedPartitions > 500);
    }
}
