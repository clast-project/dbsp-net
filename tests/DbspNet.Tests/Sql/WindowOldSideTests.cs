// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Sql.Compiler;
using Xunit;

namespace DbspNet.Tests.Sql;

/// <summary>
/// The window operators keep no emitted output (docs/design-flat-trace-state.md §3.11): a row's
/// previous output is recomputed from the partition before the delta, and the retraction must
/// cancel what was emitted bit for bit. Floating-point sums depend on the order peers are folded
/// in, so peers here are sums that round differently by order (1e16 ± 1), several are deleted per
/// tick, and the integrated view must equal a from-scratch run over the live rows every tick.
/// </summary>
public sealed class WindowOldSideTests
{
    private static readonly double[] Values = [1e16, -1e16, 1.0, 1.0, 0.1, 0.2, 3.3, -1e16, 1e16, 7.0, -0.3, 2.5];

    private static readonly string[] WindowSql =
    [
        "SUM(x) OVER (PARTITION BY p ORDER BY t)",
        "SUM(x) OVER (PARTITION BY p ORDER BY t RANGE BETWEEN 2 PRECEDING AND CURRENT ROW)",
        "SUM(x) OVER (PARTITION BY p)",
        "AVG(x) OVER (PARTITION BY p ORDER BY t DESC)",
        "LAG(x, 1, 0.0) OVER (PARTITION BY p ORDER BY t)",
        "MIN(x) OVER (ORDER BY t)",
        // Exact aggregates: on blocks, large partitions fold from chunk partials (§3.15).
        "COUNT(*) OVER (PARTITION BY p ORDER BY t)",
        "MAX(x) OVER (PARTITION BY p ORDER BY t)",
        "MIN(id) OVER (PARTITION BY p ORDER BY t)",
        "COUNT(x) OVER (PARTITION BY p ORDER BY t RANGE BETWEEN 3 PRECEDING AND CURRENT ROW)",
        "MAX(x) OVER (PARTITION BY p ORDER BY t DESC)",
        "COUNT(CASE WHEN x > 1 THEN x END) OVER (PARTITION BY p ORDER BY t)",
        // Offsets: on blocks, large partitions read only the neighbourhood of a change (§3.19).
        "LEAD(x, 2) OVER (PARTITION BY p ORDER BY t, id)",
        "LAG(id, 1) OVER (PARTITION BY p ORDER BY t DESC, id)",
        "FIRST_VALUE(x) OVER (PARTITION BY p ORDER BY t)",
        "LAST_VALUE(id) OVER (PARTITION BY p ORDER BY t DESC)",
        "LAG(x) OVER (PARTITION BY p ORDER BY CASE WHEN t > 5 THEN t END NULLS FIRST, id)",
        "LEAD(id, 3, -1) OVER (PARTITION BY p ORDER BY CASE WHEN t < 4 THEN t END DESC NULLS LAST, id)",
    ];

    // blocks: the window's input held as blocks (§3.13), with tiny runs so freezes and merges
    // happen every tick; the fresh run is managed, so this also checks the two agree.
    public static TheoryData<string, bool> Windows()
    {
        var data = new TheoryData<string, bool>();
        foreach (var w in WindowSql)
        {
            data.Add(w, false);
            data.Add(w, true);
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(Windows))]
    public void IntegratedView_EqualsAFreshRun_EveryTick(string window, bool blocks)
    {
        string[] sql =
        [
            "CREATE TABLE t (id INT NOT NULL, p INT NOT NULL, t INT NOT NULL, x DOUBLE NOT NULL)",
            $"CREATE VIEW w AS SELECT id, p, t, x, {window} AS s FROM t",
        ];
        var views = new HashSet<string>(["w"], StringComparer.Ordinal);
        CompiledProgram program;
        using (blocks ? DbspNet.Core.Operators.Stateful.Blocks.TraceBlocksMode.Use(join: false, freezeEntries: 3, blockEntries: 2, window: true) : null)
        using (blocks ? DbspNet.Core.Operators.Stateful.Blocks.TraceBlocksMode.UseChunks(promoteRows: 3, chunkRows: 2) : null)
        {
            program = SqlProgram.Compile(sql, views);
        }

        var rnd = new Random(window.Length);
        var live = new List<object?[]>();
        var next = 0;
        for (var tick = 0; tick < 25; tick++)
        {
            var input = program.Table("t");

            // Delete several live rows at once (the removed rows of one partition merge into its
            // order), then insert more, mostly peers (few distinct t).
            for (var d = rnd.Next(0, 4); d > 0 && live.Count > 0; d--)
            {
                var victim = live[rnd.Next(live.Count)];
                live.Remove(victim);
                input.Delete(victim);
            }

            for (var n = rnd.Next(1, 6); n > 0; n--)
            {
                // Sometimes a second copy of a live row: weights above one fill several slots.
                var row = live.Count > 0 && rnd.Next(5) == 0
                    ? live[rnd.Next(live.Count)]
                    : new object?[] { next++, rnd.Next(2), rnd.Next(blocks ? 12 : 3), Values[rnd.Next(Values.Length)] };
                live.Add(row);
                input.Insert(row);
            }

            program.Step();

            var fresh = SqlProgram.Compile(sql, views);
            foreach (var row in live)
            {
                fresh.Table("t").Insert(row);
            }

            fresh.Step();
            Assert.Equal(fresh.Outputs["w"].CurrentView, program.Outputs["w"].CurrentView);
        }
    }
}
