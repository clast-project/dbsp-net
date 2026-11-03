// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Operators.Stateful.Blocks;
using DbspNet.Sql.Compiler;
using Xunit;

namespace DbspNet.Tests.Sql;

/// <summary>
/// Small exact aggregate groups keep no caches on a blocked trace (docs/design-flat-trace-state.md
/// §3.20): a touch refolds the group. Groups here grow past the threshold and shrink back, rows
/// repeat (weights above one) and are deleted several at a time; every tick the integrated view
/// must equal a fresh managed run over the live rows. A DOUBLE SUM keeps its caches (a refold could
/// differ in the last bits): there, each group must still have exactly one row.
/// </summary>
public sealed class AggregateCacheFreeTests
{
    private static readonly string[] Aggregates =
    [
        "MIN(v) AS lo, MAX(v) AS hi, COUNT(*) AS n, COUNT(s) AS ns, SUM(v) AS total",
        "MIN(s) AS ls, MAX(ts) AS lt",
        "SUM(d) AS sd, COUNT(*) AS n",
    ];

    public static TheoryData<int, bool> Cases()
    {
        var data = new TheoryData<int, bool>();
        for (var i = 0; i < Aggregates.Length; i++)
        {
            data.Add(i, false);
            data.Add(i, true);
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public void IntegratedView_EqualsAFreshRun_EveryTick(int aggregate, bool blocks)
    {
        string[] sql =
        [
            "CREATE TABLE t (id INT NOT NULL, g INT NOT NULL, v INT, s VARCHAR, ts TIMESTAMP, d DOUBLE NOT NULL)",
            $"CREATE VIEW a AS SELECT g, {Aggregates[aggregate]} FROM t GROUP BY g",
        ];
        var views = new HashSet<string>(["a"], StringComparer.Ordinal);
        CompiledProgram program;
        using (blocks ? TraceBlocksMode.Use(join: true, freezeEntries: 3, blockEntries: 2) : null)
        {
            program = SqlProgram.Compile(sql, views);
        }

        var rnd = new Random(aggregate * 31 + (blocks ? 1 : 0));
        var live = new List<object?[]>();
        var next = 0;
        for (var tick = 0; tick < 40; tick++)
        {
            var input = program.Table("t");

            // Phases: groups fill past the threshold, then drain back below it.
            var filling = tick % 10 < 6;
            for (var d = rnd.Next(filling ? 0 : 3, filling ? 3 : 8); d > 0 && live.Count > 0; d--)
            {
                var victim = live[rnd.Next(live.Count)];
                live.Remove(victim);
                input.Delete(victim);
            }

            for (var n = rnd.Next(filling ? 3 : 0, filling ? 9 : 3); n > 0; n--)
            {
                var row = live.Count > 0 && rnd.Next(6) == 0
                    ? live[rnd.Next(live.Count)]
                    : new object?[]
                    {
                        next++,
                        rnd.Next(3),
                        rnd.Next(4) == 0 ? null : rnd.Next(-5, 50),
                        rnd.Next(4) == 0 ? null : "s" + rnd.Next(6),
                        rnd.Next(4) == 0 ? null : new DbspNet.Sql.TypeSystem.Timestamp(rnd.Next(1000) * 1_000_000L),
                        rnd.Next(3) switch { 0 => 1e16, 1 => -1e16, _ => rnd.NextDouble() },
                    };
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
            if (aggregate < 2)
            {
                Assert.Equal(fresh.Outputs["a"].CurrentView, program.Outputs["a"].CurrentView);
                continue;
            }

            // A float sum: the incremental fold need not match a fresh one bit for bit, but a
            // retraction that missed (a refolded old value) would leave two rows for a group.
            static IEnumerable<(object?, object?, long)> KeysAndCounts(DbspNet.Core.Collections.ZSet<DbspNet.Core.Collections.StructuralRow, DbspNet.Core.Algebra.Z64> view) =>
                view.Select(e => (e.Key[0], e.Key[2], e.Value.Value)).OrderBy(e => (int)e.Item1!);
            Assert.Equal(KeysAndCounts(fresh.Outputs["a"].CurrentView), KeysAndCounts(program.Outputs["a"].CurrentView));
        }
    }
}
