// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Core.Operators.Stateful.Blocks;
using DbspNet.Persistence;
using DbspNet.Persistence.IO;
using DbspNet.Sql.Compiler;
using Xunit;

namespace DbspNet.Tests.Sql;

/// <summary>
/// The rank operator keeps no emitted output (docs/design-flat-trace-state.md §3.29): a touched
/// partition's old ranking is its rows before the delta, walked together with the new one. Ties
/// (few distinct ORDER BY values), second copies of live rows (weights above one), several deletes
/// a tick, and checkpoints (which drop the blocked store's cache) with restores mid-stream; every
/// tick the integrated view must equal a fresh run over the live rows.
/// </summary>
public sealed class RankOldSideTests
{
    private static readonly string[] RankSql =
    [
        "ROW_NUMBER() OVER (PARTITION BY p ORDER BY t)",
        "RANK() OVER (PARTITION BY p ORDER BY t)",
        "DENSE_RANK() OVER (PARTITION BY p ORDER BY t DESC)",
        "RANK() OVER (ORDER BY t, s NULLS FIRST)",
        "ROW_NUMBER() OVER (ORDER BY t DESC, id)",
        "DENSE_RANK() OVER (PARTITION BY p, s ORDER BY t)",
    ];

    public static TheoryData<string, bool, bool> Cases()
    {
        var data = new TheoryData<string, bool, bool>();
        foreach (var r in RankSql)
        {
            data.Add(r, false, false);
            data.Add(r, true, false);
            data.Add(r, true, true);
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task IntegratedView_EqualsAFreshRun_EveryTick(string rank, bool blocks, bool restore)
    {
        string[] sql =
        [
            "CREATE TABLE t (id INT NOT NULL, p INT NOT NULL, t INT NOT NULL, s VARCHAR)",
            $"CREATE VIEW w AS SELECT id, p, t, s, {rank} AS r FROM t",
        ];
        var views = new HashSet<string>(["w"], StringComparer.Ordinal);
        CompiledProgram Compile()
        {
            using (TraceBlocksMode.Use(join: false, freezeEntries: 3, blockEntries: 2, window: blocks))
            {
                return SqlProgram.Compile(sql, views, ArrowSqlSnapshotCodecs.Instance);
            }
        }

        var program = Compile();
        Assert.Equal(
            blocks,
            program.Circuit.Operators.OfType<PartitionedRankOp<StructuralRow, StructuralRow, StructuralRow>>().Single().IsBlocked);
        var rnd = new Random(rank.Length * 7 + (blocks ? 1 : 0) + (restore ? 2 : 0));
        var live = new List<object?[]>();
        var next = 0;
        string?[] strings = [null, "a", "bb"];
        for (var tick = 0; tick < 40; tick++)
        {
            var input = program.Table("t");
            for (var d = rnd.Next(0, 5); d > 0 && live.Count > 0; d--)
            {
                var victim = live[rnd.Next(live.Count)];
                live.Remove(victim);
                input.Delete(victim);
            }

            for (var n = rnd.Next(1, 7); n > 0; n--)
            {
                var row = live.Count > 0 && rnd.Next(5) == 0
                    ? live[rnd.Next(live.Count)]
                    : new object?[] { next++, rnd.Next(3), rnd.Next(6), strings[rnd.Next(strings.Length)] };
                live.Add(row);
                input.Insert(row);
            }

            program.Step();
            if (rnd.Next(4) == 0)
            {
                var fs = new InMemoryTableFileSystem();
                await Snapshot.WriteAsync(program.Circuit, fs);
                if (restore && rnd.Next(2) == 0)
                {
                    var restored = Compile();
                    await Snapshot.ReadAsync(restored.Circuit, fs);
                    program = restored;
                }
            }

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
