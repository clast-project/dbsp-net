// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Core.Operators.Stateful.Blocks;
using DbspNet.Persistence;
using DbspNet.Persistence.IO;
using DbspNet.Sql.Compiler;
using Xunit;

namespace DbspNet.Tests.Sql;

/// <summary>
/// Outer joins with a residual ON conjunct in the outer-join operator
/// (docs/design-flat-trace-state.md §3.30): a preserved row is matched iff some row of its key's
/// other side satisfies the residual with it. Several rows per key that disagree about matching,
/// SCD-style ranges whose bounds change under the preserved rows, second copies of rows, NULL keys
/// and NULL residual inputs, deletes, and checkpoints with restores; every tick the integrated view
/// must equal the spine family's, which still lowers these joins to the anti-join rewrite.
/// </summary>
public sealed class OuterJoinResidualTests
{
    private static readonly string[] Joins =
    [
        "SELECT f.id, f.k, f.ts, d.lo, d.hi, d.name FROM f LEFT JOIN d ON f.k = d.k AND f.ts BETWEEN d.lo AND d.hi",
        "SELECT f.id, f.k, f.ts, d.lo, d.name FROM f LEFT JOIN d ON f.k = d.k AND f.ts > d.lo AND d.name <> 'x'",
        "SELECT f.id, f.ts, d.k, d.lo, d.hi FROM f RIGHT JOIN d ON f.k = d.k AND f.ts BETWEEN d.lo AND d.hi",
        "SELECT f.id, f.k, f.ts, d.k, d.lo, d.hi FROM f FULL JOIN d ON f.k = d.k AND f.ts >= d.lo AND f.ts < d.hi",
        "SELECT f.id, f.ts, d.lo FROM f LEFT JOIN d ON f.k = d.k AND f.k2 = d.k2 AND f.ts <= d.hi",
    ];

    public static TheoryData<int, bool, bool> Cases()
    {
        var data = new TheoryData<int, bool, bool>();
        for (var q = 0; q < Joins.Length; q++)
        {
            data.Add(q, false, false);
            data.Add(q, true, false);
            data.Add(q, true, true);
            data.Add(q, false, true);
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task IntegratedView_EqualsTheRewrite_EveryTick(int query, bool blocks, bool restore)
    {
        string[] sql =
        [
            "CREATE TABLE f (id INT NOT NULL, k INT, k2 INT, ts INT)",
            "CREATE TABLE d (k INT, k2 INT, lo INT, hi INT, name VARCHAR)",
            $"CREATE VIEW v AS {Joins[query]}",
        ];
        var views = new HashSet<string>(["v"], StringComparer.Ordinal);
        CompiledProgram Compile()
        {
            using (TraceBlocksMode.Use(join: blocks, freezeEntries: 3, blockEntries: 2))
            {
                return SqlProgram.Compile(sql, views, ArrowSqlSnapshotCodecs.Instance);
            }
        }

        var program = Compile();
        // The operator, not the rewrite: no semi-join's inner join, no DISTINCT.
        var ops = program.Circuit.Operators;
        Assert.Equal(
            1,
            ops.OfType<IncrementalLeftJoinOp<StructuralRow, StructuralRow, StructuralRow, StructuralRow, Z64>>().Count()
            + ops.OfType<IncrementalFullJoinOp<StructuralRow, StructuralRow, StructuralRow, StructuralRow, Z64>>().Count());
        Assert.Empty(ops.OfType<DistinctOp<StructuralRow, Z64>>());

        var rnd = new Random(query * 13 + (blocks ? 1 : 0) + (restore ? 2 : 0));
        var liveF = new List<object?[]>();
        var liveD = new List<object?[]>();
        var next = 0;
        int? Small(int n, int nullOneIn) => rnd.Next(nullOneIn) == 0 ? null : rnd.Next(n);
        for (var tick = 0; tick < 50; tick++)
        {
            Churn(program.Table("f"), liveF, rnd.Next(0, 4), rnd.Next(1, 6), () => [next++, Small(3, 8), Small(2, 6), Small(12, 6)]);

            // Dimension changes: new versions, and a range's bounds moving (delete + insert).
            Churn(program.Table("d"), liveD, rnd.Next(0, 3), rnd.Next(0, 3), () =>
            {
                var lo = rnd.Next(10);
                return [Small(3, 8), Small(2, 6), lo, rnd.Next(5) == 0 ? null : lo + rnd.Next(4), rnd.Next(4) == 0 ? "x" : "n" + rnd.Next(3)];
            });

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

            var oracle = SqlProgram.Compile(sql, views, options: CompileOptions.Default with { TraceFamily = TraceFamily.Spine });
            foreach (var row in liveF)
            {
                oracle.Table("f").Insert(row);
            }

            foreach (var row in liveD)
            {
                oracle.Table("d").Insert(row);
            }

            oracle.Step();
            Assert.Equal(oracle.Outputs["v"].CurrentView, program.Outputs["v"].CurrentView);
        }

        void Churn(DbspNet.Sql.Compiler.TableInput input, List<object?[]> live, int deletes, int inserts, Func<object?[]> make)
        {
            for (var d = deletes; d > 0 && live.Count > 0; d--)
            {
                var victim = live[rnd.Next(live.Count)];
                live.Remove(victim);
                input.Delete(victim);
            }

            for (var n = inserts; n > 0; n--)
            {
                // Sometimes a second copy of a live row.
                var row = live.Count > 0 && rnd.Next(6) == 0 ? live[rnd.Next(live.Count)] : make();
                live.Add(row);
                input.Insert(row);
            }
        }
    }
}
