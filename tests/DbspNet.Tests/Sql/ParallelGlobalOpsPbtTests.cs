// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System;
using System.Collections.Generic;
using System.Linq;
using CsCheck;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using Xunit;

namespace DbspNet.Tests.Sql;

/// <summary>
/// Parallel ≡ serial for the "global" operators the structural parallel program
/// compile used to refuse because they have no key to shard on: an unpartitioned
/// rank-in-output, LAG/LEAD, window aggregate, windowed TOP-K and ORDER BY … LIMIT
/// (all gathered onto worker 0), and cross joins (a one-row GROUP-BY-less aggregate
/// right side is broadcast; anything else gathers both sides). Each program is
/// compiled serially (<see cref="SqlProgram.Compile"/>) and in parallel at W=2 and
/// W=4, driven with the same random tick script, and every output view's integrated
/// state is compared after every tick.
/// <para>
/// Streams are <b>arbitrary signed ±1</b> — including deletes of rows never inserted
/// — as the serial TOP-K / rank PBTs drive them (PartitionedTopKCompactPbtTests): the
/// oracle is the serial circuit running the same operators, so whatever those
/// operators do with a negative multiplicity, the parallel compile must do the same.
/// </para>
/// </summary>
public class ParallelGlobalOpsPbtTests
{
    private static readonly string[] Tables =
    [
        "CREATE TABLE a (k INT NOT NULL, x INT NOT NULL)",
        "CREATE TABLE b (k INT NOT NULL, v BIGINT NOT NULL)",
    ];

    public static readonly TheoryData<string> Cases = new()
    {
        "global_rank_over_aggregate",
        "global_lag_lead",
        "global_topk",
        "global_window_aggregate",
        "cross_join_one_row_aggregate",
        "cross_join_two_relations",
        "gold_shape",
    };

    private static (string[] Views, string[] Outputs) Program(string name) => name switch
    {
        "global_rank_over_aggregate" => (
        [
            "CREATE VIEW o AS SELECT k, s, RANK() OVER (ORDER BY s DESC) AS r, "
            + "DENSE_RANK() OVER (ORDER BY s) AS dr "
            + "FROM (SELECT k, SUM(v) AS s FROM b GROUP BY k) g",
            "CREATE VIEW rn AS SELECT k, x, ROW_NUMBER() OVER (ORDER BY x DESC) AS n FROM a",
        ], ["o", "rn"]),

        "global_lag_lead" => (
        [
            "CREATE VIEW o AS SELECT k, x, LAG(x) OVER (ORDER BY k, x) AS p, "
            + "LEAD(x, 2, -1) OVER (ORDER BY x DESC, k) AS n FROM a",
        ], ["o"]),

        "global_topk" => (
        [
            "CREATE VIEW lim AS SELECT k, x FROM a ORDER BY x DESC, k LIMIT 3",
            "CREATE VIEW off AS SELECT k, x FROM a ORDER BY x, k LIMIT 2 OFFSET 1",
            "CREATE VIEW wtk AS SELECT k, x FROM "
            + "(SELECT k, x, RANK() OVER (ORDER BY x DESC) AS r FROM a) s WHERE r <= 2",
        ], ["lim", "off", "wtk"]),

        "global_window_aggregate" => (
        [
            "CREATE VIEW o AS SELECT k, x, SUM(x) OVER () AS whole, "
            + "COUNT(*) OVER (ORDER BY k RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS framed, "
            + "MAX(x) OVER (ORDER BY k) AS running FROM a",
        ], ["o"]),

        "cross_join_one_row_aggregate" => (
        [
            "CREATE VIEW o AS SELECT a.x, g.total FROM a CROSS JOIN (SELECT SUM(v) AS total FROM b) g",
            "CREATE VIEW h AS SELECT a.k, g.c FROM a CROSS JOIN "
            + "(SELECT COUNT(*) AS c, MAX(v) AS m FROM b) g WHERE a.x < g.m",
            // The GROUP-BY-less aggregate on its own (the cross joins depend on it).
            "CREATE VIEW g AS SELECT SUM(v) AS s, COUNT(*) AS c, MIN(k) AS mk FROM b",
        ], ["o", "h", "g"]),

        "cross_join_two_relations" => (
        [
            "CREATE VIEW o AS SELECT a.k, a.x, b.v FROM a CROSS JOIN b",
            "CREATE VIEW lt AS SELECT a.k, b.k AS bk FROM a CROSS JOIN b WHERE a.x < b.v",
            // The one-row aggregate on the LEFT is not broadcast: gather-both.
            "CREATE VIEW gl AS SELECT g.total, a.k FROM (SELECT SUM(v) AS total FROM b) g CROSS JOIN a",
        ], ["o", "lt", "gl"]),

        // ivm-bench's gold views: per-key aggregate → GROUP-BY-less global stats →
        // CROSS JOIN → global RANK / DENSE_RANK and a global LAG, all in one program.
        "gold_shape" => (
        [
            "CREATE VIEW per_k AS SELECT k, COUNT(*) AS c, SUM(v) AS s FROM b GROUP BY k",
            "CREATE VIEW gold AS WITH g AS (SELECT SUM(s) AS tot, MAX(c) AS maxc FROM per_k), "
            + "scored AS (SELECT p.k, p.c, p.s, p.s * 100 / NULLIF(g.tot, 0) AS pct, g.maxc, "
            + "RANK() OVER (ORDER BY p.s DESC, p.k) AS r_s, "
            + "DENSE_RANK() OVER (ORDER BY p.c DESC) AS r_c, "
            + "LAG(p.s) OVER (ORDER BY p.k) AS prev_s "
            + "FROM per_k p CROSS JOIN g) SELECT * FROM scored",
        ], ["per_k", "gold"]),

        _ => throw new ArgumentOutOfRangeException(nameof(name)),
    };

    private sealed record Event(bool IsA, int K, int X, long Weight);

    // Small domains so ties, collisions and deletes of absent rows are common.
    private static readonly Gen<Event> GenEvent =
        Gen.Select(Gen.Bool, Gen.Int[0, 3], Gen.Int[0, 4], Gen.OneOfConst(1L, 1L, -1L))
            .Select(p => new Event(p.Item1, p.Item2, p.Item3, p.Item4));

    private static readonly Gen<Event[][]> GenTicks = GenEvent.Array[0, 8].Array[1, 6];

    [Theory]
    [MemberData(nameof(Cases))]
    public void ParallelCompileSucceeds(string name)
    {
        var (views, outputs) = Program(name);
        foreach (var w in new[] { 2, 4 })
        {
            Assert.True(
                SqlProgram.TryCompileParallel(Statements(views), Set(outputs), w, out var p),
                $"{name}: parallel compile refused at W={w}");
            p!.Dispose();
        }
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public void ParallelEqualsSerial_EveryTick(string name)
    {
        var (views, outputs) = Program(name);
        GenTicks.Sample(ticks => Check(views, outputs, ticks), iter: 60);
    }

    private static bool Check(string[] views, string[] outputs, Event[][] ticks)
    {
        var statements = Statements(views);
        var serial = SqlProgram.Compile(statements, Set(outputs));
        var parallels = new List<ParallelCompiledProgram>();
        try
        {
            foreach (var w in new[] { 2, 4 })
            {
                Assert.True(SqlProgram.TryCompileParallel(statements, Set(outputs), w, out var p));
                parallels.Add(p!);
            }

            foreach (var tick in ticks)
            {
                foreach (var e in tick)
                {
                    serial.Table(e.IsA ? "a" : "b").Push([(Row(e), e.Weight)]);
                    foreach (var p in parallels)
                    {
                        p.Table(e.IsA ? "a" : "b").Push([(Row(e), e.Weight)]);
                    }
                }

                serial.Step();
                foreach (var p in parallels)
                {
                    p.Step();
                }

                foreach (var name in outputs)
                {
                    var expected = Materialize(serial.Outputs[name].CurrentView);
                    foreach (var p in parallels)
                    {
                        var actual = Materialize(p.Outputs[name].CurrentView);
                        if (!expected.SequenceEqual(actual))
                        {
                            Console.Error.WriteLine(
                                $"view {name}, W={p.Workers}\nserial:   {string.Join(", ", expected)}"
                                + $"\nparallel: {string.Join(", ", actual)}");
                            return false;
                        }
                    }
                }
            }

            return true;
        }
        finally
        {
            foreach (var p in parallels)
            {
                p.Dispose();
            }
        }
    }

    private static object?[] Row(Event e) => e.IsA ? [e.K, e.X] : [e.K, (long)e.X];

    private static string[] Statements(string[] views) => [.. Tables, .. views];

    private static HashSet<string> Set(string[] outputs) => new(outputs, StringComparer.Ordinal);

    // Canonical sorted "row x weight" list of an integrated view (weights summed).
    private static List<string> Materialize(ZSet<StructuralRow, Z64> zset)
    {
        var map = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach (var kv in zset)
        {
            var cells = new string[kv.Key.Count];
            for (var i = 0; i < cells.Length; i++)
            {
                cells[i] = kv.Key[i]?.ToString() ?? "<null>";
            }

            var key = string.Join("|", cells);
            map[key] = map.GetValueOrDefault(key) + kv.Value.Value;
        }

        return map.Where(kv => kv.Value != 0)
            .Select(kv => kv.Key + " x" + kv.Value)
            .OrderBy(s => s, StringComparer.Ordinal)
            .ToList();
    }
}
