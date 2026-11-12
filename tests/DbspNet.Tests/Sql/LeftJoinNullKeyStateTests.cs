// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
// A fused LEFT join keeps its preserved side's NULL-keyed rows in the join's left trace, where
// the operator pads them (docs/design-bounded-memory.md §5.46). Required: those groups survive
// a checkpoint and restore, managed and blocked, and the restored program continues exactly as a
// twin that never stopped; and the fused program equals the unfused one on every tick.
using DbspNet.Core.Operators.Stateful.Blocks;
using DbspNet.Persistence;
using DbspNet.Persistence.IO;
using DbspNet.Persistence.Runs;
using DbspNet.Sql.Compiler;
using Xunit;

namespace DbspNet.Tests.Sql;

public class LeftJoinNullKeyStateTests
{
    private static readonly string[] Sql =
    [
        "CREATE TABLE f (id INT NOT NULL, code INT, amt BIGINT NOT NULL)",
        "CREATE TABLE d (code INT, label VARCHAR NOT NULL)",
        // The chain projects before the join (code moves to index 0), then a second LEFT join
        // keys on a column the first may have padded with NULL.
        "CREATE VIEW j AS SELECT x.id, x.amt, d1.label, d2.label AS label2 " +
            "FROM (SELECT code, id, amt + 1 AS amt FROM f WHERE amt <> 3) x " +
            "LEFT JOIN d d1 ON x.code = d1.code LEFT JOIN d d2 ON CAST(LENGTH(d1.label) AS INT) = d2.code",
        "CREATE VIEW g AS SELECT label, COUNT(*) AS n, SUM(amt) AS s FROM j GROUP BY label",
    ];

    private static readonly string[] Views = ["j", "g"];

    private static CompiledProgram Compile(bool blocked, bool fused = true)
    {
        var prev = IndexFusionMode.Enabled;
        IndexFusionMode.Enabled = fused;
        try
        {
            using var mode = TraceBlocksMode.Use(join: blocked, freezeEntries: 8, blockEntries: 4);
            return SqlProgram.Compile(Sql, new HashSet<string>(Views, StringComparer.Ordinal), ArrowSqlSnapshotCodecs.Instance);
        }
        finally
        {
            IndexFusionMode.Enabled = prev;
        }
    }

    // NULL codes on both sides (a NULL-coded dimension row must match nothing), deletes of
    // NULL-keyed fact rows, and dimension rows that come and go so padded rows gain and lose matches.
    private static void Tick(CompiledProgram p, int tick)
    {
        var rng = new Random(tick);
        for (var i = 0; i < 20; i++)
        {
            int? code = rng.Next(4) == 0 ? null : rng.Next(0, 6);
            p.Table("f").Insert(rng.Next(0, 30), code, (long)rng.Next(6));
        }

        if (rng.Next(2) == 0)
        {
            p.Table("d").Insert(rng.Next(3) == 0 ? null : rng.Next(0, 7), "L" + new string('x', rng.Next(5)));
        }

        if (tick > 0)
        {
            var old = new Random(tick - 1);
            for (var i = 0; i < 6; i++)
            {
                int? code = old.Next(4) == 0 ? null : old.Next(0, 6);
                p.Table("f").Delete(old.Next(0, 30), code, (long)old.Next(6));
            }

            if (old.Next(2) == 0 && tick % 3 == 0)
            {
                p.Table("d").Delete(old.Next(3) == 0 ? null : old.Next(0, 7), "L" + new string('x', old.Next(5)));
            }
        }

        p.Step();
    }

    private static string Digest(CompiledProgram p) =>
        string.Join(" / ", Views.Select(v => string.Join("; ", p.Outputs[v].CurrentView
            .Select(e => string.Join("|", Enumerable.Range(0, p.Outputs[v].Schema.Count).Select(i => e.Key[i] is { } x ? FormattableString.Invariant($"{x}") : "<null>")) + "#" + e.Value.Value)
            .Order(StringComparer.Ordinal))));

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Fused_equals_unfused_on_every_tick(bool blocked)
    {
        var fused = Compile(blocked);
        var unfused = Compile(blocked, fused: false);
        for (var tick = 0; tick < 12; tick++)
        {
            Tick(fused, tick);
            Tick(unfused, tick);
            Assert.Equal(Digest(unfused), Digest(fused));
        }
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, true)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    public async Task Null_keyed_left_groups_survive_a_checkpoint(bool writeBlocked, bool readBlocked)
    {
        RunCheckpoint.Verify = true;
        using var layout = StateLayout.Use(true);
        var fs = new InMemoryTableFileSystem();
        var p = Compile(writeBlocked);
        for (var tick = 0; tick < 8; tick++)
        {
            Tick(p, tick);
        }

        await Snapshot.WriteAsync(p.Circuit, fs);
        var restored = Compile(readBlocked);
        await Snapshot.ReadAsync(restored.Circuit, fs);

        var twin = Compile(readBlocked, fused: false);
        for (var tick = 0; tick < 8; tick++)
        {
            Tick(twin, tick);
        }

        for (var tick = 100; tick < 108; tick++)
        {
            Tick(twin, tick);
            Tick(restored, tick);
            Assert.Equal(Digest(twin), Digest(restored));
        }
    }
}
