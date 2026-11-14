// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
// An aggregate that elides its exchange (its input is already partitioned on a subset of its group key)
// sits where its INPUT's exchange hashed it. Its partition claim must say so: claiming its own whole group
// key (Iota) is true as co-location but false as a hash, and an aggregate elided below it then re-shards
// its state by that wrong hash on a restore at a different worker count (docs/design-bounded-memory.md
// §5.57.6). Required: three stacked aggregates (one exchange, two elisions) equal the serial program after
// a checkpoint resharded to another worker count.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Persistence;
using DbspNet.Persistence.IO;
using DbspNet.Sql.Compiler;
using Xunit;

namespace DbspNet.Tests.Sql;

public class ElidedAggregateClaimTests
{
    private static readonly string[] Sql =
    [
        "CREATE TABLE t (id INT NOT NULL, v BIGINT NOT NULL)",
        // a0 exchanges on id; a1 groups by (id, sv) and elides; a2 groups by (id, sv, n) and elides on a1's claim.
        "CREATE VIEW stack AS SELECT id, sv, n, COUNT(*) AS c FROM " +
            "(SELECT id, sv, COUNT(*) AS n FROM (SELECT id, SUM(v) AS sv FROM t GROUP BY id) a0 GROUP BY id, sv) a1 " +
            "GROUP BY id, sv, n",
    ];

    private static readonly HashSet<string> Views = new(["stack"], StringComparer.Ordinal);

    private static void Push(Func<string, object> table, int tick)
    {
        var rng = new Random(500 + tick);
        for (var i = 0; i < 60; i++)
        {
            var row = new object?[] { rng.Next(0, 40), (long)rng.Next(0, 6) };
            switch (table("t"))
            {
                case TableInput ti: ti.Insert(row); break;
                case ShardedTableInput si: si.Insert(row); break;
                default: throw new InvalidOperationException();
            }
        }
    }

    private static ParallelCompiledProgram Parallel(int workers)
    {
        Assert.True(SqlProgram.TryCompileParallel(Sql, Views, workers, out var p, ArrowSqlSnapshotCodecs.Instance));
        return p!;
    }

    private static Dictionary<string, long> Materialize(ZSet<StructuralRow, Z64> zset, int width)
    {
        var d = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach (var kv in zset)
        {
            var key = string.Join("|", Enumerable.Range(0, width).Select(i => FormattableString.Invariant($"{kv.Key[i]}")));
            d[key] = d.GetValueOrDefault(key) + kv.Value.Value;
            if (d[key] == 0)
            {
                d.Remove(key);
            }
        }

        return d;
    }

    private static int Exchanges(ParallelCompiledProgram p)
    {
        var c = p.Circuit.Replicas[0];
        return Enumerable.Range(0, c.Operators.Count)
            .Count(k => c.Operators[k].GetType().Name.StartsWith("Exchange", StringComparison.Ordinal));
    }

    [Fact]
    public void Both_outer_aggregates_elide()
    {
        using var p = Parallel(4);
        Assert.Equal(1, Exchanges(p));
    }

    [Theory]
    [InlineData(4, 3)]
    [InlineData(3, 5)]
    [InlineData(4, 4)]
    public async Task Stacked_elided_aggregates_survive_a_resharded_checkpoint(int from, int to)
    {
        var fs = new InMemoryTableFileSystem();
        var serial = SqlProgram.Compile(Sql, Views);
        using (var producer = Parallel(from))
        {
            for (var tick = 0; tick < 3; tick++)
            {
                Push(name => serial.Table(name), tick);
                serial.Step();
                Push(name => producer.Table(name), tick);
                producer.Step();
            }

            await ParallelSnapshot.WriteAsync(producer.Circuit, fs);
        }

        using var restored = Parallel(to);
        if (from != to)
        {
            Assert.True(await ParallelReshard.RewriteAsync(fs, restored) > 0);
        }

        await ParallelSnapshot.ReadAsync(restored.Circuit, fs);
        var width = serial.Outputs["stack"].Schema.Count;
        for (var tick = 3; tick < 6; tick++)
        {
            Push(name => serial.Table(name), tick);
            serial.Step();
            Push(name => restored.Table(name), tick);
            restored.Step();
            var expected = Materialize(serial.Outputs["stack"].CurrentView, width);
            Assert.True(expected.Count > 0);
            Assert.Equal(expected, Materialize(restored.Outputs["stack"].CurrentView, width));
        }
    }
}
