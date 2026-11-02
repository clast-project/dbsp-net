// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Core.Operators.Stateful.Blocks;
using DbspNet.Persistence.Blocks;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;
using Xunit;

namespace DbspNet.Tests.Operators.Stateful;

/// <summary>
/// <see cref="BlockedIndexedTrace"/> (docs/design-flat-trace-state.md §2.3–§2.5) against the
/// managed <see cref="IndexedZSetTrace{TKey,TValue,TWeight}"/> under random interleavings of
/// integrate (including cancelling deltas), frontier drops, forced freezes, probes and
/// materialisation — with tiny blocks and freeze thresholds so every path (many levels, merges,
/// giant hash ranges, collisions) runs, and a seam that forces key-hash collisions.
/// </summary>
public class BlockedIndexedTraceTests
{
    private static readonly Schema KeySchema = new([
        new SchemaColumn("k", new SqlBigintType(false)),
        new SchemaColumn("s", new SqlVarcharType(null, true)),
    ]);

    private static readonly Schema ValueSchema = new([
        new SchemaColumn("i", new SqlIntegerType(true)),
        new SchemaColumn("t", new SqlVarcharType(null, true)),
        new SchemaColumn("d", new SqlDoubleType(true)),
    ]);

    private static readonly string?[] Strings = [null, "", "a", "é", "日本"];

    private struct Collect : ITraceVisitor
    {
        public Dictionary<StructuralRow, long> Net;

        public void Row(StructuralRow value, Z64 weight) => Add(value, weight.Value);

        public void Entry(ITraceBlock block, int index, Z64 weight) => Add(block.Value(index), weight.Value);

        private void Add(StructuralRow value, long weight)
        {
            Net[value] = Net.GetValueOrDefault(value) + weight;
            if (Net[value] == 0)
            {
                Net.Remove(value);
            }
        }
    }

    private static StructuralRow Key(Random rnd) =>
        new((long)rnd.Next(-3, 12), Strings[rnd.Next(Strings.Length)] is { } s ? Utf8String.Of(s) : null);

    private static StructuralRow Value(Random rnd) => new(
        rnd.Next(5) == 0 ? null : rnd.Next(4),
        Strings[rnd.Next(Strings.Length)] is { } s ? Utf8String.Of(s) : null,
        rnd.Next(5) == 0 ? null : (double)rnd.Next(3) / 2);

    public static TheoryData<int> Seeds() => new(Enumerable.Range(1, 60));

    [Theory]
    [MemberData(nameof(Seeds))]
    public void BlockedTrace_BehavesLikeTheManagedTrace(int seed)
    {
        var rnd = new Random(seed);
        var format = BlockSchema.TryCreate(KeySchema, ValueSchema)!;
        Func<StructuralRow, int>? hash = seed % 3 == 0 ? static k => k.GetHashCode() & 7 : null;
        var blocked = new BlockedIndexedTrace(
            format, freezeEntries: rnd.Next(1, 60), maxManaged: rnd.Next(0, 4), blockEntries: rnd.Next(1, 9), hash: hash,
            blockBytes: seed % 4 == 1 ? rnd.Next(40, 400) : BlockedIndexedTrace.BlockBytes);
        var oracle = new IndexedZSetTrace<StructuralRow, StructuralRow, Z64>();
        static long Monotone(StructuralRow key) => (long)key[0]!;

        for (var step = 0; step < 120; step++)
        {
            switch (rnd.Next(10))
            {
                case < 6:
                {
                    var delta = new IndexedZSetBuilder<StructuralRow, StructuralRow, Z64>();
                    if (rnd.Next(3) == 0 && !oracle.Current.IsEmpty)
                    {
                        // Cancel (part of) what the trace holds.
                        foreach (var (key, group) in oracle.Current)
                        {
                            foreach (var (value, weight) in group)
                            {
                                if (rnd.Next(2) == 0)
                                {
                                    delta.Add(key, value, Z64.Negate(weight));
                                }
                            }
                        }
                    }

                    for (var n = rnd.Next(0, 25); n > 0; n--)
                    {
                        delta.Add(Key(rnd), Value(rnd), new Z64(rnd.Next(2) == 0 ? rnd.Next(1, 3) : -rnd.Next(1, 3)));
                    }

                    var built = delta.Build();
                    blocked.Integrate(built);
                    oracle.Integrate(built);
                    break;
                }

                case 6:
                {
                    var threshold = (long)rnd.Next(-4, 10);
                    blocked.DropKeysBelow(threshold, Monotone);
                    oracle.DropKeysBelow(threshold, Monotone);
                    break;
                }

                case 7:
                    blocked.Freeze();
                    break;

                default:
                    blocked.CheckRuns();
                    Assert.Equal(oracle.Current, blocked.Materialize());
                    Assert.Equal(oracle.Current.GroupCount, blocked.GroupCount);
                    break;
            }

            for (var p = 0; p < 6; p++)
            {
                var key = Key(rnd);
                var visitor = new Collect { Net = new() };
                blocked.Probe(key, ref visitor);
                var expected = new Dictionary<StructuralRow, long>();
                foreach (var (value, weight) in oracle.Current.GroupFor(key))
                {
                    expected[value] = weight.Value;
                }

                Assert.Equal(expected.OrderBy(e => e.Key.ToString()), visitor.Net.OrderBy(e => e.Key.ToString()));
            }
        }

        blocked.Freeze();
        blocked.CheckRuns();
        Assert.Equal(oracle.Current, blocked.Materialize());
    }

    [Fact]
    public void Integrate_CopiesNothing_AndFreezeMergesGeometrically()
    {
        var format = BlockSchema.TryCreate(KeySchema, ValueSchema)!;
        var trace = new BlockedIndexedTrace(format, freezeEntries: 4, maxManaged: 100, blockEntries: 2);
        var delta = new IndexedZSetBuilder<StructuralRow, StructuralRow, Z64>();
        delta.Add(new StructuralRow(1L, null), new StructuralRow(1, null, null), Z64.One);
        var one = delta.Build();
        trace.Integrate(one);
        Assert.Equal(1, trace.ManagedLevels);
        for (var i = 0; i < 40; i++)
        {
            var d = new IndexedZSetBuilder<StructuralRow, StructuralRow, Z64>();
            d.Add(new StructuralRow((long)i, null), new StructuralRow(i, null, null), Z64.One);
            trace.Integrate(d.Build());
        }

        // Each run is at most a quarter of the next older one.
        var runs = trace.RunEntries;
        Assert.NotEmpty(runs);
        for (var r = 0; r + 1 < runs.Count; r++)
        {
            Assert.True(runs[r] * 4 < runs[r + 1], string.Join(",", runs));
        }
    }
}
