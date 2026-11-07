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

    // Doubles include −0.0 (hashes and equals as 0.0) and two NaN payloads (equal cells, different
    // row hashes): what makes a value lookup by hash then cells exact, or not.
    private static readonly double[] Doubles = [0.0, -0.0, 0.5, 1.0, double.NaN, BitConverter.Int64BitsToDouble(0x7ff8000000000001)];

    private static StructuralRow Value(Random rnd) => new(
        rnd.Next(5) == 0 ? null : rnd.Next(4),
        Strings[rnd.Next(Strings.Length)] is { } s ? Utf8String.Of(s) : null,
        rnd.Next(5) == 0 ? null : Doubles[rnd.Next(Doubles.Length)]);

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void IntegrateEntries_LargeBatch_SortsAndEncodesInHashRanges(bool collide)
    {
        // Above the parallel threshold: hash ranges built on several threads, concatenated.
        var rnd = new Random(collide ? 11 : 12);
        var format = BlockSchema.TryCreate(KeySchema, ValueSchema)!;
        var blocked = new BlockedIndexedTrace(format, blockEntries: 16, hash: collide ? static k => k.GetHashCode() & 7 : null);
        var oracle = new IndexedZSetTrace<StructuralRow, StructuralRow, Z64>();
        for (var batch = 0; batch < 2; batch++)
        {
            var raw = new List<(StructuralRow Key, StructuralRow Value, long Weight)>();
            for (var n = 0; n < 150_000; n++)
            {
                var key = collide ? Key(rnd) : new StructuralRow((long)rnd.Next(100_000), Utf8String.Of("k"));
                raw.Add((key, Value(rnd), rnd.Next(3) == 0 ? -1 : 1));
            }

            var delta = new IndexedZSetBuilder<StructuralRow, StructuralRow, Z64>();
            foreach (var (k, v, w) in raw)
            {
                delta.Add(k, v, new Z64(w));
            }

            blocked.IntegrateEntries(raw.Count, i => raw[i].Key, i => raw[i].Value, i => raw[i].Weight);
            oracle.Integrate(delta.Build());
            blocked.CheckRuns();
            Assert.Equal(oracle.Current, blocked.Materialize());
        }
    }

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
                    var raw = new List<(StructuralRow Key, StructuralRow Value, long Weight)>();
                    if (rnd.Next(3) == 0 && !oracle.Current.IsEmpty)
                    {
                        // Cancel (part of) what the trace holds.
                        foreach (var (key, group) in oracle.Current)
                        {
                            foreach (var (value, weight) in group)
                            {
                                if (rnd.Next(2) == 0)
                                {
                                    raw.Add((key, value, -weight.Value));
                                }
                            }
                        }
                    }

                    for (var n = rnd.Next(0, 25); n > 0; n--)
                    {
                        var entry = (Key(rnd), Value(rnd), (long)(rnd.Next(2) == 0 ? rnd.Next(1, 3) : -rnd.Next(1, 3)));
                        raw.Add(entry);
                        if (rnd.Next(4) == 0)
                        {
                            raw.Add(entry with { Item3 = -entry.Item3 }); // cancels in the same batch
                        }
                    }

                    foreach (var (k, v, w) in raw)
                    {
                        delta.Add(k, v, new Z64(w));
                    }

                    var built = delta.Build();
                    if (rnd.Next(2) == 0)
                    {
                        // The same entries unconsolidated (zero weights too), straight into a run (§3.10).
                        if (raw.Count > 0)
                        {
                            raw.Add(raw[0] with { Weight = 0 });
                        }

                        blocked.IntegrateEntries(raw.Count, i => raw[i].Key, i => raw[i].Value, i => raw[i].Weight);
                    }
                    else
                    {
                        blocked.Integrate(built);
                    }

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

            // The group view against the oracle group, and merged with a random delta.
            for (var p = 0; p < 4; p++)
            {
                var key = Key(rnd);
                var expected = oracle.Current.GroupFor(key);
                var view = blocked.GroupView(key);
                Assert.Equal(expected.IsEmpty, view.IsEmpty);
                Assert.Equal(expected.SumWeights(), view.SumWeights());
                for (var v = 0; v < 8; v++)
                {
                    var value = v < 3 && !expected.IsEmpty ? expected.ElementAt(rnd.Next(expected.Count)).Key : Value(rnd);
                    Assert.Equal(expected.WeightOf(value), view.WeightOf(value));
                }

                var groupDelta = new ZSetBuilder<StructuralRow, Z64>();
                foreach (var (value, weight) in expected)
                {
                    if (rnd.Next(2) == 0)
                    {
                        groupDelta.Add(value, Z64.Negate(weight));
                    }
                }

                if (rnd.Next(2) == 0)
                {
                    groupDelta.Add(Value(rnd), new Z64(rnd.Next(1, 3)));
                }

                var d = groupDelta.Build();
                var merged = new LazyViewMergeMultiset<StructuralRow>(blocked.GroupView(key), d);
                var truth = expected + d;
                Assert.Equal(truth.IsEmpty, merged.IsEmpty);
                Assert.Equal(truth.SumWeights(), merged.SumWeights());
                Assert.Equal(truth, ZSet.FromEntries(merged));
                Assert.Equal(expected, ZSet.FromEntries(view));
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

                // As dictionaries: two NaN payloads print alike, so no textual order is stable.
                Assert.Equal(expected.Count, visitor.Net.Count);
                Assert.All(expected, e => Assert.Equal(e.Value, visitor.Net.GetValueOrDefault(e.Key)));
            }
        }

        blocked.Freeze();
        blocked.CheckRuns();
        Assert.Equal(oracle.Current, blocked.Materialize());
    }

    [Fact]
    public void Integrate_CopiesNothing_AndFreezeMergesGeometrically()
    {
        using var pressure = MemoryPressure.Use(PressureLevel.Low); // asserts the Low thresholds (§5.28)
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
