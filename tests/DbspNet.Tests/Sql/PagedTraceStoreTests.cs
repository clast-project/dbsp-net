// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using Clast.DatabaseDecimal.Values;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;
using Xunit;

namespace DbspNet.Tests.Sql;

/// <summary>
/// <see cref="PagedZSetStore"/> against the in-place <see cref="ZSet{TKey,TWeight}"/> it replaces
/// (docs/design-paged-trace-state.md): after every tick of random signed deltas the two must hold
/// the same rows with the same weights, enumerate in the same order, answer the same
/// <c>WeightOf</c>, and decode each kept cell to the identical bits — including the kinds where
/// <c>Equals</c> is not bitwise (NaN, ±0.0), NULL patterns, empty and page-sized strings, and rows
/// that escape because a cell is not its column's kind.
/// </summary>
public class PagedTraceStoreTests
{
    private static readonly Schema Wide = new(
    [
        new SchemaColumn("l", new SqlBigintType(true)),
        new SchemaColumn("i", new SqlIntegerType(true)),
        new SchemaColumn("d", new SqlDoubleType(true)),
        new SchemaColumn("r", new SqlRealType(true)),
        new SchemaColumn("b", new SqlBooleanType(true)),
        new SchemaColumn("dt", new SqlDateType(true)),
        new SchemaColumn("tm", new SqlTimeType(true)),
        new SchemaColumn("ts", new SqlTimestampType(true)),
        new SchemaColumn("iv", new SqlIntervalType(IntervalQualifier.Day, true)),
        new SchemaColumn("m", new SqlDecimalType(18, 2, true)),
        new SchemaColumn("s", new SqlVarcharType(null, true)),
        new SchemaColumn("s2", new SqlVarcharType(null, true)),
        new SchemaColumn("n", SqlNullType.Instance),
    ]);

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public void RandomSignedDeltas_MatchTheZSet(int seed)
    {
        var random = new Random(seed);
        var reference = ZSet.Empty<StructuralRow, Z64>().Clone();
        var store = PagedZSetStore.For(Wide);
        var live = new List<StructuralRow>();

        for (var tick = 0; tick < 60; tick++)
        {
            var builder = new ZSetBuilder<StructuralRow, Z64>();
            var n = random.Next(0, tick < 5 ? 400 : 120);
            for (var k = 0; k < n; k++)
            {
                // Mostly fresh rows; some re-adds and retractions of rows already held (as a
                // different but equal object, so equality — not identity — is exercised).
                var row = live.Count > 0 && random.Next(3) == 0
                    ? Copy(live[random.Next(live.Count)])
                    : RandomRow(random);
                var weight = random.Next(4) == 0 ? -random.Next(1, 3) : random.Next(1, 3);
                builder.Add(row, new Z64(weight));
                live.Add(row);
            }

            var delta = builder.Build();
            reference.MergeInPlace(delta);
            store.Integrate(delta);
            AssertSame(reference, store);

            foreach (var (row, _) in delta)
            {
                Assert.Equal(reference.WeightOf(row), store.WeightOf(Copy(row)));
            }

            Assert.Equal(Z64.Zero, store.WeightOf(RandomRow(random, salt: 1_000_000)));

            if (tick == 30)
            {
                // Load's reset: retract everything, then integrate a snapshot back.
                var snapshot = store.Materialize();
                reference.MergeInPlace(reference.Negate());
                store.Clear();
                Assert.Equal(0, store.Count);
                reference.MergeInPlace(snapshot);
                store.Integrate(snapshot);
                AssertSame(reference, store);
            }
        }

        Assert.True(store.EscapedCount > 0, "the random rows should include some that escape");
    }

    [Fact]
    public void DoubleEquality_FollowsEqualsNotBits()
    {
        var schema = new Schema([new SchemaColumn("d", new SqlDoubleType(true))]);
        var store = PagedZSetStore.For(schema);
        var reference = ZSet.Empty<StructuralRow, Z64>().Clone();

        // +0.0 and −0.0 are Equal and hash alike, so −0.0 folds into +0.0 and +0.0's bits stay.
        // NaNs with different payloads are Equal but hash apart, so the Z-set keeps both — and
        // so must the store.
        var nan2 = BitConverter.Int64BitsToDouble(BitConverter.DoubleToInt64Bits(double.NaN) ^ 1);
        foreach (var v in new[] { 0.0, -0.0, double.NaN, nan2, -0.0 })
        {
            var delta = ZSet.Singleton(new StructuralRow(v), Z64.One);
            reference.MergeInPlace(delta);
            store.Integrate(delta);
        }

        AssertSame(reference, store);
        Assert.Equal(new Z64(3), store.WeightOf(new StructuralRow(-0.0)));
        Assert.Equal(Z64.One, store.WeightOf(new StructuralRow(double.NaN)));
        Assert.Equal(3, store.Count);
    }

    [Fact]
    public void StringChurn_CompactsAndKeepsContents()
    {
        var schema = new Schema([new SchemaColumn("k", new SqlBigintType(false)), new SchemaColumn("s", new SqlVarcharType(null, true))]);
        var store = PagedZSetStore.For(schema);
        var reference = ZSet.Empty<StructuralRow, Z64>().Clone();
        var text = Utf8String.Of(new string('x', 20_000));
        var big = Utf8String.Of(new string('y', 300_000));   // its own page

        for (var round = 0; round < 1_000; round++)
        {
            var builder = new ZSetBuilder<StructuralRow, Z64>();
            builder.Add(new StructuralRow((long)round, round % 100 == 0 ? big : text), Z64.One);
            if (round > 0)
            {
                // Keep every tenth row; retract the rest, so dead bytes pass the threshold.
                var previous = round - 1;
                if (previous % 10 != 0)
                {
                    builder.Add(new StructuralRow((long)previous, previous % 100 == 0 ? big : text), new Z64(-1));
                }
            }

            var delta = builder.Build();
            reference.MergeInPlace(delta);
            store.Integrate(delta);
        }

        AssertSame(reference, store);
        Assert.True(store.PageBytesAllocated < 20L << 20, $"strings were not compacted: {store.PageBytesAllocated} B in pages");
    }

    [Fact]
    public void ArityMismatch_Escapes()
    {
        var store = PagedZSetStore.For(new Schema([new SchemaColumn("a", new SqlBigintType(true))]));
        store.Integrate(ZSet.Singleton(new StructuralRow(1L, 2L), Z64.One));
        store.Integrate(ZSet.Singleton(new StructuralRow(1L), Z64.One));
        Assert.Equal(2, store.Count);
        Assert.Equal(1, store.EscapedCount);
        Assert.Equal(Z64.One, store.WeightOf(new StructuralRow(1L, 2L)));
        Assert.Equal(Z64.One, store.WeightOf(new StructuralRow(1L)));
        Assert.Equal(Z64.Zero, store.WeightOf(new StructuralRow(2L)));
    }

    private static void AssertSame(ZSet<StructuralRow, Z64> reference, PagedZSetStore store)
    {
        Assert.Equal(reference.Count, store.Count);
        var expected = reference.ToList();
        var actual = store.Materialize().ToList();
        Assert.Equal(expected.Count, actual.Count);
        for (var i = 0; i < expected.Count; i++)
        {
            Assert.Equal(expected[i].Value, actual[i].Value);
            var (e, a) = (expected[i].Key, actual[i].Key);
            Assert.Equal(e.GetHashCode(), a.GetHashCode());
            Assert.Equal(e.Count, a.Count);
            for (var c = 0; c < e.Count; c++)
            {
                Assert.Equal(e[c]?.GetType(), a[c]?.GetType());
                Assert.True(Equals(e[c], a[c]), $"row {i} cell {c}: {e[c]} vs {a[c]}");
                if (e[c] is double de)
                {
                    Assert.Equal(BitConverter.DoubleToInt64Bits(de), BitConverter.DoubleToInt64Bits((double)a[c]!));
                }
            }
        }
    }

    private static StructuralRow Copy(StructuralRow row)
    {
        var values = new object?[row.Count];
        for (var i = 0; i < values.Length; i++)
        {
            values[i] = row[i] is Utf8String u ? Utf8String.FromBytes(u.Span.ToArray()) : row[i];
        }

        return new StructuralRow(values);
    }

    // A small value domain per column, so equal rows recur and hash buckets collide.
    private static StructuralRow RandomRow(Random random, int salt = 0)
    {
        object? Maybe(object value) => random.Next(8) == 0 ? null : value;
        object? Text() => random.Next(10) switch
        {
            0 => null,
            1 => Utf8String.Empty,
            2 => "a CLR string, not a Utf8String",   // escapes
            _ => Utf8String.Of("s" + random.Next(30)),
        };

        var doubles = new[] { 0.0, -0.0, double.NaN, 1.5, -2.25, double.PositiveInfinity };
        return new StructuralRow(
            Maybe((long)random.Next(40) + salt),
            Maybe(random.Next(5) == 0 ? (object)(long)random.Next(3) : random.Next(3)),   // a long in an INT column escapes
            Maybe(doubles[random.Next(doubles.Length)]),
            Maybe((float)random.Next(3) / 2),
            Maybe(random.Next(2) == 0),
            Maybe(new Date32(random.Next(3))),
            Maybe(new Time64(random.Next(3))),
            Maybe(new Timestamp(random.Next(3) * 1_000_000L)),
            Maybe(new Interval(random.Next(2), random.Next(3))),
            Maybe(new Decimal128((Int128)random.Next(3) * 100)),
            Text(),
            Text(),
            random.Next(40) == 0 ? Utf8String.Of("x") : null);   // a value in the NULL column escapes
    }
}
