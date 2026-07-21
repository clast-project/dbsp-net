// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System;
using System.Collections.Generic;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Tests.Sql;

/// <summary>
/// Unit-level validation of the monomorphized window-aggregate order key
/// (design §23.7): the <see cref="LongKeyComparer{TRow}"/> that compares the
/// <b>unboxed</b> monotone <c>long</c> key must induce exactly the order of the
/// boxed single-key <see cref="SortKeyComparer{TRow}"/> it replaces — including
/// the carrier→long mapping's monotonicity (Date32 days, Timestamp / Time64
/// microseconds), NULL positioning, DESC, and the row-level tiebreak — and
/// <see cref="TypedPlanCompiler.BuildUnboxedOrderKey"/> must take the unboxed
/// path only for recognised carriers, falling back (returning <c>null</c>) for
/// any other key type so the boxed comparer stays live there.
/// </summary>
public class WindowOrderKeyMonomorphizeTests
{
    /// <summary>A test row: an order key (its boxed carrier value + the monotone
    /// long the compiler would extract) plus a total-order tiebreak field, so
    /// <c>Comparer&lt;Row&gt;.Default</c> — the tiebreak both comparers share —
    /// is deterministic.</summary>
    private sealed record Row(object? Boxed, long? Mapped, int Tie) : IComparable<Row>
    {
        public int CompareTo(Row? other) => other is null ? 1 : Tie.CompareTo(other.Tie);
    }

    // A carrier value at logical position v (or SQL NULL when v is null), in both
    // the boxed (what SortKeyComparer sees) and unboxed-long (what LongKeyComparer
    // sees) forms. The long is the same monotone mapping BuildUnboxedOrderKey emits.
    private static (object? Boxed, long? Mapped) MakeKey(string carrier, int? v)
    {
        if (v is null)
        {
            return (null, null);
        }

        return carrier switch
        {
            "int" => (v.Value, v.Value),
            "long" => ((long)v.Value, v.Value),
            "date" => (new Date32(v.Value), v.Value),
            "time" => (new Time64(v.Value), v.Value),
            "ts" => (new Timestamp(v.Value), v.Value),
            _ => throw new ArgumentOutOfRangeException(nameof(carrier), carrier, null),
        };
    }

    [Theory]
    // carrier × descending × nullsFirst — the full single-key parameter space.
    [InlineData("int", false, false)]
    [InlineData("int", false, true)]
    [InlineData("int", true, false)]
    [InlineData("int", true, true)]
    [InlineData("long", false, false)]
    [InlineData("long", true, true)]
    [InlineData("date", false, false)]
    [InlineData("date", false, true)]
    [InlineData("date", true, false)]
    [InlineData("date", true, true)]
    [InlineData("time", false, false)]
    [InlineData("time", true, true)]
    [InlineData("ts", false, false)]
    [InlineData("ts", false, true)]
    [InlineData("ts", true, false)]
    [InlineData("ts", true, true)]
    public void LongKeyComparer_OrderEquivalentToBoxedSortKeyComparer(
        string carrier, bool descending, bool nullsFirst)
    {
        // The boxed comparer is the incumbent; the unboxed comparer is the §23.7
        // replacement. They must agree in sign on every ordered pair.
        var boxed = new SortKeyComparer<Row>(
            new Func<Row, object?>[] { r => r.Boxed },
            new[] { descending },
            new[] { nullsFirst },
            Comparer<Row>.Default);
        var mono = new LongKeyComparer<Row>(
            r => r.Mapped, descending, nullsFirst, Comparer<Row>.Default);

        // A spread of key positions with deliberate duplicates (to exercise the
        // tiebreak) and NULLs (to exercise absolute NULL positioning), plus a few
        // extremes. Each row's Tie is unique so the tiebreak is a strict order.
        var samples = new List<Row>();
        var rng = new Random(20260720);
        int?[] positions = { -3, -1, 0, 0, 1, 1, 2, 5, null, null, int.MinValue / 2, int.MaxValue / 2 };
        var tie = 0;
        foreach (var p in positions)
        {
            var (b, m) = MakeKey(carrier, p);
            samples.Add(new Row(b, m, tie++));
        }

        // Some rows share a key position but differ in Tie — force those adjacencies.
        for (var i = 0; i < 40; i++)
        {
            var p = rng.Next(0, 4) == 0 ? (int?)null : rng.Next(-2, 3);
            var (b, m) = MakeKey(carrier, p);
            samples.Add(new Row(b, m, tie++));
        }

        for (var i = 0; i < samples.Count; i++)
        {
            for (var j = 0; j < samples.Count; j++)
            {
                var a = samples[i];
                var b = samples[j];
                Assert.Equal(
                    Math.Sign(boxed.Compare(a, b)),
                    Math.Sign(mono.Compare(a, b)));
            }
        }
    }

    // ---- BuildUnboxedOrderKey: carriers take the unboxed path, others fall back --

    private static Delegate? UnboxedFor(SqlType keyType)
    {
        var rowType = TypedRowEmitter.EmitRowType(new Schema([new SchemaColumn("k", keyType)]));
        Assert.NotNull(rowType);
        return TypedPlanCompiler.BuildUnboxedOrderKey(new ResolvedColumn(0, keyType), rowType!);
    }

    public static IEnumerable<object[]> Carriers()
    {
        // Every carrier the resolver permits as an ordered-window key, nullable and
        // not — each must take the unboxed monotone-long path.
        foreach (var nullable in new[] { false, true })
        {
            yield return new object[] { new SqlIntegerType(nullable) };
            yield return new object[] { new SqlBigintType(nullable) };
            yield return new object[] { new SqlDateType(nullable) };
            yield return new object[] { new SqlTimeType(nullable) };
            yield return new object[] { new SqlTimestampType(nullable) };
        }
    }

    [Theory]
    [MemberData(nameof(Carriers))]
    public void BuildUnboxedOrderKey_Carrier_TakesUnboxedPath(SqlType keyType) =>
        Assert.NotNull(UnboxedFor(keyType));

    public static IEnumerable<object[]> NonCarriers()
    {
        // Types that emit a valid typed row but do not map to a monotone long
        // carrier — the guard must return null so CompileWindowAggregate keeps the
        // boxed SortKeyComparer live. (REAL is excluded: TypedRowEmitter has no
        // float32 row field, so such a window never reaches the typed path at all.)
        yield return new object[] { new SqlVarcharType(null, false) };
        yield return new object[] { new SqlDoubleType(false) };
        yield return new object[] { new SqlDecimalType(18, 4, false) };
        yield return new object[] { new SqlBooleanType(false) };
    }

    [Theory]
    [MemberData(nameof(NonCarriers))]
    public void BuildUnboxedOrderKey_NonCarrier_FallsBackToBoxed(SqlType keyType) =>
        Assert.Null(UnboxedFor(keyType));
}
