// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using Clast.DatabaseDecimal.Values;
using DbspNet.Core.Collections;
using DbspNet.Sql.TypeSystem;
using Xunit;

namespace DbspNet.Tests.Collections;

/// <summary>
/// The row hash's two load-bearing properties (docs/design-incremental-persistence.md §11.1):
/// it depends only on the values (so it survives a process boundary — <c>System.HashCode</c> did
/// not), and the boxed and typed seed paths agree, which is what lets a typed row and a
/// backing-array row share a dictionary.
/// </summary>
public class StructuralRowHashTests
{
    /// <summary>
    /// Golden values. They are not magic: they pin the algorithm so a change that would silently
    /// invalidate a persisted hash, a persisted Bloom block or a recorded digest has to be
    /// deliberate. Recomputing them is fine — noticing is the point.
    /// </summary>
    [Fact]
    public void RowHashIsPinnedToValues()
    {
        Assert.Equal(1996530801, new StructuralRow(42L, 7L).GetHashCode());
        Assert.Equal(232275324, new StructuralRow(Utf8String.Of("ACME"), 1.5).GetHashCode());
        Assert.Equal(-1001248903, new StructuralRow(null, null).GetHashCode());
    }

    [Fact]
    public void NullIsNotZero()
    {
        Assert.NotEqual(
            new StructuralRow(new object?[] { null }).GetHashCode(),
            new StructuralRow(0L).GetHashCode());
    }

    [Fact]
    public void ArityIsPartOfTheHash()
    {
        Assert.NotEqual(
            new StructuralRow(1L).GetHashCode(),
            new StructuralRow(1L, 0L).GetHashCode());
    }

    [Fact]
    public void ColumnOrderMatters()
    {
        Assert.NotEqual(
            new StructuralRow(1L, 2L).GetHashCode(),
            new StructuralRow(2L, 1L).GetHashCode());
    }

    [Theory]
    [MemberData(nameof(Cells))]
    public void BoxedAndTypedSeedsAgree(object value, ulong typedSeed)
    {
        // The contract the typed compile path depends on: whatever the emitters call for a
        // statically-typed column must equal what the boxed path computes for the same value.
        Assert.Equal(typedSeed, StructuralRowHash.Cell(value));
    }

    public static TheoryData<object, ulong> Cells() => new()
    {
        { 42L, StructuralRowHash.Cell(42L) },
        { 42, StructuralRowHash.Cell(42) },
        { 1.5, StructuralRowHash.Cell(1.5) },
        { 1.5f, StructuralRowHash.Cell(1.5f) },
        { true, StructuralRowHash.Cell(true) },
        { "ACME", StructuralRowHash.Cell("ACME") },
        { Utf8String.Of("ACME"), SqlCellHash.Of(Utf8String.Of("ACME")) },
        { new Decimal128((System.Int128)12345), SqlCellHash.Of(new Decimal128((System.Int128)12345)) },
        { new Date32(19000), SqlCellHash.Of(new Date32(19000)) },
        { new Time64(1234), SqlCellHash.Of(new Time64(1234)) },
        { new Timestamp(1234567), SqlCellHash.Of(new Timestamp(1234567)) },
        { new Interval(3, 500), SqlCellHash.Of(new Interval(3, 500)) },
    };

    [Fact]
    public void NullableSeedsMatchTheirBoxedForm()
    {
        // A boxed long? with a value IS a boxed long, so the nullable overload has to agree with
        // the non-nullable one, and an empty one with the null cell.
        Assert.Equal(StructuralRowHash.Cell((object)42L), StructuralRowHash.Cell((long?)42L));
        Assert.Equal(StructuralRowHash.Cell((object?)null), StructuralRowHash.Cell((long?)null));
        Assert.Equal(StructuralRowHash.Cell((object)1.5), StructuralRowHash.Cell((double?)1.5));
        Assert.Equal(
            StructuralRowHash.Cell((object)Utf8String.Of("x")),
            SqlCellHash.Of((Utf8String?)Utf8String.Of("x")));
        Assert.Equal(StructuralRowHash.Cell((object?)null), SqlCellHash.Of((Utf8String?)null));
    }

    [Fact]
    public void MinusZeroHashesLikeZero()
    {
        Assert.Equal(StructuralRowHash.Cell(0.0), StructuralRowHash.Cell(-0.0));
    }

    /// <summary>
    /// Quality guard, not a proof: a weak mixer shows up here immediately. 100k sequential
    /// two-column rows must produce nearly that many distinct folded hashes, and spread evenly
    /// over dictionary buckets.
    /// </summary>
    [Fact]
    public void SequentialRowsSpreadWell()
    {
        const int n = 100_000;
        var distinct = new HashSet<int>();
        var buckets = new int[1024];
        for (var i = 0; i < n; i++)
        {
            var h = new StructuralRow((long)i, Utf8String.Of("sym" + (i % 997))).GetHashCode();
            distinct.Add(h);
            buckets[(uint)h % 1024]++;
        }

        Assert.True(distinct.Count > n - 20, $"only {distinct.Count} distinct hashes for {n} rows");

        // Pearson chi-square over the 1024 buckets. For a uniform hash the statistic is ~1023
        // +/- ~45; a mixer that leaks structure blows far past this bound, while ordinary
        // randomness never does.
        var expected = n / 1024.0;
        var chi2 = buckets.Sum(c => (c - expected) * (c - expected) / expected);
        Assert.True(chi2 < 1300, $"bucket chi-square {chi2:F0} — hash is not spreading");
    }
}
