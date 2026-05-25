// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using Clast.DatabaseDecimal;
using Clast.DatabaseDecimal.Text;
using Clast.DatabaseDecimal.Values;
using DbspNet.Core.Collections;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Tests.Collections;

public class StructuralRowComparerTests
{
    private static readonly StructuralRowComparer Cmp = StructuralRowComparer.Instance;

    private static int Sign(int c) => c < 0 ? -1 : c > 0 ? 1 : 0;

    private static Decimal128 Dec(string text, byte scale) =>
        DecimalText.ParseDecimal128(text, DecimalType.Numeric(18, scale));

    [Fact]
    public void EqualRows_CompareZero()
    {
        var a = new StructuralRow(1L, "x", true);
        var b = new StructuralRow(1L, "x", true);

        Assert.Equal(0, Cmp.Compare(a, b));
    }

    [Fact]
    public void SameReference_CompareZero()
    {
        var a = new StructuralRow(1L, "x");

        Assert.Equal(0, Cmp.Compare(a, a));
    }

    [Fact]
    public void FirstColumn_DecidesOrder()
    {
        var a = new StructuralRow(1L, "z");
        var b = new StructuralRow(2L, "a");

        Assert.Equal(-1, Sign(Cmp.Compare(a, b)));
        Assert.Equal(1, Sign(Cmp.Compare(b, a)));
    }

    [Fact]
    public void TieOnFirstColumn_SecondColumnBreaksTie()
    {
        var a = new StructuralRow(1L, "a");
        var b = new StructuralRow(1L, "b");

        Assert.Equal(-1, Sign(Cmp.Compare(a, b)));
        Assert.Equal(1, Sign(Cmp.Compare(b, a)));
    }

    [Fact]
    public void Null_SortsBeforeNonNull()
    {
        var withNull = new StructuralRow(1L, null);
        var withValue = new StructuralRow(1L, "anything");

        Assert.Equal(-1, Sign(Cmp.Compare(withNull, withValue)));
        Assert.Equal(1, Sign(Cmp.Compare(withValue, withNull)));
    }

    [Fact]
    public void BothNullInSlot_ContinuesToNextColumn()
    {
        var a = new StructuralRow(null, 1L);
        var b = new StructuralRow(null, 2L);

        // First slot equal (both null) — second column decides.
        Assert.Equal(-1, Sign(Cmp.Compare(a, b)));
    }

    [Fact]
    public void BothNullInSlot_OtherwiseEqual_CompareZero()
    {
        var a = new StructuralRow(1L, null, 3L);
        var b = new StructuralRow(1L, null, 3L);

        Assert.Equal(0, Cmp.Compare(a, b));
    }

    [Fact]
    public void ShorterArity_SortsFirst_WhenSharedPrefixEqual()
    {
        var shorter = new StructuralRow(1L, 2L);
        var longer = new StructuralRow(1L, 2L, 3L);

        Assert.Equal(-1, Sign(Cmp.Compare(shorter, longer)));
        Assert.Equal(1, Sign(Cmp.Compare(longer, shorter)));
    }

    [Fact]
    public void EmptyRows_CompareZero()
    {
        var a = new StructuralRow(Array.Empty<object?>());
        var b = new StructuralRow(Array.Empty<object?>());

        Assert.Equal(0, Cmp.Compare(a, b));
    }

    [Fact]
    public void NullReference_SortsBeforeAnyRow()
    {
        var row = new StructuralRow(1L);

        Assert.Equal(-1, Sign(Cmp.Compare(null, row)));
        Assert.Equal(1, Sign(Cmp.Compare(row, null)));
        Assert.Equal(0, Cmp.Compare(null, null));
    }

    [Fact]
    public void Sorting_ProducesAscendingOrder()
    {
        var rows = new List<StructuralRow>
        {
            new(3L, "c"),
            new(1L, "b"),
            new(1L, "a"),
            new(2L, "z"),
        };

        rows.Sort(Cmp);

        Assert.Equal(
            new[]
            {
                new StructuralRow(1L, "a"),
                new StructuralRow(1L, "b"),
                new StructuralRow(2L, "z"),
                new StructuralRow(3L, "c"),
            },
            rows);
    }

    [Fact]
    public void UsableAsSortedSetComparer_EquatesEqualRows()
    {
        var set = new SortedSet<StructuralRow>(Cmp)
        {
            new StructuralRow(1L, "a"),
            new StructuralRow(1L, "a"), // duplicate by value — should not grow the set
            new StructuralRow(2L, "b"),
        };

        Assert.Equal(2, set.Count);
    }

    // ---- Compare(x, y) == 0  <=>  x.Equals(y), across the runtime value types ----

    [Theory]
    [InlineData(1L, 1L, true)]
    [InlineData(1L, 2L, false)]
    public void Invariant_Long(long a, long b, bool equal) => AssertInvariant(a, b, equal);

    [Theory]
    [InlineData(1.5d, 1.5d, true)]
    [InlineData(1.5d, 2.5d, false)]
    public void Invariant_Double(double a, double b, bool equal) => AssertInvariant(a, b, equal);

    [Theory]
    [InlineData(true, true, true)]
    [InlineData(false, true, false)]
    public void Invariant_Bool(bool a, bool b, bool equal) => AssertInvariant(a, b, equal);

    [Fact]
    public void Invariant_DoubleNaN_IsSelfEqual()
    {
        // .NET defines NaN.Equals(NaN) and NaN.CompareTo(NaN)==0 (unlike ==),
        // so the comparer/equality agreement holds for NaN too.
        AssertInvariant(double.NaN, double.NaN, equal: true);
        AssertInvariant(double.NaN, 0d, equal: false);
    }

    [Fact]
    public void Invariant_Utf8String()
    {
        AssertInvariant(Utf8String.Of("alice"), Utf8String.Of("alice"), equal: true);
        AssertInvariant(Utf8String.Of("alice"), Utf8String.Of("bob"), equal: false);
    }

    [Fact]
    public void Utf8String_OrdersByOrdinalBytes()
    {
        var a = new StructuralRow(Utf8String.Of("apple"));
        var b = new StructuralRow(Utf8String.Of("banana"));

        Assert.Equal(-1, Sign(Cmp.Compare(a, b)));
    }

    [Fact]
    public void Invariant_Decimal128_SameScale()
    {
        AssertInvariant(Dec("12.34", 2), Dec("12.34", 2), equal: true);
        AssertInvariant(Dec("12.34", 2), Dec("56.78", 2), equal: false);
    }

    [Fact]
    public void Decimal128_OrdersByValue_AtFixedScale()
    {
        var small = new StructuralRow(Dec("1.00", 2));
        var large = new StructuralRow(Dec("2.00", 2));

        Assert.Equal(-1, Sign(Cmp.Compare(small, large)));
        Assert.Equal(1, Sign(Cmp.Compare(large, small)));
    }

    private static void AssertInvariant(object a, object b, bool equal)
    {
        var rowA = new StructuralRow(a);
        var rowB = new StructuralRow(b);

        Assert.Equal(equal, rowA.Equals(rowB));
        Assert.Equal(equal, Cmp.Compare(rowA, rowB) == 0);

        if (!equal)
        {
            // Strict, antisymmetric ordering for unequal rows.
            Assert.Equal(-Sign(Cmp.Compare(rowA, rowB)), Sign(Cmp.Compare(rowB, rowA)));
            Assert.NotEqual(0, Cmp.Compare(rowA, rowB));
        }
    }
}
