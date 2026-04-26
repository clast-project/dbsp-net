using DbspNet.Core.Collections;

namespace DbspNet.Tests.Collections;

public class StructuralRowTests
{
    [Fact]
    public void EqualValues_AreStructurallyEqual()
    {
        var a = new StructuralRow(1, "x", true);
        var b = new StructuralRow(1, "x", true);

        Assert.Equal(a, b);
        Assert.True(a == b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
    }

    [Fact]
    public void DifferentValues_AreNotEqual()
    {
        var a = new StructuralRow(1, "x", true);
        var b = new StructuralRow(1, "x", false);

        Assert.NotEqual(a, b);
        Assert.True(a != b);
    }

    [Fact]
    public void OrderMatters()
    {
        var a = new StructuralRow(1, 2);
        var b = new StructuralRow(2, 1);

        Assert.NotEqual(a, b);
    }

    [Fact]
    public void NullValue_IsEqualToNullValue()
    {
        var a = new StructuralRow(1, null, 3);
        var b = new StructuralRow(1, null, 3);

        Assert.Equal(a, b);
    }

    [Fact]
    public void NullAndValue_AreNotEqual()
    {
        var a = new StructuralRow(1, null, 3);
        var b = new StructuralRow(1, 2, 3);

        Assert.NotEqual(a, b);
    }

    [Fact]
    public void UsableAsDictionaryKey()
    {
        var d = new Dictionary<StructuralRow, int>
        {
            [new StructuralRow(1, "a")] = 10,
            [new StructuralRow(2, "b")] = 20,
        };

        Assert.Equal(10, d[new StructuralRow(1, "a")]);
        Assert.Equal(20, d[new StructuralRow(2, "b")]);
        Assert.False(d.ContainsKey(new StructuralRow(1, "b")));
    }

    [Fact]
    public void DifferentArity_NotEqual()
    {
        var a = new StructuralRow(1, 2);
        var b = new StructuralRow(1, 2, 3);

        Assert.NotEqual(a, b);
    }

    [Fact]
    public void ToString_RendersReadably()
    {
        var r = new StructuralRow(1, "x", null, true);
        Assert.Equal("(1, 'x', NULL, True)", r.ToString());
    }

    [Fact]
    public void EnumerationPreservesOrder()
    {
        var r = new StructuralRow(1, "a", 2);
        var values = r.ToList();
        Assert.Equal(new object?[] { 1, "a", 2 }, values);
    }
}
