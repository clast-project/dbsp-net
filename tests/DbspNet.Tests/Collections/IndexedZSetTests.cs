using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;

namespace DbspNet.Tests.Collections;

public class IndexedZSetTests
{
    [Fact]
    public void Empty_IsEmpty()
    {
        var iz = IndexedZSet.Empty<string, int, Z64>();
        Assert.True(iz.IsEmpty);
        Assert.Equal(0, iz.GroupCount);
    }

    [Fact]
    public void Builder_GroupsByKey()
    {
        var b = new IndexedZSetBuilder<string, int, Z64>();
        b.Add("a", 1, new Z64(2));
        b.Add("a", 2, new Z64(3));
        b.Add("b", 1, new Z64(1));
        var iz = b.Build();

        Assert.Equal(2, iz.GroupCount);
        Assert.Equal(new Z64(2), iz.GroupFor("a").WeightOf(1));
        Assert.Equal(new Z64(3), iz.GroupFor("a").WeightOf(2));
        Assert.Equal(new Z64(1), iz.GroupFor("b").WeightOf(1));
    }

    [Fact]
    public void Builder_DropsGroupsWithAllZeroWeights()
    {
        var b = new IndexedZSetBuilder<string, int, Z64>();
        b.Add("a", 1, new Z64(2));
        b.Add("a", 1, new Z64(-2));
        b.Add("b", 1, new Z64(3));
        var iz = b.Build();

        Assert.Equal(1, iz.GroupCount);
        Assert.False(iz.ContainsKey("a"));
        Assert.True(iz.ContainsKey("b"));
    }

    [Fact]
    public void GroupFor_UnknownKey_IsEmpty()
    {
        var iz = IndexedZSet.Empty<string, int, Z64>();
        Assert.True(iz.GroupFor("missing").IsEmpty);
    }

    [Fact]
    public void IndexBy_GroupsRowsUsingKeyExtractor()
    {
        var rows = ZSet.FromEntries(new[]
        {
            ((1, "a"), new Z64(1)),
            ((1, "b"), new Z64(2)),
            ((2, "a"), new Z64(3)),
        });

        var indexed = IndexedZSet.IndexBy(rows, r => r.Item1);

        Assert.Equal(2, indexed.GroupCount);
        Assert.Equal(new Z64(1), indexed.GroupFor(1).WeightOf((1, "a")));
        Assert.Equal(new Z64(2), indexed.GroupFor(1).WeightOf((1, "b")));
        Assert.Equal(new Z64(3), indexed.GroupFor(2).WeightOf((2, "a")));
    }

    [Fact]
    public void Flatten_RoundTrips()
    {
        var rows = ZSet.FromEntries(new[]
        {
            ((1, "a"), new Z64(1)),
            ((1, "b"), new Z64(2)),
            ((2, "a"), new Z64(3)),
        });

        var indexed = IndexedZSet.IndexBy(rows, r => r.Item1);
        var flat = indexed.Flatten();

        // Flatten yields (key, row) pairs. Map them back to the original row shape.
        var reconstructed = flat.MapKeys(kv => kv.Value);
        Assert.Equal(rows, reconstructed);
    }

    [Fact]
    public void Equality_IsStructural()
    {
        var b1 = new IndexedZSetBuilder<string, int, Z64>();
        b1.Add("a", 1, new Z64(1));
        b1.Add("b", 1, new Z64(1));
        var iz1 = b1.Build();

        var b2 = new IndexedZSetBuilder<string, int, Z64>();
        b2.Add("b", 1, new Z64(1));
        b2.Add("a", 1, new Z64(1));
        var iz2 = b2.Build();

        Assert.Equal(iz1, iz2);
    }
}
