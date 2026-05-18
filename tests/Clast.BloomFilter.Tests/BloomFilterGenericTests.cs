namespace Clast.BloomFilter.Tests;

public class BloomFilterGenericTests
{
    [Fact]
    public void IntKeys_NoFalseNegatives()
    {
        var b = BloomFilterBuilder<int>.WithCapacity(1024, 0.01, 1 << 20);
        for (int i = 0; i < 1024; i++) b.Add(i * 17);
        var f = b.Build();
        for (int i = 0; i < 1024; i++) Assert.True(f.MightContain(i * 17));
    }

    [Fact]
    public void StringKeys_NoFalseNegatives()
    {
        var b = BloomFilterBuilder<string>.WithCapacity(512, 0.01, 1 << 20);
        for (int i = 0; i < 512; i++) b.Add($"row-{i}");
        var f = b.Build();
        for (int i = 0; i < 512; i++) Assert.True(f.MightContain($"row-{i}"));
    }

    [Fact]
    public void GuidKeys_NoFalseNegatives()
    {
        var rng = new Random(Seed: 42);
        var guids = new Guid[256];
        Span<byte> bytes = stackalloc byte[16];
        for (int i = 0; i < guids.Length; i++)
        {
            rng.NextBytes(bytes);
            guids[i] = new Guid(bytes);
        }

        var b = BloomFilterBuilder<Guid>.WithCapacity(guids.Length, 0.01, 1 << 20);
        foreach (var g in guids) b.Add(g);
        var f = b.Build();
        foreach (var g in guids) Assert.True(f.MightContain(g));
    }

    [Fact]
    public void CustomKeyType_UsesEqualityComparerFallback()
    {
        var b = BloomFilterBuilder<(int, string)>.WithCapacity(256, 0.01, 1 << 20);
        for (int i = 0; i < 256; i++) b.Add((i, $"k{i}"));
        var f = b.Build();
        for (int i = 0; i < 256; i++) Assert.True(f.MightContain((i, $"k{i}")));
    }

    [Fact]
    public void Hash64_Default_ReturnsSameInstanceAcrossCalls()
    {
        // Mirrors EqualityComparer<T>.Default — the per-type instance
        // is cached and reused.
        Assert.Same(Hash64.Default<int>(), Hash64.Default<int>());
        Assert.Same(Hash64.Default<string>(), Hash64.Default<string>());
    }

    [Fact]
    public void Hash64_Default_DistinguishesTypes()
    {
        // Specializations must produce different hashes than the fallback
        // would on the same logical value — at minimum, int 0 and the
        // string "0" should hash differently. (If both went through
        // string.GetHashCode they'd produce the same hash.)
        var intHash = Hash64.Default<int>().Hash(0);
        var stringHash = Hash64.Default<string>().Hash("0");
        Assert.NotEqual(intHash, stringHash);
    }

    [Fact]
    public void Builder_RoundTripsViaRawBytes()
    {
        var b = BloomFilterBuilder<int>.WithCapacity(64, 0.01, 1 << 20);
        for (int i = 0; i < 64; i++) b.Add(i);

        var bytes = b.Inner.ToArray();

        var rehydrated = new BloomFilter<int>(new SplitBlockBloomFilter((byte[])bytes.Clone()));
        for (int i = 0; i < 64; i++) Assert.True(rehydrated.MightContain(i));
    }
}
