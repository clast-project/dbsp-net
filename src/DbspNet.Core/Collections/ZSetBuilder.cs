using DbspNet.Core.Algebra;

namespace DbspNet.Core.Collections;

/// <summary>
/// Efficient incremental builder for <see cref="ZSet{TKey,TWeight}"/>. All
/// ZSet mutation flows through this type so the zero-weight-never-stored
/// invariant can be enforced in exactly one place.
/// </summary>
public sealed class ZSetBuilder<TKey, TWeight>
    where TKey : notnull
    where TWeight : struct, IZRing<TWeight>
{
    private Dictionary<TKey, TWeight>? _entries;

    public ZSetBuilder()
    {
        _entries = new Dictionary<TKey, TWeight>();
    }

    internal ZSetBuilder(Dictionary<TKey, TWeight> entries)
    {
        _entries = entries;
    }

    public int Count => Entries.Count;

    private Dictionary<TKey, TWeight> Entries =>
        _entries ?? throw new InvalidOperationException("ZSetBuilder used after Build().");

    /// <summary>
    /// Adds <paramref name="weight"/> to the weight of <paramref name="key"/>.
    /// If the resulting weight is zero, removes the key entirely.
    /// </summary>
    public void Add(TKey key, TWeight weight)
    {
        if (TWeight.IsZero(weight))
        {
            return;
        }

        var d = Entries;
        if (d.TryGetValue(key, out var existing))
        {
            var sum = TWeight.Add(existing, weight);
            if (TWeight.IsZero(sum))
            {
                d.Remove(key);
            }
            else
            {
                d[key] = sum;
            }
        }
        else
        {
            d[key] = weight;
        }
    }

    public void AddRange(IEnumerable<KeyValuePair<TKey, TWeight>> entries)
    {
        ArgumentNullException.ThrowIfNull(entries);
        foreach (var (k, w) in entries)
        {
            Add(k, w);
        }
    }

    public ZSet<TKey, TWeight> Build()
    {
        var d = Entries;
        _entries = null; // prevent reuse
        if (d.Count == 0)
        {
            return ZSet<TKey, TWeight>.Empty;
        }

        return new ZSet<TKey, TWeight>(d);
    }
}

internal static class ZSetBuilder
{
    public static ZSetBuilder<TKey, TWeight> From<TKey, TWeight>(IReadOnlyDictionary<TKey, TWeight> source)
        where TKey : notnull
        where TWeight : struct, IZRing<TWeight>
    {
        var d = new Dictionary<TKey, TWeight>(capacity: source.Count);
        foreach (var (k, w) in source)
        {
            d[k] = w;
        }

        return new ZSetBuilder<TKey, TWeight>(d);
    }
}
