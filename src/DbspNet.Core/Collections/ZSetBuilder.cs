// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
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

    /// <summary>
    /// Creates a builder whose backing dictionary is pre-sized to
    /// <paramref name="capacity"/> entries. Capacity is a pure allocation hint —
    /// the built Z-set is identical to one from the parameterless ctor — but it
    /// lets an operator that knows its delta's size avoid the dictionary-resize
    /// churn that growing from empty pays (~3× the steady backing at large deltas;
    /// docs/design-row-representation.md §16.7). A negative capacity is treated as
    /// zero (grow from empty).
    /// </summary>
    public ZSetBuilder(int capacity)
    {
        _entries = new Dictionary<TKey, TWeight>(capacity > 0 ? capacity : 0);
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

    /// <summary>
    /// Clears the backing dictionary, keeping its grown capacity, so this builder
    /// can be refilled and <see cref="BuildShared"/> again on the next tick without
    /// reallocating — the cross-tick delta pooling of
    /// docs/design-row-representation.md §16.7/§20. Use only on a builder whose last
    /// <see cref="BuildShared"/> output is dead (no <c>z⁻¹</c> or external consumer
    /// retains it across ticks); for the owning-handoff path use <see cref="Build"/>.
    /// </summary>
    public void Reset() => Entries.Clear();

    /// <summary>
    /// Wraps the backing dictionary in a Z-set <b>without</b> transferring ownership
    /// — the builder keeps the dictionary so <see cref="Reset"/> can reclaim it next
    /// tick. The returned Z-set therefore <b>shares</b> the builder's dictionary and
    /// is invalidated by the next <see cref="Reset"/>; the caller guarantees it is
    /// dead by then (the §20 dead-after-tick / no-<c>z⁻¹</c> edge constraint).
    /// </summary>
    public ZSet<TKey, TWeight> BuildShared() => new(Entries);
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
