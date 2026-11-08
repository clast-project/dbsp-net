// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;

namespace DbspNet.Core.Collections;

/// <summary>
/// A Z-set's entries held outside a hash map (docs/design-bounded-memory.md §5.14, D1): the fourth
/// backing of <see cref="ZSet{TKey,TWeight}"/>, after a dictionary, a chunked map and one inline
/// entry. The one implementation is a sorted, consolidated block run (<c>BlockedRows</c>) that a
/// spilling builder makes for a delta past the in-step spill threshold. Immutable once built.
/// </summary>
internal abstract class ZSetExternalStore<TKey, TWeight>
    where TKey : notnull
    where TWeight : struct, IZRing<TWeight>
{
    /// <summary>Entries (exact: the store is consolidated).</summary>
    public abstract int Count { get; }

    /// <summary>The weight of <paramref name="key"/>, found by its hash.</summary>
    public abstract bool TryGetWeight(TKey key, out TWeight weight);

    /// <summary>Every entry, each decoded as it is read.</summary>
    public abstract IEnumerator<KeyValuePair<TKey, TWeight>> GetEnumerator();

    /// <summary>A builder for a set of the same rows (a filter, a sum, a negation): it spills the
    /// same way, under the same format.</summary>
    public abstract ZSetBuilder<TKey, TWeight> NewBuilder(int capacity);

    /// <summary>One of <paramref name="parts"/> builders filled side by side (an exchange's split, one per
    /// destination): each freezes at its share of the step chunk, so together they hold one chunk managed
    /// (docs/design-bounded-memory.md §5.32).</summary>
    public virtual ZSetBuilder<TKey, TWeight> NewBuilder(int capacity, int parts) => NewBuilder(capacity);

    /// <summary>This set plus <paramref name="other"/>, or null to take the generic path (a
    /// builder fed both).</summary>
    public virtual ZSet<TKey, TWeight>? Plus(ZSet<TKey, TWeight> other) => null;

    /// <summary>Approximate heap bytes the store holds resident (the census).</summary>
    public abstract long ResidentBytes { get; }
}

/// <summary>
/// A builder's backing that can leave memory (docs/design-bounded-memory.md §5.14, D2): adds go to a
/// managed map; past a threshold the map is frozen into a block run, and the built set is block
/// backed. A builder holds either a map or one of these in its one store field.
/// </summary>
internal abstract class ZSetSpillingStore<TKey, TWeight>
    where TKey : notnull
    where TWeight : struct, IZRing<TWeight>
{
    /// <summary>Entries added so far: exact in memory, an upper bound once a chunk has frozen
    /// (entries in separate chunks can cancel).</summary>
    public abstract int Count { get; }

    public abstract void Add(TKey key, TWeight weight);

    /// <summary>The set: managed while nothing has frozen (unless forced), else block backed.</summary>
    public abstract ZSet<TKey, TWeight> Build();

    /// <summary>Forget what was added (a pooled builder's next tick).</summary>
    public abstract void Reset();
}

/// <summary>
/// An indexed Z-set's groups held outside a hash map (docs/design-bounded-memory.md §5.22, D4): hash-
/// ordered block runs of (key, value, weight) entries, as a blocked trace holds them. The one
/// implementation is <c>BlockedGroups</c>, which a spilling indexed builder makes for a delta past the
/// in-step spill threshold. Immutable once built.
/// </summary>
internal abstract class IndexedZSetExternalStore<TKey, TValue, TWeight>
    where TKey : notnull
    where TValue : notnull
    where TWeight : struct, IZRing<TWeight>
{
    /// <summary>Keys with a non-empty group (exact).</summary>
    public abstract int GroupCount { get; }

    /// <summary>The group of <paramref name="key"/>, found by its hash (empty if none).</summary>
    public abstract ZSet<TValue, TWeight> GroupFor(TKey key);

    /// <summary>Every non-empty group, each decoded as it is read, in key-hash order.</summary>
    public abstract IEnumerator<KeyValuePair<TKey, ZSet<TValue, TWeight>>> GetEnumerator();
}

/// <summary>
/// An indexed builder's backing that can leave memory (§5.22, D4): adds go to a managed builder; past a
/// threshold its groups are frozen into a block run, and the built set is block backed.
/// </summary>
internal abstract class IndexedZSetSpillingStore<TKey, TValue, TWeight>
    where TKey : notnull
    where TValue : notnull
    where TWeight : struct, IZRing<TWeight>
{
    public abstract void Add(TKey key, TValue value, TWeight weight);

    /// <summary>The set: managed while nothing has frozen (unless forced), else block backed.</summary>
    public abstract IndexedZSet<TKey, TValue, TWeight> Build();
}
