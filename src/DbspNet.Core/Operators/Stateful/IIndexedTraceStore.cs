// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Stateful;

/// <summary>
/// Alternative state for a join's <see cref="IndexedZSetTrace{TKey,TValue,TWeight}"/>: the
/// integral of an indexed Z-set held without an <see cref="IndexedZSet{TKey,TValue,TWeight}"/>
/// of row objects. The SQL layer's paged store is the one implementation
/// (docs/design-paged-trace-state.md, increment 3).
/// </summary>
/// <remarks>
/// Groups and values are addressed by <c>int</c> handles, so a probe allocates nothing of its
/// own. Groups enumerate in the order the equivalent <see cref="IndexedZSet{TKey,TValue,TWeight}"/>
/// would enumerate its keys, and a group's values in the order its inner Z-set would — the join
/// emits in exactly the same order as over the in-memory trace. A row returned by
/// <see cref="KeyOf"/> / <see cref="ValueAt"/> may be a view onto the stored slot: valid until
/// the next <see cref="Integrate"/> or <see cref="DropKeysBelow"/>, and not to be retained.
/// </remarks>
internal interface IIndexedTraceStore<TKey, TValue, TWeight>
    where TKey : notnull
    where TValue : notnull
    where TWeight : struct, IZRing<TWeight>
{
    int GroupCount { get; }

    void Integrate(IndexedZSet<TKey, TValue, TWeight> delta);

    /// <summary>The group holding <paramref name="key"/>, or −1.</summary>
    int FindGroup(TKey key);

    /// <summary>The first group in key order, or −1.</summary>
    int FirstGroup();

    int NextGroup(int group);

    TKey KeyOf(int group);

    /// <summary>The group's first value, or −1.</summary>
    int FirstValue(int group);

    int NextValue(int value);

    TValue ValueAt(int value);

    TWeight WeightAt(int value);

    /// <summary>A fresh indexed Z-set of the contents, in the in-memory trace's order.</summary>
    IndexedZSet<TKey, TValue, TWeight> Materialize();

    /// <inheritdoc cref="IndexedZSetTrace{TKey,TValue,TWeight}.DropKeysBelow"/>
    IReadOnlyList<TKey> DropKeysBelow(long threshold, Func<TKey, long> monotoneKey);
}
