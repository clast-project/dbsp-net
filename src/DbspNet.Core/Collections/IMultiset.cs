// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections.Generic;
using DbspNet.Core.Algebra;

namespace DbspNet.Core.Collections;

/// <summary>
/// Read-only view of a weighted multiset: the operations an aggregator needs
/// from its per-group value collection — enumerate the <c>(value, weight)</c>
/// entries, look a value's net weight up, and total the weights — without
/// committing to a hash-backed <see cref="ZSet{TKey,TWeight}"/>.
/// </summary>
/// <remarks>
/// <para><see cref="ZSet{TKey,TWeight}"/> is the canonical implementation
/// (its dictionary answers <see cref="WeightOf"/> in O(1)). The point of the
/// abstraction is the <i>other</i> implementation: a sorted
/// <c>(value, weight)</c> run sliced straight out of the spine batch columns,
/// so the spine aggregate can hand an aggregator the post-delta multiset
/// <b>without rebuilding and re-hashing a Z-set every tick</b> — the
/// merge-execution win from docs/design-row-representation.md §8. Such a run
/// answers <see cref="WeightOf"/> by binary search (comparing the key, not
/// hashing the whole row) and <see cref="SumWeights"/> / enumeration by a
/// linear pass.</para>
/// <para>Widening aggregator parameters from <c>ZSet</c> to this interface is
/// caller-transparent: every existing caller passes a <c>ZSet</c>, which
/// is-a <c>IMultiset</c>.</para>
/// </remarks>
public interface IMultiset<TKey, TWeight> : IEnumerable<KeyValuePair<TKey, TWeight>>
    where TKey : notnull
    where TWeight : struct, IZRing<TWeight>
{
    /// <summary>True when the multiset holds no entries.</summary>
    bool IsEmpty { get; }

    /// <summary>The net weight of <paramref name="key"/>, or <c>Zero</c> if absent.</summary>
    TWeight WeightOf(TKey key);

    /// <summary>The sum of every entry's weight (the "group is present" linear gate).</summary>
    TWeight SumWeights();
}
