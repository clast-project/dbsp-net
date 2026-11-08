// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Stateful;

/// <summary>
/// The keys of either side of a two-input step, each once with its left group, without a set of them
/// (docs/design-bounded-memory.md §5.34): the left's keys with their groups as enumerated, then the right's
/// whose left group is empty. The outer joins built a <c>HashSet</c> sized for both deltas, the allocation
/// extended TPC-DS tick 18 failed in. A right key's one left probe is both the membership test and the
/// group, and a left key needs none: a block-backed left delta is probed less than before, not more (a
/// separate membership test doubled the probes and op 1693's time, xt99/xt100).
/// </summary>
internal static class TouchedKeys
{
    public static IEnumerable<(TKey Key, ZSet<TLeft, TWeight> Left)> Of<TKey, TLeft, TRight, TWeight>(
        IndexedZSet<TKey, TLeft, TWeight> left,
        IndexedZSet<TKey, TRight, TWeight> right)
        where TKey : notnull
        where TLeft : notnull
        where TRight : notnull
        where TWeight : struct, IZRing<TWeight>
    {
        foreach (var (k, group) in left)
        {
            yield return (k, group);
        }

        foreach (var k in right.Keys)
        {
            var group = left.GroupFor(k);
            if (group.IsEmpty)
            {
                yield return (k, group);
            }
        }
    }
}
