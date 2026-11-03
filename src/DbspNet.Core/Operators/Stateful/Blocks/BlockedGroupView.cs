// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Stateful.Blocks;

/// <summary>
/// A key's group in a <see cref="BlockedIndexedTrace"/>, read without decoding it
/// (docs/design-flat-trace-state.md §3.5): the key's group in each managed level, and its entry
/// range in each run. Consolidated answers, as an aggregate's before-group needs:
/// <list type="bullet">
/// <item><see cref="WeightOf"/> — a dictionary probe per managed level; per run, a scan of the key's
/// entries comparing stored value hashes, then (on a match) the encoded value cell by cell;</item>
/// <item><see cref="SumWeights"/> — the stored weights;</item>
/// <item><see cref="IsEmpty"/> — the first entry whose net weight across levels is non-zero;</item>
/// <item>enumeration — decodes the group once (an aggregator that walks the whole group).</item>
/// </list>
/// </summary>
internal sealed class BlockedGroupView : IMultiset<StructuralRow, Z64>, ILazyGroup<StructuralRow>
{
    private readonly StructuralRow _key;
    private readonly List<ZSet<StructuralRow, Z64>> _managed;
    private readonly List<(ITraceBlock Block, int From, int To)> _ranges;
    private ZSet<StructuralRow, Z64>? _decoded;

    internal BlockedGroupView(StructuralRow key, List<ZSet<StructuralRow, Z64>> managed, List<(ITraceBlock Block, int From, int To)> ranges)
    {
        _key = key;
        _managed = managed;
        _ranges = ranges;
    }

    public bool IsEmpty => _decoded is { } d ? d.IsEmpty : !AnyNonZero(null);

    /// <summary>Entries the group can hold, across levels, counted without decoding (an upper bound:
    /// a range may hold another key with the same hash, and entries in separate levels may cancel).</summary>
    public int EntryBound
    {
        get
        {
            var n = 0;
            foreach (var group in _managed)
            {
                n += group.Count;
            }

            foreach (var (_, from, to) in _ranges)
            {
                n += to - from;
            }

            return n;
        }
    }

    /// <inheritdoc/>
    public bool AnyNonZero(Func<StructuralRow, Z64>? adjust)
    {
        if (_decoded is { } decoded)
        {
            foreach (var (value, weight) in decoded)
            {
                if (!Z64.IsZero(adjust is null ? weight : Z64.Add(weight, adjust(value))))
                {
                    return true;
                }
            }

            return false;
        }

        // Entry by entry (a block value decoded only when reached): the first value whose net
        // across levels, adjusted, is non-zero answers. A group cancelled everywhere scans to the end.
        foreach (var group in _managed)
        {
            foreach (var (value, _) in group)
            {
                if (NonZero(value))
                {
                    return true;
                }
            }
        }

        foreach (var (block, from, to) in _ranges)
        {
            for (var i = from; i < to; i++)
            {
                if (block.KeyEquals(i, _key) && NonZero(block.Value(i)))
                {
                    return true;
                }
            }
        }

        return false;

        bool NonZero(StructuralRow value)
        {
            var net = WeightOf(value);
            return !Z64.IsZero(adjust is null ? net : Z64.Add(net, adjust(value)));
        }
    }

    public Z64 WeightOf(StructuralRow key)
    {
        ArgumentNullException.ThrowIfNull(key);
        if (_decoded is { } d)
        {
            return d.WeightOf(key);
        }

        var weight = Z64.Zero;
        foreach (var group in _managed)
        {
            weight = Z64.Add(weight, group.WeightOf(key));
        }

        var hash = key.GetHashCode();
        foreach (var (block, from, to) in _ranges)
        {
            for (var i = from; i < to; i++)
            {
                if (block.ValueHash(i) == hash && block.ValueEquals(i, key) && block.KeyEquals(i, _key))
                {
                    weight = Z64.Add(weight, new Z64(block.Weight(i)));
                }
            }
        }

        return weight;
    }

    public Z64 SumWeights()
    {
        var sum = Z64.Zero;
        foreach (var group in _managed)
        {
            sum = Z64.Add(sum, group.SumWeights());
        }

        foreach (var (block, from, to) in _ranges)
        {
            for (var i = from; i < to; i++)
            {
                if (block.KeyEquals(i, _key))
                {
                    sum = Z64.Add(sum, new Z64(block.Weight(i)));
                }
            }
        }

        return sum;
    }

    /// <summary>The group decoded and consolidated (once).</summary>
    public ZSet<StructuralRow, Z64> Decoded()
    {
        if (_decoded is { } d)
        {
            return d;
        }

        var builder = new ZSetBuilder<StructuralRow, Z64>();
        foreach (var group in _managed)
        {
            foreach (var (value, weight) in group)
            {
                builder.Add(value, weight);
            }
        }

        foreach (var (block, from, to) in _ranges)
        {
            for (var i = from; i < to; i++)
            {
                if (block.KeyEquals(i, _key))
                {
                    builder.Add(block.Value(i), new Z64(block.Weight(i)));
                }
            }
        }

        return _decoded = builder.Build();
    }

    public IEnumerator<KeyValuePair<StructuralRow, Z64>> GetEnumerator() => Decoded().GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}
