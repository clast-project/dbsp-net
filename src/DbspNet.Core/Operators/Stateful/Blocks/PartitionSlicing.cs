// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
namespace DbspNet.Core.Operators.Stateful.Blocks;

/// <summary>
/// D5 for partitioned operators (docs/design-bounded-memory.md §5.43, §5.47): with in-step spilling
/// on and blocked state, a delta past the step chunk is stepped a slice of partitions at a time, so a
/// slice's touched partitions, scratch and cached state are all the step holds, and the store's
/// write-back budget can drop its cache between slices. Partitions are independent, so the output is
/// the sum of the slices' outputs. Shared by rank and TOP-K.
/// </summary>
internal static class PartitionSlicing
{
    /// <summary>Each slice is one more builder over the delta, so past this the slices grow instead.</summary>
    internal const int MaxSlices = 256;

    /// <summary>How many slices a blocked operator steps a <paramref name="deltaRows"/>-row delta in
    /// (1: not sliced).</summary>
    internal static int Count(bool blocked, int deltaRows)
    {
        if (!blocked || SpillSettings.StepSpillRows is not { } rows)
        {
            return 1;
        }

        var per = rows == 0 ? SpillSettings.ForcedChunkRows : rows;
        return (int)Math.Min(MaxSlices, (deltaRows + per - 1) / per);
    }

    /// <summary>A partition key's slice as a contiguous range of its hash (§5.53): monotone in the
    /// hash as unsigned, so slices stepped in index order write their bulk runs in the record's own hash
    /// order and they concatenate. The top bits, not the low ones workers split by.</summary>
    internal static int Range(int hash, int slices) => (int)(((ulong)(uint)hash * (uint)slices) >> 32);

    /// <summary>A partition key's slice from its hash: mixed (workers already split by hash % W, which
    /// slicing by the same low bits would repeat), then reduced to [0, slices).</summary>
    internal static int Of(int hash, int slices)
    {
        var h = (uint)hash * 0x9E3779B1u;
        return (int)(((ulong)h * (uint)slices) >> 32);
    }
}
