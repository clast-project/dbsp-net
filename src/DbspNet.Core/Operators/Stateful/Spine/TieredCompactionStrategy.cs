// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
namespace DbspNet.Core.Operators.Stateful.Spine;

/// <summary>
/// Classic tiered LSM compaction: when any level reaches
/// <see cref="BatchesPerLevel"/> batches, merge them all into one batch
/// at the next level. Lower bound is 2 (which effectively gives
/// leveled-like behaviour: at most one batch per level).
/// </summary>
/// <remarks>
/// <para>Read amplification is <c>O(BatchesPerLevel × log N)</c> in
/// the worst case (each level holds up to <c>BatchesPerLevel − 1</c>
/// batches; total levels grow logarithmically with state size). Write
/// amplification is <c>O(log N)</c> — each entry is rewritten once per
/// level it traverses.</para>
/// <para>The default of 4 balances the two: small enough that per-key
/// reads don't fan out badly, large enough that compaction doesn't
/// trigger on every other tick.</para>
/// </remarks>
public sealed class TieredCompactionStrategy : ICompactionStrategy
{
    /// <summary>Default tiered strategy with 4 batches per level.</summary>
    public static TieredCompactionStrategy Default { get; } = new(4);

    /// <summary>Number of batches a level accumulates before compaction.</summary>
    public int BatchesPerLevel { get; }

    public TieredCompactionStrategy(int batchesPerLevel)
    {
        if (batchesPerLevel < 2)
        {
            throw new ArgumentOutOfRangeException(
                nameof(batchesPerLevel), batchesPerLevel,
                "tiered compaction requires at least 2 batches per level");
        }

        BatchesPerLevel = batchesPerLevel;
    }

    public CompactionAction? NextAction(SpineState state)
    {
        for (var level = 0; level < state.BatchSizes.Count; level++)
        {
            if (state.BatchSizes[level].Count >= BatchesPerLevel)
            {
                return new CompactionAction(level, BatchesPerLevel);
            }
        }

        return null;
    }
}
