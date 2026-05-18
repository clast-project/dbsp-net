namespace DbspNet.Core.Operators.Stateful.Spine;

/// <summary>
/// Policy that decides when and how a <c>Spine*Trace</c> compacts its
/// batches. After every <c>Integrate</c>, the spine repeatedly asks the
/// strategy for an action and applies it until the strategy returns
/// <c>null</c>, at which point the spine is considered settled.
/// </summary>
/// <remarks>
/// The strategy is the only knob between tiered, leveled, and size-aware
/// compaction. The data structure, read paths, and snapshot surface stay
/// the same across policies — swapping strategies is purely local. See
/// <see cref="TieredCompactionStrategy"/> for the default.
/// </remarks>
public interface ICompactionStrategy
{
    /// <summary>
    /// Inspect the spine's current per-level batch sizes and return the
    /// next compaction action, or <c>null</c> if nothing needs compacting.
    /// </summary>
    CompactionAction? NextAction(SpineState state);
}

/// <summary>
/// Read-only snapshot of a spine's batch layout, supplied to
/// <see cref="ICompactionStrategy.NextAction"/>.
/// <see cref="BatchSizes"/> is indexed by level — outer list is level
/// number (0 = newest), inner list is the per-batch entry count at that
/// level in insertion order.
/// </summary>
public readonly record struct SpineState(IReadOnlyList<IReadOnlyList<int>> BatchSizes);

/// <summary>
/// Instruction returned by an <see cref="ICompactionStrategy"/>: merge
/// the first <see cref="BatchCount"/> batches at <see cref="SourceLevel"/>
/// into a single batch appended to <c>SourceLevel + 1</c>.
/// </summary>
/// <remarks>
/// The spine always merges the *first* N batches at a level — i.e. the
/// oldest at that level. This keeps the youngest batch (the most recent
/// arrival) at level 0 alone for as long as possible, which matters for
/// hot-key read locality.
/// </remarks>
public readonly record struct CompactionAction(int SourceLevel, int BatchCount);
