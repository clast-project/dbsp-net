// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
namespace DbspNet.Core.Operators.Stateful;

/// <summary>
/// Which state representation partitioned TOP-K uses
/// (docs/design-state-representation.md §3 row A).
/// </summary>
internal enum PartitionedTopKState
{
    /// <summary>
    /// The default: <see cref="PartitionedTopKCompactOp{TRow,TKey}"/> — one flat
    /// partition table with each partition's rows stored inline (one row) or in a small
    /// sorted array, no materialised last-emitted window; and, on the typed path, an
    /// unboxed emitted-struct partition key.
    /// </summary>
    Compact = 0,

    /// <summary>The compact operator but with the boxed <c>StructuralRow</c> partition
    /// key on the typed path — isolates the key lever from the container lever.</summary>
    CompactBoxedKey,

    /// <summary>The pre-arc operators (<see cref="PartitionedTopKOp{TRow,TKey}"/> /
    /// <see cref="PartitionedTopKNarrowOp{TRow,TKey}"/>) with the boxed key — the A/B
    /// baseline and fallback.</summary>
    Legacy,
}

/// <summary>
/// Process-wide selection of the partitioned TOP-K state representation. Read once
/// at compile / operator-construction time.
/// </summary>
/// <remarks>
/// <see cref="Override"/> is a thread-static knob for tests; when unset, the
/// <c>DBSPNET_TOPK_STATE</c> environment variable (<c>compact</c> /
/// <c>compactboxedkey</c> / <c>legacy</c>) picks the arm, which lets the decay and
/// w1profile instruments A/B the arms without code changes. Default
/// <see cref="PartitionedTopKState.Compact"/>.
/// </remarks>
internal static class PartitionedTopKStateMode
{
    private static readonly PartitionedTopKState FromEnvironment = Parse(
        Environment.GetEnvironmentVariable("DBSPNET_TOPK_STATE"));

    /// <summary>Thread-static override; <see langword="null"/> ⇒ the environment default.</summary>
    [ThreadStatic]
    internal static PartitionedTopKState? Override;

    internal static PartitionedTopKState Current => Override ?? FromEnvironment;

    private static PartitionedTopKState Parse(string? value) => value?.Trim().ToUpperInvariant() switch
    {
        "LEGACY" => PartitionedTopKState.Legacy,
        "COMPACTBOXEDKEY" => PartitionedTopKState.CompactBoxedKey,
        _ => PartitionedTopKState.Compact,
    };
}
