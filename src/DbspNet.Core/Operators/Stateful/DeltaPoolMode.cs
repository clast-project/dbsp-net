// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
namespace DbspNet.Core.Operators.Stateful;

/// <summary>
/// Process-wide opt-in for <b>cross-tick delta-builder pooling</b> in the stateful
/// operators (docs/design-row-representation.md §20 — the term-1 / per-tick
/// allocation lever). When enabled, an operator reuses one
/// <see cref="DbspNet.Core.Collections.ZSetBuilder{TKey,TWeight}"/> across ticks
/// (<c>Reset()</c> + refill + <c>BuildShared()</c>) instead of allocating a fresh
/// builder/dictionary every <c>Step()</c> — reclaiming the steady backing the
/// §16.8 pre-sizing still re-allocates.
/// </summary>
/// <remarks>
/// <para>Default <see langword="false"/> — byte-identical to the
/// fresh-builder-per-tick behaviour (each op constructs no pooled builder and
/// keeps allocating). Read once at operator construction, so it needs no builder
/// signature change (dodging the typed-compiler reflection gotcha) and mirrors the
/// <c>SpineStagingConfig</c> / <c>FlatAggregateMode</c> gated-seam discipline.</para>
/// <para><b>Soundness envelope.</b> A pooled builder's <c>BuildShared</c> output
/// shares the builder's dictionary, which the next tick's <c>Reset</c> clears — so
/// it is correct <b>only on a dead-after-tick edge</b>: nothing may retain the
/// operator's output Z-set across ticks. The single cross-tick aliaser of a delta
/// in this engine is <c>DelayOp</c> (<c>z⁻¹</c>); flat (non-recursive) pipelines put
/// no <c>z⁻¹</c> on an operator-output delta, so they are safe, while
/// recursive-CTE circuits are <b>not</b> and must be excluded. A circuit's
/// externally-read terminal output is also unsafe unless the consumer copies each
/// tick. Productionising this needs the compiler to engage pooling per-edge after a
/// "no-<c>z⁻¹</c>, non-terminal" analysis; the seam is the prototype channel.</para>
/// </remarks>
internal static class DeltaPoolMode
{
    /// <summary>
    /// When true, stateful operators reuse one delta builder across ticks. Default
    /// false (fresh builder per tick, byte-identical). Thread-static: each thread's
    /// operators read that thread's compile-scoped value.
    /// </summary>
    [ThreadStatic]
    internal static bool Enabled;
}
