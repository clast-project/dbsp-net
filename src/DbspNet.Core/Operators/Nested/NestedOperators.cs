// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful;

namespace DbspNet.Core.Operators.Nested;

/// <summary>
/// <see cref="CircuitBuilder"/> entry point for the nested-circuit (fixpoint)
/// primitive. See <see cref="FixpointOperator{TRow}"/> for the construction and
/// <see cref="NestedScopeBuilder{TRow}"/> for the operators usable in the body.
/// </summary>
public static class NestedOperators
{
    /// <summary>
    /// Wire a least-fixpoint loop <c>R = body(I, R)</c> and return the stream
    /// carrying its per-tick delta. The <paramref name="body"/> receives a
    /// nested-scope builder and the feedback stream (<c>R</c> from the previous
    /// inner iteration), and returns the stream that becomes the next <c>R</c>;
    /// it should end in <see cref="NestedScopeBuilder{TRow}.Distinct"/> so the
    /// fixpoint is set-valued and the loop converges.
    /// </summary>
    /// <param name="builder">The owning circuit builder.</param>
    /// <param name="body">Wires the recursive body; see remarks above.</param>
    /// <param name="resultSnapshotCodec">
    /// Codec for the previous-tick fixpoint. Supply it (together with a name and
    /// codec on every <see cref="NestedScopeBuilder{TRow}.Import"/>) to make the
    /// fixpoint snapshottable; omit it for a non-persisted loop.
    /// </param>
    /// <param name="maxIterations">Divergence guard for the inner loop.</param>
    /// <typeparam name="TRow">The row type flowing on every inner stream.</typeparam>
    public static Stream<ZSet<TRow, Z64>> Fixpoint<TRow>(
        this CircuitBuilder builder,
        Func<NestedScopeBuilder<TRow>, Stream<ZSet<TRow, Z64>>, Stream<ZSet<TRow, Z64>>> body,
        IZSetTraceCodec<TRow, Z64>? resultSnapshotCodec = null,
        int maxIterations = 10_000)
        where TRow : notnull
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(body);

        var scope = new NestedScopeBuilder<TRow>();
        var recRef = new Stream<ZSet<TRow, Z64>>(ZSet<TRow, Z64>.Empty);
        var next = body(scope, recRef);
        ArgumentNullException.ThrowIfNull(next);

        var output = new Stream<ZSet<TRow, Z64>>(ZSet<TRow, Z64>.Empty);
        builder.AddRawOperator(new FixpointOperator<TRow>(
            scope.Operators, scope.Imports, recRef, next, output, resultSnapshotCodec, maxIterations));
        return output;
    }

    /// <summary>
    /// Wire a cross-tick-incremental least-fixpoint loop for a <em>linear</em>
    /// recursion <c>R = distinct(base ∪ step(R))</c> and return the stream
    /// carrying its per-tick delta. The <paramref name="body"/> wires both
    /// branches into the scope and returns them separately — <c>Base</c> (no
    /// self-reference) and <c>Step</c> (linear in the feedback stream
    /// <c>recRef</c>) — so the operator can extend the preserved fixpoint
    /// semi-naively on insert-only ticks (recompute fallback on retractions).
    /// See <see cref="SemiNaiveFixpointOperator{TRow}"/>.
    /// </summary>
    /// <param name="builder">The owning circuit builder.</param>
    /// <param name="body">Wires base and step; returns both streams.</param>
    /// <param name="resultSnapshotCodec">
    /// Codec for the materialised fixpoint. Supply it (with a name and codec on
    /// every <see cref="NestedScopeBuilder{TRow}.Import"/>) to make the loop
    /// snapshottable; omit it for a non-persisted loop.
    /// </param>
    /// <param name="maxIterations">Divergence guard for the inner loop.</param>
    /// <typeparam name="TRow">The row type flowing on every inner stream.</typeparam>
    public static Stream<ZSet<TRow, Z64>> SemiNaiveFixpoint<TRow>(
        this CircuitBuilder builder,
        Func<NestedScopeBuilder<TRow>, Stream<ZSet<TRow, Z64>>,
             (Stream<ZSet<TRow, Z64>> Base, Stream<ZSet<TRow, Z64>> Step)> body,
        IZSetTraceCodec<TRow, Z64>? resultSnapshotCodec = null,
        int maxIterations = 10_000)
        where TRow : notnull
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(body);

        var scope = new NestedScopeBuilder<TRow>();
        var recRef = new Stream<ZSet<TRow, Z64>>(ZSet<TRow, Z64>.Empty);
        var (baseStream, stepStream) = body(scope, recRef);
        ArgumentNullException.ThrowIfNull(baseStream);
        ArgumentNullException.ThrowIfNull(stepStream);

        var output = new Stream<ZSet<TRow, Z64>>(ZSet<TRow, Z64>.Empty);
        builder.AddRawOperator(new SemiNaiveFixpointOperator<TRow>(
            scope.Operators, scope.Imports, recRef, baseStream, stepStream, output, resultSnapshotCodec, maxIterations));
        return output;
    }
}
