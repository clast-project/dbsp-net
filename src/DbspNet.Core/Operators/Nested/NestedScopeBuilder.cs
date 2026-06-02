// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Circuit.Operators;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful;

namespace DbspNet.Core.Operators.Nested;

/// <summary>
/// Build-time API for the body of a nested (fixpoint) circuit — the loop that
/// computes a least fixpoint <c>R = body(I, R)</c> on a second, <em>inner</em>
/// clock nested inside the outer tick. The body is wired from ordinary Z-set
/// operators; the owning <see cref="FixpointOperator{TRow}"/> drives them to a
/// fixpoint each outer tick. See <see cref="NestedOperators.Fixpoint{TRow}"/>.
/// </summary>
/// <remarks>
/// <para>
/// Every operator wired here is <b>stateless</b> (a pure function of its inputs
/// this iteration), so firing the body repeatedly within one outer tick is a
/// from-scratch recomputation — the parity-with-batch baseline. Persisting
/// per-operator traces across outer ticks for true cross-tick incrementality
/// (the DRED retraction story) is the next stage and would swap these for the
/// incremental Core operators; the loop driver and brackets stay the same.
/// </para>
/// <para>
/// All inner streams carry <c>ZSet&lt;TRow, Z64&gt;</c>, so the recursive
/// result and every imported relation share the row type <typeparamref name="TRow"/>
/// (for SQL that is <c>StructuralRow</c>). A body should normally end in
/// <see cref="Distinct"/> so the fixpoint is set-valued and the loop terminates.
/// </para>
/// </remarks>
/// <typeparam name="TRow">The row type flowing on every inner stream.</typeparam>
public sealed class NestedScopeBuilder<TRow>
    where TRow : notnull
{
    private readonly List<IOperator> _operators = [];
    private readonly List<ImportBinding<TRow>> _imports = [];

    internal NestedScopeBuilder()
    {
    }

    /// <summary>The body operators, in topological (wiring) order.</summary>
    internal IReadOnlyList<IOperator> Operators => _operators;

    /// <summary>The outer→inner stream bindings the driver integrates each tick.</summary>
    internal IReadOnlyList<ImportBinding<TRow>> Imports => _imports;

    /// <summary>
    /// Bring an outer-circuit stream into the loop. The driver integrates the
    /// outer stream's per-tick delta into a trace and exposes the running
    /// integral here, constant across the inner iterations of one outer tick.
    /// </summary>
    /// <param name="outer">The outer-circuit delta stream to import.</param>
    /// <param name="name">
    /// Stable name disambiguating this import in a snapshot; required (with
    /// <paramref name="snapshotCodec"/>) for the fixpoint to be snapshottable.
    /// </param>
    /// <param name="snapshotCodec">Codec for the import's integrated trace.</param>
    public Stream<ZSet<TRow, Z64>> Import(
        Stream<ZSet<TRow, Z64>> outer,
        string? name = null,
        IZSetTraceCodec<TRow, Z64>? snapshotCodec = null)
    {
        ArgumentNullException.ThrowIfNull(outer);
        var inner = new Stream<ZSet<TRow, Z64>>(ZSet<TRow, Z64>.Empty);
        _imports.Add(new ImportBinding<TRow>(outer, inner, name, snapshotCodec));
        return inner;
    }

    /// <summary>Keep only rows satisfying <paramref name="predicate"/>.</summary>
    public Stream<ZSet<TRow, Z64>> Filter(Stream<ZSet<TRow, Z64>> input, Func<TRow, bool> predicate)
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(predicate);
        var output = new Stream<ZSet<TRow, Z64>>(ZSet<TRow, Z64>.Empty);
        _operators.Add(new ApplyOp<ZSet<TRow, Z64>, ZSet<TRow, Z64>>(input, output, z => z.Filter(predicate)));
        return output;
    }

    /// <summary>Pointwise row transform; same-target rows accumulate weights.</summary>
    public Stream<ZSet<TRow, Z64>> Map(Stream<ZSet<TRow, Z64>> input, Func<TRow, TRow> projection)
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(projection);
        var output = new Stream<ZSet<TRow, Z64>>(ZSet<TRow, Z64>.Empty);
        _operators.Add(new ApplyOp<ZSet<TRow, Z64>, ZSet<TRow, Z64>>(input, output, z => z.MapKeys(projection)));
        return output;
    }

    /// <summary>Z-set addition of two inner streams (the recursive UNION ALL).</summary>
    public Stream<ZSet<TRow, Z64>> Union(Stream<ZSet<TRow, Z64>> left, Stream<ZSet<TRow, Z64>> right)
    {
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);
        var output = new Stream<ZSet<TRow, Z64>>(ZSet<TRow, Z64>.Empty);
        _operators.Add(new Apply2Op<ZSet<TRow, Z64>, ZSet<TRow, Z64>, ZSet<TRow, Z64>>(
            left, right, output, (a, b) => a + b));
        return output;
    }

    /// <summary>
    /// Collapse to set semantics — weight 1 per distinct positive-weight row.
    /// A recursive body ends here so the fixpoint is set-valued and terminates.
    /// </summary>
    public Stream<ZSet<TRow, Z64>> Distinct(Stream<ZSet<TRow, Z64>> input)
    {
        ArgumentNullException.ThrowIfNull(input);
        var output = new Stream<ZSet<TRow, Z64>>(ZSet<TRow, Z64>.Empty);
        _operators.Add(new ApplyOp<ZSet<TRow, Z64>, ZSet<TRow, Z64>>(input, output, ToSet));
        return output;
    }

    /// <summary>
    /// Inner equi-join: for every left row whose <paramref name="leftKey"/>
    /// matches a right row's <paramref name="rightKey"/>, emit
    /// <paramref name="combine"/> of the two with the product of their weights.
    /// </summary>
    public Stream<ZSet<TRow, Z64>> Join<TKey>(
        Stream<ZSet<TRow, Z64>> left,
        Stream<ZSet<TRow, Z64>> right,
        Func<TRow, TKey> leftKey,
        Func<TRow, TKey> rightKey,
        Func<TRow, TRow, TRow> combine)
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);
        ArgumentNullException.ThrowIfNull(leftKey);
        ArgumentNullException.ThrowIfNull(rightKey);
        ArgumentNullException.ThrowIfNull(combine);
        var output = new Stream<ZSet<TRow, Z64>>(ZSet<TRow, Z64>.Empty);
        _operators.Add(new Apply2Op<ZSet<TRow, Z64>, ZSet<TRow, Z64>, ZSet<TRow, Z64>>(
            left, right, output, (l, r) => BatchJoin(l, r, leftKey, rightKey, combine)));
        return output;
    }

    private static ZSet<TRow, Z64> ToSet(ZSet<TRow, Z64> z)
    {
        if (z.IsEmpty)
        {
            return z;
        }

        var builder = new ZSetBuilder<TRow, Z64>();
        foreach (var (row, w) in z)
        {
            if (Z64.IsPositive(w))
            {
                builder.Add(row, Z64.One);
            }
        }

        return builder.Build();
    }

    private static ZSet<TRow, Z64> BatchJoin<TKey>(
        ZSet<TRow, Z64> left,
        ZSet<TRow, Z64> right,
        Func<TRow, TKey> leftKey,
        Func<TRow, TKey> rightKey,
        Func<TRow, TRow, TRow> combine)
        where TKey : notnull
    {
        if (left.IsEmpty || right.IsEmpty)
        {
            return ZSet<TRow, Z64>.Empty;
        }

        var rightByKey = new Dictionary<TKey, List<(TRow Row, Z64 Weight)>>();
        foreach (var (row, w) in right)
        {
            var key = rightKey(row);
            if (!rightByKey.TryGetValue(key, out var list))
            {
                rightByKey[key] = list = [];
            }

            list.Add((row, w));
        }

        var builder = new ZSetBuilder<TRow, Z64>();
        foreach (var (lrow, lw) in left)
        {
            if (rightByKey.TryGetValue(leftKey(lrow), out var matches))
            {
                foreach (var (rrow, rw) in matches)
                {
                    builder.Add(combine(lrow, rrow), Z64.Multiply(lw, rw));
                }
            }
        }

        return builder.Build();
    }
}

/// <summary>
/// A loop import: the outer delta stream, the inner stream the driver feeds the
/// running integral to, and (when the fixpoint is snapshottable) a stable name
/// and codec for that integral's trace.
/// </summary>
internal sealed record ImportBinding<TRow>(
    Stream<ZSet<TRow, Z64>> Outer,
    Stream<ZSet<TRow, Z64>> Inner,
    string? Name,
    IZSetTraceCodec<TRow, Z64>? SnapshotCodec)
    where TRow : notnull;
