// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Linear;

/// <summary>
/// Extension methods on <see cref="CircuitBuilder"/> for the linear Z-set
/// operators: Map, Filter, Union, Negate, FlatMap. All satisfy
/// <c>Q(a + b) = Q(a) + Q(b)</c> and <c>Q(n·a) = n·Q(a)</c>, which means the
/// incremental form is identical to the batch form — deltas flow straight
/// through with no state.
/// </summary>
public static class LinearOperators
{
    /// <summary>
    /// Create a Z-set-typed input stream with zero = empty Z-set and merge =
    /// multiset addition (Z-set plus).
    /// </summary>
    public static (InputHandle<ZSet<TRow, TWeight>> Handle, Stream<ZSet<TRow, TWeight>> Stream) ZSetInput<TRow, TWeight>(
        this CircuitBuilder builder)
        where TRow : notnull
        where TWeight : struct, IZRing<TWeight>
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder.Input(ZSet<TRow, TWeight>.Empty, (a, b) => a + b);
    }

    /// <summary>
    /// Pointwise row transform. Rows that map to the same target accumulate
    /// weights in the output.
    /// </summary>
    public static Stream<ZSet<TOut, TWeight>> MapRows<TIn, TOut, TWeight>(
        this CircuitBuilder builder,
        Stream<ZSet<TIn, TWeight>> input,
        Func<TIn, TOut> projection)
        where TIn : notnull
        where TOut : notnull
        where TWeight : struct, IZRing<TWeight>
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(projection);
        return builder.Apply(input, z => z.MapKeys(projection));
    }

    /// <summary>Keep only rows satisfying the predicate.</summary>
    public static Stream<ZSet<TRow, TWeight>> Filter<TRow, TWeight>(
        this CircuitBuilder builder,
        Stream<ZSet<TRow, TWeight>> input,
        Func<TRow, bool> predicate)
        where TRow : notnull
        where TWeight : struct, IZRing<TWeight>
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(predicate);
        return builder.Apply(input, z => z.Filter(predicate));
    }

    /// <summary>Z-set addition of two streams, tick-by-tick.</summary>
    public static Stream<ZSet<TRow, TWeight>> Union<TRow, TWeight>(
        this CircuitBuilder builder,
        Stream<ZSet<TRow, TWeight>> left,
        Stream<ZSet<TRow, TWeight>> right)
        where TRow : notnull
        where TWeight : struct, IZRing<TWeight>
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder.Apply(left, right, (a, b) => a + b);
    }

    /// <summary>Z-set subtraction of two streams, tick-by-tick.</summary>
    public static Stream<ZSet<TRow, TWeight>> Difference<TRow, TWeight>(
        this CircuitBuilder builder,
        Stream<ZSet<TRow, TWeight>> left,
        Stream<ZSet<TRow, TWeight>> right)
        where TRow : notnull
        where TWeight : struct, IZRing<TWeight>
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder.Apply(left, right, (a, b) => a - b);
    }

    /// <summary>Negate every weight in the Z-set stream.</summary>
    public static Stream<ZSet<TRow, TWeight>> Negate<TRow, TWeight>(
        this CircuitBuilder builder,
        Stream<ZSet<TRow, TWeight>> input)
        where TRow : notnull
        where TWeight : struct, IZRing<TWeight>
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder.Apply(input, z => -z);
    }

    /// <summary>
    /// For every row, emit zero or more rows (sharing the source row's weight).
    /// Rows that resolve to the same output accumulate weights.
    /// </summary>
    public static Stream<ZSet<TOut, TWeight>> FlatMapRows<TIn, TOut, TWeight>(
        this CircuitBuilder builder,
        Stream<ZSet<TIn, TWeight>> input,
        Func<TIn, IEnumerable<TOut>> expand)
        where TIn : notnull
        where TOut : notnull
        where TWeight : struct, IZRing<TWeight>
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(expand);

        return builder.Apply(input, z =>
        {
            var b = new ZSetBuilder<TOut, TWeight>();
            foreach (var (row, w) in z)
            {
                foreach (var expanded in expand(row))
                {
                    b.Add(expanded, w);
                }
            }

            return b.Build();
        });
    }
}
