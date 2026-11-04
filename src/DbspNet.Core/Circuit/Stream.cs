// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
namespace DbspNet.Core.Circuit;

/// <summary>
/// An edge in a DBSP circuit carrying a value of type <typeparamref name="T"/>.
/// Every step of the circuit writes a new current value; consumers read it
/// in topological order during the same step.
/// </summary>
public sealed class Stream<T> : IReleasableStream
{
    internal T Current = default!;

    /// <summary>The block format of this stream's rows (an <c>ITraceBlockFormat</c> for a structural
    /// Z-set stream), set by the compiler only while in-step delta spilling is on
    /// (docs/design-bounded-memory.md §5.14): an operator writing this stream builds its delta with
    /// <c>DeltaSpill.Rows(DeltaFormat, …)</c>. Null: the plain builder.</summary>
    internal object? DeltaFormat;

    int IReleasableStream.Producer { get; set; } = -2;

    bool IReleasableStream.Pinned { get; set; }

    bool IReleasableStream.CommittedEveryTick { get; set; }

    // The released value's Count, so an operator's LastOutputRows (introspection, read after the
    // step) still reports the tick's output once release has dropped the value; −1 when not released.
    private long _releasedCount = -1;

    void IReleasableStream.Release()
    {
        _releasedCount = Current is null || CountOf is null ? 0 : CountOf(Current);
        Current = default!;
    }

    /// <summary>The current value's <c>Count</c>, or the count it had when release dropped it
    /// (docs/design-bounded-memory.md §3b); 0 for no value or a type without <c>Count</c>.</summary>
    internal long LastCount => Current is { } v ? (CountOf is null ? 0 : CountOf(v)) : Math.Max(_releasedCount, 0);

    // T's public int or long Count property, read through a compiled delegate; null if it has none.
    private static readonly Func<T, long>? CountOf = BuildCountOf();

    private static Func<T, long>? BuildCountOf()
    {
        var prop = typeof(T).GetProperty("Count", System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance, null, typeof(int), Type.EmptyTypes, null)
            ?? typeof(T).GetProperty("Count", System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance, null, typeof(long), Type.EmptyTypes, null);
        if (prop is null)
        {
            return null;
        }

        var x = System.Linq.Expressions.Expression.Parameter(typeof(T), "x");
        var body = System.Linq.Expressions.Expression.Convert(System.Linq.Expressions.Expression.Property(x, prop), typeof(long));
        return System.Linq.Expressions.Expression.Lambda<Func<T, long>>(body, x).Compile();
    }

    internal Stream()
    {
    }

    internal Stream(T initial)
    {
        Current = initial;
    }

    internal void SetCurrent(T value)
    {
        Current = value;
        _releasedCount = -1;
        if (StreamRelease.Enabled && ((IReleasableStream)this).Producer == -2)
        {
            var producer = StreamRelease.CurrentOp;
            ((IReleasableStream)this).Producer = producer;
            StreamRelease.CurrentFirstTick?.Written(this, producer);
        }
    }

    /// <summary>
    /// Reads the most recently emitted value on this stream. Typically only
    /// called on streams wrapped in an <see cref="OutputHandle{T}"/>; internal
    /// operators read current values through their own wiring.
    /// </summary>
    public T Peek() => Current;
}
