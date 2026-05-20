// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
namespace DbspNet.Core.Circuit;

/// <summary>
/// An edge in a DBSP circuit carrying a value of type <typeparamref name="T"/>.
/// Every step of the circuit writes a new current value; consumers read it
/// in topological order during the same step.
/// </summary>
public sealed class Stream<T>
{
    internal T Current = default!;

    internal Stream()
    {
    }

    internal Stream(T initial)
    {
        Current = initial;
    }

    internal void SetCurrent(T value) => Current = value;

    /// <summary>
    /// Reads the most recently emitted value on this stream. Typically only
    /// called on streams wrapped in an <see cref="OutputHandle{T}"/>; internal
    /// operators read current values through their own wiring.
    /// </summary>
    public T Peek() => Current;
}
