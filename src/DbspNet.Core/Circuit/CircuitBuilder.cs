// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Circuit.Operators;

namespace DbspNet.Core.Circuit;

/// <summary>
/// Build-time API for a <see cref="RootCircuit"/>. Register inputs, wire
/// operators, and declare outputs — each call returns a <see cref="Stream{T}"/>
/// or handle you can compose further.
/// </summary>
public sealed class CircuitBuilder
{
    private readonly RootCircuit _root;

    internal CircuitBuilder(RootCircuit root)
    {
        _root = root;
    }

    /// <summary>
    /// Create an input port with a zero value (used if no push is made on a
    /// given tick) and a merge function (used when <see cref="InputHandle{T}.Push"/>
    /// is called multiple times between two <see cref="RootCircuit.Step"/> calls).
    /// </summary>
    public (InputHandle<T> Handle, Stream<T> Stream) Input<T>(T zero, Func<T, T, T> merge)
    {
        ArgumentNullException.ThrowIfNull(merge);
        var stream = new Stream<T>(zero);
        var handle = new InputHandle<T>(_root, stream, zero, merge);
        _root.AddInput(handle);
        return (handle, stream);
    }

    /// <summary>Create an output handle observing the given stream.</summary>
    public OutputHandle<T> Output<T>(Stream<T> source)
    {
        ArgumentNullException.ThrowIfNull(source);
        return new OutputHandle<T>(source);
    }

    /// <summary>
    /// Register a raw operator with the circuit in topological order.
    /// Internal hook used by stateful operators that need direct control
    /// over their inputs and outputs.
    /// </summary>
    internal void AddRawOperator(IOperator op)
    {
        ArgumentNullException.ThrowIfNull(op);
        _root.AddOperator(op);
    }

    /// <summary>
    /// Pointwise stateless unary transform. For every tick, the output
    /// equals <c>transform(input.Current)</c>.
    /// </summary>
    public Stream<TOut> Apply<TIn, TOut>(Stream<TIn> input, Func<TIn, TOut> transform)
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(transform);
        var output = new Stream<TOut>();
        _root.AddOperator(new ApplyOp<TIn, TOut>(input, output, transform));
        return output;
    }

    /// <summary>
    /// Pointwise stateless binary transform. For every tick, the output
    /// equals <c>transform(left.Current, right.Current)</c>.
    /// </summary>
    public Stream<TOut> Apply<TIn1, TIn2, TOut>(
        Stream<TIn1> left,
        Stream<TIn2> right,
        Func<TIn1, TIn2, TOut> transform)
    {
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);
        ArgumentNullException.ThrowIfNull(transform);
        var output = new Stream<TOut>();
        _root.AddOperator(new Apply2Op<TIn1, TIn2, TOut>(left, right, output, transform));
        return output;
    }

    /// <summary>
    /// The z^-1 delay: on tick <c>t</c> the output emits the value that
    /// <paramref name="input"/> carried on tick <c>t-1</c>; on tick 0 it
    /// emits <paramref name="initial"/>.
    /// </summary>
    public Stream<T> Delay<T>(Stream<T> input, T initial)
    {
        ArgumentNullException.ThrowIfNull(input);
        var output = new Stream<T>(initial);
        _root.AddOperator(new DelayOp<T>(input, output, initial));
        return output;
    }
}
