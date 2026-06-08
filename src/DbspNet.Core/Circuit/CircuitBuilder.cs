// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit.Operators;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful;

namespace DbspNet.Core.Circuit;

/// <summary>
/// Build-time API for a <see cref="RootCircuit"/>. Register inputs, wire
/// operators, and declare outputs — each call returns a <see cref="Stream{T}"/>
/// or handle you can compose further.
/// </summary>
public sealed class CircuitBuilder
{
    private readonly RootCircuit _root;
    private readonly ParallelBuildContext? _parallel;

    internal CircuitBuilder(RootCircuit root, ParallelBuildContext? parallel = null)
    {
        _root = root;
        _parallel = parallel;
    }

    /// <summary>
    /// The circuit's logical clock as a read-only frontier — the <c>NOW()</c>
    /// value temporal-filter operators read each tick. See
    /// <see cref="RootCircuit.Clock"/>.
    /// </summary>
    internal DbspNet.Core.Operators.Stateful.IFrontier LogicalClock => _root.Clock;

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

    /// <summary>
    /// As <see cref="Input{T}(T, Func{T, T, T})"/>, but also registers the input
    /// under a logical <paramref name="name"/>. Naming is inert for a plain
    /// single circuit; it exists so <see cref="ParallelCircuit"/> can reach each
    /// worker's copy of this input by name (see
    /// <see cref="ParallelCircuit.WorkerInput{T}"/>). Duplicate names within one
    /// circuit are a build error. (A separate overload, not an optional
    /// parameter, so the original arity stays stable for reflection-based
    /// callers such as the SQL compiler.)
    /// </summary>
    public (InputHandle<T> Handle, Stream<T> Stream) Input<T>(T zero, Func<T, T, T> merge, string name)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        var port = Input(zero, merge);
        _root.RegisterNamedPort(name, port.Handle);
        return port;
    }

    /// <summary>Create an output handle observing the given stream.</summary>
    public OutputHandle<T> Output<T>(Stream<T> source)
    {
        ArgumentNullException.ThrowIfNull(source);
        return new OutputHandle<T>(source);
    }

    /// <summary>
    /// As <see cref="Output{T}(Stream{T})"/>, but also registers the output
    /// under a logical <paramref name="name"/>; see the named
    /// <see cref="Input{T}(T, Func{T, T, T}, string)"/> overload. Reachable per
    /// worker via <see cref="ParallelCircuit.WorkerOutput{T}"/>.
    /// </summary>
    public OutputHandle<T> Output<T>(Stream<T> source, string name)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        var handle = Output(source);
        _root.RegisterNamedPort(name, handle);
        return handle;
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

    /// <summary>
    /// The all-to-all shuffle: re-partition a sharded Z-set stream by
    /// <c>hash(key) % W</c> so that, downstream, every row for a given key is
    /// co-located on one worker — what a key-sensitive operator (join,
    /// group-by, distinct) needs when its key differs from the current
    /// sharding. <paramref name="partition"/> maps a key to a hash; the operator
    /// takes it modulo the worker count.
    /// </summary>
    /// <remarks>
    /// Outside a <see cref="ParallelCircuit"/>, or with a single worker, there
    /// is nothing to shuffle — every key already lives on the only shard — so
    /// this returns <paramref name="input"/> unchanged and adds no operator,
    /// keeping the single-threaded path free of exchange overhead.
    /// </remarks>
    public Stream<ZSet<TKey, TWeight>> Exchange<TKey, TWeight>(
        Stream<ZSet<TKey, TWeight>> input,
        Func<TKey, int> partition)
        where TKey : notnull
        where TWeight : struct, IZRing<TWeight>
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(partition);

        if (_parallel is null || _parallel.Workers <= 1)
        {
            return input;
        }

        var workers = _parallel.Workers;
        var coordinator = _parallel.NextCoordinator(
            () => new ExchangeCoordinator<List<KeyValuePair<TKey, TWeight>>>(workers));
        var output = new Stream<ZSet<TKey, TWeight>>(ZSet<TKey, TWeight>.Empty);
        _root.AddOperator(new ExchangeOp<TKey, TWeight>(
            input, output, partition, coordinator, _parallel.WorkerId, _parallel.Abort));
        return output;
    }

    /// <summary>
    /// Fused <see cref="Exchange{TKey,TWeight}"/> + <c>GroupProject(keyOf,
    /// identity)</c>: re-partition a sharded Z-set by <paramref name="partition"/>
    /// and group the result by <paramref name="keyOf"/> into an indexed Z-set,
    /// ready for a join or aggregate — without materializing the intermediate
    /// flat Z-set the two-operator form would. The row becomes the inner value.
    /// </summary>
    /// <remarks>
    /// Outside a <see cref="ParallelCircuit"/>, or with a single worker, there is
    /// nothing to shuffle, so this degrades to a plain
    /// <see cref="StatefulOperators.GroupProject{TKey,TRow,TValue,TWeight}"/> —
    /// keeping the single-threaded path byte-for-byte what it was before the fused
    /// operator existed.
    /// </remarks>
    public Stream<IndexedZSet<TKey, TRow, TWeight>> ExchangeIndex<TKey, TRow, TWeight>(
        Stream<ZSet<TRow, TWeight>> input,
        Func<TRow, int> partition,
        Func<TRow, TKey> keyOf)
        where TKey : notnull
        where TRow : notnull
        where TWeight : struct, IZRing<TWeight>
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(partition);
        ArgumentNullException.ThrowIfNull(keyOf);

        if (_parallel is null || _parallel.Workers <= 1)
        {
            return this.GroupProject(input, keyOf, static row => row);
        }

        var workers = _parallel.Workers;
        var coordinator = _parallel.NextCoordinator(
            () => new ExchangeCoordinator<List<KeyValuePair<TRow, TWeight>>>(workers));
        var output = new Stream<IndexedZSet<TKey, TRow, TWeight>>(
            IndexedZSet<TKey, TRow, TWeight>.Empty);
        _root.AddOperator(new ExchangeIndexOp<TKey, TRow, TWeight>(
            input, output, partition, keyOf, coordinator, _parallel.WorkerId, _parallel.Abort));
        return output;
    }
}
