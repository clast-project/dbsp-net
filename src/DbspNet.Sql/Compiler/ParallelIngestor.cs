// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Sql.Plan;

namespace DbspNet.Sql.Compiler;

/// <summary>
/// Drives a single table's input into a <see cref="ParallelCircuit"/>'s W
/// replicas. Hides the type of the emitted row so <see cref="TypedTableInput"/>
/// can stay non-generic.
/// </summary>
internal interface ITableIngestor
{
    /// <summary>Push a batch of raw boundary rows, sharded across the replicas.</summary>
    void Push(IReadOnlyList<(object?[] Values, long Weight)> raw);

    /// <summary>Push a single raw boundary row (the <c>Insert</c> / <c>Delete</c> path).</summary>
    void PushOne(object?[] values, long weight);
}

/// <summary>
/// The data-parallel ingest path for one table: encode raw boundary rows to the
/// emitted struct, hash-shard by <c>partition(row) % W</c>, and queue each shard
/// on its replica's <see cref="InputHandle{T}"/>. The expensive boundary encode
/// runs on the <see cref="ParallelCircuit"/>'s worker threads rather than the
/// calling thread, so ingest scales with W instead of being the serial bottleneck.
/// </summary>
/// <remarks>
/// <para>
/// A large batch is ingested in two phases over
/// <see cref="ParallelCircuit.RunDataParallel"/>:
/// <list type="number">
///   <item><b>Encode</b> — worker <c>w</c> encodes a disjoint chunk of the batch,
///     recording each row's emitted struct, weight, and target shard
///     (<c>partition % W</c>). Each row is encoded and hashed exactly once;
///     workers write disjoint indices, so there is no contention.</item>
///   <item><b>Scatter</b> — after a barrier, worker <c>w</c> scans the encoded
///     array and builds its own shard from the rows whose target is <c>w</c>,
///     then pushes it to its replica. Only integer comparisons and the Z-set
///     build run here; no encoding is repeated.</item>
/// </list>
/// The per-shard contents are byte-identical to the serial split, so the result
/// and the recovery-stable worker placement are unchanged.
/// </para>
/// <para>
/// Small batches, single rows, and <c>W == 1</c> take the serial path: the
/// parallel hand-off costs more than it saves below the threshold, and this is a
/// throughput optimization (one big push), not a per-row latency one.
/// </para>
/// </remarks>
internal sealed class ParallelIngestor<TRow> : ITableIngestor
    where TRow : notnull
{
    /// <summary>Batches smaller than this take the serial path (see remarks).</summary>
    internal const int ParallelThreshold = 4096;

    private readonly ParallelCircuit _circuit;
    private readonly int _workers;
    private readonly Schema _schema;
    private readonly Func<object?[], object> _factory;
    private readonly Func<TRow, int> _partition;
    private readonly InputHandle<ZSet<TRow, Z64>>[] _handles;

    public ParallelIngestor(
        ParallelCircuit circuit,
        string name,
        Schema schema,
        Func<object?[], object> factory,
        Func<TRow, int> partition)
    {
        _circuit = circuit;
        _workers = circuit.Workers;
        _schema = schema;
        _factory = factory;
        _partition = partition;
        _handles = new InputHandle<ZSet<TRow, Z64>>[_workers];
        for (var w = 0; w < _workers; w++)
        {
            _handles[w] = circuit.WorkerInput<ZSet<TRow, Z64>>(name, w);
        }
    }

    public void Push(IReadOnlyList<(object?[] Values, long Weight)> raw)
    {
        ArgumentNullException.ThrowIfNull(raw);
        var n = raw.Count;
        if (n == 0)
        {
            return;
        }

        if (_workers == 1 || n < ParallelThreshold)
        {
            PushSerial(raw);
            return;
        }

        PushParallel(raw, n);
    }

    public void PushOne(object?[] values, long weight)
    {
        var row = Encode(values);
        var shard = NonNegativeShard(_partition(row));
        var builder = new ZSetBuilder<TRow, Z64>();
        builder.Add(row, new Z64(weight));
        _handles[shard].Push(builder.Build());
    }

    /// <summary>Encode one raw boundary row to the emitted struct, validating arity.</summary>
    private TRow Encode(object?[] values)
    {
        if (values.Length != _schema.Count)
        {
            throw new ArgumentException(
                $"row arity {values.Length} does not match schema arity {_schema.Count}",
                nameof(values));
        }

        return (TRow)_factory(BoundaryEncoder.Encode(_schema, values));
    }

    /// <summary>Serial split: encode + bucket into W shards on the calling thread.</summary>
    private void PushSerial(IReadOnlyList<(object?[] Values, long Weight)> raw)
    {
        var workers = _workers;
        var buckets = new ZSetBuilder<TRow, Z64>[workers];
        for (var j = 0; j < workers; j++)
        {
            buckets[j] = new ZSetBuilder<TRow, Z64>();
        }

        foreach (var (values, weight) in raw)
        {
            var row = Encode(values);
            buckets[NonNegativeShard(_partition(row))].Add(row, new Z64(weight));
        }

        for (var j = 0; j < workers; j++)
        {
            var shard = buckets[j].Build();
            if (!shard.IsEmpty)
            {
                _handles[j].Push(shard);
            }
        }
    }

    /// <summary>Two-phase parallel split over the replica worker threads.</summary>
    private void PushParallel(IReadOnlyList<(object?[] Values, long Weight)> raw, int n)
    {
        var workers = _workers;
        var encoded = new TRow[n];
        var weights = new long[n];
        var target = new int[n];

        _circuit.RunDataParallel((worker, sync) =>
        {
            // Phase 1 — encode this worker's disjoint chunk (encode + hash once).
            var lo = (int)((long)n * worker / workers);
            var hi = (int)((long)n * (worker + 1) / workers);
            for (var i = lo; i < hi; i++)
            {
                var row = Encode(raw[i].Values);
                encoded[i] = row;
                weights[i] = raw[i].Weight;
                target[i] = NonNegativeShard(_partition(row));
            }

            sync.Sync();

            // Phase 2 — build and push this worker's shard (no re-encoding).
            var builder = new ZSetBuilder<TRow, Z64>();
            for (var i = 0; i < n; i++)
            {
                if (target[i] == worker)
                {
                    builder.Add(encoded[i], new Z64(weights[i]));
                }
            }

            var shard = builder.Build();
            if (!shard.IsEmpty)
            {
                _handles[worker].Push(shard);
            }
        });
    }

    /// <summary>Map a (possibly negative) partition hash to a shard in <c>[0, W)</c>.</summary>
    private int NonNegativeShard(int hash) => ((hash % _workers) + _workers) % _workers;
}
