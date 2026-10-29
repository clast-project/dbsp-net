// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Buffers.Binary;
using System.Runtime.InteropServices;
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful.Aggregators;

namespace DbspNet.Core.Operators.Stateful;

/// <summary>
/// Trace-free per-group aggregate for <see cref="ILinearAggregator{TValue,TOut}"/>s
/// (docs/design-state-representation.md §4G). Where
/// <see cref="IncrementalAggregateOp{TKey,TValue,TOut}"/> integrates every input row
/// so aggregators can probe post-delta weights, this operator keeps, per group, only
/// the summed row weight, the aggregator's linear state and the last emitted value.
/// </summary>
/// <remarks>
/// <para><b>Group lifetime.</b> A group is present (emits a value) while its summed
/// row weight is non-zero — the same linear gate as the typed composite's
/// <c>SumWeights() != 0</c>. Its state is dropped when the weight is zero <i>and</i>
/// the aggregator state has cancelled to zero, so a transiently cancelling (but not
/// empty) group keeps its sums exactly as the trace-backed operator keeps them.</para>
/// <para><b>Persistence.</b> There is no trace to rebuild from, so each group's
/// (weight, state) is written verbatim as a keyed blob (<c>lagg.arrows</c>).
/// A snapshot written by the trace-backed operator it replaces (see
/// <see cref="IFingerprintAlias"/>) restores by folding the saved trace.</para>
/// </remarks>
internal sealed class LinearAggregateOp<TKey, TValue, TOut> : IOperator, ISnapshotable, IIntrospectable, IFingerprintAlias
    where TKey : notnull
    where TValue : notnull
    where TOut : notnull
{
    internal const string BlobFileName = "lagg.arrows";

    private struct Group
    {
        public long Weight;
        public object? State;
        public TOut Last;
        public bool HasLast;
    }

    private readonly Stream<IndexedZSet<TKey, TValue, Z64>> _input;
    private readonly Stream<ZSet<(TKey Key, TOut Value), Z64>> _output;
    private readonly ILinearAggregator<TValue, TOut> _aggregator;
    private readonly Dictionary<TKey, Group> _groups = new();
    private readonly IIndexedZSetTraceCodec<TKey, TValue, Z64>? _snapshotCodec;
    private readonly IFrontier? _frontier;
    private readonly Func<TKey, long>? _monotoneKey;
    private int _lastOutputSize;
    private long _lastGcFrontier = long.MinValue;
    private long _gcDropped;

    public LinearAggregateOp(
        Stream<IndexedZSet<TKey, TValue, Z64>> input,
        Stream<ZSet<(TKey Key, TOut Value), Z64>> output,
        ILinearAggregator<TValue, TOut> aggregator,
        IIndexedZSetTraceCodec<TKey, TValue, Z64>? snapshotCodec = null,
        IFrontier? frontier = null,
        Func<TKey, long>? monotoneKey = null)
    {
        _input = input;
        _output = output;
        _aggregator = aggregator;
        _snapshotCodec = snapshotCodec;
        _frontier = frontier;
        _monotoneKey = monotoneKey;
    }

    /// <summary>Number of groups currently holding state.</summary>
    internal int RetainedGroupCount => _groups.Count;

    public Type FingerprintAliasType => typeof(IncrementalAggregateOp<TKey, TValue, TOut>);

    public void Step()
    {
        var delta = _input.Current;
        if (delta.IsEmpty)
        {
            _output.SetCurrent(ZSet<(TKey, TOut), Z64>.Empty);
            CollectGarbage();
            return;
        }

        var builder = new ZSetBuilder<(TKey, TOut), Z64>(_lastOutputSize);
        foreach (var (key, groupDelta) in delta)
        {
            long dw = 0;
            foreach (var (_, w) in groupDelta)
            {
                dw = checked(dw + w.Value);
            }

            ref var g = ref CollectionsMarshal.GetValueRefOrAddDefault(_groups, key, out _);
            g.Weight = checked(g.Weight + dw);
            _aggregator.Fold(ref g.State, groupDelta);

            var hadOld = g.HasLast;
            var old = g.Last;
            var present = g.Weight != 0;
            var now = present ? _aggregator.Result(g.State) : default!;

            if (present)
            {
                g.Last = now;
                g.HasLast = true;
            }
            else
            {
                g.Last = default!;
                g.HasLast = false;
                if (_aggregator.IsZero(g.State))
                {
                    _groups.Remove(key);   // g is dead past this point
                }
            }

            if (hadOld && present && EqualityComparer<TOut>.Default.Equals(old, now))
            {
                continue;
            }

            if (hadOld)
            {
                builder.Add((key, old), new Z64(-1));
            }

            if (present)
            {
                builder.Add((key, now), new Z64(1));
            }
        }

        var result = builder.Build();
        _lastOutputSize = result.Count;
        _output.SetCurrent(result);
        CollectGarbage();
    }

    private void CollectGarbage()
    {
        if (_frontier is null || _monotoneKey is null)
        {
            return;
        }

        var frontier = _frontier.Value;
        if (frontier == long.MinValue || frontier <= _lastGcFrontier)
        {
            return;
        }

        _lastGcFrontier = frontier;
        List<TKey>? drop = null;
        foreach (var key in _groups.Keys)
        {
            if (_monotoneKey(key) < frontier)
            {
                (drop ??= []).Add(key);
            }
        }

        if (drop is null)
        {
            return;
        }

        foreach (var key in drop)
        {
            _groups.Remove(key);
            _gcDropped++;
        }
    }

    // ---- persistence ----

    public string SchemaFingerprint => _snapshotCodec?.SchemaFingerprint ?? string.Empty;

    public async ValueTask SaveAsync(ISnapshotWriter writer, CancellationToken cancellationToken = default)
    {
        var codec = RequireCodec();
        var entries = new List<(TKey Key, byte[] Blob)>(_groups.Count);
        var scratch = new System.Buffers.ArrayBufferWriter<byte>();
        foreach (var (key, g) in _groups)
        {
            scratch.Clear();
            BinaryPrimitives.WriteInt64LittleEndian(scratch.GetSpan(8), g.Weight);
            scratch.Advance(8);
            _aggregator.WriteState(scratch, g.State);
            entries.Add((key, scratch.WrittenSpan.ToArray()));
        }

        await codec.SaveKeyedBlobsAsync(writer, BlobFileName, entries, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask LoadAsync(ISnapshotReader reader, CancellationToken cancellationToken = default)
    {
        var codec = RequireCodec();
        _groups.Clear();
        if (!await reader.ExistsAsync(BlobFileName, cancellationToken).ConfigureAwait(false)
            && await reader.ExistsAsync("trace.arrows", cancellationToken).ConfigureAwait(false))
        {
            // A snapshot written by IncrementalAggregateOp (restored through the
            // fingerprint alias): fold its integrated input trace.
            var trace = await codec.LoadAsync(reader, "trace.arrows", cancellationToken).ConfigureAwait(false);
            foreach (var (key, rows) in trace)
            {
                var g = default(Group);
                foreach (var (_, w) in rows)
                {
                    g.Weight = checked(g.Weight + w.Value);
                }

                _aggregator.Fold(ref g.State, rows);
                Restore(key, g);
            }

            return;
        }

        var blobs = await codec.LoadKeyedBlobsAsync(reader, BlobFileName, cancellationToken).ConfigureAwait(false);
        foreach (var (key, blob) in blobs)
        {
            var g = default(Group);
            g.Weight = BinaryPrimitives.ReadInt64LittleEndian(blob);
            g.State = _aggregator.ReadState(blob.AsSpan(8));
            Restore(key, g);
        }
    }

    private void Restore(TKey key, Group g)
    {
        if (g.Weight != 0)
        {
            g.Last = _aggregator.Result(g.State);
            g.HasLast = true;
        }
        else if (_aggregator.IsZero(g.State))
        {
            return;
        }

        _groups[key] = g;
    }

    private IIndexedZSetTraceCodec<TKey, TValue, Z64> RequireCodec()
    {
        if (_snapshotCodec is null || !_snapshotCodec.SupportsKeyedBlobs)
        {
            throw new NotSupportedException(
                "LinearAggregateOp persists per-group state as keyed blobs; it needs a snapshot codec " +
                "that supports them (SupportsKeyedBlobs).");
        }

        return _snapshotCodec;
    }

    // ---- introspection ----

    public string MetricName => "LinearAggregate";

    public long RetainedRows => _groups.Count;

    public long LastOutputRows => _output.Current.Count;

    public long? GcFrontier => Metric.Frontier(_frontier);

    public long GcDroppedTotal => _gcDropped;
}
