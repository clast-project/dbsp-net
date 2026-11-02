// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Stateful.Blocks;

/// <summary>
/// DISTINCT's or a stored view's Z-set held as blocks (docs/design-flat-trace-state.md §3.7): a
/// <see cref="BlockedIndexedTrace"/> whose keys are the rows and whose value is empty. Each tick's
/// delta is a managed level as it arrived; frozen runs are ordinary Z-set run blocks, so a
/// checkpoint is a Z-set chain either kind of trace restores.
/// </summary>
/// <remarks>
/// <para><b>Count</b> is formed on first read, then kept by probing each delta row's weight before
/// it is integrated — paid only once someone has asked.</para>
/// <para><b>Current</b> is materialised on read and held weakly: a view read once per batch
/// does not keep a decoded copy alive.</para>
/// </remarks>
internal sealed class BlockedZSetTrace : IRowTrace<StructuralRow, Z64>
{
    private readonly WeakReference<ZSet<StructuralRow, Z64>?> _current = new(null);
    private int _count = -1;

    // The trace keeps each delta as a level; a pooled one is reused next tick, so copy it.
    private readonly bool _cloneDeltas = DeltaPoolMode.Enabled;

    public BlockedZSetTrace(ITraceBlockFormat format) =>
        Trace = new BlockedIndexedTrace(format, TraceBlocksMode.FreezeEntries, blockEntries: TraceBlocksMode.BlockEntries);

    internal BlockedIndexedTrace Trace { get; }

    public int Count => _count >= 0 ? _count : _count = Current.Count;

    public long RetainedRows => Count;

    /// <summary>Never: restored blocks are read on first probe, but nothing is promoted.</summary>
    public bool IsCold => false;

    public ZSet<StructuralRow, Z64> Current
    {
        get
        {
            if (_current.TryGetTarget(out var cached) && cached is not null)
            {
                return cached;
            }

            var formed = Trace.MaterializeRows();
            _current.SetTarget(formed);
            return formed;
        }
    }

    public Z64 WeightOf(StructuralRow key)
    {
        var sum = new Sum();
        Trace.Probe(key, ref sum);
        return new Z64(sum.Weight);
    }

    public void Integrate(ZSet<StructuralRow, Z64> delta)
    {
        ArgumentNullException.ThrowIfNull(delta);
        if (delta.IsEmpty)
        {
            return;
        }

        if (_count >= 0)
        {
            foreach (var (row, dw) in delta)
            {
                var before = WeightOf(row).Value;
                var after = before + dw.Value;
                if (before == 0 && after != 0)
                {
                    _count++;
                }
                else if (before != 0 && after == 0)
                {
                    _count--;
                }
            }
        }

        _current.SetTarget(null);
        Trace.IntegrateRows(_cloneDeltas ? delta.Clone() : delta);
    }

    public IEnumerable<KeyValuePair<StructuralRow, Z64>> EnumerateWhole() => Current;

    public int DropKeysBelow(long threshold, Func<StructuralRow, long> monotoneKey)
    {
        Trace.DropKeysBelow(threshold, monotoneKey);
        _current.SetTarget(null);
        _count = -1;
        return 0;
    }

    public void Idle() => Trace.Freeze();

    public ValueTask SaveAsync(
        IZSetTraceCodec<StructuralRow, Z64> codec, ISnapshotWriter writer, string file, CancellationToken cancellationToken) =>
        codec is IBlockedTraceCodec blocked
            ? blocked.SaveBlockedAsync(writer, file, Trace, cancellationToken)
            : codec.SaveAsync(writer, file, Current, cancellationToken);

    public async ValueTask LoadAsync(
        IZSetTraceCodec<StructuralRow, Z64> codec, ISnapshotReader reader, string file, bool lazy, CancellationToken cancellationToken)
    {
        _count = -1;
        _current.SetTarget(null);
        if (codec is IBlockedTraceCodec blocked)
        {
            await blocked.LoadBlockedAsync(reader, file, Trace, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            Trace.IntegrateRows(await codec.LoadAsync(reader, file, cancellationToken).ConfigureAwait(false));
        }
    }

    // Sums a row's weight across levels.
    private struct Sum : ITraceVisitor
    {
        public long Weight;

        public void Row(StructuralRow value, Z64 weight) => Weight += weight.Value;

        public void Entry(ITraceBlock block, int index, Z64 weight) => Weight += weight.Value;
    }
}
