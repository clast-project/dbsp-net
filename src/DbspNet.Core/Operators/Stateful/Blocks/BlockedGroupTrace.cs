// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Stateful.Blocks;

/// <summary>
/// An outer join's side, or an aggregate's input, held as a <see cref="BlockedIndexedTrace"/>
/// (docs/design-flat-trace-state.md §3.3–§3.4). <see cref="GroupFor"/> reads the key from every level and sums its entries by value, so
/// the group is consolidated — what an outer join's match-presence test needs; the entries it
/// returns are decoded rows, which the join's combine takes as they are.
/// </summary>
internal sealed class BlockedGroupTrace : IGroupTrace<StructuralRow, StructuralRow, Z64>
{
    public BlockedGroupTrace(ITraceBlockFormat format) =>
        Trace = new BlockedIndexedTrace(format, TraceBlocksMode.FreezeEntries, blockEntries: TraceBlocksMode.BlockEntries);

    internal BlockedIndexedTrace Trace { get; }

    public ZSet<StructuralRow, Z64> GroupFor(StructuralRow key)
    {
        var visitor = new Sum { Builder = null };
        Trace.Probe(key, ref visitor);
        return visitor.Builder?.Build() ?? ZSet<StructuralRow, Z64>.Empty;
    }

    public void Integrate(IndexedZSet<StructuralRow, StructuralRow, Z64> delta) => Trace.Integrate(delta);

    public IReadOnlyList<StructuralRow>? DropKeysBelow(long threshold, Func<StructuralRow, long> monotoneKey)
    {
        Trace.DropKeysBelow(threshold, monotoneKey);
        return null;
    }

    /// <summary>Always: a blocked trace never decodes its whole state to rebuild caches.</summary>
    public bool IsLazy => true;

    public int GroupCount => Trace.GroupCount;

    public void Idle() => Trace.Freeze();

    public ValueTask SaveAsync(
        IIndexedZSetTraceCodec<StructuralRow, StructuralRow, Z64> codec, ISnapshotWriter writer, string file, CancellationToken cancellationToken) =>
        codec is IBlockedTraceCodec blocked
            ? blocked.SaveBlockedAsync(writer, file, Trace, cancellationToken)
            : codec.SaveAsync(writer, file, Trace.Materialize(), cancellationToken);

    public async ValueTask LoadAsync(
        IIndexedZSetTraceCodec<StructuralRow, StructuralRow, Z64> codec, ISnapshotReader reader, string file, CancellationToken cancellationToken)
    {
        if (codec is IBlockedTraceCodec blocked)
        {
            await blocked.LoadBlockedAsync(reader, file, Trace, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            Trace.Integrate(await codec.LoadAsync(reader, file, cancellationToken).ConfigureAwait(false));
        }
    }

    // Sums the key's entries across levels; the builder drops what cancels.
    private struct Sum : ITraceVisitor
    {
        public ZSetBuilder<StructuralRow, Z64>? Builder;

        public void Row(StructuralRow value, Z64 weight) => (Builder ??= new()).Add(value, weight);

        public void Entry(ITraceBlock block, int index, Z64 weight) => (Builder ??= new()).Add(block.Value(index), weight);
    }
}
