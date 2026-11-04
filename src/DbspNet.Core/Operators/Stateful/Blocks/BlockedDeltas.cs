// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections;
using System.Runtime.CompilerServices;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Stateful.Blocks;

/// <summary>
/// In-step delta spilling (docs/design-bounded-memory.md §5.14, D1–D3): where operators get a delta
/// builder for an output stream. A stream the compiler gave a block format
/// (<c>Stream.DeltaFormat</c>, set only while <c>DBSPNET_STEP_SPILL_ROWS</c> is on) gets a builder
/// that freezes into block runs past the threshold; every other stream gets the plain builder, so
/// below the threshold, and with the knob off, a delta is exactly what it was.
/// </summary>
internal static class DeltaSpill
{
    /// <summary>Deltas built block backed, and their rows, since start (diagnostics, tests).</summary>
    internal static long BlockedBuilds, BlockedRows;

    /// <summary>A builder for a structural delta of <paramref name="format"/> (null: plain).</summary>
    public static ZSetBuilder<StructuralRow, Z64> Rows(object? format, int capacity)
    {
        if (format is null || SpillSettings.StepSpillRows is not { } rows)
        {
            return new ZSetBuilder<StructuralRow, Z64>(capacity);
        }

        return new ZSetBuilder<StructuralRow, Z64>(Store((ITraceBlockFormat)format, rows, alwaysBlocked: rows == 0, capacity));
    }

    internal static SpillingRows Store(ITraceBlockFormat format, long rows, bool alwaysBlocked, int capacity) =>
        new(format, rows == 0 ? SpillSettings.ForcedChunkRows : (int)Math.Min(rows, int.MaxValue), alwaysBlocked, capacity);

    /// <summary>As <see cref="Rows"/> for a generic operator: plain unless the rows are structural.</summary>
    public static ZSetBuilder<TKey, TWeight> For<TKey, TWeight>(object? format, int capacity)
        where TKey : notnull
        where TWeight : struct, IZRing<TWeight>
    {
        if (format is null || typeof(TKey) != typeof(StructuralRow) || typeof(TWeight) != typeof(Z64))
        {
            return new ZSetBuilder<TKey, TWeight>(capacity);
        }

        return Unsafe.As<ZSetBuilder<TKey, TWeight>>(Rows(format, capacity));
    }
}

/// <summary>
/// D2: a delta builder's store that consolidates in a managed map up to a threshold, then freezes the
/// map into a sorted block run of a private Z-set <see cref="BlockedIndexedTrace"/> and starts a new
/// map. The trace's ordinary freeze and merge make the runs (and its spiller, S1, writes the large
/// ones to files the block cache can evict), so a producer's managed peak is one chunk, not its output.
/// </summary>
internal sealed class SpillingRows : ZSetSpillingStore<StructuralRow, Z64>
{
    private readonly ITraceBlockFormat _format;
    private readonly int _chunkRows;
    private readonly bool _alwaysBlocked;
    private readonly int _capacity;
    private ZSetBuilder<StructuralRow, Z64> _chunk;
    private BlockedIndexedTrace? _trace;
    private long _frozen;

    /// <param name="chunkRows">Managed rows at which a chunk freezes into a run.</param>
    /// <param name="alwaysBlocked">Build block backed even when nothing froze (the forced mode, and a
    /// sum with a block-backed set).</param>
    public SpillingRows(ITraceBlockFormat format, int chunkRows, bool alwaysBlocked, int capacity)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(chunkRows, 1);
        _format = format;
        _alwaysBlocked = alwaysBlocked;
        _chunkRows = chunkRows;

        // A capacity hint past the threshold would pre-size the very map this bounds.
        _capacity = Math.Clamp(capacity, 0, _chunkRows);
        _chunk = new ZSetBuilder<StructuralRow, Z64>(_capacity);
    }

    public override int Count => (int)Math.Min(_chunk.Count + _frozen, int.MaxValue);

    public override void Add(StructuralRow key, Z64 weight)
    {
        var m = StepSpillProfile.Enabled ? StepSpillProfile.Begin() : default;
        _chunk.Add(key, weight);
        if (_chunk.Count >= _chunkRows)
        {
            FreezeChunk();
        }

        if (StepSpillProfile.Enabled)
        {
            StepSpillProfile.End(StepSpillProfile.ChunkAdd, m);
        }
    }

    private void FreezeChunk()
    {
        var m = StepSpillProfile.Enabled ? StepSpillProfile.Begin() : default;
        FreezeChunkCore();
        if (StepSpillProfile.Enabled)
        {
            StepSpillProfile.End(StepSpillProfile.ChunkFreeze, m);
        }
    }

    private void FreezeChunkCore()
    {
        var part = _chunk.Build();
        _chunk = new ZSetBuilder<StructuralRow, Z64>(_capacity);
        if (part.IsEmpty)
        {
            return;
        }

        _trace ??= new BlockedIndexedTrace(_format, freezeEntries: int.MaxValue, maxManaged: int.MaxValue, blockEntries: TraceBlocksMode.BlockEntries);
        _trace.IntegrateRows(part);
        _trace.Freeze();
        _frozen += part.Count;
    }

    public override ZSet<StructuralRow, Z64> Build()
    {
        if (_trace is null && !_alwaysBlocked)
        {
            return _chunk.Build();
        }

        FreezeChunk();
        var trace = _trace;
        _trace = null;
        _frozen = 0;
        if (trace is null)
        {
            return ZSet<StructuralRow, Z64>.Empty;
        }

        var mm = StepSpillProfile.Enabled ? StepSpillProfile.Begin() : default;
        trace.MergeAllRuns();
        if (StepSpillProfile.Enabled)
        {
            StepSpillProfile.End(StepSpillProfile.FinalMerge, mm);
        }

        if (trace.Runs.Count == 0)
        {
            return ZSet<StructuralRow, Z64>.Empty; // every entry cancelled
        }

        var built = new BlockedRows(trace, _format);
        Interlocked.Increment(ref DeltaSpill.BlockedBuilds);
        Interlocked.Add(ref DeltaSpill.BlockedRows, built.Count);
        return new ZSet<StructuralRow, Z64>(built);
    }

    public override void Reset()
    {
        _chunk = new ZSetBuilder<StructuralRow, Z64>(_capacity);
        _trace = null;
        _frozen = 0;
    }
}

/// <summary>
/// D1: a Z-set held as one consolidated, hash-ordered run of blocks (a frozen delta, §5.14). Count is
/// the run's entries; a lookup probes by the key's hash; enumeration decodes a block at a time (each
/// row's strings copied, so a row a consumer keeps does not pin the block's bytes). The blocks may be
/// file-backed once spilled, read back through the block cache.
/// </summary>
internal sealed class BlockedRows : ZSetExternalStore<StructuralRow, Z64>
{
    private readonly BlockedIndexedTrace _trace;
    private readonly ITraceBlockFormat _format;
    private readonly int _count;

    public BlockedRows(BlockedIndexedTrace trace, ITraceBlockFormat format)
    {
        _trace = trace;
        _format = format;
        _count = (int)trace.Entries;
    }

    /// <summary>The run (tests, census).</summary>
    internal BlockedIndexedTrace Trace => _trace;

    public override int Count => _count;

    public override long ResidentBytes
    {
        get
        {
            long bytes = 0;
            foreach (var run in _trace.Runs)
            {
                foreach (var b in run.Blocks)
                {
                    bytes += b.Resident ? b.Hashes.Length * 64L : 0; // rough: the census only ranks
                }
            }

            return bytes;
        }
    }

    public override bool TryGetWeight(StructuralRow key, out Z64 weight)
    {
        var m = StepSpillProfile.Enabled ? StepSpillProfile.Begin() : default;
        var sum = new Sum();
        _trace.Probe(key, ref sum);
        weight = new Z64(sum.Weight);
        if (StepSpillProfile.Enabled)
        {
            StepSpillProfile.End(StepSpillProfile.Probe, m);
        }

        return sum.Found && sum.Weight != 0;
    }

    public override IEnumerator<KeyValuePair<StructuralRow, Z64>> GetEnumerator()
    {
        if (StepSpillProfile.Enabled)
        {
            Interlocked.Increment(ref StepSpillProfile.Enumerations);
        }

        return new Cursor(_trace.Runs);
    }

    public override ZSetBuilder<StructuralRow, Z64> NewBuilder(int capacity) => DeltaSpill.Rows(_format, capacity);

    /// <summary>A sum with a block-backed set is a merge of the two runs: the other side, if managed,
    /// is frozen into a run first (it is the small one in practice: a union's null-padded side).</summary>
    public override ZSet<StructuralRow, Z64>? Plus(ZSet<StructuralRow, Z64> other)
    {
        var m = StepSpillProfile.Enabled ? StepSpillProfile.Begin() : default;
        var result = PlusCore(other);
        if (StepSpillProfile.Enabled)
        {
            StepSpillProfile.End(StepSpillProfile.PlusMerge, m);
        }

        return result;
    }

    private ZSet<StructuralRow, Z64>? PlusCore(ZSet<StructuralRow, Z64> other)
    {
        if (other.IsEmpty)
        {
            return new ZSet<StructuralRow, Z64>(this);
        }

        BlockedIndexedTrace theirs;
        if (other.External is BlockedRows blocked)
        {
            theirs = blocked._trace;
        }
        else if (other.IsExternal)
        {
            return null;
        }
        else
        {
            var rows = SpillSettings.StepSpillRows ?? 0;
            var store = DeltaSpill.Store(_format, rows, alwaysBlocked: true, other.Count);
            foreach (var (k, w) in other)
            {
                store.Add(k, w);
            }

            if (store.Build().External is not BlockedRows frozen)
            {
                return new ZSet<StructuralRow, Z64>(this); // every entry cancelled
            }

            theirs = frozen._trace;
        }

        var sum = new BlockedIndexedTrace(_format, freezeEntries: int.MaxValue, maxManaged: int.MaxValue, blockEntries: TraceBlocksMode.BlockEntries);
        foreach (var run in theirs.Runs.Reverse())
        {
            sum.AdoptRun(run.Blocks, run.Entries, run.Persisted);
        }

        foreach (var run in _trace.Runs.Reverse())
        {
            sum.AdoptRun(run.Blocks, run.Entries, run.Persisted);
        }

        // The adopted runs are new Run objects over the same blocks, but a spilled file lives as long as
        // its ORIGINAL run (the spiller tracks that weakly): keep both sources reachable until the merge
        // has read them, or a sweep can delete a file mid-merge (found by the forced suite under S1).
        sum.MergeAllRuns();
        GC.KeepAlive(theirs);
        GC.KeepAlive(_trace);
        if (sum.Runs.Count == 0)
        {
            return ZSet<StructuralRow, Z64>.Empty;
        }

        var built = new BlockedRows(sum, _format);
        Interlocked.Increment(ref DeltaSpill.BlockedBuilds);
        Interlocked.Add(ref DeltaSpill.BlockedRows, built.Count);
        return new ZSet<StructuralRow, Z64>(built);
    }

    private struct Sum : ITraceVisitor
    {
        public long Weight;
        public bool Found;

        public void Row(StructuralRow value, Z64 weight)
        {
            Weight += weight.Value;
            Found = true;
        }

        public void Entry(ITraceBlock block, int index, Z64 weight)
        {
            Weight += weight.Value;
            Found = true;
        }
    }

    // Walks the runs' blocks in order (one run once built); a block's body is held while on it.
    private sealed class Cursor(IReadOnlyList<BlockedIndexedTrace.Run> runs) : IEnumerator<KeyValuePair<StructuralRow, Z64>>
    {
        private int _run;
        private int _block = -1;
        private int _index;
        private ITraceBlock? _body;
        private KeyValuePair<StructuralRow, Z64> _current;

        public KeyValuePair<StructuralRow, Z64> Current => _current;

        object IEnumerator.Current => _current;

        public bool MoveNext()
        {
            if (!StepSpillProfile.Enabled)
            {
                return MoveNextCore();
            }

            var m = StepSpillProfile.Begin();
            var more = MoveNextCore();
            StepSpillProfile.End(StepSpillProfile.Enumerate, m);
            if (more)
            {
                Interlocked.Increment(ref StepSpillProfile.RowsDecoded);
            }

            return more;
        }

        private bool MoveNextCore()
        {
            while (true)
            {
                if (_body is not null && ++_index < _body.Count)
                {
                    _current = new(_body.Key(_index), new Z64(_body.Weight(_index)));
                    return true;
                }

                if (!NextBlock())
                {
                    _body = null;
                    return false;
                }

                _index = -1;
            }
        }

        private bool NextBlock()
        {
            while (_run < runs.Count)
            {
                var blocks = runs[_run].Blocks;
                if (++_block < blocks.Length)
                {
                    _body = blocks[_block].Body;
                    return true;
                }

                _run++;
                _block = -1;
            }

            return false;
        }

        public void Reset() => throw new NotSupportedException();

        public void Dispose() => _body = null;
    }
}
