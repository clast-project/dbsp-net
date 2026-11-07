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

    /// <summary>Indexed deltas built block backed, and their entries (D4, §5.22).</summary>
    internal static long BlockedGroupBuilds, BlockedGroupEntries;

    /// <summary>Block-backed deltas a trace adopted the runs of, and their rows (§5.19).</summary>
    internal static long Adopted, AdoptedRows;

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
/// map. The runs are kept as frozen, never merged (docs/design-bounded-memory.md §5.17: merging was
/// 64% of the cost); S1's spiller writes the large ones to files the block cache can evict, so a
/// producer's managed peak is one chunk, not its output. Each run's sorted key hashes are kept beside
/// it (4 B a row) so the built set's exact count needs no block read.
/// </summary>
internal sealed class SpillingRows : ZSetSpillingStore<StructuralRow, Z64>
{
    private readonly ITraceBlockFormat _format;
    private readonly int _chunkRows;
    private readonly bool _alwaysBlocked;
    private readonly int _capacity;
    private ZSetBuilder<StructuralRow, Z64> _chunk;
    private BlockedIndexedTrace? _trace;
    private List<uint[]> _hashes = []; // newest first, as the trace's runs
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
        if (_chunk.Count >= _chunkRows
            || (_chunk.Count >= MemoryPressure.CriticalStepRows && MemoryPressure.Current == PressureLevel.Critical))
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

        // The run's hashes, in its order (the trace sorts by the row's own hash, as here), taken before
        // the freeze: a spill inside it makes the blocks file-backed.
        var hashes = new uint[part.Count];
        var n = 0;
        foreach (var (row, _) in part)
        {
            hashes[n++] = (uint)row.GetHashCode();
        }

        Array.Sort(hashes);
        _trace ??= new BlockedIndexedTrace(
            _format, freezeEntries: int.MaxValue, maxManaged: int.MaxValue, blockEntries: TraceBlocksMode.BlockEntries, compact: false);
        _trace.IntegrateRows(part);
        _trace.Freeze();
        _hashes.Insert(0, hashes);
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
        var hashes = _hashes;
        _trace = null;
        _hashes = [];
        _frozen = 0;
        if (trace is null || BlockedRows.Create([new BlockedRows.Part(trace, [.. hashes])], _format) is not { } built)
        {
            return ZSet<StructuralRow, Z64>.Empty; // nothing added, or every entry cancelled
        }

        return new ZSet<StructuralRow, Z64>(built);
    }

    public override void Reset()
    {
        _chunk = new ZSetBuilder<StructuralRow, Z64>(_capacity);
        _trace = null;
        _hashes = [];
        _frozen = 0;
    }
}

/// <summary>
/// D1: a Z-set held as hash-ordered block runs (a frozen delta, §5.14), each consolidated but NOT merged
/// with the others (§5.17). A reader merges them by hash: an entry whose hash only one run holds is
/// read as it is; the entries of a hash several runs hold are decoded and summed. The exact count is the
/// same walk over the runs' hash arrays alone, made once at build, so it reads no block unless two runs
/// share a hash. A lookup probes every run. Rows are decoded with strings copied, so a row a consumer
/// keeps does not pin a block's bytes. The blocks may be file-backed once spilled.
/// </summary>
/// <remarks>The runs are read through their owning traces (<see cref="Part"/>), never adopted into a
/// new one: a spilled file lives as long as its original run object (the spiller tracks it weakly), and
/// holding the owner keeps that run reachable and follows a spill's rebind.</remarks>
internal sealed class BlockedRows : ZSetExternalStore<StructuralRow, Z64>
{
    /// <summary>A trace's runs, newest first, and each run's sorted key hashes.</summary>
    internal readonly record struct Part(BlockedIndexedTrace Trace, uint[][] Hashes);

    private readonly Part[] _parts;
    private readonly ITraceBlockFormat _format;
    private readonly int _count;

    private BlockedRows(Part[] parts, ITraceBlockFormat format)
    {
        _parts = parts;
        _format = format;
        var m = StepSpillProfile.Enabled ? StepSpillProfile.Begin() : default;
        _count = CountEntries();
        if (StepSpillProfile.Enabled)
        {
            StepSpillProfile.End(StepSpillProfile.BuildCount, m);
        }
    }

    /// <summary>The set over <paramref name="parts"/>, or null when it is empty (every entry cancelled).</summary>
    internal static BlockedRows? Create(Part[] parts, ITraceBlockFormat format)
    {
        var built = new BlockedRows(parts, format);
        if (built._count == 0)
        {
            return null;
        }

        Interlocked.Increment(ref DeltaSpill.BlockedBuilds);
        Interlocked.Add(ref DeltaSpill.BlockedRows, built._count);
        return built;
    }

    /// <summary>Runs across every part (tests, census).</summary>
    internal int RunCount => _parts.Sum(p => p.Trace.Runs.Count);

    public override int Count => _count;

    public override long ResidentBytes
    {
        get
        {
            long bytes = 0;
            foreach (var part in _parts)
            {
                foreach (var h in part.Hashes)
                {
                    bytes += 4L * h.Length;
                }

                foreach (var run in part.Trace.Runs)
                {
                    foreach (var b in run.Blocks)
                    {
                        bytes += b.Resident ? b.Hashes.Length * 64L : 0; // rough: the census only ranks
                    }
                }
            }

            return bytes;
        }
    }

    // Every run with its hashes, read through the owners now (a spill may have rebound a run).
    private (BlockedIndexedTrace.Run Run, uint[] Hashes)[] Runs()
    {
        var list = new List<(BlockedIndexedTrace.Run, uint[])>();
        foreach (var part in _parts)
        {
            var runs = part.Trace.Runs;
            for (var r = 0; r < runs.Count; r++)
            {
                list.Add((runs[r], part.Hashes[r]));
            }
        }

        return [.. list];
    }

    // The net entries: hashes only one run holds count as they are; a shared hash's entries are decoded.
    private int CountEntries()
    {
        var runs = Runs();
        if (runs.Length == 1)
        {
            return runs[0].Hashes.Length;
        }

        long count = 0;
        var merge = new HashMerge(runs);
        while (merge.Next(out var hash, out var tied))
        {
            if (tied.Count == 1)
            {
                count += merge.Skip(tied[0], hash);
                continue;
            }

            foreach (var w in merge.Sum(tied, hash).Values)
            {
                count += w != 0 ? 1 : 0;
            }
        }

        return (int)count;
    }

    public override bool TryGetWeight(StructuralRow key, out Z64 weight)
    {
        var m = StepSpillProfile.Enabled ? StepSpillProfile.Begin() : default;
        var sum = new Sum();
        foreach (var part in _parts)
        {
            part.Trace.Probe(key, ref sum);
        }

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

        var runs = Runs();

        IEnumerator<KeyValuePair<StructuralRow, Z64>> inner = runs.Length == 1 ? new Cursor(runs[0].Run) : Merged(runs).GetEnumerator();

        // The enumerator holds its runs' spilled files until it is collected (§5.20), as a trace's lazy
        // readers do: it may outlive every other reference to this set (an exchange gathers a bucket it
        // has already taken from the mailbox, §5.27).
        foreach (var (run, _) in runs)
        {
            run.Lease?.Hold(inner);
        }

        return StepSpillProfile.Enabled ? new Profiled(inner) : inner;
    }

    private static IEnumerable<KeyValuePair<StructuralRow, Z64>> Merged((BlockedIndexedTrace.Run Run, uint[] Hashes)[] runs)
    {
        var merge = new HashMerge(runs);
        while (merge.Next(out var hash, out var tied))
        {
            if (tied.Count == 1)
            {
                var c = merge.Cursors[tied[0]];
                var n = merge.Skip(tied[0], hash, advanceCursor: false);
                for (var i = 0; i < n; i++)
                {
                    var body = c.Body;
                    yield return new(body.Key(c.Index), new Z64(body.Weight(c.Index)));
                    c.Advance();
                }

                continue;
            }

            foreach (var (row, w) in merge.Sum(tied, hash))
            {
                if (w != 0)
                {
                    yield return new(row, new Z64(w));
                }
            }
        }
    }

    public override ZSetBuilder<StructuralRow, Z64> NewBuilder(int capacity) => DeltaSpill.Rows(_format, capacity);

    /// <summary>Hand this set's runs to <paramref name="trace"/> as its own (a view or DISTINCT integrating
    /// it, §5.19): no row decoded or encoded. False when the trace cannot take them.</summary>
    internal bool AdoptInto(BlockedIndexedTrace trace)
    {
        var parts = _parts.Select(p => p.Trace).ToArray();
        var runs = parts.SelectMany(t => t.Runs).ToArray();
        if (!trace.TryAdoptRuns(runs, _format, parts.All(t => t.OrdersByRowHash)))
        {
            return false;
        }

        Interlocked.Increment(ref DeltaSpill.Adopted);
        Interlocked.Add(ref DeltaSpill.AdoptedRows, _count);
        return true;
    }

    /// <summary>A sum with a block-backed set is the union of their runs, merged only when read; a
    /// managed side is frozen into a run first (the small one in practice: a union's null-padded side).</summary>
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

        BlockedRows theirs;
        if (other.External is BlockedRows blocked)
        {
            theirs = blocked;
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

            theirs = frozen;
        }

        return Create([.. theirs._parts, .. _parts], _format) is { } sum
            ? new ZSet<StructuralRow, Z64>(sum)
            : ZSet<StructuralRow, Z64>.Empty;
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

    // A k-way walk over runs in hash order, by their hash arrays; a run's cursor follows its position
    // and reads a block's body only when an entry is decoded.
    private sealed class HashMerge
    {
        private readonly uint[][] _hashes;
        private readonly int[] _pos;
        private readonly PriorityQueue<int, uint> _queue;
        private readonly List<int> _tied = [];

        public HashMerge((BlockedIndexedTrace.Run Run, uint[] Hashes)[] runs)
        {
            _hashes = [.. runs.Select(r => r.Hashes)];
            Cursors = [.. runs.Select(r => new RunCursor(r.Run))];
            _pos = new int[runs.Length];
            _queue = new PriorityQueue<int, uint>(runs.Length);
            for (var r = 0; r < runs.Length; r++)
            {
                if (_hashes[r].Length > 0)
                {
                    _queue.Enqueue(r, _hashes[r][0]);
                }
            }
        }

        public RunCursor[] Cursors { get; }

        // The next hash, and the runs holding it (dequeued until Skip or Sum puts them back).
        public bool Next(out uint hash, out List<int> tied)
        {
            tied = _tied;
            tied.Clear();
            if (!_queue.TryDequeue(out var r, out hash))
            {
                return false;
            }

            tied.Add(r);
            while (_queue.TryPeek(out var r2, out var h2) && h2 == hash)
            {
                _queue.Dequeue();
                tied.Add(r2);
            }

            return true;
        }

        // Pass run r's entries of hash (returns how many) and requeue it; the cursor follows unless the
        // caller advances it while reading them.
        public int Skip(int r, uint hash, bool advanceCursor = true)
        {
            var a = _hashes[r];
            var p = _pos[r];
            var e = p;
            while (e < a.Length && a[e] == hash)
            {
                e++;
            }

            if (advanceCursor)
            {
                for (var i = p; i < e; i++)
                {
                    Cursors[r].Advance();
                }
            }

            _pos[r] = e;
            if (e < a.Length)
            {
                _queue.Enqueue(r, a[e]);
            }

            return e - p;
        }

        // The entries of hash across the tied runs, decoded and summed by row.
        public Dictionary<StructuralRow, long> Sum(List<int> tied, uint hash)
        {
            var sums = new Dictionary<StructuralRow, long>();
            foreach (var r in tied.ToArray())
            {
                var c = Cursors[r];
                var n = Skip(r, hash, advanceCursor: false);
                for (var i = 0; i < n; i++)
                {
                    var body = c.Body;
                    var row = body.Key(c.Index);
                    sums[row] = sums.GetValueOrDefault(row) + body.Weight(c.Index);
                    c.Advance();
                }
            }

            return sums;
        }
    }

    // A position in one run: block and entry; the body is fetched (a file block read) only when asked.
    private sealed class RunCursor(BlockedIndexedTrace.Run run)
    {
        private int _block;
        private ITraceBlock? _body;

        public int Index { get; private set; }

        public ITraceBlock Body => _body ??= run.Blocks[_block].Body;

        public void Advance()
        {
            if (++Index >= run.Blocks[_block].Count)
            {
                _block++;
                Index = 0;
                _body = null;
            }
        }
    }

    // Charges each row's read to the profile's enumeration phase.
    private sealed class Profiled(IEnumerator<KeyValuePair<StructuralRow, Z64>> inner) : IEnumerator<KeyValuePair<StructuralRow, Z64>>
    {
        public KeyValuePair<StructuralRow, Z64> Current => inner.Current;

        object IEnumerator.Current => inner.Current;

        public bool MoveNext()
        {
            var m = StepSpillProfile.Begin();
            var more = inner.MoveNext();
            StepSpillProfile.End(StepSpillProfile.Enumerate, m);
            if (more)
            {
                Interlocked.Increment(ref StepSpillProfile.RowsDecoded);
            }

            return more;
        }

        public void Reset() => throw new NotSupportedException();

        public void Dispose() => inner.Dispose();
    }

    // One run's blocks in order; a block's body is held while on it.
    private sealed class Cursor(BlockedIndexedTrace.Run run) : IEnumerator<KeyValuePair<StructuralRow, Z64>>
    {
        private int _block = -1;
        private int _index;
        private ITraceBlock? _body;
        private KeyValuePair<StructuralRow, Z64> _current;

        public KeyValuePair<StructuralRow, Z64> Current => _current;

        object IEnumerator.Current => _current;

        public bool MoveNext()
        {
            while (true)
            {
                if (_body is not null && ++_index < _body.Count)
                {
                    _current = new(_body.Key(_index), new Z64(_body.Weight(_index)));
                    return true;
                }

                if (++_block >= run.Blocks.Length)
                {
                    _body = null;
                    return false;
                }

                _body = run.Blocks[_block].Body;
                _index = -1;
            }
        }

        public void Reset() => throw new NotSupportedException();

        public void Dispose() => _body = null;
    }
}
