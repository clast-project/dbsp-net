// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful.Aggregators;

namespace DbspNet.Core.Operators.Stateful.Blocks;

/// <summary>
/// A window operator's per-partition input, read in order and only as far as a change reaches
/// (docs/design-flat-trace-state.md §3.15, §3.18).
/// </summary>
/// <remarks>
/// <para>A partition's representation follows its size. A <b>small</b> partition is managed: a
/// sorted dictionary of its rows, so touching it decodes nothing (on blocks every touch decoded
/// the partition, and small partitions are touched often relative to their size, §3.17). A
/// partition that grows past <c>promoteAt</c> rows becomes <b>large</b>: a list of reference-free
/// chunks, each one block of rows sorted by (order value, total order), with its first and last
/// order value, its rows' order values, and, when the window's aggregate combines exactly, its
/// partial aggregate. A peer group (equal order value) never spans chunks, so a value routes to one
/// chunk. A change rewrites only the chunks it lands in; appends touch only the last. A large
/// partition stays large as it shrinks, since its chunks shrink with it, and leaves the store when
/// empty.</para>
/// <para>Reads see the state as of the last <see cref="Integrate"/>, consolidated, in order.</para>
/// </remarks>
internal sealed class OrderedPartitionStore
{
    private readonly ITraceBlockFormat _chunkFormat;
    private readonly Func<StructuralRow, long>? _valueOf;
    private readonly IComparer<StructuralRow> _order;
    private readonly IPartialAggregator<StructuralRow, StructuralRow>? _partial;
    private readonly int _promoteAt;
    private readonly int _chunkRows;
    private readonly IComparer<StructuralRow> _rowOrder;
    private readonly Dictionary<StructuralRow, SortedDictionary<StructuralRow, long>> _small = new();
    private readonly Dictionary<StructuralRow, List<Chunk>> _large = new();
    private long _smallRows;

    // With a cold format (§3.22): small partitions' record is this shared trace (partition key →
    // row), and _small a write-back cache. A touched partition is loaded into the cache once and
    // changed there; its changes queue in _pending and reach the trace in one run when the cache
    // is dropped (a checkpoint, or past a cap). Writing the trace every tick made its merges decode
    // and re-encode the growing state again and again. Without a cold format, _small is the record.
    private readonly BlockedIndexedTrace? _cold;
    private readonly ITraceBlockFormat? _coldFormat;
    private readonly Dictionary<StructuralRow, List<(StructuralRow Row, long Weight)>> _pending = new();

    // The write-back cache's estimated managed bytes (_small's cached partitions and _pending's queue),
    // and the budget all stores' caches share (docs/design-bounded-memory.md §5.8). A store that ends a
    // step with the shared total over it drops its cache: one huge step otherwise kept a whole input as
    // managed partitions (extended TPC-DS: one rank's 14.8M rows, 14.5 GB) until an idle tick.
    private long _cacheBytes;
    private readonly long _writeBackBudget = SpillSettings.WindowWriteBackBytes;

    // Per cached row besides the row itself: its SortedDictionary node and its queued (row, weight).
    private const int CachedRowOverhead = 48 + 24;

    // Per cached partition besides its key: the cache's and the queue's dictionary entries, the
    // SortedDictionary and the queue's list.
    private const int CachedPartitionOverhead = 48 + 64 + 48 + 40;

    // Every live cold-backed store, weakly: the shared total sums them (a store is never disposed, so
    // a running counter would keep a dropped program's bytes).
    private static readonly List<WeakReference<OrderedPartitionStore>> Live = [];

    /// <summary>Write-back caches dropped by the budget (diagnostics).</summary>
    internal static long WriteBackDrops;

    /// <summary>The cache's estimated managed bytes (tests, census).</summary>
    internal long CacheBytes => _cacheBytes;

    /// <summary>All live stores' estimated cache bytes (diagnostics: compare with a census).</summary>
    internal static long LiveCacheBytesTotal => LiveCacheBytes();

    private static long LiveCacheBytes()
    {
        long total = 0;
        lock (Live)
        {
            for (var i = Live.Count - 1; i >= 0; i--)
            {
                if (Live[i].TryGetTarget(out var store))
                {
                    total += Interlocked.Read(ref store._cacheBytes);
                }
                else
                {
                    Live.RemoveAt(i);
                }
            }
        }

        return total;
    }

    // Keys the cold trace has ever held (a Bloom filter: no false negatives), so a partition new
    // this tick is read from the tick's own rows instead of probed for (§3.17).
    private KeyFilter _keys = new();


    private static readonly StructuralRow NoKey = new([]);

    // A row's partition key: what a restored large partition, known by hash only, is matched by.
    private readonly Func<StructuralRow, StructuralRow>? _keyOf;

    // Large partitions restored from a checkpoint's block set and not touched since (§3g): by the
    // partition key's hash, each its chunks' blocks (read lazily) and row counts. Touching a key
    // with that hash reads them into _large.
    private readonly Dictionary<uint, List<List<(ITraceBlock Block, int Rows)>>> _unloaded = new();

    /// <param name="chunkFormat">Blocks of a large partition's chunks: no key, the row.</param>
    /// <param name="valueOf">The ORDER BY value; null for a whole-partition frame, whose partitions
    /// stay small (with every row one peer group there is nothing to cut at).</param>
    /// <param name="order">The total order (ORDER BY, then the whole row).</param>
    /// <param name="partial">The window's aggregate when it combines exactly: chunks then carry partials.</param>
    public OrderedPartitionStore(
        ITraceBlockFormat chunkFormat,
        Func<StructuralRow, long>? valueOf,
        IComparer<StructuralRow> order,
        IPartialAggregator<StructuralRow, StructuralRow>? partial,
        int promoteAt = 512,
        int chunkRows = 512,
        ITraceBlockFormat? coldFormat = null,
        Func<StructuralRow, StructuralRow>? keyOf = null)
    {
        _keyOf = keyOf;
        _chunkFormat = chunkFormat;
        _valueOf = valueOf;
        _order = order;
        _partial = partial is { IsExactPartial: true } ? partial : null;
        _promoteAt = valueOf is null ? int.MaxValue : promoteAt;
        _chunkRows = Math.Max(1, chunkRows);
        _coldFormat = coldFormat;
        _cold = coldFormat is null ? null : new BlockedIndexedTrace(coldFormat, TraceBlocksMode.FreezeEntries, blockEntries: TraceBlocksMode.BlockEntries);
        if (_cold is not null && _writeBackBudget != long.MaxValue)
        {
            lock (Live)
            {
                Live.Add(new WeakReference<OrderedPartitionStore>(this));
            }
        }
        _rowOrder = Comparer<StructuralRow>.Create((a, b) =>
        {
            var c = ValueOf(a).CompareTo(ValueOf(b));
            return c != 0 ? c : _order.Compare(a, b);
        });
    }

    /// <summary>Whether chunks carry partial aggregates (the aggregate combines exactly).</summary>
    public bool HasPartials => _partial is not null;

    /// <summary>The large partitions' chunk blocks (the residency census).</summary>
    internal IEnumerable<ITraceBlock> ChunkBlocks()
    {
        foreach (var chunks in _large.Values)
        {
            foreach (var c in chunks)
            {
                yield return c.Block;
            }
        }

        foreach (var parts in _unloaded.Values)
        {
            foreach (var part in parts)
            {
                foreach (var (block, _) in part)
                {
                    yield return block;
                }
            }
        }
    }

    /// <summary>
    /// With a block cache that can evict (docs/design-bounded-memory.md §3.7): write the small
    /// partitions' record as run files the cache reads back, so its blocks can leave memory. Only
    /// the cache reads it: the operator's own snapshot stays the state restore reads. Call after
    /// <see cref="DropCache"/>, which flushes the record.
    /// </summary>
    public async ValueTask SaveBackingAsync(Circuit.ISnapshotWriter writer, string fileName, CancellationToken cancellationToken)
    {
        if (!BlockAccess.Evicting)
        {
            return;
        }

        if (_cold is { IsEmpty: false } cold && _coldFormat?.BackingCodec is { } codec)
        {
            await codec.SaveBackingAsync(writer, fileName, cold, cancellationToken).ConfigureAwait(false);
        }

        // Large partitions' chunks not yet backed: one file of their blocks this checkpoint; each
        // chunk takes its block's file-backed twin when the checkpoint commits.
        if (_chunkFormat.BackingCodec is { } chunkCodec)
        {
            var pending = _large.Values.SelectMany(cs => cs).Where(c => !c.Backed).ToList();
            if (pending.Count > 0)
            {
                await chunkCodec.SaveBackingBlocksAsync(
                    writer,
                    "chunks.backing",
                    [.. pending.Select(c => c.Block)],
                    backed =>
                    {
                        for (var k = 0; k < pending.Count; k++)
                        {
                            pending[k].Block = backed[k];
                            pending[k].Backed = true;
                        }
                    },
                    cancellationToken).ConfigureAwait(false);
            }
        }
    }

    /// <summary>The small partitions' shared record, if blocked (the residency census).</summary>
    internal BlockedIndexedTrace? ColdRecord => _cold;

    /// <summary>Partitions and rows in the managed small-partition map or cache (the residency census).</summary>
    internal (int Partitions, long Rows) SmallManaged => (_small.Count, _small.Values.Sum(p => (long)p.Count));

    /// <summary>After a restore from the store's own files (§3g, tests): file blocks of the record and
    /// of large partitions, how many of them have been read, and large partitions not yet touched.</summary>
    internal (int Blocks, int Read, int Untouched) Laziness
    {
        get
        {
            var blocks = 0;
            var read = 0;
            void Count(ITraceBlock b)
            {
                blocks++;
                read += b.Resident ? 1 : 0;
            }

            foreach (var run in _cold?.Runs ?? [])
            {
                foreach (var b in run.Blocks)
                {
                    Count(b);
                }
            }

            foreach (var b in ChunkBlocks())
            {
                Count(b);
            }

            return (blocks, read, _unloaded.Values.Sum(p => p.Count));
        }
    }

    /// <summary>Large partitions (tests).</summary>
    internal int LargePartitions => _large.Count + _unloaded.Values.Sum(p => p.Count);

    /// <summary>Rows held.</summary>
    public long Entries
    {
        get
        {
            var n = _cold is null ? _smallRows : _cold.Entries + _pending.Values.Sum(p => (long)p.Count);
            foreach (var chunks in _large.Values)
            {
                foreach (var c in chunks)
                {
                    n += c.Count;
                }
            }

            foreach (var parts in _unloaded.Values)
            {
                foreach (var part in parts)
                {
                    foreach (var (_, rows) in part)
                    {
                        n += rows;
                    }
                }
            }

            return n;
        }
    }

    private sealed class Chunk(ITraceBlock block, long[] values, object? partial)
    {
        // Replaced once, by the block's file-backed twin, when a checkpoint that wrote it commits (§3.7).
        public ITraceBlock Block = block;
        public bool Backed;
        public readonly long[] Values = values;
        public readonly object? Partial = partial;

        public int Count => Values.Length;

        public long First => Values[0];

        public long Last => Values[^1];
    }

    private long ValueOf(StructuralRow row) => _valueOf?.Invoke(row) ?? 0L;

    /// <summary>The order value the store routes a row by.</summary>
    public Func<StructuralRow, long> ValueOfRow => ValueOf;

    // Compare as a delegate built once (a method group would allocate one per sort).
    private Comparison<(StructuralRow Row, long Weight, long Value)> _compare => _compareCache ??= Compare;

    private Comparison<(StructuralRow Row, long Weight, long Value)>? _compareCache;

    private int Compare((StructuralRow Row, long Weight, long Value) a, (StructuralRow Row, long Weight, long Value) b)
    {
        var c = a.Value.CompareTo(b.Value);
        return c != 0 ? c : _order.Compare(a.Row, b.Row);
    }

    /// <summary>Apply a tick's change (or restored rows): (partition key, row, weight).</summary>
    public void Integrate(List<(StructuralRow Key, StructuralRow Row, long Weight)> rows)
    {
        if (rows.Count == 0)
        {
            return;
        }

        // A cached row's estimated bytes: the mean of a sample of this change's rows, plus overhead.
        var perRow = _cold is null ? 0 : ManagedSize.MeanRow(rows.Count, i => rows[i].Row) + CachedRowOverhead;
        long added = 0;
        var byKey = new Dictionary<StructuralRow, List<(StructuralRow Row, long Weight, long Value)>>();
        foreach (var (key, row, weight) in rows)
        {
            if (!byKey.TryGetValue(key, out var list))
            {
                byKey[key] = list = new();
            }

            list.Add((row, weight, ValueOf(row)));
        }

        foreach (var (key, change) in byKey)
        {
            EnsureLoaded(key);
            if (_large.ContainsKey(key))
            {
                ApplyLarge(key, change);
            }
            else if (change.Count > _promoteAt)
            {
                // A bulk change alone past the threshold (a load, a restored partition): straight to
                // chunks, never built as a sorted dictionary first.
                PromoteWith(key, change);
            }
            else if (_cold is null)
            {
                ApplySmall(key, change);
            }
            else
            {
                // Into the cache (loaded once: nothing for a key the filter has never seen), and
                // queued for the record.
                if (!_small.ContainsKey(key))
                {
                    Load(key);
                    added += CachedPartitionOverhead + ManagedSize.Row(key) + (_small[key].Count * perRow);
                }

                ApplySmall(key, change);
                added += change.Count * perRow;
                if (!_pending.TryGetValue(key, out var queued))
                {
                    _pending[key] = queued = new();
                }

                foreach (var (row, weight, _) in change)
                {
                    queued.Add((row, weight));
                }

                _keys.Add(key);
            }
        }

        if (added != 0)
        {
            Interlocked.Add(ref _cacheBytes, added);
        }
    }

    // A partition's raw content: the record's entries for it plus its queued changes (a cached
    // partition that emptied leaves the cache with its changes still queued).
    private List<(StructuralRow Row, long Weight, long Value)> RecordOf(StructuralRow key)
    {
        var raw = new List<(StructuralRow Row, long Weight, long Value)>();
        if (_keys.MayContain(key))
        {
            var visitor = new Collect(raw, this);
            _cold!.Probe(key, ref visitor);
        }

        if (_pending.TryGetValue(key, out var queued))
        {
            foreach (var (row, weight) in queued)
            {
                raw.Add((row, weight, ValueOf(row)));
            }
        }

        return raw;
    }

    // A partition's content into the cache.
    private void Load(StructuralRow key)
    {
        var rows = new SortedDictionary<StructuralRow, long>(_rowOrder);
        {
            var raw = RecordOf(key);
            foreach (var (row, weight, _) in raw)
            {
                rows.TryGetValue(row, out var current);
                if (current + weight == 0)
                {
                    rows.Remove(row);
                }
                else
                {
                    rows[row] = current + weight;
                }
            }
        }

        _small[key] = rows;
    }

    // The queued changes into the record, as one run.
    private void Flush()
    {
        if (_pending.Count == 0)
        {
            return;
        }

        var entries = new List<(StructuralRow Key, StructuralRow Row, long Weight)>();
        foreach (var (key, rows) in _pending)
        {
            foreach (var (row, weight) in rows)
            {
                entries.Add((key, row, weight));
            }
        }

        _pending.Clear();
        _cold!.IntegrateEntries(entries.Count, i => entries[i].Key, i => entries[i].Row, i => entries[i].Weight);
    }

    private void ApplySmall(StructuralRow key, List<(StructuralRow Row, long Weight, long Value)> change)
    {
        if (!_small.TryGetValue(key, out var rows))
        {
            _small[key] = rows = new SortedDictionary<StructuralRow, long>(_rowOrder);
        }

        foreach (var (row, weight, _) in change)
        {
            rows.TryGetValue(row, out var current);
            var next = current + weight;
            if (next == 0)
            {
                if (rows.Remove(row))
                {
                    _smallRows--;
                }
            }
            else
            {
                if (current == 0)
                {
                    _smallRows++;
                }

                rows[row] = next;
            }
        }

        if (rows.Count == 0)
        {
            _small.Remove(key);
        }
    }

    /// <summary>An idle tick: freeze the cold trace. The read cache stays, since an operator's idle
    /// tick is not the end of a batch (an empty delta between two that touch the same partitions),
    /// unless it has grown past <see cref="CacheCap"/> partitions. A checkpoint drops it
    /// (<see cref="DropCache"/>).</summary>
    public void Freeze()
    {
        if (_cold is not null && _small.Count > CacheCap)
        {
            DropCache();
        }
    }

    /// <summary>Partitions the read cache may hold past an idle tick (a run without checkpoints).</summary>
    internal const int CacheCap = 1 << 18;

    /// <summary>Drop the read cache (a checkpoint: the heap it leaves holds only the record).</summary>
    public void DropCache()
    {
        if (_cold is not null)
        {
            Flush();
            _small.Clear();
            _cold.Freeze();
            Interlocked.Exchange(ref _cacheBytes, 0);
        }
    }

    /// <summary>The step's reads are done: past the write-back budget (§5.8), drop the cache into
    /// the record. Partitions touched again reload from it.</summary>
    public void EndTick()
    {
        if (_cold is null || _writeBackBudget == long.MaxValue || Interlocked.Read(ref _cacheBytes) == 0)
        {
            return;
        }

        // At High pressure or above every store drops its cache at its step's end (§5.28).
        if (MemoryPressure.Current >= PressureLevel.High
            || Interlocked.Read(ref _cacheBytes) > _writeBackBudget || LiveCacheBytes() > _writeBackBudget)
        {
            DropCache();
            Interlocked.Increment(ref WriteBackDrops);
        }
    }

    /// <summary>Forget everything (a restore replaces the state).</summary>
    public void Clear()
    {
        _small.Clear();
        _large.Clear();
        _unloaded.Clear();
        _smallRows = 0;
        Interlocked.Exchange(ref _cacheBytes, 0);
        _cold?.Clear();
        _keys = new KeyFilter();
        _pending.Clear();
    }

    /// <summary>Every row and weight (a whole-state write).</summary>
    public IEnumerable<(StructuralRow Row, long Weight)> All()
    {
        if (_cold is not null)
        {
            Flush();
            foreach (var (_, group) in _cold.MaterializeUncached())
            {
                foreach (var (row, weight) in group)
                {
                    yield return (row, weight.Value);
                }
            }
        }
        else
        {
            foreach (var rows in _small.Values)
            {
                foreach (var (row, weight) in rows)
                {
                    yield return (row, weight);
                }
            }
        }

        foreach (var chunks in _large.Values)
        {
            foreach (var c in chunks)
            {
                var body = c.Block.Body;
                for (var i = 0; i < c.Count; i++)
                {
                    yield return (body.Value(i), body.Weight(i));
                }
            }
        }

        foreach (var parts in _unloaded.Values)
        {
            foreach (var part in parts)
            {
                foreach (var (block, rows) in part)
                {
                    var body = block.Body;
                    for (var i = 0; i < rows; i++)
                    {
                        yield return (body.Value(i), body.Weight(i));
                    }
                }
            }
        }
    }

    // Read the restored large partitions whose key hashes as `key`'s into _large (§3g): each
    // chunk's rows are decoded once, for its order values and partial; its block stays as it is,
    // so the next checkpoint names it again instead of writing it.
    private void EnsureLoaded(StructuralRow key)
    {
        if (_unloaded.Count == 0 || !_unloaded.Remove((uint)key.GetHashCode(), out var parts))
        {
            return;
        }

        foreach (var part in parts)
        {
            var chunks = new List<Chunk>(part.Count);
            StructuralRow? partitionKey = null;
            foreach (var (block, rows) in part)
            {
                var body = block.Body;
                var values = new long[rows];
                object? partial = _partial?.NewAccumulator();
                for (var i = 0; i < rows; i++)
                {
                    var row = body.Value(i);
                    partitionKey ??= _keyOf!(row);
                    values[i] = ValueOf(row);
                    if (partial is not null)
                    {
                        _partial!.Add(partial, row, body.Weight(i));
                    }
                }

                chunks.Add(new Chunk(block, values, partial) { Backed = true });
            }

            if (partitionKey is not null)
            {
                _large[partitionKey] = chunks;
            }
        }
    }

    private const int StoreVersion = 1;

    /// <summary>Whether the store can checkpoint as its own files here (§3g): small partitions'
    /// record as a run chain, large partitions' chunks as a block set. Otherwise the operator writes
    /// its rows.</summary>
    public bool CanSave(Circuit.ISnapshotWriter writer) =>
        _cold is not null && _keyOf is not null
        && _coldFormat!.BackingCodec is { } cold && cold.SupportsBlockSets(writer)
        && _chunkFormat.BackingCodec is { } chunks && chunks.SupportsBlockSets(writer);

    /// <summary>
    /// Checkpoint the store as its own files (docs/design-bounded-memory.md §3g), nothing decoded:
    /// the small partitions' record (<paramref name="name"/>.record) as the run chain a blocked
    /// trace writes, and the large partitions' chunks as a block set (<paramref name="name"/>.chunks)
    /// whose metadata holds each partition's key hash and chunk sizes and the record's key filter.
    /// A chunk an earlier checkpoint wrote is named, not copied. Requires <see cref="CanSave"/>.
    /// </summary>
    public async ValueTask SaveAsync(Circuit.ISnapshotWriter writer, string name, CancellationToken cancellationToken)
    {
        DropCache();
        await _coldFormat!.BackingCodec!.SaveBlockedAsync(writer, name + ".record", _cold!, cancellationToken).ConfigureAwait(false);

        var blocks = new List<ITraceBlock>();
        var rebind = new List<Action<ITraceBlock>>();
        var meta = new System.IO.MemoryStream();
        using (var w = new System.IO.BinaryWriter(meta, System.Text.Encoding.UTF8, leaveOpen: true))
        {
            w.Write(StoreVersion);
            _keys.Write(w);
            w.Write(_large.Count + _unloaded.Values.Sum(p => p.Count));
            foreach (var (key, chunks) in _large)
            {
                w.Write((uint)key.GetHashCode());
                w.Write(chunks.Count);
                foreach (var c in chunks)
                {
                    w.Write(c.Count);
                    blocks.Add(c.Block);
                    rebind.Add(twin => { c.Block = twin; c.Backed = true; });
                }
            }

            foreach (var (hash, parts) in _unloaded)
            {
                foreach (var part in parts)
                {
                    w.Write(hash);
                    w.Write(part.Count);
                    for (var j = 0; j < part.Count; j++)
                    {
                        w.Write(part[j].Rows);
                        blocks.Add(part[j].Block);
                        var (list, at) = (part, j);
                        rebind.Add(twin => list[at] = (twin, list[at].Rows));
                    }
                }
            }
        }

        await _chunkFormat.BackingCodec!.SaveBlockSetAsync(
            writer,
            name + ".chunks",
            this,
            blocks,
            meta.GetBuffer().AsMemory(0, (int)meta.Length),
            twins =>
            {
                for (var i = 0; i < twins.Length; i++)
                {
                    rebind[i](twins[i]);
                }
            },
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Restore what <see cref="SaveAsync"/> wrote, lazily: the record's runs are adopted
    /// (blocks read on first probe), and large partitions stay unread until a key touches them.
    /// False (nothing changed) when the snapshot holds no such files.</summary>
    public async ValueTask<bool> TryLoadAsync(Circuit.ISnapshotReader reader, string name, CancellationToken cancellationToken)
    {
        if (_cold is null || _keyOf is null || _coldFormat!.BackingCodec is not { } coldCodec || _chunkFormat.BackingCodec is not { } chunkCodec
            || await chunkCodec.LoadBlockSetAsync(reader, name + ".chunks", this, cancellationToken).ConfigureAwait(false) is not { } set)
        {
            return false;
        }

        Clear();
        await coldCodec.LoadBlockedAsync(reader, name + ".record", _cold, cancellationToken).ConfigureAwait(false);
        using var r = new System.IO.BinaryReader(new System.IO.MemoryStream(set.Meta), System.Text.Encoding.UTF8);
        if (r.ReadInt32() != StoreVersion)
        {
            throw new InvalidDataException($"'{name}.chunks' is a window store of another version");
        }

        _keys = KeyFilter.Read(r);
        var partitions = r.ReadInt32();
        var b = 0;
        for (var p = 0; p < partitions; p++)
        {
            var hash = r.ReadUInt32();
            var count = r.ReadInt32();
            var part = new List<(ITraceBlock Block, int Rows)>(count);
            for (var j = 0; j < count; j++)
            {
                part.Add((set.Blocks[b++], r.ReadInt32()));
            }

            if (!_unloaded.TryGetValue(hash, out var parts))
            {
                _unloaded[hash] = parts = [];
            }

            parts.Add(part);
        }

        if (b != set.Blocks.Length)
        {
            throw new InvalidDataException($"'{name}.chunks' names {set.Blocks.Length} blocks, its partitions {b}");
        }

        return true;
    }

    /// <summary>
    /// A partition's rows in order (order value, then total order), consolidated: of a large
    /// partition only the chunks overlapping [<paramref name="from"/>, <paramref name="to"/>]. With
    /// <paramref name="needPrefix"/> (a running frame folds from the start), <paramref name="before"/>
    /// is the partial of the chunks below them (folded in ascending order), and without partials
    /// the read starts at the first chunk. Of a small partition, every row (<paramref name="before"/>
    /// null: the caller folds from the start). A small partition found larger than the threshold is
    /// promoted to chunks here.
    /// </summary>
    public List<(StructuralRow Row, long Weight, long Value)> Read(
        StructuralRow key, long from, long to, bool needPrefix, out object? before)
    {
        before = null;
        var t0 = BlockTraceProfile.Now();
        EnsureLoaded(key);
        if (_large.TryGetValue(key, out var chunks))
        {
            var first = FirstChunkReaching(chunks, from);
            if (!needPrefix)
            {
                // The caller needs only the range (a bounded frame).
            }
            else if (_partial is not null && first > 0)
            {
                var acc = _partial.Clone(chunks[0].Partial!);
                for (var c = 1; c < first; c++)
                {
                    _partial.CombineInto(acc, chunks[c].Partial!);
                }

                before = acc;
            }
            else
            {
                first = 0; // no partials: the caller folds from the start
            }

            var rows = new List<(StructuralRow Row, long Weight, long Value)>();
            for (var c = first; c < chunks.Count && chunks[c].First <= to; c++)
            {
                var chunk = chunks[c];
                var body = chunk.Block.Body;
                for (var i = 0; i < chunk.Count; i++)
                {
                    rows.Add((body.Value(i), body.Weight(i), chunk.Values[i]));
                }
            }

            if (BlockTraceProfile.Enabled)
            {
                BlockTraceProfile.Section(14, t0, rows.Count);
            }

            return rows;
        }

        // A small partition: already ordered and consolidated — from the cache, or (on blocks) read
        // once into it: the tick's own rows when new, otherwise probed and consolidated by sort.
        var all = new List<(StructuralRow Row, long Weight, long Value)>();
        if (_small.TryGetValue(key, out var small))
        {
            all.Capacity = small.Count;
            foreach (var (row, weight) in small)
            {
                all.Add((row, weight, ValueOf(row)));
            }
        }
        else if (_cold is not null)
        {
            // Not cached: not touched since the cache was dropped, or emptied since.
            var raw = RecordOf(key);

            raw.Sort(_compare);
            foreach (var e in raw)
            {
                if (all.Count > 0 && Compare(all[^1], e) == 0)
                {
                    all[^1] = (all[^1].Row, all[^1].Weight + e.Weight, e.Value);
                }
                else
                {
                    all.Add(e);
                }
            }

            all.RemoveAll(e => e.Weight == 0);
            if (all.Count > 0 && all.Count <= _promoteAt)
            {
                var cached = new SortedDictionary<StructuralRow, long>(_rowOrder);
                foreach (var (row, weight, _) in all)
                {
                    cached[row] = weight;
                }

                _small[key] = cached;
                Interlocked.Add(ref _cacheBytes, CachedPartitionOverhead + ManagedSize.Row(key)
                    + (all.Count * (ManagedSize.MeanRow(all.Count, i => all[i].Row, sample: 4) + 48)));
            }
        }

        if (BlockTraceProfile.Enabled)
        {
            BlockTraceProfile.Section(12, t0, all.Count);
        }

        if (all.Count > _promoteAt)
        {
            var tPromote = BlockTraceProfile.Now();
            if (_cold is not null)
            {
                RetractColdKey(key);
            }
            else
            {
                _smallRows -= all.Count;
            }

            _small.Remove(key);
            _large[key] = Cut(all);
            if (BlockTraceProfile.Enabled)
            {
                BlockTraceProfile.Section(13, tPromote, all.Count);
            }
        }

        return all;
    }

    /// <summary>
    /// For positional (offset) windows: a partition's rows in order, consolidated. Of a large
    /// partition, the chunks overlapping [<paramref name="from"/>, <paramref name="to"/>] widened by
    /// whole chunks until at least <paramref name="marginRows"/> rows lie on each side, or the
    /// partition ends; of a small one, every row. <paramref name="atStart"/> and
    /// <paramref name="atEnd"/> say whether the rows read begin and end the partition.
    /// </summary>
    public List<(StructuralRow Row, long Weight, long Value)> ReadAround(
        StructuralRow key, long from, long to, long marginRows, out bool atStart, out bool atEnd)
    {
        EnsureLoaded(key);
        if (!_large.TryGetValue(key, out var chunks))
        {
            atStart = atEnd = true;
            return Read(key, long.MinValue, long.MaxValue, needPrefix: false, out _);
        }

        var first = FirstChunkReaching(chunks, from);
        var last = first;
        while (last + 1 < chunks.Count && chunks[last + 1].First <= to)
        {
            last++;
        }

        for (long margin = 0; first > 0 && margin < marginRows;)
        {
            margin += chunks[--first].Count;
        }

        for (long margin = 0; last + 1 < chunks.Count && margin < marginRows;)
        {
            margin += chunks[++last].Count;
        }

        atStart = first == 0;
        atEnd = last == chunks.Count - 1;
        var rows = new List<(StructuralRow Row, long Weight, long Value)>();
        for (var c = first; c <= last; c++)
        {
            var chunk = chunks[c];
            var body = chunk.Block.Body;
            for (var i = 0; i < chunk.Count; i++)
            {
                rows.Add((body.Value(i), body.Weight(i), chunk.Values[i]));
            }
        }

        return rows;
    }

    // A partition is moving to chunks: take what the record holds for it out (its raw entries,
    // negated), and drop its queued changes (the chunks hold its whole content).
    private void RetractColdKey(StructuralRow key)
    {
        _pending.Remove(key);
        if (!_keys.MayContain(key))
        {
            return;
        }

        var raw = new List<(StructuralRow Row, long Weight, long Value)>();
        var visitor = new Collect(raw, this);
        _cold!.Probe(key, ref visitor);
        if (raw.Count == 0)
        {
            return;
        }

        var retract = new IndexedZSetBuilder<StructuralRow, StructuralRow, Z64>();
        foreach (var (row, weight, _) in raw)
        {
            retract.Add(key, row, new Z64(-weight));
        }

        _cold.Integrate(retract.Build());
    }

    private readonly struct Collect(List<(StructuralRow Row, long Weight, long Value)> into, OrderedPartitionStore store) : ITraceVisitor
    {
        public void Row(StructuralRow value, Z64 weight) => into.Add((value, weight.Value, store.ValueOf(value)));

        public void Entry(ITraceBlock block, int index, Z64 weight)
        {
            var row = block.Value(index);
            into.Add((row, weight.Value, store.ValueOf(row)));
        }
    }

    // The first chunk whose last value is not below `from` (the chunk a change at `from` lands in).
    private static int FirstChunkReaching(List<Chunk> chunks, long from)
    {
        int lo = 0, hi = chunks.Count;
        while (lo < hi)
        {
            var mid = (lo + hi) >>> 1;
            if (chunks[mid].Last < from)
            {
                lo = mid + 1;
            }
            else
            {
                hi = mid;
            }
        }

        return Math.Min(lo, chunks.Count - 1);
    }

    // A small partition (if any) and a change to it, together past the threshold: the partition
    // leaves the managed map, and the sum becomes chunks.
    private void PromoteWith(StructuralRow key, List<(StructuralRow Row, long Weight, long Value)> change)
    {
        if (_cold is not null)
        {
            // The partition's rows so far (the cache's, which include queued changes, or else the
            // record's) join the change, and leave the record.
            if (_small.Remove(key, out var cached))
            {
                foreach (var (row, weight) in cached)
                {
                    change.Add((row, weight, ValueOf(row)));
                }
            }
            else
            {
                change.AddRange(RecordOf(key));
            }

            RetractColdKey(key);
            _keys.Add(key);
        }
        else if (_small.Remove(key, out var existing))
        {
            _smallRows -= existing.Count;
            foreach (var (row, weight) in existing)
            {
                change.Add((row, weight, ValueOf(row)));
            }
        }

        change.Sort(_compare);
        var merged = new List<(StructuralRow Row, long Weight, long Value)>(change.Count);
        foreach (var e in change)
        {
            if (merged.Count > 0 && Compare(merged[^1], e) == 0)
            {
                merged[^1] = (merged[^1].Row, merged[^1].Weight + e.Weight, e.Value);
            }
            else
            {
                merged.Add(e);
            }
        }

        merged.RemoveAll(e => e.Weight == 0);
        if (merged.Count > 0)
        {
            _large[key] = Cut(merged);
        }
    }

    // A large partition's change: each touched chunk is decoded, merged with its share of the
    // change (consolidating equal rows) and re-cut.
    private void ApplyLarge(StructuralRow key, List<(StructuralRow Row, long Weight, long Value)> delta)
    {
        var chunks = _large[key];
        delta.Sort(Compare);

        // Route by value: a value lands in the first chunk whose last value is not below it.
        var byChunk = new SortedDictionary<int, List<(StructuralRow Row, long Weight, long Value)>>();
        foreach (var e in delta)
        {
            var c = FirstChunkReaching(chunks, e.Value);
            if (!byChunk.TryGetValue(c, out var list))
            {
                byChunk[c] = list = new();
            }

            list.Add(e);
        }

        // Highest chunk first, so replacing one with several leaves the lower indexes valid.
        foreach (var (c, share) in byChunk.Reverse())
        {
            var chunk = chunks[c];
            var merged = new List<(StructuralRow Row, long Weight, long Value)>(chunk.Count + share.Count);
            var body = chunk.Block.Body;
            int i = 0, j = 0;
            while (i < chunk.Count || j < share.Count)
            {
                var a = i < chunk.Count ? (body.Value(i), body.Weight(i), chunk.Values[i]) : default;
                var cmp = i >= chunk.Count ? 1 : j >= share.Count ? -1 : Compare(a, share[j]);
                if (cmp < 0)
                {
                    merged.Add(a);
                    i++;
                }
                else if (cmp > 0)
                {
                    merged.Add(share[j++]);
                }
                else
                {
                    var w = a.Item2 + share[j].Weight;
                    if (w != 0)
                    {
                        merged.Add((a.Item1, w, a.Item3));
                    }

                    i++;
                    j++;
                }
            }

            // A row the change left at zero weight (only in the share) drops out here.
            merged.RemoveAll(e => e.Weight == 0);
            chunks.RemoveAt(c);
            chunks.InsertRange(c, Cut(merged));
        }

        if (chunks.Count == 0)
        {
            _large.Remove(key);
        }
    }

    // Sorted rows cut into chunks of about _chunkRows, never inside a peer group.
    private List<Chunk> Cut(List<(StructuralRow Row, long Weight, long Value)> sorted)
    {
        var chunks = new List<Chunk>();
        var start = 0;
        while (start < sorted.Count)
        {
            var end = Math.Min(sorted.Count, start + _chunkRows);
            if (sorted.Count - end < _chunkRows / 2)
            {
                end = sorted.Count; // no runt at the end
            }

            while (end < sorted.Count && sorted[end].Value == sorted[end - 1].Value)
            {
                end++;
            }

            chunks.Add(Build(sorted, start, end));
            start = end;
        }

        return chunks;
    }

    private Chunk Build(List<(StructuralRow Row, long Weight, long Value)> sorted, int start, int end)
    {
        var n = end - start;
        var keys = new StructuralRow[n];
        var rows = new StructuralRow[n];
        var weights = new long[n];
        var values = new long[n];
        var hashes = new int[n];
        object? partial = _partial?.NewAccumulator();
        for (var i = 0; i < n; i++)
        {
            var (row, weight, value) = sorted[start + i];
            keys[i] = NoKey;
            rows[i] = row;
            weights[i] = weight;
            values[i] = value;
            if (partial is not null)
            {
                _partial!.Add(partial, row, weight);
            }
        }

        return new Chunk(_chunkFormat.Encode(keys, rows, weights, hashes, n), values, partial);
    }

    // A scalable Bloom filter over partition-key hashes: reference-free bit arrays, a new one twice
    // as large once the newest is full; a key is absent only if every filter says so.
    private sealed class KeyFilter
    {
        private const int BitsPerKey = 10;
        private const int Probes = 4;
        private readonly List<(ulong[] Bits, long Capacity)> _filters = [(new ulong[(1024 * BitsPerKey + 63) / 64], 1024)];
        private long _inNewest;

        public void Add(StructuralRow key)
        {
            var (bits, capacity) = _filters[^1];
            if (_inNewest >= capacity)
            {
                capacity *= 2;
                bits = new ulong[(capacity * BitsPerKey + 63) / 64];
                _filters.Add((bits, capacity));
                _inNewest = 0;
            }

            var m = (ulong)bits.Length * 64;
            var (h1, h2) = Hashes(key);
            for (var i = 0UL; i < Probes; i++)
            {
                var bit = (h1 + (i * h2)) % m;
                bits[bit >> 6] |= 1UL << (int)(bit & 63);
            }

            _inNewest++;
        }

        public bool MayContain(StructuralRow key)
        {
            var (h1, h2) = Hashes(key);
            foreach (var (bits, _) in _filters)
            {
                var m = (ulong)bits.Length * 64;
                var all = true;
                for (var i = 0UL; i < Probes && all; i++)
                {
                    var bit = (h1 + (i * h2)) % m;
                    all = (bits[bit >> 6] & (1UL << (int)(bit & 63))) != 0;
                }

                if (all)
                {
                    return true;
                }
            }

            return false;
        }

        public void Write(System.IO.BinaryWriter w)
        {
            w.Write(_inNewest);
            w.Write(_filters.Count);
            foreach (var (bits, capacity) in _filters)
            {
                w.Write(capacity);
                w.Write(bits.Length);
                w.Write(System.Runtime.InteropServices.MemoryMarshal.AsBytes(bits.AsSpan()));
            }
        }

        public static KeyFilter Read(System.IO.BinaryReader r)
        {
            var f = new KeyFilter();
            f._filters.Clear();
            f._inNewest = r.ReadInt64();
            var n = r.ReadInt32();
            for (var i = 0; i < n; i++)
            {
                var capacity = r.ReadInt64();
                var bits = new ulong[r.ReadInt32()];
                r.ReadExactly(System.Runtime.InteropServices.MemoryMarshal.AsBytes(bits.AsSpan()));
                f._filters.Add((bits, capacity));
            }

            return f;
        }

        private static (ulong, ulong) Hashes(StructuralRow key)
        {
            var x = (ulong)(uint)key.GetHashCode() * 0x9E3779B97F4A7C15UL;
            x ^= x >> 29;
            return (x, (x >> 32) | 1);
        }
    }
}
