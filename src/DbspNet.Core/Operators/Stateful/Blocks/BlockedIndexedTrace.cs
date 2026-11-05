// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Buffers;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.IO;

namespace DbspNet.Core.Operators.Stateful.Blocks;

/// <summary>What a probe of a <see cref="BlockedIndexedTrace"/> hands back: managed entries as
/// rows, block entries in place, so the caller boxes only the cells it keeps.</summary>
internal interface ITraceVisitor
{
    /// <summary>An entry of a managed level.</summary>
    void Row(StructuralRow value, Z64 weight);

    /// <summary>Entry <paramref name="index"/> of <paramref name="block"/>.</summary>
    void Entry(ITraceBlock block, int index, Z64 weight);
}

/// <summary>
/// An indexed trace (structural key and value, <see cref="Z64"/>) held as an in-memory LSM of
/// levels, newest last for managed and newest first for runs (docs/design-flat-trace-state.md §2.3):
/// <list type="bullet">
/// <item><b>managed</b> — a tick's delta exactly as it arrived; integrating copies nothing;</item>
/// <item><b>run</b> — immutable, consolidated, reference-free <see cref="ITraceBlock"/>s in key-hash
/// order.</item>
/// </list>
/// </summary>
/// <remarks>
/// <para><b>Freeze and merge.</b> Once the managed levels hold <c>freezeEntries</c> entries, or more
/// than <c>maxManaged</c> levels wait, they are consolidated into one run, and runs merge while the
/// newest is at least a quarter of the next (geometric). In this first cut both happen on the
/// calling thread; the background merger is phase 3 step 3.</para>
/// <para><b>Probes are not consolidated.</b> A probe visits the key in every level, so a value at
/// +1 in one level and −1 in another is visited twice. That is exact for a bilinear consumer (a
/// join: the output builder cancels them), and wrong for anything that needs the group's net
/// weights — those use <see cref="Materialize"/>.</para>
/// <para><b>Frontier GC</b> is recorded, not applied: <see cref="DropKeysBelow"/> raises each
/// existing level's floor, a probe skips a level whose floor is above the key's monotone
/// projection, and freeze/merge drop those entries physically. A level integrated after the drop
/// starts with no floor, so the result is exactly the eager drop's: keys below the frontier lose
/// what they held then, and keep what arrives later.</para>
/// </remarks>
internal sealed class BlockedIndexedTrace
{
    // A managed level: an indexed delta, or (a Z-set trace, §3.7) a Z-set delta whose rows are
    // keys with the empty value.
    private sealed class Managed(IndexedZSet<StructuralRow, StructuralRow, Z64>? set, ZSet<StructuralRow, Z64>? rows, long entries)
    {
        public IndexedZSet<StructuralRow, StructuralRow, Z64>? Set { get; } = set;

        public ZSet<StructuralRow, Z64>? Rows { get; } = rows;

        public long Entries { get; } = entries;

        public long Floor { get; set; } = long.MinValue;

        public ZSet<StructuralRow, Z64> Group(StructuralRow key) =>
            Rows is { } rows
                ? rows.WeightOf(key) is { Value: not 0 } w ? ZSet<StructuralRow, Z64>.Inline(NoValue, w) : ZSet<StructuralRow, Z64>.Empty
                : Set!.GroupFor(key);

        /// <summary>Every entry, as (key, value, weight).</summary>
        public IEnumerable<(StructuralRow Key, StructuralRow Value, Z64 Weight)> AllEntries()
        {
            if (Rows is { } rows)
            {
                foreach (var (row, weight) in rows)
                {
                    yield return (row, NoValue, weight);
                }

                yield break;
            }

            foreach (var (key, group) in Set!)
            {
                foreach (var (value, weight) in group)
                {
                    yield return (key, value, weight);
                }
            }
        }
    }

    internal sealed class Run(ITraceBlock[] blocks, long entries)
    {
        private readonly uint[] _first = [.. blocks.Select(b => b.FirstHash)];
        private readonly uint[] _last = [.. blocks.Select(b => b.LastHash)];

        public ITraceBlock[] Blocks { get; } = blocks;

        public long Entries { get; } = entries;

        public long Floor { get; set; } = long.MinValue;

        /// <summary>The run file this run's blocks were written to (or restored from), once a
        /// checkpoint that wrote it has committed; a later checkpoint names it instead of writing
        /// it again. Runs are immutable, so it stays valid until the run is merged away.</summary>
        public (ITableFileSystem Store, string Path)? Persisted { get; set; }

        /// <summary>The run this one adopted the blocks of (§5.19): kept reachable while this run lives,
        /// since a spilled file lives as long as the run object the spiller tracked.</summary>
        public Run? AdoptedFrom { get; init; }

        /// <summary>The first block that can hold <paramref name="hash"/>, or −1. A giant group's
        /// hash can span consecutive blocks; the caller continues while the next block's first hash
        /// is still <paramref name="hash"/>.</summary>
        public int Find(uint hash)
        {
            int lo = 0, hi = _last.Length;
            while (lo < hi)
            {
                var mid = (lo + hi) >>> 1;
                if (_last[mid] < hash)
                {
                    lo = mid + 1;
                }
                else
                {
                    hi = mid;
                }
            }

            return lo < _first.Length && _first[lo] <= hash ? lo : -1;
        }

        /// <summary>The first hash of block <paramref name="b"/>.</summary>
        public uint FirstOf(int b) => _first[b];
    }

    private readonly ITraceBlockFormat _format;
    private readonly Func<StructuralRow, int> _hash;
    private readonly int _freezeEntries;
    private readonly int _maxManaged;
    private readonly int _blockEntries;
    private readonly int _blockBytes;

    // False for a spilling delta builder's trace (docs/design-bounded-memory.md §5.17): its runs are
    // kept as frozen, never merged — a reader merges them by hash instead.
    private readonly bool _compact;
    private readonly List<Managed> _managed = new();
    private readonly List<Run> _runs = new();
    private long _managedEntries;
    private Func<StructuralRow, long>? _monotoneKey;
    private IndexedZSet<StructuralRow, StructuralRow, Z64>? _materialized;

    /// <param name="format">The block encoding (key and value schema).</param>
    /// <param name="freezeEntries">Managed entries that trigger a freeze.</param>
    /// <param name="maxManaged">Managed levels allowed to wait before a freeze.</param>
    /// <param name="blockEntries">Entries per block, cut where the hash changes (a block can exceed it
    /// up to <paramref name="blockBytes"/>).</param>
    /// <param name="hash">The key hash (a test seam for forced collisions; default the row's own).</param>
    /// <param name="blockBytes">Encoded bytes at which a block is cut even inside one hash
    /// (default <see cref="BlockBytes"/>).</param>
    public BlockedIndexedTrace(
        ITraceBlockFormat format,
        int freezeEntries = 1 << 16,
        int maxManaged = 8,
        int blockEntries = 1024,
        Func<StructuralRow, int>? hash = null,
        int blockBytes = BlockBytes,
        bool compact = true)
    {
        _blockBytes = blockBytes;
        _compact = compact;
        ArgumentNullException.ThrowIfNull(format);
        ArgumentOutOfRangeException.ThrowIfLessThan(blockEntries, 1);
        _format = format;
        _freezeEntries = freezeEntries;
        _maxManaged = maxManaged;
        _blockEntries = blockEntries;
        _hash = hash ?? (static row => row.GetHashCode());
        _defaultHash = hash is null;
    }

    // Whether entries are ordered by the row's own hash (so another trace's runs can be adopted).
    private readonly bool _defaultHash;

    /// <summary>
    /// Adopt <paramref name="runs"/> — consolidated Z-set runs of a block-backed delta — as runs of this
    /// Z-set trace, without decoding or encoding a row (docs/design-bounded-memory.md §5.19): false, and
    /// nothing changes, unless their format reads as this trace's and both order by the row's hash.
    /// New run objects over the same blocks, each keeping its original reachable (a spilled file's
    /// owner); a floor set here never touches the delta's. The trace then compacts as after a freeze.
    /// </summary>
    internal bool TryAdoptRuns(IReadOnlyList<Run> runs, ITraceBlockFormat format, bool defaultHash)
    {
        if (!_defaultHash || !defaultHash || _format.ValueColumns != 0 || !_format.Adopts(format)
            || _managed.Any(l => l.Rows is null))
        {
            return false;
        }

        _materialized = null;
        foreach (var run in runs)
        {
            _runs.Insert(0, new Run(run.Blocks, run.Entries) { Persisted = run.Persisted, AdoptedFrom = run });
        }

        Compact();
        return true;
    }

    /// <summary>Whether entries are ordered by the row's own hash.</summary>
    internal bool OrdersByRowHash => _defaultHash;

    /// <summary>Forget every level (a restore replaces the state).</summary>
    public void Clear()
    {
        _managed.Clear();
        _runs.Clear();
        _managedEntries = 0;
        _materialized = null;
    }

    /// <summary>Entries held across every level (cancelling entries in separate levels both count):
    /// a cheap upper bound on the net entries.</summary>
    public long Entries => _managedEntries + _runs.Sum(r => r.Entries);

    /// <summary>Managed levels waiting to freeze.</summary>
    public int ManagedLevels => _managed.Count;

    /// <summary>Run sizes in entries, newest first.</summary>
    public IReadOnlyList<long> RunEntries => [.. _runs.Select(r => r.Entries)];

    /// <summary>Keys dropped physically by freeze and merge under frontier GC (a key dropped from
    /// two levels counts twice).</summary>
    public long DroppedKeys { get; private set; }

    /// <summary>Distinct keys with a non-zero net weight (materialises the trace).</summary>
    public int GroupCount => Materialize().GroupCount;

    /// <summary>Whether no level holds an entry (cancelled entries in separate levels still count).</summary>
    public bool IsEmpty => _managed.Count == 0 && _runs.Count == 0;

    /// <summary>Push <paramref name="delta"/> as a new managed level; the trace keeps it, so it must
    /// not be mutated afterwards (clone a pooled delta first).</summary>
    public void Integrate(IndexedZSet<StructuralRow, StructuralRow, Z64> delta)
    {
        ArgumentNullException.ThrowIfNull(delta);
        if (delta.IsEmpty)
        {
            return;
        }

        _materialized = null;
        long entries = 0;
        foreach (var (_, group) in delta)
        {
            entries += group.Count;
        }

        Push(new Managed(delta, null, entries));
    }

    /// <summary>The value of every entry of a Z-set trace (<see cref="IntegrateRows"/>): no columns.</summary>
    internal static readonly StructuralRow NoValue = new([]);

    /// <summary>Push a Z-set delta as a new managed level: each row a key with the empty value
    /// (<see cref="NoValue"/>; the format has no value columns). A trace takes Z-set or indexed
    /// deltas, never both. The trace keeps it, so it must not be mutated afterwards.</summary>
    public void IntegrateRows(ZSet<StructuralRow, Z64> delta)
    {
        ArgumentNullException.ThrowIfNull(delta);
        if (delta.IsEmpty)
        {
            return;
        }

        _materialized = null;
        Push(new Managed(null, delta, delta.Count));
    }

    /// <summary>
    /// Push <paramref name="count"/> entries as one new run, encoded straight from the caller's
    /// entries: sorted by key hash once and encoded once, with no managed level, so a large batch (an
    /// output sink's first write carries a whole view) is not frozen in chunks and merged up the
    /// levels (docs/design-flat-trace-state.md §3.10). The entries need not be ordered; equal
    /// (key, value) entries are summed and zero weights dropped. <paramref name="value"/> is called
    /// once per entry while its block is encoded, so values can be formed on the fly.
    /// </summary>
    public void IntegrateEntries(int count, Func<int, StructuralRow> key, Func<int, StructuralRow> value, Func<int, long> weight)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(value);
        ArgumentNullException.ThrowIfNull(weight);
        if (count == 0)
        {
            return;
        }

        var t0 = BlockTraceProfile.Now();
        _materialized = null;

        // Hash above index: one primitive sort orders by hash, ties by arrival. A large batch is cut
        // into hash ranges (the top bits) that sort and encode in parallel; a range's blocks are a
        // contiguous stretch of the run, so the run is their concatenation in range order. Visiting
        // rows in hash order is a cache miss per row, which is what the parallelism hides.
        var order = new ulong[count];
        var bits = count < ParallelEntries ? 0 : Math.Min(8, (int)Math.Ceiling(Math.Log2(Environment.ProcessorCount * 2)));
        var buckets = 1 << bits;
        var sizes = new int[buckets + 1];
        var hashes = new uint[count];
        Parallel.For(0, (count + HashChunk - 1) / HashChunk, c =>
        {
            for (int i = c * HashChunk, end = Math.Min(count, i + HashChunk); i < end; i++)
            {
                hashes[i] = (uint)_hash(key(i));
            }
        });

        foreach (var h in hashes)
        {
            sizes[bits == 0 ? 1 : (int)(h >> (32 - bits)) + 1]++;
        }

        for (var b = 0; b < buckets; b++)
        {
            sizes[b + 1] += sizes[b];
        }

        var next = sizes[..buckets];
        for (var i = 0; i < count; i++)
        {
            var h = hashes[i];
            order[next[bits == 0 ? 0 : (int)(h >> (32 - bits))]++] = ((ulong)h << 32) | (uint)i;
        }

        var tHashed = BlockTraceProfile.Now();
        var parts = new (List<ITraceBlock> Blocks, long Entries)[buckets];
        Parallel.For(0, buckets, b =>
        {
            Array.Sort(order, sizes[b], sizes[b + 1] - sizes[b]);
            parts[b] = BuildRange(order, sizes[b], sizes[b + 1], key, value, weight);
        });

        var blocks = new List<ITraceBlock>();
        long entries = 0;
        foreach (var (partBlocks, partEntries) in parts)
        {
            blocks.AddRange(partBlocks);
            entries += partEntries;
        }

        if (blocks.Count > 0)
        {
            _runs.Insert(0, new Run([.. blocks], entries));
        }

        if (BlockTraceProfile.Enabled)
        {
            BlockTraceProfile.Section(3, t0);
            BlockTraceProfile.Section(7, tHashed);
            BlockTraceProfile.Freeze(t0, entries);
        }

        Compact();
    }

    /// <summary>
    /// Entries whose keys are other blocks' entries and whose value is one BIGINT, each with weight
    /// +1, as one run (docs/design-bounded-memory.md §3h): sorted by the hash their source blocks
    /// already carry, cut into blocks by bytes and entries without splitting a hash, and copied
    /// without decoding. The
    /// entries must be distinct (key, value) pairs. False, with nothing changed, when the format
    /// cannot gather: integrate rows instead.
    /// </summary>
    internal bool IntegrateGathered(ITraceBlock[] sources, int[] indexes, long[] values)
    {
        var count = indexes.Length;
        if (count == 0)
        {
            return true;
        }

        var order = new ulong[count];
        for (var i = 0; i < count; i++)
        {
            order[i] = ((ulong)sources[i].Hashes[indexes[i]] << 32) | (uint)i;
        }

        Array.Sort(order);
        // Cut as a freeze cuts rows: once a block reaches the byte budget or the entry count, at the
        // next hash boundary (a hash is never split here).
        var blocks = new List<ITraceBlock>();
        var from = 0;
        while (from < count)
        {
            var to = from;
            long bytes = 0;
            while (to < count && (to == from || ((order[to] >> 32) == (order[to - 1] >> 32))
                || (bytes < _blockBytes && to - from < _blockEntries)))
            {
                var i = (int)(uint)order[to];
                bytes += _format.GatheredEntryBytes(sources[i], indexes[i]);
                to++;
            }

            var n = to - from;
            var src = new ITraceBlock[n];
            var idx = new int[n];
            var vals = new long[n];
            var weights = new long[n];
            var hashes = new int[n];
            for (var k = 0; k < n; k++)
            {
                var i = (int)(uint)order[from + k];
                src[k] = sources[i];
                idx[k] = indexes[i];
                vals[k] = values[i];
                weights[k] = 1;
                hashes[k] = (int)(order[from + k] >> 32);
            }

            if (_format.Gather(src, idx, vals, weights, hashes, n) is not { } block)
            {
                return false;
            }

            blocks.Add(block);
            from = to;
        }

        _materialized = null;
        _runs.Insert(0, new Run([.. blocks], count));
        Compact();
        return true;
    }

    // A batch below this many entries is sorted and encoded on the calling thread.
    private const int ParallelEntries = 1 << 16;

    private const int HashChunk = 1 << 14;

    // Entries order[from..to) (sorted, hash above index) as blocks.
    private (List<ITraceBlock> Blocks, long Entries) BuildRange(
        ulong[] order, int from, int to, Func<int, StructuralRow> key, Func<int, StructuralRow> value, Func<int, long> weight)
    {
        var run = new RunBuilder(this);
        long entries = 0;
        var keys = new List<StructuralRow>();
        var members = new List<List<int>>();
        for (var i = from; i < to;)
        {
            var hash = (uint)(order[i] >> 32);
            var j = i + 1;
            while (j < to && (uint)(order[j] >> 32) == hash)
            {
                j++;
            }

            if (j - i == 1)
            {
                var only = (int)(uint)order[i];
                if (weight(only) is var w && w != 0)
                {
                    run.Add(hash, key(only), value(only), w);
                    entries++;
                }
            }
            else
            {
                // A hash's entries: grouped by key (a run keeps a key's entries contiguous), and
                // equal values within a key summed (a run is consolidated).
                keys.Clear();
                members.Clear();
                for (var e = i; e < j; e++)
                {
                    var index = (int)(uint)order[e];
                    var k = key(index);
                    var g = keys.IndexOf(k);
                    if (g < 0)
                    {
                        keys.Add(k);
                        members.Add(new List<int>());
                        g = keys.Count - 1;
                    }

                    members[g].Add(index);
                }

                for (var g = 0; g < keys.Count; g++)
                {
                    var values = new Dictionary<StructuralRow, long>();
                    var arrival = new List<StructuralRow>();
                    foreach (var index in members[g])
                    {
                        var v = value(index);
                        if (values.TryGetValue(v, out var sum))
                        {
                            values[v] = sum + weight(index);
                        }
                        else
                        {
                            values[v] = weight(index);
                            arrival.Add(v);
                        }
                    }

                    foreach (var v in arrival)
                    {
                        if (values[v] != 0)
                        {
                            run.Add(hash, keys[g], v, values[v]);
                            entries++;
                        }
                    }
                }
            }

            i = j;
        }

        return run.Finish() is { } built ? ([.. built.Blocks], entries) : (new List<ITraceBlock>(), 0);
    }

    private void Push(Managed level)
    {
        _managed.Add(level);
        _managedEntries += level.Entries;
        if (_managedEntries >= _freezeEntries || _managed.Count > _maxManaged)
        {
            Freeze();
        }
    }

    /// <summary>Throws unless every run is consolidated (no repeated key and value, no zero weight)
    /// and in ascending hash order across its blocks — the invariants freeze and merge keep, which
    /// probes and <see cref="Materialize"/> cannot see.</summary>
    internal void CheckRuns()
    {
        foreach (var run in _runs)
        {
            var seen = new HashSet<(StructuralRow, StructuralRow)>();
            uint? previous = null;
            foreach (var block in Bodies(run.Blocks))
            {
                var hashes = block.Hashes;
                if (previous is { } p && hashes.Length > 0 && hashes[0] < p)
                {
                    throw new InvalidOperationException($"hash {hashes[0]} descends across blocks");
                }

                for (var i = 0; i < block.Count; i++)
                {
                    if (i > 0 && hashes[i] < hashes[i - 1])
                    {
                        throw new InvalidOperationException("hashes descend within a block");
                    }

                    if (hashes[i] != (uint)_hash(block.Key(i)))
                    {
                        throw new InvalidOperationException("an entry's hash is not its key's");
                    }

                    if (block.Weight(i) == 0 || !seen.Add((block.Key(i), block.Value(i))))
                    {
                        throw new InvalidOperationException($"run not consolidated at {block.Key(i)} {block.Value(i)}");
                    }
                }

                previous = hashes.Length > 0 ? hashes[^1] : previous;
            }
        }
    }

    /// <summary>Entries in managed levels, waiting to freeze (the residency census).</summary>
    internal long ManagedEntries => _managedEntries;

    /// <summary>Runs, newest first (checkpoints).</summary>
    internal IReadOnlyList<Run> Runs => _runs;

    /// <summary>The block format of this trace's runs.</summary>
    internal ITraceBlockFormat Format => _format;

    /// <summary>Get the trace ready to be written as runs (docs/design-flat-trace-state.md §2.7):
    /// freeze the managed levels, and rewrite any run with a GC floor so that what it holds is what
    /// the trace holds. Returns the runs, newest first.</summary>
    internal IReadOnlyList<Run> PrepareCheckpoint()
    {
        Freeze();
        for (var r = 0; r < _runs.Count; r++)
        {
            if (_runs[r].Floor == long.MinValue)
            {
                continue;
            }

            var filtered = Merge(_runs[r], new Run([], 0));
            if (filtered is null)
            {
                _runs.RemoveAt(r--);
            }
            else
            {
                _runs[r] = filtered;
            }
        }

        return [.. _runs];
    }

    /// <summary>Adopt a restored run as the newest level (a chain's runs in order, base first).</summary>
    internal void AdoptRun(ITraceBlock[] blocks, long entries, (ITableFileSystem Store, string Path)? persisted)
    {
        ArgumentNullException.ThrowIfNull(blocks);
        if (blocks.Length == 0)
        {
            return;
        }

        _materialized = null;
        _runs.Insert(0, new Run(blocks, entries) { Persisted = persisted });
    }

    private static IEnumerable<ITraceBlock> Bodies(ITraceBlock[] blocks)
    {
        foreach (var b in blocks)
        {
            yield return b.Body;
        }
    }

    /// <summary>Replace <paramref name="run"/> by the same entries read from its run file
    /// (docs/design-bounded-memory.md §3.1): <paramref name="blocks"/> are the file's blocks, which
    /// can drop their bytes and read them again. False if the run is no longer a level (merged
    /// away since the checkpoint wrote it). Called between steps, like a checkpoint.</summary>
    internal Run? Rebind(Run run, ITraceBlock[] blocks)
    {
        ArgumentNullException.ThrowIfNull(run);
        ArgumentNullException.ThrowIfNull(blocks);
        var at = _runs.IndexOf(run);
        if (at < 0 || blocks.Length == 0)
        {
            return null;
        }

        var rebound = new Run(blocks, run.Entries) { Floor = run.Floor, Persisted = run.Persisted };
        _runs[at] = rebound;
        return rebound;
    }

    /// <summary>Visit every entry of <paramref name="key"/> in every level (unconsolidated).</summary>
    public void Probe<TVisitor>(StructuralRow key, ref TVisitor visitor)
        where TVisitor : struct, ITraceVisitor
    {
        var t0 = BlockTraceProfile.Now();
        ProbeCore(key, ref visitor);
        if (BlockTraceProfile.Enabled)
        {
            BlockTraceProfile.Probe(t0);
        }
    }

    private void ProbeCore<TVisitor>(StructuralRow key, ref TVisitor visitor)
        where TVisitor : struct, ITraceVisitor
    {
        var projected = _monotoneKey is { } mk ? mk(key) : long.MaxValue;
        foreach (var level in _managed)
        {
            if (projected < level.Floor)
            {
                continue;
            }

            if (level.Rows is { } rows)
            {
                if (rows.WeightOf(key) is { Value: not 0 } w)
                {
                    visitor.Row(NoValue, w);
                }

                continue;
            }

            foreach (var (value, weight) in level.Set!.GroupFor(key))
            {
                visitor.Row(value, weight);
            }
        }

        if (_runs.Count == 0)
        {
            return;
        }

        var hash = (uint)_hash(key);
        foreach (var run in _runs)
        {
            if (projected < run.Floor)
            {
                continue;
            }

            var b = run.Find(hash);
            if (b < 0)
            {
                continue;
            }

            for (; b < run.Blocks.Length && run.FirstOf(b) <= hash; b++)
            {
                var block = run.Blocks[b].Body;
                var hashes = block.Hashes;
                for (var i = block.LowerBound(hash); i < hashes.Length && hashes[i] == hash; i++)
                {
                    if (block.KeyEquals(i, key))
                    {
                        visitor.Entry(block, i, new Z64(block.Weight(i)));
                    }
                }
            }
        }
    }

    /// <summary>The key's group as a view that decodes nothing until asked to enumerate
    /// (<see cref="BlockedGroupView"/>): its managed groups and its entry ranges per run.</summary>
    public BlockedGroupView GroupView(StructuralRow key)
    {
        ArgumentNullException.ThrowIfNull(key);
        var projected = _monotoneKey is { } mk ? mk(key) : long.MaxValue;
        var managed = new List<ZSet<StructuralRow, Z64>>();
        foreach (var level in _managed)
        {
            if (projected >= level.Floor && level.Group(key) is { IsEmpty: false } group)
            {
                managed.Add(group);
            }
        }

        var ranges = new List<(ITraceBlock, int, int)>();
        if (_runs.Count > 0)
        {
            var hash = (uint)_hash(key);
            foreach (var run in _runs)
            {
                if (projected < run.Floor || run.Find(hash) is var b && b < 0)
                {
                    continue;
                }

                for (; b < run.Blocks.Length && run.FirstOf(b) <= hash; b++)
                {
                    var block = run.Blocks[b].Body;
                    var hashes = block.Hashes;
                    var from = block.LowerBound(hash);
                    var to = from;
                    while (to < hashes.Length && hashes[to] == hash)
                    {
                        to++;
                    }

                    if (to > from)
                    {
                        ranges.Add((block, from, to));
                    }
                }
            }
        }

        return new BlockedGroupView(key, managed, ranges);
    }

    /// <summary>Drop every key whose <paramref name="monotoneKey"/> projection is below
    /// <paramref name="threshold"/> — recorded as level floors, applied on probe and at
    /// freeze/merge (see the remarks).</summary>
    public void DropKeysBelow(long threshold, Func<StructuralRow, long> monotoneKey)
    {
        ArgumentNullException.ThrowIfNull(monotoneKey);
        _monotoneKey = monotoneKey;
        _materialized = null;
        foreach (var level in _managed)
        {
            level.Floor = Math.Max(level.Floor, threshold);
        }

        foreach (var run in _runs)
        {
            run.Floor = Math.Max(run.Floor, threshold);
        }
    }

    /// <summary>The trace's net contents as an <see cref="IndexedZSet{TKey,TValue,TWeight}"/>
    /// (snapshot, metrics, tests). Cached until the next change.</summary>
    public IndexedZSet<StructuralRow, StructuralRow, Z64> Materialize() =>
        _materialized ??= MaterializeUncached();

    /// <summary>As <see cref="Materialize"/>, without caching the result: for a one-off whole read
    /// (a full checkpoint write) that must not leave a decoded copy of the trace alive (§3.13).</summary>
    public IndexedZSet<StructuralRow, StructuralRow, Z64> MaterializeUncached()
    {
        if (_materialized is { } cached)
        {
            return cached;
        }

        var builder = new IndexedZSetBuilder<StructuralRow, StructuralRow, Z64>();
        foreach (var level in _managed)
        {
            foreach (var (key, value, weight) in level.AllEntries())
            {
                if (!Dropped(key, level.Floor))
                {
                    builder.Add(key, value, weight);
                }
            }
        }

        foreach (var run in _runs)
        {
            foreach (var block in Bodies(run.Blocks))
            {
                StructuralRow? key = null;
                for (var i = 0; i < block.Count; i++)
                {
                    key = key is not null && block.KeyEquals(i, key) ? key : block.Key(i);
                    if (!Dropped(key, run.Floor))
                    {
                        builder.Add(key, block.Value(i), new Z64(block.Weight(i)));
                    }
                }
            }
        }

        return builder.Build();
    }

    /// <summary>A Z-set trace's net contents (<see cref="IntegrateRows"/>): every row with a non-zero
    /// weight. Not cached: the caller decides how long to hold it.</summary>
    public ZSet<StructuralRow, Z64> MaterializeRows()
    {
        var builder = new ZSetBuilder<StructuralRow, Z64>();
        foreach (var level in _managed)
        {
            foreach (var (row, weight) in level.Rows ?? throw new InvalidOperationException("not a Z-set trace"))
            {
                if (!Dropped(row, level.Floor))
                {
                    builder.Add(row, weight);
                }
            }
        }

        foreach (var run in _runs)
        {
            foreach (var block in Bodies(run.Blocks))
            {
                for (var i = 0; i < block.Count; i++)
                {
                    var row = block.Key(i);
                    if (!Dropped(row, run.Floor))
                    {
                        builder.Add(row, new Z64(block.Weight(i)));
                    }
                }
            }
        }

        return builder.Build();
    }

    /// <summary>A Z-set trace's net contents streamed from its runs in hash order, consolidated across
    /// runs without forming the whole set; null while managed levels wait (the caller materialises).</summary>
    /// <summary>
    /// Every entry with its consolidated weight, as (block, index, weight), in hash order, without
    /// decoding (docs/design-bounded-memory.md §3h): an entry whose hash only one run holds passes
    /// through; the entries of a hash several runs hold are decoded only to sum equal rows, and the
    /// first of each set of equals stands for them. Zero sums are dropped. Null when a managed level
    /// waits or a run carries a GC floor (rows to drop are found by decoding): read rows instead.
    /// </summary>
    internal IEnumerable<(ITraceBlock Block, int Index, long Weight)>? StreamEntries()
    {
        if (_runs.Any(r => r.Floor != long.MinValue) || _managed.Any(l => l.Floor != long.MinValue || l.Rows is null))
        {
            return null;
        }

        // Rows not yet frozen are encoded into one scratch run, as a freeze would, but the trace is
        // left as it is: a reader must not change what the next tick or checkpoint sees.
        if (_managed.Count == 0)
        {
            return StreamEntriesCore([.. _runs]);
        }

        var pending = new ZSetBuilder<StructuralRow, Z64>();
        foreach (var level in _managed)
        {
            foreach (var (row, weight) in level.Rows!)
            {
                pending.Add(row, weight);
            }
        }

        var rows = pending.Build().Where(e => e.Value.Value != 0).Select(e => (Hash: (uint)_hash(e.Key), e.Key, e.Value.Value)).ToArray();
        Array.Sort(rows, static (a, b) => a.Hash.CompareTo(b.Hash));
        var builder = new RunBuilder(this);
        foreach (var (hash, row, weight) in rows)
        {
            builder.Add(hash, row, NoValue, weight);
        }

        return builder.Finish() is { } scratch ? StreamEntriesCore([scratch, .. _runs]) : StreamEntriesCore([.. _runs]);
    }

    private static IEnumerable<(ITraceBlock Block, int Index, long Weight)> StreamEntriesCore(Run[] runs)
    {
        var cursors = runs.Select(r => new Cursor(r)).ToArray();
        var group = new List<(ITraceBlock Block, int Index, StructuralRow Row, long Weight)>();
        while (true)
        {
            var hash = uint.MaxValue;
            var any = false;
            var sources = 0;
            foreach (var c in cursors)
            {
                if (!c.Done && (!any || c.Hash < hash))
                {
                    hash = c.Hash;
                    any = true;
                }
            }

            if (!any)
            {
                yield break;
            }

            foreach (var c in cursors)
            {
                sources += !c.Done && c.Hash == hash ? 1 : 0;
            }

            if (sources == 1)
            {
                var c = cursors.First(c => !c.Done && c.Hash == hash);
                while (!c.Done && c.Hash == hash)
                {
                    var w = c.Block.Weight(c.Index);
                    if (w != 0)
                    {
                        yield return (c.Block, c.Index, w);
                    }

                    c.Advance();
                }

                continue;
            }

            group.Clear();
            foreach (var c in cursors)
            {
                while (!c.Done && c.Hash == hash)
                {
                    group.Add((c.Block, c.Index, c.Block.Key(c.Index), c.Block.Weight(c.Index)));
                    c.Advance();
                }
            }

            for (var i = 0; i < group.Count; i++)
            {
                if (group[i].Row is not { } row)
                {
                    continue;
                }

                var sum = group[i].Weight;
                for (var j = i + 1; j < group.Count; j++)
                {
                    if (group[j].Row is { } other && other.Equals(row))
                    {
                        sum += group[j].Weight;
                        group[j] = (group[j].Block, group[j].Index, null!, 0);
                    }
                }

                if (sum != 0)
                {
                    yield return (group[i].Block, group[i].Index, sum);
                }
            }
        }
    }

    public IEnumerable<KeyValuePair<StructuralRow, Z64>>? StreamRows() =>
        _managed.Count > 0 ? null : StreamRunsCore([.. _runs]);

    private IEnumerable<KeyValuePair<StructuralRow, Z64>> StreamRunsCore(Run[] runs)
    {
        var cursors = runs.Select(r => new Cursor(r)).ToArray();
        var decoded = new (ITraceBlock? Block, StructuralRow[] Rows)[cursors.Length];
        var group = new List<(StructuralRow Key, StructuralRow Value, long Weight)>();
        while (true)
        {
            var hash = uint.MaxValue;
            var any = false;
            foreach (var c in cursors)
            {
                if (!c.Done && (!any || c.Hash < hash))
                {
                    hash = c.Hash;
                    any = true;
                }
            }

            if (!any)
            {
                yield break;
            }

            group.Clear();
            var sources = 0;
            for (var r = 0; r < cursors.Length; r++)
            {
                var c = cursors[r];
                if (c.Done || c.Hash != hash)
                {
                    continue;
                }

                sources++;
                while (!c.Done && c.Hash == hash)
                {
                    // A block is decoded whole on entry, a column at a time, its strings aliasing the
                    // block's bytes: what the Arrow write that reads these rows reads fastest.
                    if (!ReferenceEquals(decoded[r].Block, c.Block))
                    {
                        decoded[r] = (c.Block, c.Block.KeysAliased());
                    }

                    var row = decoded[r].Rows[c.Index];
                    if (!Dropped(row, c.Run.Floor))
                    {
                        group.Add((row, NoValue, c.Block.Weight(c.Index)));
                    }

                    c.Advance();
                }
            }

            if (sources > 1)
            {
                Consolidate(group);
            }

            foreach (var (row, _, weight) in group)
            {
                yield return new KeyValuePair<StructuralRow, Z64>(row, new Z64(weight));
            }
        }
    }

    /// <summary>Consolidate the managed levels into one run, then merge runs geometrically.</summary>
    public void Freeze()
    {
        if (_managed.Count == 0)
        {
            return;
        }

        var t0 = BlockTraceProfile.Now();
        if (_managed[0].Rows is not null)
        {
            FreezeRows(t0);
            return;
        }

        // One level with no floor is already consolidated: freeze it as it is. Otherwise sum the
        // levels (dropping what their floors cover) first.
        IndexedZSet<StructuralRow, StructuralRow, Z64> source;
        if (_managed.Count == 1 && _managed[0].Floor == long.MinValue)
        {
            source = _managed[0].Set!;
        }
        else
        {
            var builder = new IndexedZSetBuilder<StructuralRow, StructuralRow, Z64>();
            var dropped = new HashSet<StructuralRow>();
            foreach (var level in _managed)
            {
                foreach (var (key, group) in level.Set!)
                {
                    if (Dropped(key, level.Floor))
                    {
                        dropped.Add(key);
                        continue;
                    }

                    foreach (var (value, weight) in group)
                    {
                        builder.Add(key, value, weight);
                    }
                }
            }

            DroppedKeys += dropped.Count;
            source = builder.Build();
        }

        _managed.Clear();
        _managedEntries = 0;

        // Groups in key-hash order (pooled arrays: a large freeze would otherwise put two arrays
        // on the large-object heap), each group's entries streamed into blocks.
        var count = source.GroupCount;
        var hashes = ArrayPool<uint>.Shared.Rent(count);
        var groups = ArrayPool<KeyValuePair<StructuralRow, ZSet<StructuralRow, Z64>>>.Shared.Rent(count);
        long entries = 0;
        try
        {
            var g = 0;
            foreach (var group in source)
            {
                hashes[g] = (uint)_hash(group.Key);
                groups[g++] = group;
            }

            Array.Sort(hashes, groups, 0, g);
            var run = new RunBuilder(this);
            for (var i = 0; i < g; i++)
            {
                var key = groups[i].Key;
                foreach (var (value, weight) in groups[i].Value)
                {
                    run.Add(hashes[i], key, value, weight.Value);
                    entries++;
                }
            }

            if (run.Finish() is { } frozen)
            {
                _runs.Insert(0, frozen);
            }
        }
        finally
        {
            ArrayPool<KeyValuePair<StructuralRow, ZSet<StructuralRow, Z64>>>.Shared.Return(groups, clearArray: true);
            ArrayPool<uint>.Shared.Return(hashes);
        }

        if (BlockTraceProfile.Enabled)
        {
            BlockTraceProfile.Freeze(t0, entries);
        }

        Compact();
    }

    // Freeze for a Z-set trace: the levels' rows summed (unless one level, unfloored, already is),
    // sorted by hash, each a key with the empty value.
    private void FreezeRows(long t0)
    {
        ZSet<StructuralRow, Z64> source;
        if (_managed.Count == 1 && _managed[0].Floor == long.MinValue)
        {
            source = _managed[0].Rows!;
        }
        else
        {
            var builder = new ZSetBuilder<StructuralRow, Z64>();
            var dropped = new HashSet<StructuralRow>();
            foreach (var level in _managed)
            {
                foreach (var (row, weight) in level.Rows!)
                {
                    if (Dropped(row, level.Floor))
                    {
                        dropped.Add(row);
                    }
                    else
                    {
                        builder.Add(row, weight);
                    }
                }
            }

            DroppedKeys += dropped.Count;
            source = builder.Build();
        }

        _managed.Clear();
        _managedEntries = 0;

        var count = source.Count;
        var hashes = ArrayPool<uint>.Shared.Rent(count);
        var rows = ArrayPool<KeyValuePair<StructuralRow, Z64>>.Shared.Rent(count);
        var g = 0;
        try
        {
            foreach (var entry in source)
            {
                if (entry.Value.Value == 0)
                {
                    continue;
                }

                hashes[g] = (uint)_hash(entry.Key);
                rows[g++] = entry;
            }

            Array.Sort(hashes, rows, 0, g);
            var run = new RunBuilder(this);
            for (var i = 0; i < g; i++)
            {
                run.Add(hashes[i], rows[i].Key, NoValue, rows[i].Value.Value);
            }

            if (run.Finish() is { } frozen)
            {
                _runs.Insert(0, frozen);
            }
        }
        finally
        {
            ArrayPool<KeyValuePair<StructuralRow, Z64>>.Shared.Return(rows, clearArray: true);
            ArrayPool<uint>.Shared.Return(hashes);
        }

        if (BlockTraceProfile.Enabled)
        {
            BlockTraceProfile.Freeze(t0, g);
        }

        Compact();
    }

    private bool Dropped(StructuralRow key, long floor) =>
        floor != long.MinValue && _monotoneKey!(key) < floor;

    // Merge while the newest run is at least a quarter of the next: about log4 levels.
    private void Compact()
    {
        while (_compact && _runs.Count >= 2 && _runs[0].Entries * 4 >= _runs[1].Entries)
        {
            var merged = Merge(_runs[0], _runs[1]);
            _runs.RemoveRange(0, 2);
            if (merged is not null)
            {
                _runs.Insert(0, merged);

                // A cascade of merges (adopted runs, §5.19) spills each output as it is made, so they
                // do not all wait in memory for the cascade to end.
                if (_runs.Count >= 2 && _runs[0].Entries * 4 >= _runs[1].Entries)
                {
                    BlockAccess.Spiller?.AfterCompact(this);
                }
            }
        }

        // Mid-batch spilling (docs/design-bounded-memory.md §5.9): large runs to files, evictable.
        BlockAccess.Spiller?.AfterCompact(this);
    }

    // A two-way merge in hash order. Entries of one hash from both runs are consolidated by key and
    // value (weights summed, zeros dropped); entries under a floor are dropped.
    private Run? Merge(Run newer, Run older)
    {
        var t0 = BlockTraceProfile.Now();
        var maintenance = BlockAccess.InMaintenance;
        BlockAccess.InMaintenance = true;
        var m = StepSpillProfile.Enabled ? StepSpillProfile.Begin() : default;
        try
        {
            return MergeCore(newer, older);
        }
        finally
        {
            if (StepSpillProfile.Enabled)
            {
                StepSpillProfile.End(StepSpillProfile.RunMerge, m);
            }

            BlockAccess.InMaintenance = maintenance;
            if (BlockTraceProfile.Enabled)
            {
                BlockTraceProfile.Merge(t0, newer.Entries + older.Entries);
            }
        }
    }

    private Run? MergeCore(Run newer, Run older)
    {
        var a = new Cursor(newer);
        var b = new Cursor(older);
        var output = new RunBuilder(this);
        var group = new List<(StructuralRow Key, StructuralRow Value, long Weight)>();
        var dropped = 0L;
        while (!a.Done || !b.Done)
        {
            var hash = a.Done ? b.Hash : b.Done ? a.Hash : Math.Min(a.Hash, b.Hash);
            group.Clear();
            var sources = 0;
            var droppedHere = false;
            foreach (var cursor in new[] { a, b })
            {
                if (cursor.Done || cursor.Hash != hash)
                {
                    continue;
                }

                sources++;
                StructuralRow? key = null;
                StructuralRow? lastDropped = null;
                while (!cursor.Done && cursor.Hash == hash)
                {
                    var (block, i) = (cursor.Block, cursor.Index);
                    key = key is not null && block.KeyEquals(i, key) ? key : block.Key(i);
                    if (Dropped(key, cursor.Run.Floor))
                    {
                        if (!ReferenceEquals(key, lastDropped))
                        {
                            dropped++;
                            lastDropped = key;
                        }

                        droppedHere = true;
                    }
                    else
                    {
                        group.Add((key, block.Value(i), block.Weight(i)));
                    }

                    cursor.Advance();
                }
            }

            if (sources > 1 || droppedHere)
            {
                Consolidate(group);
            }

            foreach (var (key, value, weight) in group)
            {
                output.Add(hash, key, value, weight);
            }
        }

        DroppedKeys += dropped;
        return output.Finish();
    }

    // Sum equal (key, value) entries in place, keeping first-seen order; drop zero weights.
    private static void Consolidate(List<(StructuralRow Key, StructuralRow Value, long Weight)> group)
    {
        var at = new Dictionary<(StructuralRow, StructuralRow), int>(group.Count);
        var kept = 0;
        for (var i = 0; i < group.Count; i++)
        {
            var (key, value, weight) = group[i];
            if (at.TryGetValue((key, value), out var j))
            {
                group[j] = (group[j].Key, group[j].Value, group[j].Weight + weight);
            }
            else
            {
                at[(key, value)] = kept;
                group[kept++] = group[i];
            }
        }

        group.RemoveRange(kept, group.Count - kept);
        group.RemoveAll(static e => e.Weight == 0);
    }

    private sealed class Cursor
    {
        private int _block;

        public Cursor(Run run)
        {
            Run = run;
            Skip();
        }

        public Run Run { get; }

        public bool Done => _block >= Run.Blocks.Length;

        // The current block's body, held while the cursor is on it.
        private ITraceBlock? _body;
        private int _bodyOf = -1;

        public ITraceBlock Block
        {
            get
            {
                if (_bodyOf != _block)
                {
                    _body = Run.Blocks[_block].Body;
                    _bodyOf = _block;
                }

                return _body!;
            }
        }

        public int Index { get; private set; }

        public uint Hash => Block.Hashes[Index];

        public void Advance()
        {
            Index++;
            Skip();
        }

        private void Skip()
        {
            while (_block < Run.Blocks.Length && Index >= Run.Blocks[_block].Count)
            {
                _block++;
                Index = 0;
            }
        }
    }

    /// <summary>Encoded bytes at which a block is cut: below the 85,000-byte large-object-heap
    /// threshold. LOH blocks made a 12-worker freeze six times slower than a single thread's, and
    /// ivm-bench's giant groups made most blocks LOH-sized until a hash could span blocks (§3.1).</summary>
    internal const int BlockBytes = 60_000;

    // Cuts entries (ascending hash) into blocks: after blockEntries entries where the hash changes,
    // and after the byte budget anywhere — a giant group's hash then spans consecutive blocks.
    private sealed class RunBuilder(BlockedIndexedTrace trace)
    {
        private long _bytes;
        private readonly List<ITraceBlock> _blocks = new();
        private StructuralRow[] _keys = new StructuralRow[64];
        private StructuralRow[] _values = new StructuralRow[64];
        private long[] _weights = new long[64];
        private int[] _hashes = new int[64];
        private int _count;
        private long _entries;

        // Whether the buffered block continues a hash from the previous block (a giant group).
        private bool _spanning;

        private static bool NoSpanAlign => !BlockAccess.AlignSpans;

        public void Add(uint hash, StructuralRow key, StructuralRow value, long weight)
        {
            if (_count > 0)
            {
                var last = (uint)_hashes[_count - 1];
                if (_spanning && last != hash && !NoSpanAlign)
                {
                    // A spanning hash's last block holds only it (docs/design-bounded-memory.md §3.5).
                    Flush();
                }
                else if (_bytes >= trace._blockBytes)
                {
                    var start = _count;
                    while (last == hash && start > 0 && (uint)_hashes[start - 1] == hash)
                    {
                        start--;
                    }

                    if (last == hash && start > 0 && !NoSpanAlign)
                    {
                        // The hash is about to span blocks: the entries before it make a block of
                        // their own, so the hash's blocks hold only it and a probe for a neighbour
                        // never reads the giant group (in memory, nor once written as one file block).
                        FlushPrefix(start);
                    }
                    else
                    {
                        Flush();
                        _spanning = last == hash;
                    }
                }
                else if (_count >= trace._blockEntries && last != hash)
                {
                    Flush();
                }
            }

            _bytes += trace._format.EntryBytes(key, value);

            if (_count == _keys.Length)
            {
                Array.Resize(ref _keys, _count * 2);
                Array.Resize(ref _values, _count * 2);
                Array.Resize(ref _weights, _count * 2);
                Array.Resize(ref _hashes, _count * 2);
            }

            _keys[_count] = key;
            _values[_count] = value;
            _weights[_count] = weight;
            _hashes[_count] = (int)hash;
            _count++;
            _entries++;
        }

        public Run? Finish()
        {
            Flush();
            return _blocks.Count == 0 ? null : new Run([.. _blocks], _entries);
        }

        // Flush entries [0, n) as a block and keep the rest buffered, at the front.
        private void FlushPrefix(int n)
        {
            var rest = _count - n;
            var keys = _keys[n.._count];
            var values = _values[n.._count];
            var weights = _weights[n.._count];
            var hashes = _hashes[n.._count];
            _count = n;
            Flush();
            Array.Copy(keys, _keys, rest);
            Array.Copy(values, _values, rest);
            Array.Copy(weights, _weights, rest);
            Array.Copy(hashes, _hashes, rest);
            _count = rest;
            for (var i = 0; i < rest; i++)
            {
                _bytes += trace._format.EntryBytes(_keys[i], _values[i]);
            }
        }

        private void Flush()
        {
            _spanning = false;
            if (_count == 0)
            {
                return;
            }

            var t0 = BlockTraceProfile.Now();
            _blocks.Add(trace._format.Encode(_keys, _values, _weights, _hashes, _count));
            if (BlockTraceProfile.Enabled)
            {
                BlockTraceProfile.Encode(t0);
            }
            Array.Clear(_keys, 0, _count);
            Array.Clear(_values, 0, _count);
            _count = 0;
            _bytes = 0;
        }
    }
}
