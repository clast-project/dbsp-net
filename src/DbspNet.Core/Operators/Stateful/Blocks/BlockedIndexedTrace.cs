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

    /// <summary>Runs written to a file as they were built (§5.23), since start (diagnostics).</summary>
    internal static long StreamedRuns;

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

        /// <summary>The spilled file this run's blocks read, if any (§5.20): a trace listing the run
        /// holds it, and lets it go when the run leaves its levels.</summary>
        public RunFileLease? Lease { get; set; }

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
    internal bool TryAdoptRuns(IReadOnlyList<Run> runs, ITraceBlockFormat format, bool defaultHash, bool rows = true)
    {
        // A trace holds Z-set levels or indexed levels, never both (§5.22: indexed deltas adopt too).
        if (!_defaultHash || !defaultHash || !_format.Adopts(format)
            || (rows ? _format.ValueColumns != 0 || _managed.Any(l => l.Rows is null) : _managed.Any(l => l.Set is null)))
        {
            return false;
        }

        _materialized = null;
        var adopted = runs.Select(run => new Run(run.Blocks, run.Entries) { Persisted = run.Persisted }).ToList();

        // Many runs (a delta's chunks) merge in ONE k-way pass, not a pairwise cascade: the cascade wrote
        // every row ~log2(k) times, each output spilled, and filled a disk on TPC-DS (§5.19). The merge
        // reads the delta's files now, while the delta holds them; the trace keeps only its output.
        if (adopted.Count > 2)
        {
            var merged = MergeMany(adopted);
            adopted = merged is null ? [] : [merged];
        }
        else
        {
            // Runs kept as they are read the delta's files: the trace holds them too (§5.20).
            for (var i = 0; i < adopted.Count; i++)
            {
                if (runs[i].Lease is { } lease)
                {
                    adopted[i].Lease = lease;
                    lease.Hold(this);
                }
            }
        }

        foreach (var run in adopted)
        {
            _runs.Insert(0, run);
        }

        Compact();
        return true;
    }

    /// <summary>Whether the trace merges its runs (state), as opposed to a block-backed delta's own trace.</summary>
    internal bool Compacts => _compact;

    /// <summary>Whether entries are ordered by the row's own hash.</summary>
    internal bool OrdersByRowHash => _defaultHash;

    /// <summary>Forget every level (a restore replaces the state).</summary>
    public void Clear()
    {
        _managed.Clear();
        foreach (var run in _runs)
        {
            Retire(run);
        }

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

        // A block-backed delta (D4, docs/design-bounded-memory.md §5.22) is sorted, consolidated runs:
        // adopt them rather than decode it into a level and re-encode it.
        if (delta.External is BlockedGroups blocked && blocked.AdoptInto(this))
        {
            _materialized = null;
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

                    if (arrival.Count > 1)
                    {
                        arrival.Sort(static (x, y) => ((uint)x.GetHashCode()).CompareTo((uint)y.GetHashCode())); // §5.24
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

        // At High pressure or above an insert freezes past a much lower threshold (default: every insert,
        // as Feldera's inserts all go to storage, docs/design-bounded-memory.md §5.28).
        var freezeAt = MemoryPressure.Current >= PressureLevel.High ? Math.Min(_freezeEntries, MemoryPressure.HighFreezeEntries) : _freezeEntries;
        if (_managedEntries >= freezeAt || _managed.Count > _maxManaged)
        {
            Freeze();
        }
        else if (_compact && !PressureRegistered)
        {
            PressureRelief.Register(this); // pushed when the circuit next sees a new pressure epoch
        }
    }

    /// <summary>Whether the stepping circuit's <see cref="PressureRelief"/> lists this trace (§5.28).</summary>
    internal bool PressureRegistered;

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

    /// <summary>Throws unless every run is in canonical order (§5.24): a key's entries together and
    /// in ascending value hash. Runs built by gathering (<see cref="IntegrateGathered"/>) or restored
    /// from an older file need not be; tests call this where neither happens.</summary>
    internal void CheckValueOrder()
    {
        foreach (var run in _runs)
        {
            var done = new HashSet<StructuralRow>();
            StructuralRow? key = null;
            var last = 0u;
            uint? hash = null;
            foreach (var block in Bodies(run.Blocks))
            {
                for (var i = 0; i < block.Count; i++)
                {
                    var k = block.Key(i);
                    var vh = (uint)block.Value(i).GetHashCode();
                    if (hash != block.Hashes[i])
                    {
                        done.Clear();
                        hash = block.Hashes[i];
                        key = null;
                    }

                    if (key is null || !key.Equals(k))
                    {
                        if (key is not null)
                        {
                            done.Add(key);
                        }

                        if (done.Contains(k))
                        {
                            throw new InvalidOperationException($"key {k}'s entries are not together");
                        }

                        key = k;
                    }
                    else if (vh < last)
                    {
                        throw new InvalidOperationException($"key {k}'s values descend in value hash");
                    }

                    last = vh;
                }
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

            var old = _runs[r];
            var filtered = Merge(old, new Run([], 0));
            if (filtered is null)
            {
                _runs.RemoveAt(r--);
            }
            else
            {
                _runs[r] = filtered;
            }

            Retire(old);
        }

        return Captured([.. _runs]);
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
        Retire(run);
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

    /// <summary>How many rows of a Z-set trace have a non-zero weight, counted by streaming its runs
    /// (and summing its managed levels, which a freeze bounds) — never materialized: a server's view
    /// statistics once materialized a 65M-row view to count it (§5.24).</summary>
    public int CountRows()
    {
        Dictionary<StructuralRow, long>? managed = null;
        foreach (var level in _managed)
        {
            foreach (var (row, weight) in level.Rows ?? throw new InvalidOperationException("not a Z-set trace"))
            {
                if (!Dropped(row, level.Floor))
                {
                    managed ??= [];
                    managed[row] = managed.GetValueOrDefault(row) + weight.Value;
                }
            }
        }

        long count = 0;
        foreach (var (row, weight) in StreamRunsCore(Captured([.. _runs])))
        {
            var total = weight.Value;
            if (managed is not null && managed.Remove(row, out var more))
            {
                total += more;
            }

            if (total != 0)
            {
                count++;
            }
        }

        if (managed is not null)
        {
            count += managed.Values.Count(static w => w != 0);
        }

        return (int)Math.Min(count, int.MaxValue);
    }

    /// <summary>
    /// As <see cref="CountRows"/> for the sum of several Z-set traces of one layout (a parallel view's
    /// shards, whose rows may be split across workers): every shard's runs in one hash-ordered merge,
    /// so a row is counted once, by its summed weight, and nothing is materialized. A parallel view's
    /// first count formed the whole view to count it (§5.34). Null when the traces do not share the
    /// default row-hash order.
    /// </summary>
    internal static long? CountRowsAcross(IReadOnlyList<BlockedIndexedTrace> traces)
    {
        if (traces.Count == 0)
        {
            return 0;
        }

        if (traces.Any(t => !t.OrdersByRowHash))
        {
            return null;
        }

        Dictionary<StructuralRow, long>? managed = null;
        foreach (var trace in traces)
        {
            foreach (var level in trace._managed)
            {
                foreach (var (row, weight) in level.Rows ?? throw new InvalidOperationException("not a Z-set trace"))
                {
                    if (!trace.Dropped(row, level.Floor))
                    {
                        managed ??= [];
                        managed[row] = managed.GetValueOrDefault(row) + weight.Value;
                    }
                }
            }
        }

        // Replicas of one operator: one layout, one monotone key, so the first trace reads every run.
        long count = 0;
        foreach (var (row, weight) in traces[0].StreamRunsCore(Captured([.. traces.SelectMany(t => t._runs)])))
        {
            var total = weight.Value;
            if (managed is not null && managed.Remove(row, out var more))
            {
                total += more;
            }

            if (total != 0)
            {
                count++;
            }
        }

        if (managed is not null)
        {
            count += managed.Values.Count(static w => w != 0);
        }

        return count;
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
            return StreamEntriesCore(Captured([.. _runs]));
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

        return builder.Finish() is { } scratch ? StreamEntriesCore(Captured([scratch, .. _runs])) : StreamEntriesCore(Captured([.. _runs]));
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
        _managed.Count > 0 ? null : StreamRunsCore(Captured([.. _runs]));

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
            var run = new RunBuilder(this, stream: true);
            var sorted = new List<(StructuralRow Key, StructuralRow Value, long Weight)>();
            var scratch = new MergeScratch();
            for (var i = 0; i < g; i++)
            {
                var key = groups[i].Key;
                if (groups[i].Value.Count == 1)
                {
                    foreach (var (value, weight) in groups[i].Value)
                    {
                        run.Add(hashes[i], key, value, weight.Value);
                        entries++;
                    }

                    continue;
                }

                // A key's values in value-hash order (§5.24): a later merge can stream a giant group.
                sorted.Clear();
                foreach (var (value, weight) in groups[i].Value)
                {
                    sorted.Add((key, value, weight.Value));
                }

                Canonical(sorted, scratch);
                foreach (var (_, value, weight) in sorted)
                {
                    run.Add(hashes[i], key, value, weight);
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
            var run = new RunBuilder(this, stream: true);
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

    // A run left this trace's levels: its spilled file (if any) loses this trace as a holder, and is
    // deleted at once if no reader still holds it (§5.20).
    private void Retire(Run run) => run.Lease?.Release(this);

    // Runs captured by a reader that outlives the call (a lazy enumeration): the array holds each
    // spilled file until the reader is collected, whatever the trace merges meanwhile (§5.20).
    private static Run[] Captured(Run[] runs)
    {
        foreach (var run in runs)
        {
            run.Lease?.Hold(runs);
        }

        return runs;
    }

    private bool Dropped(StructuralRow key, long floor) =>
        floor != long.MinValue && _monotoneKey!(key) < floor;

    // Merge while the newest run is at least a quarter of the next: about log4 levels.
    private void Compact()
    {
        while (_compact && _runs.Count >= 2 && _runs[0].Entries * 4 >= _runs[1].Entries)
        {
            var (newer, older) = (_runs[0], _runs[1]);
            var merged = Merge(newer, older);
            _runs.RemoveRange(0, 2);
            Retire(newer);
            Retire(older);
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

    // A k-way merge in hash order (adopted runs, §5.19): MergeCore's grouping for any number of runs —
    // entries of one hash from several runs consolidated by key and value, those under a floor dropped.
    private Run? MergeMany(IReadOnlyList<Run> runs)
    {
        var t0 = BlockTraceProfile.Now();
        var maintenance = BlockAccess.InMaintenance;
        BlockAccess.InMaintenance = true;
        var m = StepSpillProfile.Enabled ? StepSpillProfile.Begin() : default;
        try
        {
            var cursors = runs.Select(r => new Cursor(r)).ToArray();
            var queue = new PriorityQueue<int, uint>(cursors.Length);
            for (var c = 0; c < cursors.Length; c++)
            {
                if (!cursors[c].Done)
                {
                    queue.Enqueue(c, cursors[c].Hash);
                }
            }

            var output = new RunBuilder(this, stream: true);
            output.CopyMerge = CanCopyFrom(cursors.FirstOrDefault(c => !c.Done));
            var scratch = new MergeScratch();
            var tied = new List<Cursor>();
            var dropped = 0L;
            while (queue.TryDequeue(out var first, out var hash))
            {
                tied.Clear();
                tied.Add(cursors[first]);
                var ids = scratch.Ids;
                ids.Clear();
                ids.Add(first);
                while (queue.TryPeek(out var next, out var h) && h == hash)
                {
                    queue.Dequeue();
                    tied.Add(cursors[next]);
                    ids.Add(next);
                }

                dropped += MergeHash(tied, hash, output, scratch);
                foreach (var c in ids)
                {
                    if (!cursors[c].Done)
                    {
                        queue.Enqueue(c, cursors[c].Hash);
                    }
                }
            }

            DroppedKeys += dropped;
            return output.Finish();
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
                BlockTraceProfile.Merge(t0, runs.Sum(r => r.Entries));
            }
        }
    }

    // Whether a merge reading cursor's run can copy its entries (§5.36).
    private bool CanCopyFrom(Cursor? cursor) =>
        CopyMerges && cursor is not null && _format.CopiedEntryBytes(cursor.Block, cursor.Index) > 0;

    private Run? MergeCore(Run newer, Run older)
    {
        var a = new Cursor(newer);
        var b = new Cursor(older);
        var output = new RunBuilder(this, stream: true);
        output.CopyMerge = CanCopyFrom(!a.Done ? a : !b.Done ? b : null);
        var scratch = new MergeScratch();
        var tied = new List<Cursor>(2);
        var dropped = 0L;
        while (!a.Done || !b.Done)
        {
            var hash = a.Done ? b.Hash : b.Done ? a.Hash : Math.Min(a.Hash, b.Hash);
            tied.Clear();
            if (!a.Done && a.Hash == hash)
            {
                tied.Add(a);
            }

            if (!b.Done && b.Hash == hash)
            {
                tied.Add(b);
            }

            dropped += MergeHash(tied, hash, output, scratch);
        }

        DroppedKeys += dropped;
        return output.Finish();
    }

    /// <summary>A hash with at least this many entries across the runs a merge reads is merged
    /// streaming, by value hash (docs/design-bounded-memory.md §5.24), when its runs allow it.</summary>
    internal static int GiantGroupEntries { get; set; } = 1 << 16;

    /// <summary>Giant hashes merged streaming, and those that fell back to the in-memory path because
    /// a run was not in value-hash order or held several keys there (diagnostics).</summary>
    internal static long GiantMerges, GiantFallbacks;

    /// <summary>Whether a merge copies its entries between blocks rather than decoding and encoding them
    /// (docs/design-bounded-memory.md §5.36): a hash one run holds is copied as it is; entries of a hash
    /// several runs hold are decoded only to sum equal rows, and the survivors copied. Off for A/B.</summary>
    internal static bool CopyMerges { get; set; } =
        !string.Equals(Environment.GetEnvironmentVariable("DBSPNET_COPY_MERGES"), "off", StringComparison.OrdinalIgnoreCase);

    /// <summary>Hashes a copying merge copied as they were, and hashes it decoded to sum (diagnostics).</summary>
    internal static long CopiedHashes, SummedHashes;

    private sealed class MergeScratch
    {
        public readonly List<(StructuralRow Key, StructuralRow Value, long Weight)> Group = [];
        public readonly List<int> Ids = [];
        public readonly List<StructuralRow> Keys = [];
        public ulong[] Order = new ulong[16];
        public readonly List<(StructuralRow Key, StructuralRow Value, long Weight, ITraceBlock Block, int Index)> Copies = [];
        public readonly Dictionary<(StructuralRow, StructuralRow), int> At = [];
        public readonly List<Cursor> Live = [];
        public readonly Dictionary<StructuralRow, int> Sums = [];
    }

    // The tied cursors' entries of hash, consolidated, into output (each cursor left past them); the
    // keys a floor dropped. A small group is decoded whole and summed by (key, value); a giant one,
    // when every run holds it as one key in value-hash order, is merged by value hash, so only the
    // entries of one value hash are held at a time. Either way the output is in canonical order: a
    // key's entries together, by value hash.
    private long MergeHash(List<Cursor> tied, uint hash, RunBuilder output, MergeScratch scratch)
    {
        if (IsGiant(tied, hash) && TryMergeGiant(tied, hash, output, scratch, out var giantDropped))
        {
            return giantDropped;
        }

        if (output.CopyMerge)
        {
            if (tied.Count > 1 && ValueHashMerges && TryMergeTiedByValueHash(tied, hash, output, scratch, out var tiedDropped))
            {
                return tiedDropped;
            }

            return MergeHashCopying(tied, hash, output, scratch);
        }

        var group = scratch.Group;
        group.Clear();
        var dropped = 0L;
        var droppedHere = false;
        foreach (var cursor in tied)
        {
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

        if (tied.Count > 1 || droppedHere)
        {
            Consolidate(group);
        }

        Canonical(group, scratch);
        foreach (var (key, value, weight) in group)
        {
            output.Add(hash, key, value, weight);
        }

        return dropped;
    }

    // MergeHash copying entries whole (§5.36). A hash one run holds is consolidated and in canonical order
    // there: its entries are copied as they are, with no row read unless a floor needs the key. Entries of
    // a hash several runs hold are decoded to sum equal (key, value) entries; each survivor is copied
    // from its first entry with the summed weight, in canonical order (a key's entries together, by
    // value hash), as the decoding merge writes them.
    private long MergeHashCopying(List<Cursor> tied, uint hash, RunBuilder output, MergeScratch scratch)
    {
        var dropped = 0L;
        if (tied.Count == 1)
        {
            var c = tied[0];
            var floor = c.Run.Floor;
            StructuralRow? key = null;
            StructuralRow? lastDropped = null;
            while (!c.Done && c.Hash == hash)
            {
                var (block, i) = (c.Block, c.Index);
                if (floor != long.MinValue)
                {
                    key = key is not null && block.KeyEquals(i, key) ? key : block.Key(i);
                    if (Dropped(key, floor))
                    {
                        if (!ReferenceEquals(key, lastDropped))
                        {
                            dropped++;
                            lastDropped = key;
                        }

                        c.Advance();
                        continue;
                    }
                }

                output.AddCopy(hash, block, i, block.Weight(i));
                c.Advance();
            }

            Interlocked.Increment(ref CopiedHashes);
            return dropped;
        }

        var entries = scratch.Copies;
        entries.Clear();
        foreach (var cursor in tied)
        {
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
                }
                else
                {
                    entries.Add((key, block.Value(i), block.Weight(i), block, i));
                }

                cursor.Advance();
            }
        }

        // Sum equal entries in place, first seen first; drop zeros.
        var at = scratch.At;
        at.Clear();
        var kept = 0;
        for (var j = 0; j < entries.Count; j++)
        {
            var e = entries[j];
            if (at.TryGetValue((e.Key, e.Value), out var first))
            {
                var f = entries[first];
                entries[first] = (f.Key, f.Value, f.Weight + e.Weight, f.Block, f.Index);
            }
            else
            {
                at[(e.Key, e.Value)] = kept;
                entries[kept++] = e;
            }
        }

        entries.RemoveRange(kept, entries.Count - kept);
        entries.RemoveAll(static e => e.Weight == 0);

        var n = entries.Count;
        if (n > 1)
        {
            if (scratch.Order.Length < n)
            {
                scratch.Order = new ulong[Math.Max(n, scratch.Order.Length * 2)];
            }

            var keys = scratch.Keys;
            keys.Clear();
            var order = scratch.Order.AsSpan(0, n);
            var items = System.Runtime.InteropServices.CollectionsMarshal.AsSpan(entries);
            for (var j = 0; j < n; j++)
            {
                var k = items[j].Key;
                var ord = keys.FindIndex(x => ReferenceEquals(x, k) || x.Equals(k));
                if (ord < 0)
                {
                    keys.Add(k);
                    ord = keys.Count - 1;
                }

                order[j] = ((ulong)(uint)ord << 32) | (uint)items[j].Value.GetHashCode();
            }

            order.Sort(items);
        }

        foreach (var e in entries)
        {
            output.AddCopy(hash, e.Block, e.Index, e.Weight);
        }

        Interlocked.Increment(ref SummedHashes);
        return dropped;
    }

    /// <summary>Whether a copying merge sums a hash several runs hold by stored value hash when every run
    /// holds it as one key (docs/design-bounded-memory.md §5.40), decoding only entries whose value hashes
    /// tie, as a giant group's merge does; otherwise it decodes every entry of that hash. Off for A/B
    /// (<c>DBSPNET_VALUE_HASH_MERGES=off</c>).</summary>
    internal static bool ValueHashMerges { get; set; } =
        !string.Equals(Environment.GetEnvironmentVariable("DBSPNET_VALUE_HASH_MERGES"), "off", StringComparison.OrdinalIgnoreCase);

    /// <summary>Hashes several runs held that a copying merge summed by value hash (diagnostics).</summary>
    internal static long ValueHashMerged;

    // A copying merge of a hash several runs hold (§5.40): when each run holds it as one key, the same in
    // every run, its values in ascending value hash (canonical order), the runs are walked k-way by stored
    // value hash and entries copied whole; a value is decoded only where entries share its value hash, to
    // sum equal rows. Unlike the giant merge, the hash is not flushed into blocks of its own. False, with
    // the cursors untouched, when a run holds another key there or is out of order.
    private bool TryMergeTiedByValueHash(List<Cursor> tied, uint hash, RunBuilder output, MergeScratch scratch, out long dropped)
    {
        dropped = 0;
        var key = tied[0].Block.Key(tied[0].Index);
        foreach (var cursor in tied)
        {
            if (!cursor.OneKeyAscending(hash, key))
            {
                return false;
            }
        }

        var live = scratch.Live;
        live.Clear();
        foreach (var cursor in tied)
        {
            if (Dropped(key, cursor.Run.Floor))
            {
                dropped++;
                while (!cursor.Done && cursor.Hash == hash)
                {
                    cursor.Advance();
                }
            }
            else
            {
                live.Add(cursor);
            }
        }

        // The walk: the least value hash across the live runs, its entries gathered and summed.
        var group = scratch.Copies;
        Dictionary<StructuralRow, int>? sums = null;
        while (true)
        {
            var any = false;
            var min = 0u;
            foreach (var c in live)
            {
                if (!c.Done && c.Hash == hash)
                {
                    var vh = (uint)c.Block.ValueHash(c.Index);
                    if (!any || vh < min)
                    {
                        (min, any) = (vh, true);
                    }
                }
            }

            if (!any)
            {
                break;
            }

            group.Clear();
            foreach (var c in live)
            {
                while (!c.Done && c.Hash == hash && (uint)c.Block.ValueHash(c.Index) == min)
                {
                    group.Add((key, null!, c.Block.Weight(c.Index), c.Block, c.Index));
                    c.Advance();
                }
            }

            if (group.Count == 1)
            {
                var (_, _, weight, block, index) = group[0];
                if (weight != 0)
                {
                    output.AddCopy(hash, block, index, weight);
                }

                continue;
            }

            // Entries sharing a value hash: equal rows sum, first seen first; zeros dropped.
            sums ??= scratch.Sums;
            sums.Clear();
            var kept = 0;
            for (var j = 0; j < group.Count; j++)
            {
                var e = group[j];
                var value = e.Block.Value(e.Index);
                if (sums.TryGetValue(value, out var first))
                {
                    var f = group[first];
                    group[first] = (f.Key, f.Value, f.Weight + e.Weight, f.Block, f.Index);
                }
                else
                {
                    sums[value] = kept;
                    group[kept++] = (e.Key, value, e.Weight, e.Block, e.Index);
                }
            }

            for (var j = 0; j < kept; j++)
            {
                var (_, _, weight, block, index) = group[j];
                if (weight != 0)
                {
                    output.AddCopy(hash, block, index, weight);
                }
            }
        }

        Interlocked.Increment(ref ValueHashMerged);
        return true;
    }

    // Whether the tied runs hold at least GiantGroupEntries entries of hash: counted from the hash
    // arrays alone, and only past a block when the hash spans blocks.
    private static bool IsGiant(List<Cursor> tied, uint hash)
    {
        long n = 0;
        foreach (var cursor in tied)
        {
            n += cursor.SpansBlocks(hash) ? cursor.Clone().CountAt(hash, GiantGroupEntries) : cursor.CountInBlock(hash);
            if (n >= GiantGroupEntries)
            {
                return true;
            }
        }

        return false;
    }

    private bool TryMergeGiant(List<Cursor> tied, uint hash, RunBuilder output, MergeScratch scratch, out long dropped)
    {
        dropped = 0;

        // Check on copies first, so a fallback finds the cursors untouched: each run holds the hash
        // as one key, the same in every run, its values in ascending value hash.
        StructuralRow? key = null;
        foreach (var cursor in tied)
        {
            var probe = cursor.Clone();
            if (key is null)
            {
                key = probe.Block.Key(probe.Index);
            }

            var first = true;
            var last = 0u;
            while (!probe.Done && probe.Hash == hash)
            {
                var (block, i) = (probe.Block, probe.Index);
                // The stored value hash: the check reads no value row (§5.35), only the merge below does.
                var vh = (uint)block.ValueHash(i);
                if (!block.KeyEquals(i, key) || (!first && vh < last))
                {
                    Interlocked.Increment(ref GiantFallbacks);
                    return false;
                }

                (first, last) = (false, vh);
                probe.Advance();
            }
        }

        // Runs whose floor drops the key give up their entries whole.
        var live = new List<Cursor>(tied.Count);
        foreach (var cursor in tied)
        {
            if (Dropped(key!, cursor.Run.Floor))
            {
                dropped++;
                while (!cursor.Done && cursor.Hash == hash)
                {
                    cursor.Advance();
                }
            }
            else
            {
                live.Add(cursor);
            }
        }

        if (live.Count > 0 && !live[0].Done && output.BeginCopy(live[0].Block))
        {
            MergeGiantCopying(live, hash, key!, output);
            Interlocked.Increment(ref GiantMerges);
            return true;
        }

        // A k-way merge by value hash: the entries of the least value hash across the runs, summed.
        var heads = new (uint Hash, StructuralRow Value, long Weight)[live.Count];
        var has = new bool[live.Count];
        for (var j = 0; j < live.Count; j++)
        {
            has[j] = Head(live[j], hash, out heads[j]);
        }

        var group = scratch.Group;
        while (true)
        {
            var any = false;
            var min = uint.MaxValue;
            for (var j = 0; j < live.Count; j++)
            {
                if (has[j] && (!any || heads[j].Hash < min))
                {
                    min = heads[j].Hash;
                    any = true;
                }
            }

            if (!any)
            {
                break;
            }

            group.Clear();
            for (var j = 0; j < live.Count; j++)
            {
                while (has[j] && heads[j].Hash == min)
                {
                    group.Add((key!, heads[j].Value, heads[j].Weight));
                    live[j].Advance();
                    has[j] = Head(live[j], hash, out heads[j]);
                }
            }

            if (group.Count > 1)
            {
                Consolidate(group);
            }

            foreach (var (_, value, weight) in group)
            {
                output.Add(hash, key!, value, weight);
            }
        }

        Interlocked.Increment(ref GiantMerges);
        return true;
    }

    // The giant merge's k-way pass by stored value hash, copying entries whole (§5.35): a value is decoded
    // only where two entries share its hash, to sum equal rows; each surviving row is copied from one of
    // its entries with the summed weight. The output is in canonical order, as the decoding merge's is.
    private static void MergeGiantCopying(List<Cursor> live, uint hash, StructuralRow key, RunBuilder output)
    {
        var heads = new uint[live.Count];
        var has = new bool[live.Count];
        for (var j = 0; j < live.Count; j++)
        {
            has[j] = HeadHash(live[j], hash, out heads[j]);
        }

        var tied = new List<(ITraceBlock Block, int Index, long Weight)>();
        Dictionary<StructuralRow, (ITraceBlock Block, int Index, long Weight)>? sums = null;
        while (true)
        {
            var any = false;
            var min = uint.MaxValue;
            for (var j = 0; j < live.Count; j++)
            {
                if (has[j] && (!any || heads[j] < min))
                {
                    min = heads[j];
                    any = true;
                }
            }

            if (!any)
            {
                break;
            }

            tied.Clear();
            for (var j = 0; j < live.Count; j++)
            {
                while (has[j] && heads[j] == min)
                {
                    var c = live[j];
                    tied.Add((c.Block, c.Index, c.Block.Weight(c.Index)));
                    c.Advance();
                    has[j] = HeadHash(c, hash, out heads[j]);
                }
            }

            if (tied.Count == 1)
            {
                var (block, index, weight) = tied[0];
                if (weight != 0)
                {
                    output.AddCopy(hash, block, index, weight);
                }

                continue;
            }

            // Entries sharing a value hash: equal rows sum, in first-seen order.
            sums ??= new Dictionary<StructuralRow, (ITraceBlock, int, long)>();
            sums.Clear();
            foreach (var (block, index, weight) in tied)
            {
                var value = block.Value(index);
                sums[value] = sums.TryGetValue(value, out var at) ? (at.Item1, at.Item2, at.Item3 + weight) : (block, index, weight);
            }

            foreach (var (block, index, weight) in sums.Values)
            {
                if (weight != 0)
                {
                    output.AddCopy(hash, block, index, weight);
                }
            }
        }

        output.EndCopy();
    }

    private static bool HeadHash(Cursor cursor, uint hash, out uint valueHash)
    {
        if (cursor.Done || cursor.Hash != hash)
        {
            valueHash = 0;
            return false;
        }

        valueHash = (uint)cursor.Block.ValueHash(cursor.Index);
        return true;
    }

    private static bool Head(Cursor cursor, uint hash, out (uint Hash, StructuralRow Value, long Weight) head)
    {
        if (cursor.Done || cursor.Hash != hash)
        {
            head = default;
            return false;
        }

        var (block, i) = (cursor.Block, cursor.Index);
        head = ((uint)block.ValueHash(i), block.Value(i), block.Weight(i));
        return true;
    }

    // A hash's entries in canonical order (§5.24): each key's entries together, keys in first-seen
    // order, and within a key by value hash, which lets a later merge stream a giant group.
    private static void Canonical(List<(StructuralRow Key, StructuralRow Value, long Weight)> group, MergeScratch scratch)
    {
        var n = group.Count;
        if (n < 2)
        {
            return;
        }

        if (scratch.Order.Length < n)
        {
            scratch.Order = new ulong[Math.Max(n, scratch.Order.Length * 2)];
        }

        var keys = scratch.Keys;
        keys.Clear();
        var order = scratch.Order.AsSpan(0, n);
        var items = System.Runtime.InteropServices.CollectionsMarshal.AsSpan(group);
        for (var i = 0; i < n; i++)
        {
            var k = items[i].Key;
            var ord = keys.Count - 1;
            if (ord < 0 || !ReferenceEquals(keys[ord], k) && !keys[ord].Equals(k))
            {
                ord = keys.FindIndex(x => ReferenceEquals(x, k) || x.Equals(k));
                if (ord < 0)
                {
                    keys.Add(k);
                    ord = keys.Count - 1;
                }
            }

            order[i] = ((ulong)(uint)ord << 32) | (uint)items[i].Value.GetHashCode();
        }

        order.Sort(items);
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

        // A copy at the same position (for a look ahead that must not move this one).
        public Cursor Clone() => new(Run, _block, Index);

        private Cursor(Run run, int block, int index)
        {
            Run = run;
            _block = block;
            Index = index;
        }

        // Whether every entry of hash from here has key (no row decoded) and they ascend by stored value
        // hash: the run holds hash as one key in canonical order. Moves nothing.
        public bool OneKeyAscending(uint hash, StructuralRow key)
        {
            var last = 0u;
            var first = true;
            var b = _block;
            var i = Index;
            while (b < Run.Blocks.Length)
            {
                if (i >= Run.Blocks[b].Count)
                {
                    b++;
                    i = 0;
                    continue;
                }

                var block = b == _block ? Block : Run.Blocks[b].Body;
                if (block.Hashes[i] != hash)
                {
                    break;
                }

                var vh = (uint)block.ValueHash(i);
                if ((!first && vh < last) || !block.KeyEquals(i, key))
                {
                    return false;
                }

                (first, last) = (false, vh);
                i++;
            }

            return true;
        }

        // Whether the hash under the cursor continues into the next block.
        public bool SpansBlocks(uint hash) =>
            Run.Blocks[_block].LastHash == hash && _block + 1 < Run.Blocks.Length && Run.FirstOf(_block + 1) == hash;

        // Entries of hash from the cursor to the end of its block.
        public long CountInBlock(uint hash)
        {
            var block = Block;
            return (block.LastHash == hash ? block.Count : block.LowerBound(hash + 1)) - Index;
        }

        // Entries of hash from here, counting at most to cap (moves this cursor: call it on a copy).
        public long CountAt(uint hash, long cap)
        {
            long n = 0;
            while (!Done && Hash == hash && n < cap)
            {
                n += CountInBlock(hash);
                Index = Run.Blocks[_block].Count;
                Skip();
            }

            return n;
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
    // stream: the run goes into the trace's levels (a freeze or merge), so once its blocks pass the
    // spiller's threshold they are written to a file as they are made (§5.23); a merge's output is then
    // bounded by a block, not by the run.
    private sealed class RunBuilder(BlockedIndexedTrace trace, bool stream = false)
    {
        private long _bytes;
        private long _flushedBytes;
        private IRunSink? _sink;
        private bool _noSink;
        private readonly List<ITraceBlock> _blocks = new();
        private StructuralRow[] _keys = new StructuralRow[64];
        private StructuralRow[] _values = new StructuralRow[64];
        private long[] _weights = new long[64];
        private int[] _hashes = new int[64];
        private int _count;
        private long _entries;

        // Whether the buffered block continues a hash from the previous block (a giant group).
        private bool _spanning;

        // Copy mode (§5.35): the buffered entries are other blocks' entries, copied whole at the flush.
        private bool _copying;
        private ITraceBlock[] _sources = [];
        private int[] _indexes = [];

        /// <summary>A merge writing every entry by <see cref="AddCopy"/> (§5.36).</summary>
        public bool CopyMerge { get; set; }

        /// <summary>Start buffering copied entries (<see cref="AddCopy"/>) of one hash: what is buffered is
        /// flushed first, so that hash's blocks hold only it. False when the format cannot copy.</summary>
        public bool BeginCopy(ITraceBlock probe)
        {
            if (trace._format.CopiedEntryBytes(probe, 0) == 0)
            {
                return false;
            }

            Flush();
            _copying = true;
            return true;
        }

        /// <summary>Add another block's entry, copied whole when its block is flushed.</summary>
        public void AddCopy(uint hash, ITraceBlock source, int index, long weight)
        {
            if (!_copying)
            {
                Flush();
                _copying = true;
            }

            Cut(hash);
            _bytes += trace._format.CopiedEntryBytes(source, index);
            if (_count == _sources.Length)
            {
                var size = Math.Max(64, _count * 2);
                Array.Resize(ref _sources, size);
                Array.Resize(ref _indexes, size);
                Array.Resize(ref _weights, Math.Max(size, _weights.Length));
                Array.Resize(ref _hashes, Math.Max(size, _hashes.Length));
            }

            _sources[_count] = source;
            _indexes[_count] = index;
            _weights[_count] = weight;
            _hashes[_count] = (int)hash;
            _count++;
            _entries++;
        }

        /// <summary>End the copied hash: its last block holds only it.</summary>
        public void EndCopy() => Flush();

        private static bool NoSpanAlign => !BlockAccess.AlignSpans;

        public void Add(uint hash, StructuralRow key, StructuralRow value, long weight)
        {
            if (_copying)
            {
                Flush();
                _copying = false;
            }

            Cut(hash);
            _bytes += trace._format.EntryBytes(key, value);

            if (_count == _keys.Length)
            {
                var size = _count * 2;
                Array.Resize(ref _keys, size);
                Array.Resize(ref _values, size);
                Array.Resize(ref _weights, Math.Max(size, _weights.Length));
                Array.Resize(ref _hashes, Math.Max(size, _hashes.Length));
            }

            _keys[_count] = key;
            _values[_count] = value;
            _weights[_count] = weight;
            _hashes[_count] = (int)hash;
            _count++;
            _entries++;
        }

        // Where a block ends before an entry of hash (decoded or copied alike): after blockEntries entries
        // where the hash changes, after the byte budget anywhere, and around a hash that spans blocks.
        private void Cut(uint hash)
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
        }

        public Run? Finish()
        {
            Flush();
            if (_sink is not null)
            {
                var (blocks, file, lease) = _sink.Finish();
                _sink = null;
                var run = new Run(blocks, _entries) { Persisted = file, Lease = lease };
                Interlocked.Increment(ref StreamedRuns);
                return run;
            }

            return _blocks.Count == 0 ? null : new Run([.. _blocks], _entries);
        }

        // Flush entries [0, n) as a block and keep the rest buffered, at the front.
        private void FlushPrefix(int n)
        {
            var rest = _count - n;
            var weights = _weights[n.._count];
            var hashes = _hashes[n.._count];
            if (_copying)
            {
                var sources = _sources[n.._count];
                var indexes = _indexes[n.._count];
                _count = n;
                Flush();
                Array.Copy(sources, _sources, rest);
                Array.Copy(indexes, _indexes, rest);
            }
            else
            {
                var keys = _keys[n.._count];
                var values = _values[n.._count];
                _count = n;
                Flush();
                Array.Copy(keys, _keys, rest);
                Array.Copy(values, _values, rest);
            }

            Array.Copy(weights, _weights, rest);
            Array.Copy(hashes, _hashes, rest);
            _count = rest;
            for (var i = 0; i < rest; i++)
            {
                _bytes += _copying ? trace._format.CopiedEntryBytes(_sources[i], _indexes[i]) : trace._format.EntryBytes(_keys[i], _values[i]);
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
            ITraceBlock block;
            if (_copying)
            {
                block = trace._format.Copy(_sources, _indexes, _weights, _hashes, _count) ?? EncodeCopies();
            }
            else
            {
                block = trace._format.Encode(_keys, _values, _weights, _hashes, _count);
            }

            _flushedBytes += _bytes;
            if (_sink is not null)
            {
                _sink.Append(block);
            }
            else
            {
                _blocks.Add(block);
                if (stream && !_noSink && BlockAccess.Spiller is { } spiller && _flushedBytes >= spiller.StreamAtBytes)
                {
                    _sink = spiller.BeginRun(trace);
                    _noSink = _sink is null;
                    if (_sink is not null)
                    {
                        foreach (var made in _blocks)
                        {
                            _sink.Append(made);
                        }

                        _blocks.Clear();
                    }
                }
            }

            if (BlockTraceProfile.Enabled)
            {
                BlockTraceProfile.Encode(t0);
            }
            if (_copying)
            {
                Array.Clear(_sources, 0, _count);
            }
            else
            {
                Array.Clear(_keys, 0, _count);
                Array.Clear(_values, 0, _count);
            }

            _count = 0;
            _bytes = 0;
        }

        // The buffered copies decoded and encoded (a source the format would not copy from).
        private ITraceBlock EncodeCopies()
        {
            var keys = new StructuralRow[_count];
            var values = new StructuralRow[_count];
            for (var i = 0; i < _count; i++)
            {
                keys[i] = _sources[i].Key(_indexes[i]);
                values[i] = _sources[i].Value(_indexes[i]);
            }

            return trace._format.Encode(keys, values, _weights, _hashes, _count);
        }
    }
}
