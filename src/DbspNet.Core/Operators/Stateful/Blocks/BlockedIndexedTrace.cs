// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;

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
    private sealed class Managed(IndexedZSet<StructuralRow, StructuralRow, Z64> set, long entries)
    {
        public IndexedZSet<StructuralRow, StructuralRow, Z64> Set { get; } = set;

        public long Entries { get; } = entries;

        public long Floor { get; set; } = long.MinValue;
    }

    internal sealed class Run(ITraceBlock[] blocks, long entries)
    {
        private readonly uint[] _first = [.. blocks.Select(b => b.FirstHash)];
        private readonly uint[] _last = [.. blocks.Select(b => b.LastHash)];

        public ITraceBlock[] Blocks { get; } = blocks;

        public long Entries { get; } = entries;

        public long Floor { get; set; } = long.MinValue;

        /// <summary>The block holding <paramref name="hash"/>, or −1.</summary>
        public int Find(uint hash)
        {
            int lo = 0, hi = _first.Length;
            while (lo < hi)
            {
                var mid = (lo + hi) >>> 1;
                if (_first[mid] <= hash)
                {
                    lo = mid + 1;
                }
                else
                {
                    hi = mid;
                }
            }

            return lo > 0 && _last[lo - 1] >= hash ? lo - 1 : -1;
        }
    }

    private readonly ITraceBlockFormat _format;
    private readonly Func<StructuralRow, int> _hash;
    private readonly int _freezeEntries;
    private readonly int _maxManaged;
    private readonly int _blockEntries;
    private readonly List<Managed> _managed = new();
    private readonly List<Run> _runs = new();
    private long _managedEntries;
    private Func<StructuralRow, long>? _monotoneKey;
    private IndexedZSet<StructuralRow, StructuralRow, Z64>? _materialized;

    /// <param name="format">The block encoding (key and value schema).</param>
    /// <param name="freezeEntries">Managed entries that trigger a freeze.</param>
    /// <param name="maxManaged">Managed levels allowed to wait before a freeze.</param>
    /// <param name="blockEntries">Entries per block (a hash never splits, so a block can exceed it).</param>
    /// <param name="hash">The key hash (a test seam for forced collisions; default the row's own).</param>
    public BlockedIndexedTrace(
        ITraceBlockFormat format,
        int freezeEntries = 1 << 16,
        int maxManaged = 8,
        int blockEntries = 1024,
        Func<StructuralRow, int>? hash = null)
    {
        ArgumentNullException.ThrowIfNull(format);
        ArgumentOutOfRangeException.ThrowIfLessThan(blockEntries, 1);
        _format = format;
        _freezeEntries = freezeEntries;
        _maxManaged = maxManaged;
        _blockEntries = blockEntries;
        _hash = hash ?? (static row => row.GetHashCode());
    }

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

        _managed.Add(new Managed(delta, entries));
        _managedEntries += entries;
        if (_managedEntries >= _freezeEntries || _managed.Count > _maxManaged)
        {
            Freeze();
        }
    }

    /// <summary>Throws unless every run is consolidated (no repeated key and value, no zero weight),
    /// in ascending hash order, with no hash split across blocks — the invariants freeze and merge
    /// keep, which probes and <see cref="Materialize"/> cannot see.</summary>
    internal void CheckRuns()
    {
        foreach (var run in _runs)
        {
            var seen = new HashSet<(StructuralRow, StructuralRow)>();
            uint? previous = null;
            foreach (var block in run.Blocks)
            {
                var hashes = block.Hashes;
                if (previous is { } p && hashes.Length > 0 && hashes[0] <= p)
                {
                    throw new InvalidOperationException($"hash {hashes[0]} repeats or descends across blocks");
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

    /// <summary>Visit every entry of <paramref name="key"/> in every level (unconsolidated).</summary>
    public void Probe<TVisitor>(StructuralRow key, ref TVisitor visitor)
        where TVisitor : struct, ITraceVisitor
    {
        var projected = _monotoneKey is { } mk ? mk(key) : long.MaxValue;
        foreach (var level in _managed)
        {
            if (projected < level.Floor)
            {
                continue;
            }

            foreach (var (value, weight) in level.Set.GroupFor(key))
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

            var block = run.Blocks[b];
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
    public IndexedZSet<StructuralRow, StructuralRow, Z64> Materialize()
    {
        if (_materialized is { } cached)
        {
            return cached;
        }

        var builder = new IndexedZSetBuilder<StructuralRow, StructuralRow, Z64>();
        foreach (var level in _managed)
        {
            foreach (var (key, group) in level.Set)
            {
                if (Dropped(key, level.Floor))
                {
                    continue;
                }

                foreach (var (value, weight) in group)
                {
                    builder.Add(key, value, weight);
                }
            }
        }

        foreach (var run in _runs)
        {
            foreach (var block in run.Blocks)
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

        return _materialized = builder.Build();
    }

    /// <summary>Consolidate the managed levels into one run, then merge runs geometrically.</summary>
    public void Freeze()
    {
        if (_managed.Count == 0)
        {
            return;
        }

        var builder = new IndexedZSetBuilder<StructuralRow, StructuralRow, Z64>();
        var dropped = new HashSet<StructuralRow>();
        foreach (var level in _managed)
        {
            foreach (var (key, group) in level.Set)
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
        var consolidated = builder.Build();
        _managed.Clear();
        _managedEntries = 0;

        var entries = new List<(uint Hash, int ValueHash, StructuralRow Key, StructuralRow Value, long Weight)>();
        foreach (var (key, group) in consolidated)
        {
            var hash = (uint)_hash(key);
            foreach (var (value, weight) in group)
            {
                entries.Add((hash, value.GetHashCode(), key, value, weight.Value));
            }
        }

        entries.Sort(static (a, b) => a.Hash != b.Hash ? a.Hash.CompareTo(b.Hash) : a.ValueHash.CompareTo(b.ValueHash));
        var run = new RunBuilder(this);
        foreach (var e in entries)
        {
            run.Add(e.Hash, e.Key, e.Value, e.Weight);
        }

        if (run.Finish() is { } frozen)
        {
            _runs.Insert(0, frozen);
        }

        Compact();
    }

    private bool Dropped(StructuralRow key, long floor) =>
        floor != long.MinValue && _monotoneKey!(key) < floor;

    // Merge while the newest run is at least a quarter of the next: about log4 levels.
    private void Compact()
    {
        while (_runs.Count >= 2 && _runs[0].Entries * 4 >= _runs[1].Entries)
        {
            var merged = Merge(_runs[0], _runs[1]);
            _runs.RemoveRange(0, 2);
            if (merged is not null)
            {
                _runs.Insert(0, merged);
            }
        }
    }

    // A two-way merge in hash order. Entries of one hash from both runs are consolidated by key and
    // value (weights summed, zeros dropped); entries under a floor are dropped.
    private Run? Merge(Run newer, Run older)
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

        public ITraceBlock Block => Run.Blocks[_block];

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

    // Cuts entries (ascending hash) into blocks of about blockEntries, only where the hash changes.
    private sealed class RunBuilder(BlockedIndexedTrace trace)
    {
        private readonly List<ITraceBlock> _blocks = new();
        private StructuralRow[] _keys = new StructuralRow[64];
        private StructuralRow[] _values = new StructuralRow[64];
        private long[] _weights = new long[64];
        private int[] _hashes = new int[64];
        private int _count;
        private long _entries;

        public void Add(uint hash, StructuralRow key, StructuralRow value, long weight)
        {
            if (_count >= trace._blockEntries && (uint)_hashes[_count - 1] != hash)
            {
                Flush();
            }

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

        private void Flush()
        {
            if (_count == 0)
            {
                return;
            }

            _blocks.Add(trace._format.Encode(_keys, _values, _weights, _hashes, _count));
            Array.Clear(_keys, 0, _count);
            Array.Clear(_values, 0, _count);
            _count = 0;
        }
    }
}
