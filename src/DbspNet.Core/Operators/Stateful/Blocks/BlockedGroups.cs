// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Runtime.CompilerServices;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Stateful.Blocks;

/// <summary>
/// D4 (docs/design-bounded-memory.md §5.22): an indexed delta builder's store that consolidates groups in
/// a managed builder up to a threshold of entries, then freezes them into a sorted block run of a private
/// indexed <see cref="BlockedIndexedTrace"/> and starts again. Runs are kept as frozen, never merged
/// (§5.17); S1 spills the large ones. Each run's sorted group-key hashes are kept beside it (4 B a group),
/// so the built set's exact group count reads blocks only where two runs share a hash.
/// </summary>
internal sealed class SpillingGroups : IndexedZSetSpillingStore<StructuralRow, StructuralRow, Z64>
{
    private readonly ITraceBlockFormat _format;
    private readonly bool _alwaysBlocked;
    private IndexedZSetBuilder<StructuralRow, StructuralRow, Z64> _chunk = new();
    private ChunkBudget _budget;
    private int _entries;
    private BlockedIndexedTrace? _trace;
    private readonly List<uint[]> _hashes = []; // newest first, as the trace's runs

    /// <param name="chunkBytes">Estimated managed bytes at which a chunk freezes sooner (0: entries only, §5.37).</param>
    public SpillingGroups(ITraceBlockFormat format, int chunkEntries, bool alwaysBlocked, long chunkBytes = 0)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(chunkEntries, 1);
        _format = format;
        _alwaysBlocked = alwaysBlocked;
        _budget = new ChunkBudget(chunkEntries, chunkBytes);
    }

    public override void Add(StructuralRow key, StructuralRow value, Z64 weight)
    {
        _chunk.Add(key, value, weight);
        _budget.Added(value, key);
        if (++_entries >= _budget.Limit
            || (_entries >= MemoryPressure.CriticalStepRows && MemoryPressure.Current == PressureLevel.Critical))
        {
            _budget.Freezing(_entries);
            FreezeChunk();
        }
    }

    private void FreezeChunk()
    {
        var m = StepSpillProfile.Enabled ? StepSpillProfile.Begin() : default;
        var part = _chunk.Build();
        _chunk = new();
        _entries = 0;
        if (!part.IsEmpty)
        {
            // The run's group-key hashes, in its order (the trace sorts by the key's own hash), taken
            // before the freeze: a spill inside it makes the blocks file-backed.
            var hashes = new uint[part.GroupCount];
            var n = 0;
            foreach (var (key, _) in part)
            {
                hashes[n++] = (uint)key.GetHashCode();
            }

            Array.Sort(hashes);
            _trace ??= new BlockedIndexedTrace(
                _format, freezeEntries: int.MaxValue, maxManaged: int.MaxValue, blockEntries: TraceBlocksMode.BlockEntries, compact: false);
            _trace.Integrate(part);
            _trace.Freeze();
            _hashes.Insert(0, hashes);
        }

        if (StepSpillProfile.Enabled)
        {
            StepSpillProfile.End(StepSpillProfile.ChunkFreeze, m);
        }
    }

    public override IndexedZSet<StructuralRow, StructuralRow, Z64> Build()
    {
        if (_trace is null && !_alwaysBlocked)
        {
            return _chunk.Build();
        }

        FreezeChunk();
        if (_trace is not { } trace || BlockedGroups.Create(trace, [.. _hashes], _format) is not { } built)
        {
            return IndexedZSet<StructuralRow, StructuralRow, Z64>.Empty; // nothing added, or every entry cancelled
        }

        return new IndexedZSet<StructuralRow, StructuralRow, Z64>(built);
    }
}

/// <summary>
/// D4: an indexed Z-set held as hash-ordered block runs of (key, value, weight) entries, each run
/// consolidated but not merged with the others. A reader merges them by key hash: groups whose hash
/// one run holds are read as they are; entries of a hash several runs hold are summed. The exact group
/// count is a walk over the runs' key-hash arrays, reading blocks only for hashes runs share. Rows are
/// decoded with strings copied. The runs are read through their owning trace, which holds their spill
/// files (§5.20).
/// </summary>
internal sealed class BlockedGroups : IndexedZSetExternalStore<StructuralRow, StructuralRow, Z64>
{
    private readonly BlockedIndexedTrace _trace;
    private readonly ITraceBlockFormat _format;
    private readonly uint[][] _hashes;
    private readonly int _groupCount;
    private readonly long _entries;

    private BlockedGroups(BlockedIndexedTrace trace, uint[][] hashes, ITraceBlockFormat format)
    {
        _trace = trace;
        _format = format;
        _hashes = hashes;
        var m = StepSpillProfile.Enabled ? StepSpillProfile.Begin() : default;
        _groupCount = CountGroups(hashes);
        _entries = trace.Runs.Sum(r => r.Entries);
        if (StepSpillProfile.Enabled)
        {
            StepSpillProfile.End(StepSpillProfile.BuildCount, m);
        }
    }

    /// <summary>The set over <paramref name="trace"/>'s runs, or null when every entry cancelled.</summary>
    internal static BlockedGroups? Create(BlockedIndexedTrace trace, uint[][] hashes, ITraceBlockFormat format)
    {
        var built = new BlockedGroups(trace, hashes, format);
        if (built._groupCount == 0)
        {
            return null;
        }

        Interlocked.Increment(ref DeltaSpill.BlockedGroupBuilds);
        Interlocked.Add(ref DeltaSpill.BlockedGroupEntries, built._entries);
        return built;
    }

    /// <summary>Runs held (tests, census).</summary>
    internal int RunCount => _trace.Runs.Count;

    public override int GroupCount => _groupCount;

    /// <summary>Probes of a block-backed indexed delta on this thread (tests, §5.36).</summary>
    [ThreadStatic]
    internal static long ThreadProbes;

    public override ZSet<StructuralRow, Z64> GroupFor(StructuralRow key)
    {
        ThreadProbes++;
        var m = StepSpillProfile.Enabled ? StepSpillProfile.Begin() : default;
        var hash = (uint)key.GetHashCode();
        var b = new ZSetBuilder<StructuralRow, Z64>();
        foreach (var run in _trace.Runs)
        {
            foreach (var (block, i) in At(run, hash))
            {
                if (block.KeyEquals(i, key))
                {
                    b.Add(block.Value(i), new Z64(block.Weight(i)));
                }
            }
        }

        var group = b.Build();
        if (StepSpillProfile.Enabled)
        {
            StepSpillProfile.End(StepSpillProfile.Probe, m);
        }

        return group;
    }

    /// <summary>Hand the runs to <paramref name="trace"/> (its levels adopt them, no row decoded), if
    /// it reads this layout; false and nothing changes otherwise.</summary>
    internal bool AdoptInto(BlockedIndexedTrace trace)
    {
        if (!trace.TryAdoptRuns(_trace.Runs, _format, _trace.OrdersByRowHash, rows: false))
        {
            return false;
        }

        Interlocked.Increment(ref DeltaSpill.Adopted);
        Interlocked.Add(ref DeltaSpill.AdoptedRows, _entries);
        return true;
    }

    // The entries of hash in a run, as (block body, index), streamed: a giant group's blocks are read
    // one at a time, never all pinned at once (§5.27).
    private static IEnumerable<(ITraceBlock Block, int Index)> At(BlockedIndexedTrace.Run run, uint hash)
    {
        var b = run.Find(hash);
        if (b < 0)
        {
            yield break;
        }

        for (; b < run.Blocks.Length && run.FirstOf(b) <= hash; b++)
        {
            var block = run.Blocks[b].Body;
            for (var i = block.LowerBound(hash); i < block.Hashes.Length && block.Hashes[i] == hash; i++)
            {
                yield return (block, i);
            }
        }
    }

    // Groups across runs: a hash only one run holds counts its keys (consolidated there, so every group
    // is non-empty); a hash several runs hold is resolved by summing their entries.
    private int CountGroups(uint[][] hashes)
    {
        var runs = _trace.Runs;
        if (runs.Count != hashes.Length)
        {
            throw new InvalidOperationException("a run's key hashes are missing");
        }

        if (runs.Count == 1)
        {
            return hashes[0].Length;
        }

        var walk = new HashWalk(hashes);
        long count = 0;
        while (walk.Next(out var hash, out var tied, out var groups))
        {
            count += tied.Count == 1 ? groups : CountTied(tied.Select(r => runs[r]).ToArray(), hash);
        }

        return (int)Math.Min(count, int.MaxValue);
    }

    // The non-empty groups of a hash several runs hold, without building them (§5.27): a key whose
    // entries all have one sign cannot cancel, so its group is non-empty whatever its values. Only a
    // key with both signs is resolved by summing. A giant group (one key over many runs) is a stream of
    // its entries, not a materialized set.
    private static int CountTied(BlockedIndexedTrace.Run[] runs, uint hash)
    {
        var keys = new List<StructuralRow>();
        var signs = new List<int>(); // bit 1: a positive weight seen, bit 2: a negative one
        foreach (var run in runs)
        {
            foreach (var (block, i) in At(run, hash))
            {
                var k = 0;
                while (k < keys.Count && !block.KeyEquals(i, keys[k]))
                {
                    k++;
                }

                if (k == keys.Count)
                {
                    keys.Add(block.Key(i));
                    signs.Add(0);
                }

                signs[k] |= block.Weight(i) > 0 ? 1 : 2;
            }
        }

        var count = 0;
        List<StructuralRow>? mixed = null;
        for (var k = 0; k < keys.Count; k++)
        {
            if (signs[k] == 3)
            {
                (mixed ??= []).Add(keys[k]);
            }
            else
            {
                count++;
            }
        }

        if (mixed is not null)
        {
            foreach (var group in Resolve(runs, hash))
            {
                if (mixed.Exists(m => m.Equals(group.Key)))
                {
                    count++;
                }
            }
        }

        return count;
    }

    // The non-empty groups of hash across runs, summed.
    private static List<KeyValuePair<StructuralRow, ZSet<StructuralRow, Z64>>> Resolve(IEnumerable<BlockedIndexedTrace.Run> runs, uint hash)
    {
        var keys = new List<StructuralRow>();
        var builders = new List<ZSetBuilder<StructuralRow, Z64>>();
        foreach (var run in runs)
        {
            foreach (var (block, i) in At(run, hash))
            {
                var k = 0;
                while (k < keys.Count && !block.KeyEquals(i, keys[k]))
                {
                    k++;
                }

                if (k == keys.Count)
                {
                    keys.Add(block.Key(i));
                    builders.Add(new ZSetBuilder<StructuralRow, Z64>());
                }

                builders[k].Add(block.Value(i), new Z64(block.Weight(i)));
            }
        }

        var groups = new List<KeyValuePair<StructuralRow, ZSet<StructuralRow, Z64>>>(keys.Count);
        for (var k = 0; k < keys.Count; k++)
        {
            if (builders[k].Build() is { IsEmpty: false } group)
            {
                groups.Add(new(keys[k], group));
            }
        }

        return groups;
    }

    public override IEnumerator<KeyValuePair<StructuralRow, ZSet<StructuralRow, Z64>>> GetEnumerator()
    {
        Interlocked.Increment(ref StepSpillProfile.Enumerations);
        var runs = _trace.Runs.ToArray();
        if (runs.Length != _hashes.Length)
        {
            throw new InvalidOperationException("a run's key hashes are missing");
        }

        var walk = new HashWalk(_hashes);
        var one = new BlockedIndexedTrace.Run[1];
        while (walk.Next(out var hash, out var tied, out _))
        {
            IEnumerable<BlockedIndexedTrace.Run> holding = tied.Count == 1 ? Single(one, runs[tied[0]]) : tied.Select(r => runs[r]).ToArray();

            // A giant group is handed out streamed (§5.36), not resolved into a managed set: a join over
            // a skewed key held 7.2M decoded rows of it per worker, and three such workers at once ran a
            // 48 GiB heap out of memory.
            if (StreamGiantGroups && GiantEntries(holding, hash) >= BlockedIndexedTrace.GiantGroupEntries
                && StreamedGroup.TryCreate([.. holding], hash) is { } streamed)
            {
                Interlocked.Increment(ref StreamedGroups);
                yield return new(streamed.Key, new ZSet<StructuralRow, Z64>(streamed));
                continue;
            }

            foreach (var group in Resolve(holding, hash))
            {
                StepSpillProfile.RowDecoded(group.Value.Count);
                yield return group;
            }
        }
    }

    /// <summary>Whether enumeration hands a giant group out streamed (§5.36). Off for A/B
    /// (<c>DBSPNET_STREAM_GIANT_GROUPS=off</c>).</summary>
    internal static bool StreamGiantGroups { get; set; } =
        !string.Equals(Environment.GetEnvironmentVariable("DBSPNET_STREAM_GIANT_GROUPS"), "off", StringComparison.OrdinalIgnoreCase);

    /// <summary>Giant groups handed out streamed (diagnostics).</summary>
    internal static long StreamedGroups;

    // A lower bound on hash's entries across the runs, from block metadata alone (no block is read): the
    // blocks the hash fills whole count fully (a spanning hash's blocks hold only it), others one.
    private static long GiantEntries(IEnumerable<BlockedIndexedTrace.Run> runs, uint hash)
    {
        long n = 0;
        foreach (var run in runs)
        {
            var b = run.Find(hash);
            if (b < 0)
            {
                continue;
            }

            for (; b < run.Blocks.Length && run.FirstOf(b) <= hash; b++)
            {
                var block = run.Blocks[b];
                n += block.FirstHash == hash && block.LastHash == hash ? block.Count : 1;
            }
        }

        return n;
    }

    /// <summary>
    /// One giant group of a block-backed indexed delta, read from its runs as it is walked (§5.36): every
    /// run holds the hash as one key, its values in ascending value hash (as freezes and merges write them),
    /// so a k-way walk by stored value hash yields the group's rows in order, decoding only the entries it
    /// yields, and summing entries of equal value hash (decoded) only where several share one. The exact
    /// count is one such walk at creation that decodes only those ties. A point lookup materializes the
    /// group once. The runs are the delta's: the group is read while the delta is.
    /// </summary>
    private sealed class StreamedGroup : ZSetExternalStore<StructuralRow, Z64>
    {
        private readonly BlockedIndexedTrace.Run[] _runs;
        private readonly uint _hash;
        private readonly int _count;
        private ZSet<StructuralRow, Z64>? _materialized;

        private StreamedGroup(BlockedIndexedTrace.Run[] runs, uint hash, StructuralRow key, int count)
        {
            _runs = runs;
            _hash = hash;
            Key = key;
            _count = count;
        }

        public StructuralRow Key { get; }

        // The group, if every run holds the hash as one key with its values in value-hash order; null
        // (resolve it instead) otherwise, or when its entries all cancel.
        public static StreamedGroup? TryCreate(BlockedIndexedTrace.Run[] runs, uint hash)
        {
            StructuralRow? key = null;
            foreach (var run in runs)
            {
                foreach (var (block, i) in At(run, hash))
                {
                    key = block.Key(i);
                    break;
                }

                if (key is not null)
                {
                    break;
                }
            }

            if (key is null)
            {
                return null;
            }

            var ok = new StrongBox<bool>(true);
            var count = 0;
            foreach (var _ in Walk(runs, hash, key, ok))
            {
                count++;
            }

            return ok.Value && count > 0 ? new StreamedGroup(runs, hash, key, count) : null;
        }

        public override int Count => _count;

        public override bool TryGetWeight(StructuralRow key, out Z64 weight)
        {
            weight = (_materialized ??= Materialize()).WeightOf(key);
            return !Z64.IsZero(weight);
        }

        public override IEnumerator<KeyValuePair<StructuralRow, Z64>> GetEnumerator()
        {
            foreach (var (block, index, value, weight) in Walk(_runs, _hash, Key, null))
            {
                yield return new(value ?? block.Value(index), new Z64(weight));
            }
        }

        public override ZSetBuilder<StructuralRow, Z64> NewBuilder(int capacity) => new(capacity);

        public override long ResidentBytes => 0;

        private ZSet<StructuralRow, Z64> Materialize()
        {
            var b = new ZSetBuilder<StructuralRow, Z64>(_count);
            using var e = GetEnumerator();
            while (e.MoveNext())
            {
                b.Add(e.Current.Key, e.Current.Value);
            }

            return b.Build();
        }

        // The group's surviving entries in value-hash order: (block, index, decoded value or null, summed
        // weight). With ok, each entry is checked (the key, ascending value hash) and the walk stops,
        // ok false, at the first that fails.
        private static IEnumerable<(ITraceBlock Block, int Index, StructuralRow? Value, long Weight)> Walk(
            BlockedIndexedTrace.Run[] runs, uint hash, StructuralRow key, StrongBox<bool>? ok)
        {
            var n = runs.Length;
            var its = new IEnumerator<(ITraceBlock Block, int Index)>[n];
            var has = new bool[n];
            var heads = new uint[n];
            for (var j = 0; j < n; j++)
            {
                its[j] = At(runs[j], hash).GetEnumerator();
                if (!Advance(j, first: true))
                {
                    yield break;
                }
            }

            var tied = new List<(ITraceBlock Block, int Index, long Weight)>();
            Dictionary<StructuralRow, (ITraceBlock Block, int Index, long Weight)>? sums = null;
            while (true)
            {
                var any = false;
                var min = 0u;
                for (var j = 0; j < n; j++)
                {
                    if (has[j] && (!any || heads[j] < min))
                    {
                        (min, any) = (heads[j], true);
                    }
                }

                if (!any)
                {
                    yield break;
                }

                tied.Clear();
                for (var j = 0; j < n; j++)
                {
                    while (has[j] && heads[j] == min)
                    {
                        var (block, index) = its[j].Current;
                        tied.Add((block, index, block.Weight(index)));
                        if (!Advance(j, first: false))
                        {
                            yield break;
                        }
                    }
                }

                if (tied.Count == 1)
                {
                    var (block, index, weight) = tied[0];
                    if (weight != 0)
                    {
                        yield return (block, index, null, weight);
                    }

                    continue;
                }

                // Entries sharing a value hash: equal rows sum, in first-seen order.
                sums ??= [];
                sums.Clear();
                var order = new List<StructuralRow>(tied.Count);
                foreach (var (block, index, weight) in tied)
                {
                    var value = block.Value(index);
                    if (sums.TryGetValue(value, out var at))
                    {
                        sums[value] = (at.Block, at.Index, at.Weight + weight);
                    }
                    else
                    {
                        sums[value] = (block, index, weight);
                        order.Add(value);
                    }
                }

                foreach (var value in order)
                {
                    var (block, index, weight) = sums[value];
                    if (weight != 0)
                    {
                        yield return (block, index, value, weight);
                    }
                }
            }

            // Move run j's cursor on; false (ok cleared) when a checked entry is not the key or goes back.
            bool Advance(int j, bool first)
            {
                if (!its[j].MoveNext())
                {
                    has[j] = false;
                    return true;
                }

                var (block, index) = its[j].Current;
                var vh = (uint)block.ValueHash(index);
                if (ok is not null && (!block.KeyEquals(index, key) || (!first && vh < heads[j])))
                {
                    ok.Value = false;
                    return false;
                }

                heads[j] = vh;
                has[j] = true;
                return true;
            }
        }
    }

    private static BlockedIndexedTrace.Run[] Single(BlockedIndexedTrace.Run[] one, BlockedIndexedTrace.Run run)
    {
        one[0] = run;
        return one;
    }

    // A k-way walk over runs' sorted key-hash arrays (in memory: no block is read), one hash at a time.
    private sealed class HashWalk
    {
        private readonly uint[][] _hashes;
        private readonly int[] _pos;
        private readonly PriorityQueue<int, uint> _queue;
        private readonly List<int> _tied = [];

        public HashWalk(uint[][] hashes)
        {
            _hashes = hashes;
            _pos = new int[hashes.Length];
            _queue = new PriorityQueue<int, uint>(hashes.Length);
            for (var r = 0; r < hashes.Length; r++)
            {
                if (hashes[r].Length > 0)
                {
                    _queue.Enqueue(r, hashes[r][0]);
                }
            }
        }

        // The next hash, the runs holding it, and how many key hashes they hold of it in all.
        public bool Next(out uint hash, out List<int> tied, out int entries)
        {
            tied = _tied;
            tied.Clear();
            entries = 0;
            if (!_queue.TryDequeue(out var r, out hash))
            {
                return false;
            }

            while (true)
            {
                tied.Add(r);
                var a = _hashes[r];
                var p = _pos[r];
                var e = p;
                while (e < a.Length && a[e] == hash)
                {
                    e++;
                }

                entries += e - p;
                _pos[r] = e;
                if (!_queue.TryPeek(out r, out var next) || next != hash)
                {
                    break;
                }

                _queue.Dequeue();
            }

            foreach (var t in tied)
            {
                if (_pos[t] < _hashes[t].Length)
                {
                    _queue.Enqueue(t, _hashes[t][_pos[t]]);
                }
            }

            return true;
        }
    }
}

/// <summary>D4: where index operators get a builder for an indexed output stream.</summary>
internal static class DeltaSpillGroups
{
    /// <summary>A builder for an indexed structural delta of <paramref name="format"/> (null: plain).</summary>
    public static IndexedZSetBuilder<TKey, TValue, TWeight> For<TKey, TValue, TWeight>(object? format)
        where TKey : notnull
        where TValue : notnull
        where TWeight : struct, IZRing<TWeight>
    {
        if (format is not ITraceBlockFormat f || SpillSettings.StepSpillRows is not { } rows
            || typeof(TKey) != typeof(StructuralRow) || typeof(TValue) != typeof(StructuralRow) || typeof(TWeight) != typeof(Z64))
        {
            return new IndexedZSetBuilder<TKey, TValue, TWeight>();
        }

        var chunk = rows == 0 ? SpillSettings.ForcedChunkRows : (int)Math.Min(rows, int.MaxValue);
        return Unsafe.As<IndexedZSetBuilder<TKey, TValue, TWeight>>(
            new IndexedZSetBuilder<StructuralRow, StructuralRow, Z64>(new SpillingGroups(f, chunk, alwaysBlocked: rows == 0,
                chunkBytes: rows == 0 ? 0 : SpillSettings.StepSpillBytes)));
    }
}
