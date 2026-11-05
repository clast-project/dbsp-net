// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;

namespace DbspNet.Persistence.Runs;

/// <summary>
/// An indexed trace's state as a chain of runs, read a key at a time (docs/design-durable-state.md
/// §1f, increment 4). A key's group is the sum of its groups across the runs; each run is asked only
/// if its Bloom filter and block index say it may hold the key, so a key absent everywhere costs a
/// few filter checks and no I/O. Runs are open detached (footer and schema in memory).
/// </summary>
/// <remarks>
/// <b>Giant groups are decoded in the background from the restore on.</b> A block never splits a
/// group, so a group larger than a block fills a block of its own, and reading its key means
/// decoding all of it. Those keys are the low-cardinality join keys almost every batch touches (on
/// ivm-bench SF=3, 14 blocks holding 8% of the state). Each block holding one group of more than
/// <see cref="GiantGroupEntries"/> entries starts decoding when the chain is opened, off the restore
/// path (resolving them at restore put ~4.4 GiB of allocation and most of a fresh-process restore's
/// time on it); the first read of a giant key waits for its block if it is still decoding.
/// </remarks>
internal sealed class RunColdGroups : IColdGroups<StructuralRow, StructuralRow, Z64>
{
    /// <summary>A single group above this many entries is read at restore, not on demand.</summary>
    internal const int GiantGroupEntries = 4 * RunFile.DefaultBlockEntries;

    private readonly RunReader[] _runs;
    private readonly IIndexedRunCodec _codec;

    // Giant single-group blocks, decoded in the background from the restore on (see the class
    // remarks): a read of a giant key waits for its block, then takes it — one group per block, so
    // the block is dropped once read.
    private readonly System.Collections.Concurrent.ConcurrentDictionary<(int Run, int Block), Task<(StructuralRow Key, ZSet<StructuralRow, Z64> Group)>> _giant;

    private RunColdGroups(
        RunReader[] runs, IIndexedRunCodec codec, int groupCount,
        System.Collections.Concurrent.ConcurrentDictionary<(int, int), Task<(StructuralRow Key, ZSet<StructuralRow, Z64> Group)>> giant)
    {
        _runs = runs;
        _codec = codec;
        GroupCount = groupCount;
        _giant = giant;
    }

    public int GroupCount { get; }

    private static long _giantWaitTicks;
    private static long _giantWaits;

    /// <summary>Reads that waited for a giant block still decoding, and the wall time waited (diagnostics).</summary>
    internal static (long Waits, double Ms) GiantWaits =>
        (Interlocked.Read(ref _giantWaits), Interlocked.Read(ref _giantWaitTicks) * 1000.0 / System.Diagnostics.Stopwatch.Frequency);

    /// <summary>None: giant groups are decoded in the background and taken when first read, not
    /// resolved at restore (which put their decode — most of a restore's allocation — on the
    /// restore path).</summary>
    public IReadOnlyCollection<StructuralRow> PreloadKeys => [];

    /// <summary>Lazy restore is the default for chains; <c>DBSPNET_RESTORE=eager</c> reads every
    /// chain whole at restore instead (the increment 3 behaviour).</summary>
    internal static bool Enabled { get; set; } =
        !string.Equals(Environment.GetEnvironmentVariable("DBSPNET_RESTORE"), "eager", StringComparison.OrdinalIgnoreCase);

    internal static async ValueTask<RunColdGroups> OpenAsync(
        RunChain chain, IIndexedRunCodec codec, Func<StructuralRow, int> hash, CancellationToken cancellationToken)
    {
        if (chain.Count < 0)
        {
            throw new InvalidDataException("a run chain without a recorded count cannot be restored lazily");
        }

        var runs = new RunReader[chain.Paths.Count];
        var giant = new System.Collections.Concurrent.ConcurrentDictionary<(int, int), Task<(StructuralRow Key, ZSet<StructuralRow, Z64> Group)>>();
        for (var r = 0; r < runs.Length; r++)
        {
            runs[r] = await RunReader.OpenDetachedAsync(chain.Store, chain.Paths[r], hash, null, cancellationToken).ConfigureAwait(false);
            for (var b = 0; b < runs[r].Blocks.Count; b++)
            {
                if (runs[r].Blocks[b] is { Groups: 1, Entries: > GiantGroupEntries })
                {
                    var (run, block) = (runs[r], b);
                    giant[(r, b)] = Task.Run(async () =>
                        (await run.ReadBlockAsync(block, codec.DecodeSingleGroup, CancellationToken.None).ConfigureAwait(false)).Entries);
                }
            }
        }

        var cold = new RunColdGroups(runs, codec, checked((int)chain.Count), giant);
        RunPins.Add(cold, chain.Store, chain.Paths);
        return cold;
    }

    public void ReleasePreload()
    {
    }

    public ZSet<StructuralRow, Z64> Read(StructuralRow key)
    {
        ZSet<StructuralRow, Z64>? only = null;
        ZSetBuilder<StructuralRow, Z64>? sum = null;
        for (var r = 0; r < _runs.Length; r++)
        {
            // One block, or a giant group's consecutive blocks (its hash spans them, §5.18): each part
            // is read, and the parts sum like groups from separate runs.
            var (first, count) = _runs[r].BlocksOf(key);
            for (var b = first; b < first + count; b++)
            {
                var group = Part(r, b, key, whole: count == 1);
                if (group.IsEmpty)
                {
                    continue;
                }

                if (only is null && sum is null)
                {
                    only = group;
                    continue;
                }

                if (sum is null)
                {
                    sum = new ZSetBuilder<StructuralRow, Z64>();
                    foreach (var (v, w) in only!)
                    {
                        sum.Add(v, w);
                    }
                }

                foreach (var (v, w) in group)
                {
                    sum.Add(v, w);
                }
            }
        }

        return sum?.Build() ?? only ?? ZSet<StructuralRow, Z64>.Empty;
    }

    // Key's entries in block b of run r: a preloaded giant block's group if it is the key's, otherwise
    // only the key's entries decoded (the whole run's, when the key has this one block).
    private ZSet<StructuralRow, Z64> Part(int r, int b, StructuralRow key, bool whole)
    {
        // Synchronous on purpose: the operator that asked is on the step thread, waiting.
        if (!_giant.TryGetValue((r, b), out var held))
        {
            return whole
                ? _runs[r].GroupOfKeyAsync(key, _codec).AsTask().GetAwaiter().GetResult()
                : _runs[r].GroupOfKeyInBlockAsync(key, b, _codec).AsTask().GetAwaiter().GetResult();
        }

        // Decoded in the background (one group per block). A different key that shares the block's
        // hash is not in it; the giant key is taken, so the block is dropped.
        if (!held.IsCompleted)
        {
            var w0 = System.Diagnostics.Stopwatch.GetTimestamp();
            held.Wait();
            Interlocked.Add(ref _giantWaitTicks, System.Diagnostics.Stopwatch.GetTimestamp() - w0);
            Interlocked.Increment(ref _giantWaits);
        }

        var (giantKey, giantGroup) = held.GetAwaiter().GetResult();
        if (!giantKey.Equals(key))
        {
            return ZSet<StructuralRow, Z64>.Empty;
        }

        _giant.TryRemove((r, b), out _);
        return giantGroup;
    }

    public IEnumerable<KeyValuePair<StructuralRow, ZSet<StructuralRow, Z64>>> ReadAll()
    {
        var into = new IndexedZSetBuilder<StructuralRow, StructuralRow, Z64>();
        foreach (var run in _runs)
        {
            run.ReadAllIntoAsync(_codec, into).AsTask().GetAwaiter().GetResult();
        }

        return into.Build();
    }
}
