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
/// <b>Giant groups are read at restore.</b> A block never splits a group, so a group larger than
/// a block fills a block of its own, and reading its key means decoding all of it. Those keys are
/// the low-cardinality join keys almost every batch touches (on ivm-bench SF=3, 14 blocks holding
/// 8% of the state, and the first resumed batch decoded nearly all of them). A block holding one
/// group of more than <see cref="GiantGroupEntries"/> entries is decoded when the chain is opened
/// and its key resolved straight away.
/// </remarks>
internal sealed class RunColdGroups : IColdGroups<StructuralRow, StructuralRow, Z64>
{
    /// <summary>A single group above this many entries is read at restore, not on demand.</summary>
    internal const int GiantGroupEntries = 4 * RunFile.DefaultBlockEntries;

    private readonly RunReader[] _runs;
    private readonly IIndexedRunCodec _codec;
    private Dictionary<(int Run, int Block), IndexedZSet<StructuralRow, StructuralRow, Z64>>? _preloaded;

    private RunColdGroups(
        RunReader[] runs, IIndexedRunCodec codec, int groupCount,
        Dictionary<(int, int), IndexedZSet<StructuralRow, StructuralRow, Z64>> preloaded, List<StructuralRow> preloadKeys)
    {
        _runs = runs;
        _codec = codec;
        GroupCount = groupCount;
        _preloaded = preloaded;
        PreloadKeys = preloadKeys;
    }

    public int GroupCount { get; }

    public IReadOnlyCollection<StructuralRow> PreloadKeys { get; private set; }

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
        var preloaded = new Dictionary<(int, int), IndexedZSet<StructuralRow, StructuralRow, Z64>>();
        var keys = new HashSet<StructuralRow>();
        for (var r = 0; r < runs.Length; r++)
        {
            runs[r] = await RunReader.OpenDetachedAsync(chain.Store, chain.Paths[r], hash, cancellationToken).ConfigureAwait(false);
            for (var b = 0; b < runs[r].Blocks.Count; b++)
            {
                if (runs[r].Blocks[b] is { Groups: 1, Entries: > GiantGroupEntries })
                {
                    var block = await runs[r].DecodeIndexedBlockAsync(b, codec, cancellationToken).ConfigureAwait(false);
                    preloaded[(r, b)] = block;
                    keys.UnionWith(block.Keys);
                }
            }
        }

        return new RunColdGroups(runs, codec, checked((int)chain.Count), preloaded, [.. keys]);
    }

    public void ReleasePreload()
    {
        _preloaded = null;
        PreloadKeys = [];
    }

    public ZSet<StructuralRow, Z64> Read(StructuralRow key)
    {
        ZSet<StructuralRow, Z64>? only = null;
        ZSetBuilder<StructuralRow, Z64>? sum = null;
        for (var r = 0; r < _runs.Length; r++)
        {
            var b = _runs[r].BlockOf(key);
            if (b < 0)
            {
                continue;
            }

            // Synchronous on purpose: the operator that asked is on the step thread, waiting.
            var block = _preloaded is not null && _preloaded.TryGetValue((r, b), out var held)
                ? held
                : _runs[r].DecodeIndexedBlockAsync(b, _codec).AsTask().GetAwaiter().GetResult();
            var group = block.GroupFor(key);
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

        return sum?.Build() ?? only ?? ZSet<StructuralRow, Z64>.Empty;
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
