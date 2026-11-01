// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful;

namespace DbspNet.Persistence.Runs;

/// <summary>
/// A partitioned operator's saved rows as a chain of partition-ordered runs, read a partition at a
/// time (docs/design-durable-state.md §1f, increment 5). The runs are ordered by the partition key's
/// hash and a block never splits a hash, so a partition is in one block of each run: reading it
/// decodes that block per run (the caller drops rows of other partitions sharing the hash).
/// </summary>
internal sealed class RunColdRows : IColdRows<StructuralRow>
{
    private readonly RunReader[] _runs;
    private readonly IZSetRunCodec _codec;

    private RunColdRows(RunReader[] runs, IZSetRunCodec codec, long count)
    {
        _runs = runs;
        _codec = codec;
        Count = count;
    }

    public long Count { get; }

    internal static async ValueTask<RunColdRows> OpenAsync(RunChain chain, IZSetRunCodec codec, CancellationToken cancellationToken)
    {
        var runs = new RunReader[chain.Paths.Count];
        for (var r = 0; r < runs.Length; r++)
        {
            // Probed by hash value (BlockOfHash), never by row; the footer pins the row hash.
            runs[r] = await RunReader.OpenDetachedAsync(
                chain.Store, chain.Paths[r], RowHash, RowHash, cancellationToken).ConfigureAwait(false);
        }

        var cold = new RunColdRows(runs, codec, chain.Count);
        RunPins.Add(cold, chain.Store, chain.Paths);
        return cold;
    }

    private static int RowHash(StructuralRow row) => row.GetHashCode();

    public ZSet<StructuralRow, Z64> ReadByHash(int hash)
    {
        ZSetBuilder<StructuralRow, Z64>? sum = null;
        ZSet<StructuralRow, Z64>? only = null;
        foreach (var run in _runs)
        {
            var b = run.BlockOfHash(hash);
            if (b < 0)
            {
                continue;
            }

            // Synchronous on purpose: the operator that asked is on the step thread, waiting.
            var block = run.DecodeZSetBlockAsync(b, _codec).AsTask().GetAwaiter().GetResult();
            if (only is null && sum is null)
            {
                only = block;
                continue;
            }

            if (sum is null)
            {
                sum = new ZSetBuilder<StructuralRow, Z64>();
                foreach (var (row, w) in only!)
                {
                    sum.Add(row, w);
                }
            }

            foreach (var (row, w) in block)
            {
                sum.Add(row, w);
            }
        }

        return sum?.Build() ?? only ?? ZSet<StructuralRow, Z64>.Empty;
    }

    public IEnumerable<KeyValuePair<StructuralRow, Z64>> ReadAll()
    {
        var into = new ZSetBuilder<StructuralRow, Z64>((int)Math.Min(int.MaxValue, Math.Max(0, Count)));
        foreach (var run in _runs)
        {
            run.ReadAllIntoAsync(_codec, into).AsTask().GetAwaiter().GetResult();
        }

        return into.Build();
    }
}
