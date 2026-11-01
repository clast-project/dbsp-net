// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful;

namespace DbspNet.Persistence.Runs;

/// <summary>
/// A Z-set trace's state (an output view, a DISTINCT trace) as a chain of row-ordered runs, read a row
/// at a time (docs/design-durable-state.md §1f, increment 6). A row's weight is the sum over the runs
/// whose Bloom filter and block index admit it — a new row usually costs filter checks only.
/// </summary>
internal sealed class RunColdZSet : IColdZSet<StructuralRow, Z64>
{
    private readonly RunReader[] _runs;
    private readonly IZSetRunCodec _codec;

    private RunColdZSet(RunReader[] runs, IZSetRunCodec codec, int count)
    {
        _runs = runs;
        _codec = codec;
        Count = count;
    }

    public int Count { get; }

    internal static async ValueTask<RunColdZSet> OpenAsync(RunChain chain, IZSetRunCodec codec, CancellationToken cancellationToken)
    {
        var runs = new RunReader[chain.Paths.Count];
        for (var r = 0; r < runs.Length; r++)
        {
            runs[r] = await RunReader.OpenDetachedAsync(chain.Store, chain.Paths[r], RowHash, null, cancellationToken).ConfigureAwait(false);
        }

        var cold = new RunColdZSet(runs, codec, checked((int)chain.Count));
        RunPins.Add(cold, chain.Store, chain.Paths);
        return cold;
    }

    private static int RowHash(StructuralRow row) => row.GetHashCode();

    public Z64 WeightOf(StructuralRow key)
    {
        var sum = Z64.Zero;
        foreach (var run in _runs)
        {
            // Synchronous on purpose: the operator that asked is on the step thread, waiting.
            // Decodes only the entries of the key's hash, not the block.
            sum = Z64.Add(sum, run.WeightOfRowAsync(key, _codec).AsTask().GetAwaiter().GetResult());
        }

        return sum;
    }

    public IEnumerable<KeyValuePair<StructuralRow, Z64>> ReadAll()
    {
        if (_runs.Length == 1)
        {
            // One run: stream it block by block, nothing summed or held.
            return StreamOne(_runs[0]);
        }

        var into = new ZSetBuilder<StructuralRow, Z64>(Count);
        foreach (var run in _runs)
        {
            run.ReadAllIntoAsync(_codec, into).AsTask().GetAwaiter().GetResult();
        }

        return into.Build();
    }

    // A single run's entries are already consolidated (one per row), so its blocks stream in
    // stored order with no hashing — the first full write after a restore reads views this way.
    private IEnumerable<KeyValuePair<StructuralRow, Z64>> StreamOne(RunReader run)
    {
        for (var b = 0; b < run.Blocks.Count; b++)
        {
            var (entries, _) = run.ReadBlockAsync(b, _codec.DecodeEntries).AsTask().GetAwaiter().GetResult();
            foreach (var (row, weight) in entries)
            {
                yield return new KeyValuePair<StructuralRow, Z64>(row, new Z64(weight));
            }
        }
    }
}
