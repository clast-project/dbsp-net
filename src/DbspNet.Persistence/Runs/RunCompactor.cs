// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Collections;
using DbspNet.Core.IO;

namespace DbspNet.Persistence.Runs;

/// <summary>
/// Merges a chain's runs into one run, off the step thread (docs/design-durable-state.md §1f,
/// increment 7). The delta runs are small by construction (a chain is extended only while its deltas
/// stay under half its base), so they are read whole and sorted by their stored order hash; the base
/// streams block by block, each base block's hash range taking the delta entries that fall in it.
/// Equal entries sum, zeros drop, and the output is cut as a written run is — a group larger than a
/// block alone in its blocks, and inside a hash only past <see cref="RunFile.MaxSpanBlockEntries"/> (§5.18)
/// — so it reads like any base run. Memory: one
/// base block plus the deltas.
/// </summary>
internal static class RunCompactor
{
    private static int RowHash(StructuralRow row) => row.GetHashCode();

    private static async ValueTask<RunReader[]> OpenAsync(RunChain chain, CancellationToken cancellationToken)
    {
        var runs = new RunReader[chain.Paths.Count];
        for (var r = 0; r < runs.Length; r++)
        {
            // Read by stored hash only; the footer pins the row hash whatever the order.
            runs[r] = await RunReader.OpenDetachedAsync(chain.Store, chain.Paths[r], RowHash, null, cancellationToken).ConfigureAwait(false);
        }

        return runs;
    }

    /// <summary>Merge a Z-set chain into <paramref name="outPath"/>; returns the entries written.</summary>
    internal static async ValueTask<long> CompactZSetAsync(
        RunChain chain, IZSetRunCodec codec, string outPath, CancellationToken cancellationToken)
    {
        var runs = await OpenAsync(chain, cancellationToken).ConfigureAwait(false);
        var deltas = new List<(uint Hash, StructuralRow Row, long Weight)>();
        for (var r = 1; r < runs.Length; r++)
        {
            for (var b = 0; b < runs[r].Blocks.Count; b++)
            {
                var (entries, hashes) = await runs[r].ReadBlockAsync(b, codec.DecodeEntries, cancellationToken).ConfigureAwait(false);
                for (var i = 0; i < entries.Length; i++)
                {
                    deltas.Add((hashes[i], entries[i].Row, entries[i].Weight));
                }
            }
        }

        var sorted = deltas.OrderBy(d => d.Hash).Select(d => (d.Hash, d.Row, d.Weight)).ToList(); // stable
        await using var writer = await RunStreamWriter.CreateAsync(
            chain.Store, outPath, codec.RunSchema, RunFile.Kind.ZSet, RowHash(RunFile.FingerprintRow), cancellationToken).ConfigureAwait(false);
        await MergeBaseAsync(
            runs[0],
            sorted,
            writer,
            async b =>
            {
                var (entries, hashes) = await runs[0].ReadBlockAsync(b, codec.DecodeEntries, cancellationToken).ConfigureAwait(false);
                return ([.. entries.Select(e => e.Row)], [.. entries.Select(e => e.Weight)], hashes);
            },
            chunk => EmitAsync(chunk.Select(e => (e.Value.Hash, e.Key, e.Value.Weight)), writer, codec, cancellationToken),
            cancellationToken).ConfigureAwait(false);
        return (await writer.FinishAsync(cancellationToken).ConfigureAwait(false)).Entries;
    }

    /// <summary>Merge an indexed chain into <paramref name="outPath"/>; returns the entries written.</summary>
    internal static async ValueTask<long> CompactIndexedAsync(
        RunChain chain, IIndexedRunCodec codec, string outPath, CancellationToken cancellationToken)
    {
        var runs = await OpenAsync(chain, cancellationToken).ConfigureAwait(false);
        var deltas = new List<(uint Hash, StructuralRow Key, StructuralRow Value, long Weight)>();
        for (var r = 1; r < runs.Length; r++)
        {
            for (var b = 0; b < runs[r].Blocks.Count; b++)
            {
                var (entries, hashes) = await runs[r].ReadBlockAsync(b, codec.DecodeEntries, cancellationToken).ConfigureAwait(false);
                for (var i = 0; i < entries.Length; i++)
                {
                    deltas.Add((hashes[i], entries[i].Key, entries[i].Value, entries[i].Weight));
                }
            }
        }

        var sorted = deltas.OrderBy(x => x.Hash).Select(x => (x.Hash, (x.Key, x.Value), x.Weight)).ToList();
        await using var writer = await RunStreamWriter.CreateAsync(
            chain.Store, outPath, codec.RunSchema, RunFile.Kind.Indexed, RowHash(RunFile.FingerprintRow), cancellationToken).ConfigureAwait(false);
        await MergeBaseAsync(
            runs[0],
            sorted,
            writer,
            async b =>
            {
                var (entries, hashes) = await runs[0].ReadBlockAsync(b, codec.DecodeEntries, cancellationToken).ConfigureAwait(false);
                return ([.. entries.Select(e => (e.Key, e.Value))], [.. entries.Select(e => e.Weight)], hashes);
            },
            chunk => EmitIndexedAsync(chunk, writer, codec, cancellationToken),
            cancellationToken).ConfigureAwait(false);
        return (await writer.FinishAsync(cancellationToken).ConfigureAwait(false)).Entries;
    }

    /// <summary>
    /// The base run streamed block by block, each block's hash range taking the (sorted) delta entries
    /// that fall in it. A block no delta touches is copied raw. A giant group's hash can span consecutive
    /// base blocks (§5.18): a delta entry of that hash that matches nothing in the first block is carried
    /// to the next ones, and emitted only where the hash's span ends, so it consolidates against every
    /// base entry of its hash while only one base block is ever decoded. Equal entries sum; zeros drop
    /// in the emit.
    /// </summary>
    private static async ValueTask MergeBaseAsync<TKey>(
        RunReader baseRun,
        List<(uint Hash, TKey Key, long Weight)> sorted,
        RunStreamWriter writer,
        Func<int, ValueTask<(TKey[] Keys, long[] Weights, uint[] Hashes)>> readBlock,
        Func<Dictionary<TKey, (uint Hash, long Weight)>, ValueTask> emit,
        CancellationToken cancellationToken)
        where TKey : notnull
    {
        var blocks = baseRun.Blocks;
        var d = 0;
        var chunk = new Dictionary<TKey, (uint Hash, long Weight)>();
        var pending = new Dictionary<TKey, (uint Hash, long Weight)>(); // of the hash the last block ended with
        for (var b = 0; b < blocks.Count; b++)
        {
            var meta = blocks[b];
            var spansOut = b + 1 < blocks.Count && blocks[b + 1].FirstHash == meta.LastHash;

            // No delta entry falls in this block and none is carried into it: copy it unchanged — no
            // decode, no row objects (most blocks: the deltas are small).
            if (pending.Count == 0 && (d >= sorted.Count || sorted[d].Hash > meta.LastHash))
            {
                var (message, keyHashes) = await baseRun.ReadBlockRawAsync(b, cancellationToken).ConfigureAwait(false);
                await writer.AppendRawAsync(message, keyHashes, meta.Entries, meta.Groups, cancellationToken).ConfigureAwait(false);
                continue;
            }

            var (keys, weights, hashes) = await readBlock(b).ConfigureAwait(false);
            chunk.Clear();
            for (var i = 0; i < keys.Length; i++)
            {
                chunk[keys[i]] = (hashes[i], weights[i]);
            }

            // Carried entries (this block begins with their hash): those this block holds merge here.
            foreach (var key in pending.Keys.ToArray())
            {
                if (chunk.TryGetValue(key, out var had))
                {
                    chunk[key] = (had.Hash, had.Weight + pending[key].Weight);
                    pending.Remove(key);
                }
            }

            // Their span ends here unless this block is wholly that hash and the next continues it.
            if (pending.Count > 0 && !(spansOut && meta.FirstHash == meta.LastHash))
            {
                foreach (var (key, e) in pending)
                {
                    chunk[key] = e;
                }

                pending.Clear();
            }

            for (; d < sorted.Count && sorted[d].Hash <= meta.LastHash; d++)
            {
                var (h, key, w) = sorted[d];
                if (chunk.TryGetValue(key, out var had))
                {
                    chunk[key] = (h, had.Weight + w);
                }
                else if (spansOut && h == meta.LastHash)
                {
                    pending[key] = pending.TryGetValue(key, out var p) ? (h, p.Weight + w) : (h, w); // may be in a later block
                }
                else
                {
                    chunk[key] = (h, w);
                }
            }

            await emit(chunk).ConfigureAwait(false);
        }

        chunk.Clear();
        foreach (var (key, e) in pending)
        {
            chunk[key] = e;
        }

        for (; d < sorted.Count; d++)
        {
            var (h, key, w) = sorted[d];
            chunk[key] = chunk.TryGetValue(key, out var had) ? (h, had.Weight + w) : (h, w);
        }

        await emit(chunk).ConfigureAwait(false);
    }

    // Blocks of a chunk: entries sorted by hash, zeros dropped, cut as RunFile cuts — never inside a
    // hash, and a hash run of a block's worth or more in a block of its own.
    private static List<(int From, int To)> Cuts(uint[] hashes)
    {
        var blocks = new List<(int, int)>();
        var start = 0;
        var i = 0;
        while (i < hashes.Length)
        {
            var end = i;
            while (end < hashes.Length && hashes[end] == hashes[i])
            {
                end++;
            }

            var runLength = end - i;
            var size = i - start;
            if (size > 0 && (size >= RunFile.DefaultBlockEntries || runLength >= RunFile.DefaultBlockEntries))
            {
                blocks.Add((start, i));
                start = i;
            }

            // A giant hash run past the cap continues from block to block (§5.18): a block is one message
            // read into one array.
            if (runLength > RunFile.MaxSpanBlockEntries)
            {
                if (i > start)
                {
                    blocks.Add((start, i)); // the entries before it, whatever the cap
                }

                for (var s = i; s < end; s += RunFile.MaxSpanBlockEntries)
                {
                    blocks.Add((s, Math.Min(end, s + RunFile.MaxSpanBlockEntries)));
                }

                start = end;
            }

            i = end;
        }

        if (start < hashes.Length)
        {
            blocks.Add((start, hashes.Length));
        }

        return blocks;
    }

    private static List<uint> Distinct(uint[] hashes, int from, int to)
    {
        var distinct = new List<uint>();
        for (var i = from; i < to; i++)
        {
            if (distinct.Count == 0 || distinct[^1] != hashes[i])
            {
                distinct.Add(hashes[i]);
            }
        }

        return distinct;
    }

    private static async ValueTask EmitAsync(
        IEnumerable<(uint Hash, StructuralRow Row, long Weight)> chunk, RunStreamWriter writer, IZSetRunCodec codec, CancellationToken cancellationToken)
    {
        var entries = chunk.Where(e => e.Weight != 0).OrderBy(e => e.Hash).ToArray();
        if (entries.Length == 0)
        {
            return;
        }

        var hashes = entries.Select(e => e.Hash).ToArray();
        foreach (var (from, to) in Cuts(hashes))
        {
            var n = to - from;
            var rows = new StructuralRow[n];
            var weights = new long[n];
            var stored = new int[n];
            for (var i = 0; i < n; i++)
            {
                (var h, rows[i], weights[i]) = entries[from + i];
                stored[i] = (int)h;
            }

            using var batch = codec.EncodeBlock(rows, weights, stored, n);
            await writer.AppendAsync(batch, Distinct(hashes, from, to), n, n, cancellationToken).ConfigureAwait(false);
        }
    }

    private static async ValueTask EmitIndexedAsync(
        Dictionary<(StructuralRow Key, StructuralRow Value), (uint Hash, long Weight)> chunk,
        RunStreamWriter writer, IIndexedRunCodec codec, CancellationToken cancellationToken)
    {
        var entries = chunk.Where(e => e.Value.Weight != 0).OrderBy(e => e.Value.Hash).ToArray();
        if (entries.Length == 0)
        {
            return;
        }

        var hashes = entries.Select(e => e.Value.Hash).ToArray();
        foreach (var (from, to) in Cuts(hashes))
        {
            var n = to - from;
            var keys = new StructuralRow[n];
            var values = new StructuralRow[n];
            var weights = new long[n];
            var stored = new int[n];
            var distinctKeys = new HashSet<StructuralRow>();
            for (var i = 0; i < n; i++)
            {
                var ((k, v), (h, w)) = entries[from + i];
                keys[i] = k;
                values[i] = v;
                weights[i] = w;
                stored[i] = (int)h;
                distinctKeys.Add(k);
            }

            using var batch = codec.EncodeBlock(keys, values, weights, stored, n);
            await writer.AppendAsync(batch, Distinct(hashes, from, to), n, distinctKeys.Count, cancellationToken).ConfigureAwait(false);
        }
    }
}
