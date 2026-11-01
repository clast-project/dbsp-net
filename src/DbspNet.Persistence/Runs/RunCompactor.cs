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
/// Equal entries sum, zeros drop, and the output is cut exactly as a written run is — never inside a
/// hash, a group larger than a block alone in its block — so it reads like any base run. Memory: one
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

        var sorted = deltas.OrderBy(d => d.Hash).ToList(); // stable
        await using var writer = await RunStreamWriter.CreateAsync(
            chain.Store, outPath, codec.RunSchema, RunFile.Kind.ZSet, RowHash(RunFile.FingerprintRow), cancellationToken).ConfigureAwait(false);
        var d = 0;
        var chunk = new Dictionary<StructuralRow, (uint Hash, long Weight)>();
        foreach (var block in runs[0].Blocks.Select((_, i) => i))
        {
            // No delta entry falls in this block's chunk: copy it unchanged — no decode, no
            // row objects (most blocks: the deltas are small). Only touched blocks are merged.
            var meta = runs[0].Blocks[block];
            if (d >= sorted.Count || sorted[d].Hash > meta.LastHash)
            {
                var (message, keyHashes) = await runs[0].ReadBlockRawAsync(block, cancellationToken).ConfigureAwait(false);
                await writer.AppendRawAsync(message, keyHashes, meta.Entries, meta.Groups, cancellationToken).ConfigureAwait(false);
                continue;
            }

            var (entries, hashes) = await runs[0].ReadBlockAsync(block, codec.DecodeEntries, cancellationToken).ConfigureAwait(false);
            chunk.Clear();
            for (var i = 0; i < entries.Length; i++)
            {
                Add(chunk, entries[i].Row, hashes[i], entries[i].Weight);
            }

            for (var last = runs[0].Blocks[block].LastHash; d < sorted.Count && sorted[d].Hash <= last; d++)
            {
                Add(chunk, sorted[d].Row, sorted[d].Hash, sorted[d].Weight);
            }

            await EmitAsync(chunk.Select(e => (e.Value.Hash, e.Key, e.Value.Weight)), writer, codec, cancellationToken).ConfigureAwait(false);
        }

        chunk.Clear();
        for (; d < sorted.Count; d++)
        {
            Add(chunk, sorted[d].Row, sorted[d].Hash, sorted[d].Weight);
        }

        await EmitAsync(chunk.Select(e => (e.Value.Hash, e.Key, e.Value.Weight)), writer, codec, cancellationToken).ConfigureAwait(false);
        return (await writer.FinishAsync(cancellationToken).ConfigureAwait(false)).Entries;

        static void Add(Dictionary<StructuralRow, (uint Hash, long Weight)> into, StructuralRow row, uint hash, long weight) =>
            into[row] = into.TryGetValue(row, out var had) ? (hash, had.Weight + weight) : (hash, weight);
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

        var sorted = deltas.OrderBy(x => x.Hash).ToList();
        await using var writer = await RunStreamWriter.CreateAsync(
            chain.Store, outPath, codec.RunSchema, RunFile.Kind.Indexed, RowHash(RunFile.FingerprintRow), cancellationToken).ConfigureAwait(false);
        var d = 0;
        var chunk = new Dictionary<(StructuralRow, StructuralRow), (uint Hash, long Weight)>();
        foreach (var block in runs[0].Blocks.Select((_, i) => i))
        {
            // No delta entry falls in this block's chunk: copy it unchanged — no decode, no
            // row objects (most blocks: the deltas are small). Only touched blocks are merged.
            var meta = runs[0].Blocks[block];
            if (d >= sorted.Count || sorted[d].Hash > meta.LastHash)
            {
                var (message, keyHashes) = await runs[0].ReadBlockRawAsync(block, cancellationToken).ConfigureAwait(false);
                await writer.AppendRawAsync(message, keyHashes, meta.Entries, meta.Groups, cancellationToken).ConfigureAwait(false);
                continue;
            }

            var (entries, hashes) = await runs[0].ReadBlockAsync(block, codec.DecodeEntries, cancellationToken).ConfigureAwait(false);
            chunk.Clear();
            for (var i = 0; i < entries.Length; i++)
            {
                Add(chunk, entries[i].Key, entries[i].Value, hashes[i], entries[i].Weight);
            }

            for (var last = runs[0].Blocks[block].LastHash; d < sorted.Count && sorted[d].Hash <= last; d++)
            {
                Add(chunk, sorted[d].Key, sorted[d].Value, sorted[d].Hash, sorted[d].Weight);
            }

            await EmitIndexedAsync(chunk, writer, codec, cancellationToken).ConfigureAwait(false);
        }

        chunk.Clear();
        for (; d < sorted.Count; d++)
        {
            Add(chunk, sorted[d].Key, sorted[d].Value, sorted[d].Hash, sorted[d].Weight);
        }

        await EmitIndexedAsync(chunk, writer, codec, cancellationToken).ConfigureAwait(false);
        return (await writer.FinishAsync(cancellationToken).ConfigureAwait(false)).Entries;

        static void Add(
            Dictionary<(StructuralRow, StructuralRow), (uint Hash, long Weight)> into, StructuralRow key, StructuralRow value, uint hash, long weight) =>
            into[(key, value)] = into.TryGetValue((key, value), out var had) ? (hash, had.Weight + weight) : (hash, weight);
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
