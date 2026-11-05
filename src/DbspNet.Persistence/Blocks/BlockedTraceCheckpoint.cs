// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Collections;
using DbspNet.Core.IO;
using DbspNet.Core.Operators.Stateful.Blocks;
using DbspNet.Persistence.Runs;
using ArrowSchema = Apache.Arrow.Schema;

namespace DbspNet.Persistence.Blocks;

/// <summary>
/// Checkpoints and restores a <see cref="BlockedIndexedTrace"/> as a run chain
/// (docs/design-flat-trace-state.md §2.6–§2.7). A checkpoint freezes the managed levels and names
/// one run file per in-memory run: a run already written by an earlier checkpoint is named again,
/// a new one is written from its blocks' bytes, with no decoding. Restore adopts each run file's
/// blocks lazily: a block is read on its first probe, and never decoded.
/// </summary>
/// <remarks>
/// <para><b>On disk the v3 invariant holds</b>: a hash never spans blocks. In memory a giant group's
/// hash may (to keep blocks off the large-object heap), so such a span is re-encoded as one block
/// when written. Any v3 reader — the managed trace's lazy restore included — reads these chains.</para>
/// <para>The chain is never compacted in the background (<see cref="RunCompactor"/>): the trace's
/// own merges replace runs, and the next checkpoint names the merged run. Its count is -1, so a
/// managed restore of it reads it eagerly rather than trusting a size nobody computed.</para>
/// </remarks>
internal static class BlockedTraceCheckpoint
{
    /// <summary>What a codec supplies to checkpoint a blocked trace: an indexed trace's codec writes
    /// indexed chains, a Z-set trace's (§3.7: rows as keys, no value columns) Z-set chains.</summary>
    internal interface ILayout
    {
        ArrowSchema RunSchema { get; }

        RunFile.Kind Kind { get; }

        /// <summary>The block encoding of the run blocks (null: the schema has none).</summary>
        BlockSchema? Blocks { get; }

        /// <summary>Write the trace whole, outside the runs layout.</summary>
        ValueTask SaveWholeAsync(Core.Circuit.ISnapshotWriter writer, string fileName, BlockedIndexedTrace trace, CancellationToken cancellationToken);

        /// <summary>Read a snapshot that is not a chain into the (empty) trace.</summary>
        ValueTask LoadWholeAsync(Core.Circuit.ISnapshotReader reader, string fileName, BlockedIndexedTrace trace, CancellationToken cancellationToken);

        /// <summary>Whether <paramref name="chain"/> sums to the trace's contents (verify mode).</summary>
        ValueTask<bool> MatchesAsync(RunChain chain, BlockedIndexedTrace trace, CancellationToken cancellationToken);
    }

    private static int RowHash(StructuralRow row) => row.GetHashCode();

    public static async ValueTask SaveAsync(
        ILayout codec, Core.Circuit.ISnapshotWriter writer, string fileName, BlockedIndexedTrace trace,
        CancellationToken cancellationToken)
    {
        if (!StateLayout.Runs || writer is not TableFileSystemSnapshotContext { Session: { } session } ctx)
        {
            await codec.SaveWholeAsync(writer, fileName, trace, cancellationToken).ConfigureAwait(false);
            return;
        }

        var t0 = Core.Operators.Stateful.Blocks.BlockTraceProfile.Now();
        var runs = trace.PrepareCheckpoint();
        if (Core.Operators.Stateful.Blocks.BlockTraceProfile.Enabled)
        {
            Core.Operators.Stateful.Blocks.BlockTraceProfile.Section(4, t0);
            t0 = Core.Operators.Stateful.Blocks.BlockTraceProfile.Now();
        }

        var paths = new List<string>(runs.Count);
        var written = new List<(BlockedIndexedTrace.Run Run, string Path, List<(RunBlock Index, TraceBlock? Body)> Bodies)>();
        long baseEntries = 0, deltaEntries = 0;

        // Chain order is base first: the oldest (largest) run.
        for (var r = runs.Count - 1; r >= 0; r--)
        {
            var run = runs[r];
            string path;
            if (run.Persisted is { } p && ReferenceEquals(p.Store, ctx.FileSystem))
            {
                path = p.Path;
            }
            else
            {
                path = "runs/" + ctx.Prefix + fileName + (written.Count == 0 ? string.Empty : "." + written.Count);
                var bodies = await WriteRunAsync(ctx.FileSystem, path, codec, run, cancellationToken).ConfigureAwait(false);
                written.Add((run, path, bodies));
            }

            paths.Add(path);
            if (paths.Count == 1)
            {
                baseEntries = run.Entries;
            }
            else
            {
                deltaEntries += run.Entries;
            }
        }

        if (Core.Operators.Stateful.Blocks.BlockTraceProfile.Enabled)
        {
            Core.Operators.Stateful.Blocks.BlockTraceProfile.Section(5, t0, written.Count);
        }

        // An empty trace writes an empty run, as the managed path does: a chain names at least one.
        if (paths.Count == 0)
        {
            var path = "runs/" + ctx.Prefix + fileName;
            await using (var empty = await RunStreamWriter.CreateAsync(
                ctx.FileSystem, path, codec.RunSchema, codec.Kind, RowHash(RunFile.FingerprintRow), cancellationToken).ConfigureAwait(false))
            {
                await empty.FinishAsync(cancellationToken).ConfigureAwait(false);
            }

            paths.Add(path);
        }

        var chain = new RunChain(ctx.FileSystem, paths, baseEntries, deltaEntries);
        session.Record(ctx.Prefix + fileName, chain);
        session.OnCommit(() =>
        {
            var schema = codec.Blocks!;
            foreach (var (run, path, bodies) in written)
            {
                run.Persisted = (ctx.FileSystem, path);

                // With a block budget (docs/design-bounded-memory.md §3.1): the run's blocks now
                // have a durable copy, so they become file blocks the cache may drop. A hash that
                // spanned blocks in memory is one block on disk, read back only when needed.
                if (BlockCache.Enabled)
                {
                    var blocks = new ITraceBlock[bodies.Count];
                    for (var b = 0; b < blocks.Length; b++)
                    {
                        blocks[b] = new LazyTraceBlock(schema, ctx.FileSystem, path, bodies[b].Index, bodies[b].Body);
                    }

                    if (trace.Rebind(run, blocks) is { } rebound)
                    {
                        RunPins.Add(rebound, ctx.FileSystem, [path]);
                    }
                }
            }
        });

        if (RunCheckpoint.Verify && !await codec.MatchesAsync(chain, trace, cancellationToken).ConfigureAwait(false))
        {
            throw new InvalidOperationException($"blocked run chain for '{ctx.Prefix}{fileName}' does not sum to the trace");
        }
    }

    public static async ValueTask LoadAsync(
        ILayout codec, Core.Circuit.ISnapshotReader reader, string fileName, BlockedIndexedTrace trace,
        CancellationToken cancellationToken)
    {
        if (await RunCheckpoint.FindChainAsync(reader, fileName, cancellationToken).ConfigureAwait(false) is not { } chain)
        {
            await codec.LoadWholeAsync(reader, fileName, trace, cancellationToken).ConfigureAwait(false);
            return;
        }

        var schema = codec.Blocks ?? throw new InvalidOperationException("a blocked trace's schema has no block encoding");
        foreach (var path in chain.Paths)
        {
            await using var run = await RunReader.OpenDetachedAsync(chain.Store, path, RowHash, cancellationToken: cancellationToken)
                .ConfigureAwait(false);
            var blocks = new ITraceBlock[run.Blocks.Count];
            for (var b = 0; b < blocks.Length; b++)
            {
                blocks[b] = new LazyTraceBlock(schema, chain.Store, path, run.Blocks[b]);
            }

            trace.AdoptRun(blocks, run.Entries, (chain.Store, path));

            // Unread blocks read the file: keep the sweep off it while the run is alive.
            if (trace.Runs.Count > 0)
            {
                RunPins.Add(trace.Runs[0], chain.Store, [path]);
            }
        }
    }

    /// <summary>
    /// Blocks in no hash order (a window partition's chunks, docs/design-bounded-memory.md §3.7) as
    /// one backing file, raw. The file's index lists each block under its ordinal in place of a hash
    /// range: nothing finds these blocks by hash. On commit each becomes a file-backed block that
    /// keeps its body, pinned so the sweep keeps the file while the block lives.
    /// </summary>
    public static async ValueTask SaveBackingBlocksAsync(
        ILayout codec, Core.Circuit.ISnapshotWriter writer, string fileName, IReadOnlyList<ITraceBlock> blocks,
        Action<ITraceBlock[]> onCommit, CancellationToken cancellationToken)
    {
        if (writer is not TableFileSystemSnapshotContext { Session: { } session } ctx || blocks.Count == 0)
        {
            return;
        }

        var schema = codec.Blocks!;
        var path = "runs/" + ctx.Prefix + fileName;
        var bodies = new List<(RunBlock Index, TraceBlock Body)>(blocks.Count);
        await using (var file = await RunStreamWriter.CreateAsync(
            ctx.FileSystem, path, codec.RunSchema, codec.Kind, RowHash(RunFile.FingerprintRow), cancellationToken).ConfigureAwait(false))
        {
            for (var b = 0; b < blocks.Count; b++)
            {
                var body = Materialized(blocks[b]);
                await file.AppendRawAsync(body.Message, [(uint)b], body.Count, 1, cancellationToken).ConfigureAwait(false);
                bodies.Add((file.Index[^1], body));
            }

            await file.FinishAsync(cancellationToken).ConfigureAwait(false);
        }

        session.OnCommit(() =>
        {
            var backed = new ITraceBlock[bodies.Count];
            for (var b = 0; b < backed.Length; b++)
            {
                var lazy = new LazyTraceBlock(schema, ctx.FileSystem, path, bodies[b].Index, bodies[b].Body);
                RunPins.Add(lazy, ctx.FileSystem, [path]);
                backed[b] = lazy;
            }

            onCommit(backed);
        });
    }

    /// <summary>A block set's manifest: magic, version.</summary>
    private const uint BlockSetMagic = 0x53424244; // "DBBS"
    private const int BlockSetVersion = 1;

    /// <summary>A block set naming more files than this is written whole (raw) to one new file:
    /// a file stays alive while any block in it does, so without this a set would pin every file
    /// a checkpoint ever wrote.</summary>
    internal const int MaxBlockSetFiles = 16;

    // Each owner's last committed block set: a checkpoint that would write the same manifest to the
    // same store names that chain again and writes nothing.
    private static readonly System.Runtime.CompilerServices.ConditionalWeakTable<object, Tuple<ITableFileSystem, RunChain, byte[]>> LastBlockSet = new();

    /// <summary>
    /// A set of blocks in no hash order, with the caller's metadata, as checkpoint state
    /// (docs/design-bounded-memory.md §3g): a window store's large-partition chunks. Blocks a
    /// checkpoint already wrote (durable file blocks in this store) are named again; the rest go raw
    /// to one new run file. The manifest (the caller's metadata, then each block's file and index
    /// entry) is a file under runs/ too, and the chain recorded as <paramref name="fileName"/> names
    /// it first, then the block files: nothing is written in the operator's directory, and a set
    /// whose manifest is unchanged since <paramref name="owner"/>'s last commit writes nothing at all.
    /// On commit, <paramref name="onCommit"/> receives each block's durable twin, in order (the same
    /// block when it was already durable).
    /// </summary>
    public static async ValueTask SaveBlockSetAsync(
        ILayout codec, Core.Circuit.ISnapshotWriter writer, string fileName, object owner, IReadOnlyList<ITraceBlock> blocks,
        ReadOnlyMemory<byte> meta, Action<ITraceBlock[]> onCommit, CancellationToken cancellationToken)
    {
        if (writer is not TableFileSystemSnapshotContext { Session: { } session } ctx)
        {
            throw new InvalidOperationException("a block set is written only inside a runs-layout checkpoint");
        }

        var schema = codec.Blocks!;
        var fs = ctx.FileSystem;
        static bool Durable(ITraceBlock b, ITableFileSystem fs) =>
            b is LazyTraceBlock { Durable: true } l && ReferenceEquals(l.Location.Store, fs);

        var paths = new List<string>();
        var pathIndex = new Dictionary<string, int>(StringComparer.Ordinal);
        foreach (var b in blocks)
        {
            if (Durable(b, fs) && pathIndex.TryAdd(((LazyTraceBlock)b).Location.Path, paths.Count))
            {
                paths.Add(((LazyTraceBlock)b).Location.Path);
            }
        }

        var rewriteAll = paths.Count > MaxBlockSetFiles;
        if (rewriteAll)
        {
            paths.Clear();
            pathIndex.Clear();
        }

        var refs = new (int File, RunBlock Index)[blocks.Count];
        var fresh = new List<(int At, TraceBlock Body)>();
        for (var i = 0; i < blocks.Count; i++)
        {
            if (!rewriteAll && Durable(blocks[i], fs))
            {
                var (_, path, index) = ((LazyTraceBlock)blocks[i]).Location;
                refs[i] = (pathIndex[path], index);
            }
            else
            {
                fresh.Add((i, Materialized(blocks[i])));
            }
        }

        var dataPath = "runs/" + ctx.Prefix + fileName;
        if (fresh.Count > 0)
        {
            var file = paths.Count;
            paths.Add(dataPath);
            await using var run = await RunStreamWriter.CreateAsync(
                fs, dataPath, codec.RunSchema, codec.Kind, RowHash(RunFile.FingerprintRow), cancellationToken).ConfigureAwait(false);
            for (var k = 0; k < fresh.Count; k++)
            {
                var body = fresh[k].Body;
                await run.AppendRawAsync(body.Message, [(uint)k], body.Count, 1, cancellationToken).ConfigureAwait(false);
                refs[fresh[k].At] = (file, run.Index[^1]);
            }

            await run.FinishAsync(cancellationToken).ConfigureAwait(false);
        }

        // The manifest: magic, version, metadata, block files, then each block's (file, index entry).
        var buffer = new System.IO.MemoryStream();
        using (var w = new System.IO.BinaryWriter(buffer, System.Text.Encoding.UTF8, leaveOpen: true))
        {
            w.Write(BlockSetMagic);
            w.Write(BlockSetVersion);
            w.Write(meta.Length);
            w.Write(meta.Span);
            w.Write(paths.Count);
            foreach (var p in paths)
            {
                w.Write(p);
            }

            w.Write(refs.Length);
            foreach (var (f, ix) in refs)
            {
                w.Write(f);
                w.Write(ix.FirstHash);
                w.Write(ix.LastHash);
                w.Write(ix.Offset);
                w.Write(ix.Length);
                w.Write(ix.Entries);
                w.Write(ix.Groups);
            }
        }

        var manifest = buffer.ToArray();
        RunChain chain;
        if (fresh.Count == 0 && LastBlockSet.TryGetValue(owner, out var last)
            && ReferenceEquals(last.Item1, fs) && manifest.AsSpan().SequenceEqual(last.Item3))
        {
            chain = last.Item2; // unchanged: name it again
        }
        else
        {
            var manifestPath = dataPath + ".manifest";
            await fs.WriteAllBytesAsync(manifestPath, manifest, cancellationToken).ConfigureAwait(false);
            long entries = 0;
            foreach (var (_, ix) in refs)
            {
                entries += ix.Entries;
            }

            chain = new RunChain(fs, [manifestPath, .. paths], entries, 0);
        }

        session.Record(ctx.Prefix + fileName, chain);
        session.OnCommit(() =>
        {
            LastBlockSet.AddOrUpdate(owner, Tuple.Create(fs, chain, manifest));
            var twins = new ITraceBlock[blocks.Count];
            for (var i = 0; i < twins.Length; i++)
            {
                twins[i] = blocks[i];
            }

            foreach (var (at, body) in fresh)
            {
                var (f, index) = refs[at];
                var lazy = new LazyTraceBlock(schema, fs, paths[f], index, body) { Durable = true };
                RunPins.Add(lazy, fs, [paths[f]]);
                twins[at] = lazy;
            }

            onCommit(twins);
        });
    }

    /// <summary>A block set written by <see cref="SaveBlockSetAsync"/>: its metadata and its
    /// blocks, adopted lazily (each read on first use); null when the snapshot has none. The set
    /// becomes <paramref name="owner"/>'s last, so its next checkpoint, if nothing changed, names
    /// this chain again.</summary>
    public static async ValueTask<(byte[] Meta, ITraceBlock[] Blocks)?> LoadBlockSetAsync(
        ILayout codec, Core.Circuit.ISnapshotReader reader, string fileName, object owner, CancellationToken cancellationToken)
    {
        if (await RunCheckpoint.FindChainAsync(reader, fileName, cancellationToken).ConfigureAwait(false) is not { Paths.Count: > 0 } chain)
        {
            return null;
        }

        var fs = chain.Store;
        var bytes = await fs.ReadAllBytesAsync(chain.Paths[0], cancellationToken).ConfigureAwait(false);
        using var r = new System.IO.BinaryReader(new System.IO.MemoryStream(bytes), System.Text.Encoding.UTF8);
        if (r.ReadUInt32() != BlockSetMagic || r.ReadInt32() is var version && version != BlockSetVersion)
        {
            throw new InvalidDataException($"'{chain.Paths[0]}' is not a block set this version reads");
        }

        var meta = r.ReadBytes(r.ReadInt32());
        var paths = new string[r.ReadInt32()];
        for (var p = 0; p < paths.Length; p++)
        {
            paths[p] = r.ReadString();
        }

        var schema = codec.Blocks ?? throw new InvalidOperationException("a block set's schema has no block encoding");
        var blocks = new ITraceBlock[r.ReadInt32()];
        for (var i = 0; i < blocks.Length; i++)
        {
            var f = r.ReadInt32();
            var index = new RunBlock(r.ReadUInt32(), r.ReadUInt32(), r.ReadInt64(), r.ReadInt32(), r.ReadInt32(), r.ReadInt32());
            var lazy = new LazyTraceBlock(schema, fs, paths[f], index) { Durable = true };
            RunPins.Add(lazy, fs, [paths[f]]);
            blocks[i] = lazy;
        }

        LastBlockSet.AddOrUpdate(owner, Tuple.Create(fs, chain, bytes));
        return (meta, blocks);
    }

    /// <summary>Write <paramref name="run"/> to <paramref name="path"/> outside any checkpoint and
    /// rebind it there at once (a <see cref="BlockSpill"/>: the file is a cache, not state).</summary>
    internal static BlockedIndexedTrace.Run? SpillRun(ITableFileSystem fs, string path, ILayout codec, BlockedIndexedTrace trace, BlockedIndexedTrace.Run run)
    {
        var bodies = WriteRunAsync(fs, path, codec, run, CancellationToken.None).AsTask().GetAwaiter().GetResult();
        var schema = codec.Blocks!;
        var blocks = new ITraceBlock[bodies.Count];
        for (var b = 0; b < blocks.Length; b++)
        {
            blocks[b] = new LazyTraceBlock(schema, fs, path, bodies[b].Index, bodies[b].Body);
        }

        run.Persisted = (fs, path);
        return trace.Rebind(run, blocks);
    }

    // One run file from a run's blocks, raw. A hash spanning blocks in memory becomes one block.
    // Returns each file block's index and, where it is byte for byte one in-memory block, that block.
    private static async ValueTask<List<(RunBlock Index, TraceBlock? Body)>> WriteRunAsync(
        ITableFileSystem fs, string path, ILayout codec, BlockedIndexedTrace.Run run, CancellationToken cancellationToken)
    {
        var schema = codec.Blocks!;
        var bodies = new List<(RunBlock Index, TraceBlock? Body)>();
        await using var writer = await RunStreamWriter.CreateAsync(
            fs, path, codec.RunSchema, codec.Kind, RowHash(RunFile.FingerprintRow), cancellationToken).ConfigureAwait(false);
        var blocks = run.Blocks;
        for (var b = 0; b < blocks.Length;)
        {
            var end = b + 1;
            while (end < blocks.Length && blocks[end].FirstHash == blocks[end - 1].LastHash)
            {
                end++;
            }

            var tj = Core.Operators.Stateful.Blocks.BlockTraceProfile.Now();
            // Without eviction a span is written whole (neighbours and all): nothing faults it in for
            // a neighbour, and slicing would cost the checkpoint (§3.6). A giant's parts are streamed
            // to the file, never concatenated in memory (§3d).
            var pieces = end == b + 1 ? [new Piece(Materialized(blocks[b]), null)]
                : BlockCache.Enabled ? Pieces(schema, blocks, b, end)
                : [new Piece(null, [.. blocks[b..end].Select(Materialized)])];
            if (end > b + 1 && Core.Operators.Stateful.Blocks.BlockTraceProfile.Enabled)
            {
                Core.Operators.Stateful.Blocks.BlockTraceProfile.Section(6, tj, pieces.Sum(p => p.Count));
            }

            foreach (var piece in Capped(pieces))
            {
                if (piece.Parts is { } parts)
                {
                    var hashes = Distinct(parts);
                    await writer.AppendConcatAsync(schema, parts, hashes, piece.Count, hashes.Count, cancellationToken).ConfigureAwait(false);
                    bodies.Add((writer.Index[^1], null));
                }
                else
                {
                    var block = piece.Block!;
                    var hashes = Distinct(block.Hashes);
                    await writer.AppendRawAsync(block.Message, hashes, block.Count, hashes.Count, cancellationToken).ConfigureAwait(false);
                    bodies.Add((writer.Index[^1], block));
                }
            }

            b = end;
        }

        await writer.FinishAsync(cancellationToken).ConfigureAwait(false);
        return bodies;
    }

    private static TraceBlock Materialized(ITraceBlock block) => block switch
    {
        TraceBlock t => t,
        LazyTraceBlock l => l.Loaded,
        _ => throw new InvalidOperationException($"unexpected block type {block.GetType().Name}"),
    };

    // Blocks [from, to) chained by hashes that span them, as file blocks: each spanning hash alone in
    // one block (its parts concatenated byte-wise, not decoded), and the entries around it in blocks
    // of their own (docs/design-bounded-memory.md §3.5) — so a probe for a neighbouring key reads a
    // small block, not the giant group. A spanning hash comes back as its parts, to be streamed.
    private static List<Piece> Pieces(BlockSchema schema, ITraceBlock[] blocks, int from, int to)
    {
        var pieces = new List<Piece>();
        var pending = new List<TraceBlock>();
        uint pendingHash = 0;
        for (var k = from; k < to; k++)
        {
            var block = Materialized(blocks[k]);
            var hashes = block.Hashes;
            var lo = 0;
            if (pending.Count > 0)
            {
                while (lo < hashes.Length && hashes[lo] == pendingHash)
                {
                    lo++;
                }

                if (lo == hashes.Length)
                {
                    pending.Add(block); // wholly the spanning hash: it continues
                    continue;
                }

                pending.Add(Slice(schema, block, 0, lo));
                pieces.Add(new Piece(null, [.. pending]));
                pending.Clear();
            }

            // The tail that the next block continues.
            var hi = k + 1 < to ? block.LowerBound(block.LastHash) : hashes.Length;
            if (hi > lo)
            {
                pieces.Add(new Piece(lo == 0 && hi == hashes.Length ? block : Slice(schema, block, lo, hi), null));
            }

            if (hi < hashes.Length)
            {
                pending.Add(Slice(schema, block, hi, hashes.Length));
                pendingHash = block.LastHash;
            }
        }

        if (pending.Count > 0)
        {
            pieces.Add(new Piece(null, [.. pending]));
        }

        return pieces;
    }

    // Entries [from, to) of a block, re-encoded (or the block itself when that is all of it).
    private static TraceBlock Slice(BlockSchema schema, TraceBlock block, int from, int to)
    {
        if (from == 0 && to == block.Count)
        {
            return block;
        }

        var n = to - from;
        var keys = new StructuralRow[n];
        var values = new StructuralRow[n];
        var weights = new long[n];
        var hashes = new int[n];
        var source = block.Hashes;
        for (var i = 0; i < n; i++)
        {
            keys[i] = block.Key(from + i);
            values[i] = block.Value(from + i);
            weights[i] = block.Weight(from + i);
            hashes[i] = (int)source[from + i];
        }

        return schema.Encode(keys, values, weights, hashes, n);
    }

    // A concatenated piece past RunFile.MaxSpanBlockBytes split at part boundaries into consecutive
    // pieces (docs/design-bounded-memory.md §5.18): a file block is one message read into one array, so
    // a giant group over ~2 GiB cannot be one block; its hash continues from block to block instead,
    // as it does in memory.
    private static IEnumerable<Piece> Capped(List<Piece> pieces)
    {
        foreach (var piece in pieces)
        {
            if (piece.Parts is not { } parts || parts.Sum(p => (long)p.Message.Length) <= RunFile.MaxSpanBlockBytes)
            {
                yield return piece;
                continue;
            }

            var chunk = new List<TraceBlock>();
            long bytes = 0;
            foreach (var part in parts)
            {
                if (chunk.Count > 0 && bytes + part.Message.Length > RunFile.MaxSpanBlockBytes)
                {
                    yield return new Piece(null, [.. chunk]);
                    chunk.Clear();
                    bytes = 0;
                }

                chunk.Add(part);
                bytes += part.Message.Length;
            }

            if (chunk.Count > 0)
            {
                yield return new Piece(null, [.. chunk]);
            }
        }
    }

    // One file block: a block written as it is (and kept as the file block's body), or a giant
    // group's parts, streamed as one block and not kept.
    private sealed record Piece(TraceBlock? Block, IReadOnlyList<TraceBlock>? Parts)
    {
        public int Count => Block?.Count ?? Parts!.Sum(p => p.Count);
    }

    private static List<uint> Distinct(IReadOnlyList<TraceBlock> parts)
    {
        var distinct = new List<uint>();
        foreach (var part in parts)
        {
            foreach (var h in part.Hashes)
            {
                if (distinct.Count == 0 || distinct[^1] != h)
                {
                    distinct.Add(h);
                }
            }
        }

        return distinct;
    }

    private static List<uint> Distinct(ReadOnlySpan<uint> hashes)
    {
        var distinct = new List<uint>();
        foreach (var h in hashes)
        {
            if (distinct.Count == 0 || distinct[^1] != h)
            {
                distinct.Add(h);
            }
        }

        return distinct;
    }

    /// <summary>The chain read whole as an indexed Z-set (verify mode).</summary>
    internal static async ValueTask<IndexedZSet<StructuralRow, StructuralRow, Core.Algebra.Z64>> ReadIndexedAsync(
        RunChain chain, IIndexedRunCodec codec, CancellationToken cancellationToken)
    {
        var into = new IndexedZSetBuilder<StructuralRow, StructuralRow, Core.Algebra.Z64>();
        foreach (var path in chain.Paths)
        {
            await using var run = await RunReader.OpenAsync(chain.Store, path, RowHash, cancellationToken).ConfigureAwait(false);
            await run.ReadAllIntoAsync(codec, into, cancellationToken).ConfigureAwait(false);
        }

        return into.Build();
    }

    /// <summary>The chain read whole as a Z-set (verify mode).</summary>
    internal static async ValueTask<ZSet<StructuralRow, Core.Algebra.Z64>> ReadZSetAsync(
        RunChain chain, IZSetRunCodec codec, CancellationToken cancellationToken)
    {
        var into = new ZSetBuilder<StructuralRow, Core.Algebra.Z64>();
        foreach (var path in chain.Paths)
        {
            await using var run = await RunReader.OpenAsync(chain.Store, path, RowHash, cancellationToken).ConfigureAwait(false);
            await run.ReadAllIntoAsync(codec, into, cancellationToken).ConfigureAwait(false);
        }

        return into.Build();
    }
}

/// <summary>A block of a restored run file, read (not decoded) on first use; its hash range and
/// size come from the run's index, so finding the block for a probe reads nothing.</summary>
internal sealed class LazyTraceBlock : ITraceBlock
{
    private readonly BlockSchema _schema;
    private readonly ITableFileSystem _store;
    private readonly string _path;
    private readonly RunBlock _index;
    private TraceBlock? _loaded;
    private string? _fullPath;

    /// <param name="loaded">The body already in memory (a block a checkpoint just wrote, byte for
    /// byte the file's); admitted to the <see cref="BlockCache"/>.</param>
    public LazyTraceBlock(BlockSchema schema, ITableFileSystem store, string path, RunBlock index, TraceBlock? loaded = null)
    {
        _schema = schema;
        _store = store;
        _path = path;
        _index = index;
        if (loaded is not null)
        {
            _loaded = loaded;
            if (BlockCache.Enabled)
            {
                BlockCache.Admit(this, loaded.ResidentBytes, referenced: false);
            }
        }
    }

    // The block cache's bookkeeping (guarded by its lock, but for the reference bit).
    internal long CacheSize { get; set; }

    internal volatile bool Referenced;

    /// <summary>Forget the body (the cache evicted it); the next read loads it again.</summary>
    internal void DropBody()
    {
        if (Core.Operators.Stateful.Blocks.BlockAccess.Enabled && _loaded is { } body)
        {
            BlockCache.TrackDropped(body);
        }

        Volatile.Write(ref _loaded, null);
    }

    public int Count => _index.Entries;

    public ITraceBlock Body => Loaded;

    bool ITraceBlock.Resident => Volatile.Read(ref _loaded) is not null;

    public uint FirstHash => _index.FirstHash;

    public uint LastHash => _index.LastHash;

    /// <summary>The block's bytes if read (the residency census).</summary>
    internal TraceBlock? Resident => _loaded;

    /// <summary>Distinct key hashes in the block, per the run file's index.</summary>
    internal int Groups => _index.Groups;

    /// <summary>The block's encoded length in the run file.</summary>
    internal int FileLength => _index.Length;

    /// <summary>Where the block lives: its store, run file and index entry.</summary>
    internal (ITableFileSystem Store, string Path, RunBlock Index) Location => (_store, _path, _index);

    /// <summary>The file is checkpoint state a snapshot's run index names (a block set's, §3g), not
    /// a cache's spill or backing file: a later checkpoint may name it again instead of copying it.</summary>
    internal bool Durable { get; init; }

    /// <summary>The block's bytes, read on first use.</summary>
    internal TraceBlock Loaded
    {
        get
        {
            if (Volatile.Read(ref _loaded) is { } body)
            {
                if (BlockCache.Enabled && !Referenced)
                {
                    Referenced = true;
                }

                return body;
            }

            return Load();
        }
    }

    public ReadOnlySpan<uint> Hashes => Loaded.Hashes;

    public long Weight(int i) => Loaded.Weight(i);

    public int LowerBound(uint hash) => Loaded.LowerBound(hash);

    public bool KeyEquals(int i, StructuralRow key) => Loaded.KeyEquals(i, key);

    public StructuralRow Key(int i) => Loaded.Key(i);

    public StructuralRow KeyAliased(int i) => Loaded.Key(i, aliasStrings: true);

    public StructuralRow[] KeysAliased() => Loaded.KeysAliased();

    public StructuralRow Value(int i) => Loaded.Value(i);

    public void ValueCellsInto(int i, object?[] destination, int at) => Loaded.ValueCellsInto(i, destination, at);

    public object? ValueCell(int i, int column) => Loaded.ValueCell(i, column);

    public int ValueHash(int i) => Loaded.ValueHash(i);

    public bool ValueEquals(int i, StructuralRow value) => Loaded.ValueEquals(i, value);

    // A probe on the step thread: synchronous by necessity. Two threads may both read it; the
    // blocks they build are identical, and either may win.
    private TraceBlock Load()
    {
        var prof = BlockCache.Profile;
        var t0 = prof ? System.Diagnostics.Stopwatch.GetTimestamp() : 0;
        if (_store is IO.Local.LocalTableFileSystem localFs)
        {
            // A local run file: a kept-open handle (§3.6), the block read straight into place.
            using var lease = IO.Local.LocalReadHandles.Rent(_fullPath ??= localFs.FullPath(_path));
            var o1 = prof ? System.Diagnostics.Stopwatch.GetTimestamp() : 0;
            long r = 0;
            var local = lease.File;
            var adopted = _schema.Adopt(_index.Length, m =>
            {
                var r0 = prof ? System.Diagnostics.Stopwatch.GetTimestamp() : 0;
                local.ReadExactly(_index.Offset, m.Span);
                if (prof)
                {
                    r = System.Diagnostics.Stopwatch.GetTimestamp() - r0;
                }
            });
            if (prof)
            {
                BlockCache.RecordFault(_index.Length, o1 - t0, r, System.Diagnostics.Stopwatch.GetTimestamp() - o1 - r);
                if (Core.Operators.Stateful.Blocks.BlockAccess.Enabled && Core.Operators.Stateful.Blocks.BlockAccess.StackPhases < 0)
                {
                    Core.Operators.Stateful.Blocks.BlockAccess.RecordStack(_index.Length);
                }
            }

            return Install(adopted);
        }

        var file = _store.OpenReadAsync(_path).AsTask().GetAwaiter().GetResult();
        var t1 = prof ? System.Diagnostics.Stopwatch.GetTimestamp() : 0;
        long readTicks = 0;
        try
        {
            TraceBlock block;
            if (file is IO.Local.LocalRandomAccessFile local)
            {
                block = _schema.Adopt(_index.Length, m =>
                {
                    var r0 = prof ? System.Diagnostics.Stopwatch.GetTimestamp() : 0;
                    local.ReadExactly(_index.Offset, m.Span);
                    if (prof)
                    {
                        readTicks = System.Diagnostics.Stopwatch.GetTimestamp() - r0;
                    }
                });
            }
            else
            {
                using var bytes = file.ReadAsync(new FileRange(_index.Offset, _index.Length)).AsTask().GetAwaiter().GetResult();
                block = _schema.Adopt(bytes.Memory.Span[.._index.Length]);
            }

            if (prof)
            {
                var t2 = System.Diagnostics.Stopwatch.GetTimestamp();
                BlockCache.RecordFault(_index.Length, t1 - t0, readTicks, t2 - t1 - readTicks);
                if (Core.Operators.Stateful.Blocks.BlockAccess.Enabled && Core.Operators.Stateful.Blocks.BlockAccess.StackPhases < 0)
                {
                    // DBSPNET_BLOCK_ACCESS_STACKS=-1: who faults blocks in (not who reads them).
                    Core.Operators.Stateful.Blocks.BlockAccess.RecordStack(_index.Length);
                }
            }

            return Install(block);
        }
        finally
        {
            file.DisposeAsync().AsTask().GetAwaiter().GetResult();
        }
    }

    // Publish a loaded body (another thread may have won) and admit it to the cache.
    private TraceBlock Install(TraceBlock block)
    {
        if (Interlocked.CompareExchange(ref _loaded, block, null) is { } other)
        {
            return other;
        }

        if (BlockCache.Enabled)
        {
            // A merge's read is a scan: admitted cold, dropped first.
            BlockCache.Admit(this, block.ResidentBytes, referenced: !Core.Operators.Stateful.Blocks.BlockAccess.InMaintenance);
        }

        return block;
    }
}
