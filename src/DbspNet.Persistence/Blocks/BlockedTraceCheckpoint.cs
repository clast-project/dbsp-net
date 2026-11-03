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
            var pieces = end == b + 1 ? [(Materialized(blocks[b]), false)] : Pieces(schema, blocks, b, end);
            if (end > b + 1 && Core.Operators.Stateful.Blocks.BlockTraceProfile.Enabled)
            {
                Core.Operators.Stateful.Blocks.BlockTraceProfile.Section(6, tj, pieces.Sum(p => p.Block.Count));
            }

            foreach (var (block, giant) in pieces)
            {
                var hashes = Distinct(block.Hashes);
                await writer.AppendRawAsync(block.Message.ToArray(), hashes, block.Count, hashes.Count, cancellationToken).ConfigureAwait(false);
                bodies.Add((writer.Index[^1], giant ? null : block));
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
    // small block, not the giant group. Giant: a spanning hash's block.
    private static List<(TraceBlock Block, bool Giant)> Pieces(BlockSchema schema, ITraceBlock[] blocks, int from, int to)
    {
        var pieces = new List<(TraceBlock, bool)>();
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
                pieces.Add((pending.Count == 1 ? pending[0] : BlockWriter.Concat(schema, pending), true));
                pending.Clear();
            }

            // The tail that the next block continues.
            var hi = k + 1 < to ? block.LowerBound(block.LastHash) : hashes.Length;
            if (hi > lo)
            {
                pieces.Add((lo == 0 && hi == hashes.Length ? block : Slice(schema, block, lo, hi), false));
            }

            if (hi < hashes.Length)
            {
                pending.Add(Slice(schema, block, hi, hashes.Length));
                pendingHash = block.LastHash;
            }
        }

        if (pending.Count > 0)
        {
            pieces.Add((pending.Count == 1 ? pending[0] : BlockWriter.Concat(schema, pending), true));
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

    public uint FirstHash => _index.FirstHash;

    public uint LastHash => _index.LastHash;

    /// <summary>The block's bytes if read (the residency census).</summary>
    internal TraceBlock? Resident => _loaded;

    /// <summary>Distinct key hashes in the block, per the run file's index.</summary>
    internal int Groups => _index.Groups;

    /// <summary>The block's encoded length in the run file.</summary>
    internal int FileLength => _index.Length;

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
        var file = _store.OpenReadAsync(_path).AsTask().GetAwaiter().GetResult();
        try
        {
            TraceBlock block;
            if (file is IO.Local.LocalRandomAccessFile local)
            {
                block = _schema.Adopt(_index.Length, m => local.ReadExactly(_index.Offset, m.Span));
            }
            else
            {
                using var bytes = file.ReadAsync(new FileRange(_index.Offset, _index.Length)).AsTask().GetAwaiter().GetResult();
                block = _schema.Adopt(bytes.Memory.Span[.._index.Length]);
            }

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
        finally
        {
            file.DisposeAsync().AsTask().GetAwaiter().GetResult();
        }
    }
}
