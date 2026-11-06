// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using DbspNet.Core.IO;

namespace DbspNet.Persistence.Runs;

/// <summary>
/// Writes a run one block at a time, for a producer that cannot hold every entry at once (compaction,
/// docs/design-durable-state.md §1f, increment 7). Blocks must arrive in ascending hash order; a block
/// may begin with the hash the previous one ended with — a giant group continuing — and readers then
/// read that hash across both (docs/design-bounded-memory.md §5.18). The layout is <see cref="RunFile"/>'s.
/// Blocks given as messages are compressed (<see cref="BlockCompression"/>, §5.26) in parallel and
/// written in order: the appending thread pays for the write, not the compression.
/// </summary>
internal sealed class RunStreamWriter : IAsyncDisposable
{
    private readonly ISequentialFile _file;
    private readonly Stream _stream;
    private readonly ArrowStreamWriter _ipc;
    private readonly RunFile.Kind _kind;
    private readonly int _fingerprint;
    private readonly List<RunBlock> _index = new();
    private readonly List<uint> _keyHashes = new();
    private readonly Queue<Pending> _pending = new();
    private readonly BlockCodec _codec;
    private long _pendingBytes;
    private long _schemaLength;
    private long _entries;
    private long _groups;
    private uint? _lastHash;

    /// <summary>The most blocks compressing at once (on the thread pool) before an append waits for the
    /// oldest to be written; 0 compresses on the appending thread. A test seam.</summary>
    internal static int MaxInFlight { get; set; } = Math.Max(1, Environment.ProcessorCount - 2);

    /// <summary>The most message bytes compressing at once.</summary>
    internal static long MaxInFlightBytes { get; set; } = 256L << 20;

    private RunStreamWriter(ISequentialFile file, Schema schema, RunFile.Kind kind, int fingerprint)
    {
        _file = file;
        _stream = file.AsStream(); // not disposed: disposing the adapter disposes the file
        _ipc = new ArrowStreamWriter(_stream, schema, leaveOpen: true);
        _kind = kind;
        _fingerprint = fingerprint;
        _codec = BlockCompression.Codec;
    }

    internal static async ValueTask<RunStreamWriter> CreateAsync(
        ITableFileSystem fs, string path, Schema schema, RunFile.Kind kind, int fingerprint, CancellationToken cancellationToken)
    {
        var file = await fs.CreateAsync(path, overwrite: true, cancellationToken).ConfigureAwait(false);
        var writer = new RunStreamWriter(file, schema, kind, fingerprint);
        await writer._ipc.WriteStartAsync(cancellationToken).ConfigureAwait(false);
        writer._schemaLength = file.Position;
        return writer;
    }

    /// <summary>Append one block, raw. <paramref name="keyHashes"/> are its distinct keys' hashes (for
    /// the Bloom filter); the block's hash range is their minimum and maximum.</summary>
    internal async ValueTask AppendAsync(
        RecordBatch batch, IReadOnlyList<uint> keyHashes, int entries, int groups, CancellationToken cancellationToken)
    {
        await DrainAsync(0, cancellationToken).ConfigureAwait(false);
        var first = keyHashes[0];
        var last = keyHashes[^1];
        var runGroups = Admit(first, groups);

        var offset = _file.Position;
        await _ipc.WriteRecordBatchAsync(batch, cancellationToken).ConfigureAwait(false);
        var length = checked((int)(_file.Position - offset));
        _index.Add(new RunBlock(first, last, offset, length, entries, groups, length));
        _keyHashes.AddRange(keyHashes);
        _entries += entries;
        _groups += runGroups;
        _lastHash = last;
    }

    /// <summary>Append a block given as its record-batch message, compressed with this writer's codec
    /// (on the thread pool, so a later append may overtake its compression but never its place in the
    /// file). <paramref name="written"/>, if given, receives its index entry once it is written: at a
    /// later append or at the finish, on the appending thread. <paramref name="message"/> must not change
    /// until then.</summary>
    internal ValueTask AppendRawAsync(
        ReadOnlyMemory<byte> message, IReadOnlyList<uint> keyHashes, int entries, int groups, CancellationToken cancellationToken,
        Action<RunBlock>? written = null)
    {
        var codec = _codec;
        Task<BlockCompression.Stored> work;
        if (codec == BlockCodec.None || message.Length < BlockCompression.MinBytes)
        {
            work = Task.FromResult(new BlockCompression.Stored(message, BlockCodec.None, null));
        }
        else if (MaxInFlight == 0)
        {
            work = Task.FromResult(BlockCompression.Compress(message, codec));
        }
        else
        {
            work = Task.Run(() => BlockCompression.Compress(message, codec), cancellationToken);
        }

        return EnqueueAsync(work, message.Length, keyHashes, entries, groups, written, cancellationToken);
    }

    /// <summary>Append a block copied as stored from another run of the same schema (no decode, no
    /// recompression), with its distinct order hashes and counts.</summary>
    internal ValueTask AppendStoredAsync(
        StoredBlock block, IReadOnlyList<uint> keyHashes, int entries, int groups, CancellationToken cancellationToken,
        Action<RunBlock>? written = null) =>
        EnqueueAsync(
            Task.FromResult(new BlockCompression.Stored(block.Bytes, block.Codec, null)), block.MessageLength,
            keyHashes, entries, groups, written, cancellationToken);

    private async ValueTask EnqueueAsync(
        Task<BlockCompression.Stored> work, int messageLength, IReadOnlyList<uint> keyHashes, int entries, int groups,
        Action<RunBlock>? written, CancellationToken cancellationToken)
    {
        var first = keyHashes[0];
        var last = keyHashes[^1];
        var runGroups = Admit(first, groups);
        _keyHashes.AddRange(keyHashes);
        _entries += entries;
        _groups += runGroups;
        _lastHash = last;

        _pending.Enqueue(new Pending(work, first, last, entries, groups, messageLength, written));
        _pendingBytes += messageLength;

        // Keep the window bounded, and write whatever is already done (in order).
        while (_pending.Count > 0
               && (_pending.Count > MaxInFlight || _pendingBytes > MaxInFlightBytes || _pending.Peek().Work.IsCompleted))
        {
            await WriteNextAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    // Write every pending block but the last `keep`.
    private async ValueTask DrainAsync(int keep, CancellationToken cancellationToken)
    {
        while (_pending.Count > keep)
        {
            await WriteNextAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private async ValueTask WriteNextAsync(CancellationToken cancellationToken)
    {
        var p = _pending.Dequeue();
        _pendingBytes -= p.MessageLength;
        // Waited for, never awaited: the rest of the write (and the caller's callbacks, which build
        // file blocks) stays on the appending thread. Continued on a pool thread, the write's state
        // machine stays reachable from that idle thread, pinning the blocks and so their files (§5.26).
        var stored = p.Work.GetAwaiter().GetResult();
        RunBlock index;
        try
        {
            var offset = _file.Position;
            await _file.WriteAsync(stored.Data, cancellationToken).ConfigureAwait(false);
            index = new RunBlock(
                p.First, p.Last, offset, checked((int)(_file.Position - offset)), p.Entries, p.Groups, p.MessageLength, stored.Codec);
        }
        finally
        {
            stored.Release();
        }

        _index.Add(index);
        p.Written?.Invoke(index);
    }

    /// <summary>Append <paramref name="parts"/> as one block, streamed from their bytes
    /// (<see cref="Blocks.BlockWriter.ConcatToAsync"/>) through the codec: a giant group is never built
    /// in memory, raw or compressed.</summary>
    internal async ValueTask AppendConcatAsync(
        Blocks.BlockSchema schema, IReadOnlyList<Blocks.TraceBlock> parts, IReadOnlyList<uint> keyHashes, int entries, int groups,
        CancellationToken cancellationToken, Action<RunBlock>? written = null)
    {
        await DrainAsync(0, cancellationToken).ConfigureAwait(false);
        var first = keyHashes[0];
        var last = keyHashes[^1];
        var runGroups = Admit(first, groups);

        var offset = _file.Position;
        int messageLength;
        if (_codec == BlockCodec.Zstd)
        {
            await using var z = BlockCompression.Compressing(_file);
            await Blocks.BlockWriter.ConcatToAsync(schema, parts, z, cancellationToken).ConfigureAwait(false);
            messageLength = checked((int)z.Position);
        }
        else
        {
            await Blocks.BlockWriter.ConcatToAsync(schema, parts, _file, cancellationToken).ConfigureAwait(false);
            messageLength = checked((int)(_file.Position - offset));
        }

        var index = new RunBlock(
            first, last, offset, checked((int)(_file.Position - offset)), entries, groups, messageLength, _codec);
        _index.Add(index);
        _keyHashes.AddRange(keyHashes);
        _entries += entries;
        _groups += runGroups;
        _lastHash = last;
        written?.Invoke(index);
    }

    /// <summary>The blocks written so far: offsets, lengths and hash ranges (complete after the finish;
    /// blocks still compressing are not yet listed).</summary>
    internal IReadOnlyList<RunBlock> Index => _index;

    // Blocks ascend; one that begins with the previous block's last hash continues that hash's group,
    // which the run's group count has already counted once.
    private int Admit(uint first, int groups)
    {
        if (_lastHash is not { } previous)
        {
            return groups;
        }

        if (first < previous)
        {
            throw new InvalidOperationException("run blocks must arrive in ascending hash order");
        }

        return first == previous ? Math.Max(0, groups - 1) : groups;
    }

    internal async ValueTask<RunInfo> FinishAsync(CancellationToken cancellationToken)
    {
        await DrainAsync(0, cancellationToken).ConfigureAwait(false);
        await _ipc.WriteEndAsync(cancellationToken).ConfigureAwait(false);
        _ipc.Dispose();
        var bloom = RunBloom.Build(_keyHashes);
        var footer = RunFile.EncodeFooter(_kind, _fingerprint, _entries, _groups, _schemaLength, _index, bloom);
        await _file.WriteAsync(footer, cancellationToken).ConfigureAwait(false);
        var trailer = new byte[RunFile.TrailerBytes];
        BinaryPrimitives.WriteInt64LittleEndian(trailer, footer.Length);
        RunFile.Magic.CopyTo(trailer.AsSpan(8));
        await _file.WriteAsync(trailer, cancellationToken).ConfigureAwait(false);
        await _file.FlushAsync(cancellationToken).ConfigureAwait(false);
        return new RunInfo(_kind, _entries, _groups, _index.Count, _file.Position);
    }

    public async ValueTask DisposeAsync()
    {
        // Abandoned mid-write (an exception): let compressions finish so their buffers return.
        while (_pending.TryDequeue(out var p))
        {
            try
            {
                (await p.Work.ConfigureAwait(false)).Release();
            }
            catch
            {
                // already failed the write
            }
        }

        await _file.DisposeAsync().ConfigureAwait(false);
    }

    private readonly record struct Pending(
        Task<BlockCompression.Stored> Work, uint First, uint Last, int Entries, int Groups, int MessageLength, Action<RunBlock>? Written);
}

/// <summary>A run block's bytes as stored, its codec, and its message length decoded.</summary>
internal readonly record struct StoredBlock(byte[] Bytes, BlockCodec Codec, int MessageLength);
