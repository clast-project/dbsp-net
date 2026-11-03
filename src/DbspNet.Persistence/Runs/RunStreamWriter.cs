// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using DbspNet.Core.IO;

namespace DbspNet.Persistence.Runs;

/// <summary>
/// Writes a run one block at a time, for a producer that cannot hold every entry at once (compaction,
/// docs/design-durable-state.md §1f, increment 7). Blocks must arrive in ascending, non-overlapping
/// hash order; the layout is exactly <see cref="RunFile"/>'s.
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
    private long _schemaLength;
    private long _entries;
    private long _groups;
    private uint? _lastHash;

    private RunStreamWriter(ISequentialFile file, Schema schema, RunFile.Kind kind, int fingerprint)
    {
        _file = file;
        _stream = file.AsStream(); // not disposed: disposing the adapter disposes the file
        _ipc = new ArrowStreamWriter(_stream, schema, leaveOpen: true);
        _kind = kind;
        _fingerprint = fingerprint;
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

    /// <summary>Append one block. <paramref name="keyHashes"/> are its distinct keys' hashes (for
    /// the Bloom filter); the block's hash range is their minimum and maximum.</summary>
    internal async ValueTask AppendAsync(
        RecordBatch batch, IReadOnlyList<uint> keyHashes, int entries, int groups, CancellationToken cancellationToken)
    {
        var first = keyHashes[0];
        var last = keyHashes[^1];
        if (_lastHash is { } previous && first <= previous)
        {
            throw new InvalidOperationException("run blocks must arrive in ascending, non-overlapping hash order");
        }

        var offset = _file.Position;
        await _ipc.WriteRecordBatchAsync(batch, cancellationToken).ConfigureAwait(false);
        _index.Add(new RunBlock(first, last, offset, checked((int)(_file.Position - offset)), entries, groups));
        _keyHashes.AddRange(keyHashes);
        _entries += entries;
        _groups += groups;
        _lastHash = last;
    }

    /// <summary>Append a block copied unchanged from another run of the same schema (its stored
    /// record-batch message), with its distinct order hashes and counts.</summary>
    internal async ValueTask AppendRawAsync(
        byte[] message, IReadOnlyList<uint> keyHashes, int entries, int groups, CancellationToken cancellationToken)
    {
        var first = keyHashes[0];
        var last = keyHashes[^1];
        if (_lastHash is { } previous && first <= previous)
        {
            throw new InvalidOperationException("run blocks must arrive in ascending, non-overlapping hash order");
        }

        // A stored block is a complete, aligned IPC record-batch message: written between the
        // writer's own batches, the stream stays well formed.
        var offset = _file.Position;
        await _file.WriteAsync(message, cancellationToken).ConfigureAwait(false);
        _index.Add(new RunBlock(first, last, offset, checked((int)(_file.Position - offset)), entries, groups));
        _keyHashes.AddRange(keyHashes);
        _entries += entries;
        _groups += groups;
        _lastHash = last;
    }

    /// <summary>The blocks written so far: offsets, lengths and hash ranges.</summary>
    internal IReadOnlyList<RunBlock> Index => _index;

    internal async ValueTask<RunInfo> FinishAsync(CancellationToken cancellationToken)
    {
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

    public ValueTask DisposeAsync() => _file.DisposeAsync();
}
