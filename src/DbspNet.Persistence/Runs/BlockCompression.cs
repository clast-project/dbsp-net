// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Buffers;
using DbspNet.Core.IO;

namespace DbspNet.Persistence.Runs;

/// <summary>How one run-file block is stored (the footer records it per block, format 5).</summary>
internal enum BlockCodec
{
    /// <summary>The record-batch message as it is.</summary>
    None = 0,

    /// <summary>The whole message as one zstd frame.</summary>
    Zstd = 1,
}

/// <summary>
/// Whole-block compression of run files (docs/design-bounded-memory.md §5.26): each block's record-batch
/// message is compressed as one unit, as Feldera compresses a data block, so a read decompresses one
/// block straight into the array it is parsed from. zstd level 1 measured best on real spill files
/// (4.97x against LZ4 3.25x and Snappy 3.01x, §5.26). Blocks a codec does not shrink are stored raw.
/// </summary>
/// <remarks>
/// <c>DBSPNET_RUN_COMPRESSION</c> = <c>zstd</c> (default) or <c>none</c>; <c>DBSPNET_RUN_ZSTD_LEVEL</c>
/// (default 1). The codec is chosen on the writing thread, so a test may override it for its own writes.
/// The API is span to span with reusable per-thread contexts, the shape of .NET 11's
/// <c>ZstandardEncoder</c>/<c>ZstandardDecoder</c>, which can replace ZstdSharp here unchanged on disk.
/// </remarks>
internal static class BlockCompression
{
    /// <summary>The process default, from the environment.</summary>
    internal static readonly BlockCodec DefaultCodec =
        Environment.GetEnvironmentVariable("DBSPNET_RUN_COMPRESSION")?.Trim().ToLowerInvariant() switch
        {
            "none" or "off" or "0" => BlockCodec.None,
            _ => BlockCodec.Zstd,
        };

    /// <summary>The zstd level writes use.</summary>
    internal static int ZstdLevel { get; set; } =
        int.TryParse(Environment.GetEnvironmentVariable("DBSPNET_RUN_ZSTD_LEVEL"), out var level) ? level : 1;

    /// <summary>Blocks shorter than this are stored raw: a frame's overhead outweighs any saving.</summary>
    internal const int MinBytes = 256;

    [ThreadStatic]
    private static BlockCodec? t_override;

    [ThreadStatic]
    private static ZstdSharp.Compressor? t_compressor;

    [ThreadStatic]
    private static int t_compressorLevel;

    [ThreadStatic]
    private static ZstdSharp.Decompressor? t_decompressor;

    /// <summary>The codec this thread's writes use.</summary>
    internal static BlockCodec Codec => t_override ?? DefaultCodec;

    /// <summary>Override this thread's codec until the scope is disposed (tests).</summary>
    internal static IDisposable Use(BlockCodec codec)
    {
        var previous = t_override;
        t_override = codec;
        return new Restore(previous);
    }

    private sealed class Restore(BlockCodec? previous) : IDisposable
    {
        public void Dispose() => t_override = previous;
    }

    /// <summary>Bytes stored for one block: the data to write, how it is coded, and the pooled array
    /// behind it (returned once written).</summary>
    internal readonly record struct Stored(ReadOnlyMemory<byte> Data, BlockCodec Codec, byte[]? Rented)
    {
        public void Release()
        {
            if (Rented is { } r)
            {
                ArrayPool<byte>.Shared.Return(r);
            }
        }
    }

    /// <summary>The block <paramref name="message"/> coded with <paramref name="codec"/>; raw when that
    /// does not make it smaller. Thread-safe (per-thread contexts).</summary>
    internal static Stored Compress(ReadOnlyMemory<byte> message, BlockCodec codec)
    {
        if (codec == BlockCodec.None || message.Length < MinBytes)
        {
            return new Stored(message, BlockCodec.None, null);
        }

        var level = ZstdLevel;
        if (t_compressor is null || t_compressorLevel != level)
        {
            t_compressor?.Dispose();
            t_compressor = new ZstdSharp.Compressor(level);
            t_compressorLevel = level;
        }

        var bound = ZstdSharp.Compressor.GetCompressBound(message.Length);
        var rented = ArrayPool<byte>.Shared.Rent(bound);
        var n = t_compressor.Wrap(message.Span, rented);
        if (n >= message.Length)
        {
            ArrayPool<byte>.Shared.Return(rented);
            return new Stored(message, BlockCodec.None, null);
        }

        return new Stored(rented.AsMemory(0, n), BlockCodec.Zstd, rented);
    }

    /// <summary>Decode a stored block into <paramref name="destination"/>, which is exactly its
    /// message length.</summary>
    internal static void Decompress(BlockCodec codec, ReadOnlySpan<byte> stored, Span<byte> destination)
    {
        switch (codec)
        {
            case BlockCodec.None:
                stored.CopyTo(destination);
                return;
            case BlockCodec.Zstd:
                var n = (t_decompressor ??= new ZstdSharp.Decompressor()).Unwrap(stored, destination);
                if (n != destination.Length)
                {
                    throw new InvalidDataException($"a zstd run block decoded to {n} bytes, expected {destination.Length}");
                }

                return;
            default:
                throw new InvalidDataException($"unknown run block codec {(int)codec}");
        }
    }

    /// <summary>Read a block stored as <paramref name="index"/> describes into <paramref name="destination"/>
    /// (its message length): <paramref name="read"/> fills a span with the stored bytes.</summary>
    internal static void ReadInto(RunBlock index, Span<byte> destination, ReadStored read)
    {
        if (index.Codec == BlockCodec.None)
        {
            read(destination);
            return;
        }

        var rented = ArrayPool<byte>.Shared.Rent(index.Length);
        try
        {
            var stored = rented.AsSpan(0, index.Length);
            read(stored);
            Decompress(index.Codec, stored, destination);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>Fills a span with stored bytes.</summary>
    internal delegate void ReadStored(Span<byte> into);

    /// <summary>A sequential file that zstd-compresses everything written to it into
    /// <paramref name="inner"/> as one frame (a giant block streamed from its parts); disposing it ends
    /// the frame and leaves <paramref name="inner"/> open. <see cref="ISequentialFile.Position"/> counts
    /// uncompressed bytes.</summary>
    internal static CompressingFile Compressing(ISequentialFile inner) => new(inner);

    internal sealed class CompressingFile : ISequentialFile
    {
        private readonly Stream _sink;
        private readonly ZstdSharp.CompressionStream _zstd;
        private long _position;

        internal CompressingFile(ISequentialFile inner)
        {
            _sink = inner.AsStream(); // not disposed: disposing the adapter disposes the file
            _zstd = new ZstdSharp.CompressionStream(_sink, ZstdLevel, leaveOpen: true);
        }

        public long Position => _position;

        public async ValueTask WriteAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
        {
            await _zstd.WriteAsync(data, cancellationToken).ConfigureAwait(false);
            _position += data.Length;
        }

        public ValueTask FlushAsync(CancellationToken cancellationToken = default) => ValueTask.CompletedTask;

        /// <summary>End the frame.</summary>
        public async ValueTask DisposeAsync()
        {
            await _zstd.DisposeAsync().ConfigureAwait(false);
            await _sink.FlushAsync().ConfigureAwait(false);
        }

        public void Dispose() => DisposeAsync().AsTask().GetAwaiter().GetResult();
    }
}
