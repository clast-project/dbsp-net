// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
// Covers the keyed-blob capability added to IIndexedZSetTraceCodec for
// docs/design-incremental-persistence.md §7.2.
//
// Per-key derived state cannot be persisted by a stateful operator alone: the operator is
// generic in TKey and only its codec knows how to encode one. This is the mechanism that
// closes that gap — the codec round-trips (key, opaque bytes) pairs using exactly the key
// encoding it already uses for the trace. IncrementalAggregateOp is the motivating consumer
// (it must persist each group's aggregator scratch state rather than re-derive it), but the
// capability is deliberately consumer-agnostic: the blob is opaque to the codec.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.IO;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Persistence;
using DbspNet.Persistence.IO.Local;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Tests.Persistence;

public class KeyedBlobCodecTests : IDisposable
{
    private readonly string _dir;

    public KeyedBlobCodecTests()
    {
        _dir = Path.Combine(Path.GetTempPath(), "dbspnet-keyedblob-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_dir))
        {
            Directory.Delete(_dir, recursive: true);
        }

        GC.SuppressFinalize(this);
    }

    // A two-column key (one nullable) so the round-trip covers more than a scalar id.
    private static Schema KeySchema() =>
        new([
            new SchemaColumn("region", new SqlVarcharType(8, true)),
            new SchemaColumn("tier", new SqlIntegerType(false)),
        ]);

    private static Schema ValueSchema() =>
        new([new SchemaColumn("amt", new SqlBigintType(false))]);

    private static IIndexedZSetTraceCodec<StructuralRow, StructuralRow, Z64> Codec() =>
        ArrowSqlSnapshotCodecs.Instance.CreateIndexedZSetTraceCodec(KeySchema(), ValueSchema());

    // VARCHAR cells in a StructuralRow are Utf8String, not System.String.
    private static StructuralRow Key(string? region, int tier) =>
        new(new object?[] { region is null ? null : Utf8String.Of(region), tier });

    private PrefixedContext Context() => new(new LocalTableFileSystem(_dir));

    [Fact]
    public void ArrowCodec_AdvertisesKeyedBlobSupport()
    {
        Assert.True(Codec().SupportsKeyedBlobs);
    }

    [Fact]
    public async Task Blobs_RoundTripByKey()
    {
        var codec = Codec();
        var ctx = Context();

        var entries = new List<(StructuralRow Key, byte[] Blob)>
        {
            (Key("west", 1), [1, 2, 3]),
            (Key("east", 2), [9]),
            (Key(null, 3), [0xFF, 0x00, 0xFF, 0x00]),
            (Key("west", 2), []),   // empty blob is a legal value, not "absent"
        };

        await codec.SaveKeyedBlobsAsync(ctx, "aggstate.arrows", entries);
        var loaded = await codec.LoadKeyedBlobsAsync(ctx, "aggstate.arrows");

        // Order is explicitly not part of the contract — compare as a map.
        var expected = entries.ToDictionary(e => Describe(e.Key), e => Convert.ToHexString(e.Blob), StringComparer.Ordinal);
        var actual = loaded.ToDictionary(e => Describe(e.Key), e => Convert.ToHexString(e.Blob), StringComparer.Ordinal);
        Assert.Equal(expected.Count, actual.Count);
        foreach (var (k, v) in expected)
        {
            Assert.True(actual.TryGetValue(k, out var got), $"key {k} missing after round-trip");
            Assert.Equal(v, got);
        }
    }

    [Fact]
    public async Task MissingFile_LoadsAsNoBlobs()
    {
        // A snapshot written before the operator started emitting blobs must load as
        // "no blobs" rather than throwing — that is what makes the consumer's fallback
        // path reachable instead of hard-failing an otherwise valid snapshot.
        var loaded = await Codec().LoadKeyedBlobsAsync(Context(), "absent.arrows");
        Assert.Empty(loaded);
    }

    [Fact]
    public async Task EmptyEntryList_RoundTrips()
    {
        var codec = Codec();
        var ctx = Context();
        await codec.SaveKeyedBlobsAsync(ctx, "empty.arrows", []);
        Assert.Empty(await codec.LoadKeyedBlobsAsync(ctx, "empty.arrows"));
    }

    [Fact]
    public async Task BlobsAreOpaque_ArbitraryBytesSurvive()
    {
        var codec = Codec();
        var ctx = Context();

        // Two doubles and a long — the shape an aggregator's moment state actually has,
        // exercised as raw bytes the codec must not interpret.
        var blob = new byte[24];
        BitConverter.TryWriteBytes(blob.AsSpan(0, 8), 1.0000000000000002e16);
        BitConverter.TryWriteBytes(blob.AsSpan(8, 8), double.NegativeInfinity);
        BitConverter.TryWriteBytes(blob.AsSpan(16, 8), long.MinValue);

        await codec.SaveKeyedBlobsAsync(ctx, "moments.arrows", [(Key("west", 1), blob)]);
        var loaded = await codec.LoadKeyedBlobsAsync(ctx, "moments.arrows");

        var got = Assert.Single(loaded).Blob;
        Assert.Equal(1.0000000000000002e16, BitConverter.ToDouble(got, 0));
        Assert.Equal(double.NegativeInfinity, BitConverter.ToDouble(got, 8));
        Assert.Equal(long.MinValue, BitConverter.ToInt64(got, 16));
    }

    [Fact]
    public async Task BlobFile_IsIndependentOfTheTraceFile()
    {
        // The blobs live in their own file, so writing them neither disturbs nor depends
        // on the trace — the consumer can add them to an operator that already snapshots.
        var codec = Codec();
        var ctx = Context();

        var trace = new IndexedZSetBuilder<StructuralRow, StructuralRow, Z64>();
        trace.Add(Key("west", 1), new StructuralRow(new object?[] { 10L }), new Z64(1));
        await codec.SaveAsync(ctx, "trace.arrows", trace.Build());
        await codec.SaveKeyedBlobsAsync(ctx, "aggstate.arrows", [(Key("west", 1), [7, 7])]);

        var reloadedTrace = await codec.LoadAsync(ctx, "trace.arrows");
        var reloadedBlobs = await codec.LoadKeyedBlobsAsync(ctx, "aggstate.arrows");

        Assert.Equal(1, reloadedTrace.GroupCount);
        Assert.Equal("0707", Convert.ToHexString(Assert.Single(reloadedBlobs).Blob));
    }

    private static string Describe(StructuralRow key) =>
        (key[0]?.ToString() ?? "<null>") + "|" + key[1];

    /// <summary>Minimal <see cref="ISnapshotWriter"/>/<see cref="ISnapshotReader"/> over a
    /// directory — the production context is internal to the persistence assembly.</summary>
    private sealed class PrefixedContext(ITableFileSystem fs) : ISnapshotWriter, ISnapshotReader
    {
        public ValueTask<ISequentialFile> CreateAsync(string filename, CancellationToken cancellationToken = default)
            => fs.CreateAsync(filename, overwrite: true, cancellationToken);

        public ValueTask<IRandomAccessFile> OpenReadAsync(string filename, CancellationToken cancellationToken = default)
            => fs.OpenReadAsync(filename, cancellationToken);

        public ValueTask<bool> ExistsAsync(string filename, CancellationToken cancellationToken = default)
            => fs.ExistsAsync(filename, cancellationToken);
    }
}
