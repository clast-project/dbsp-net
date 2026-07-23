// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
// Stage 1 of docs/design-durable-identity.md §2.3: every spine batch carries an id, and nothing
// else changes yet.
//
// The whole point of the id is that a snapshot will eventually be able to REFERENCE a batch file
// rather than copy it, which is only sound because a sealed batch is immutable. That makes one
// distinction load-bearing, and it is what these tests pin:
//
//   * compaction produces a NEW batch from several inputs — new contents, new id
//   * spilling moves an EXISTING batch to disk — same contents, same id
//
// Getting that backwards is not a cosmetic error. A new id on spill would orphan a referenced file;
// a reused id after a merge would make a manifest name contents that are not what it recorded.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.Circuit;
using DbspNet.Core.IO;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Core.Operators.Stateful.Spine;
using DbspNet.Persistence.IO;

namespace DbspNet.Tests.Operators.Stateful.Spine;

public class SpineBatchIdentityTests
{
    private static ZSet<int, Z64> Z(params (int Key, long Weight)[] entries)
    {
        var b = new ZSetBuilder<int, Z64>();
        foreach (var (k, w) in entries)
        {
            b.Add(k, new Z64(w));
        }

        return b.Build();
    }

    private static ResidentSpineBatch<int, Z64> Batch(params (int Key, long Weight)[] entries) =>
        ResidentSpineBatch<int, Z64>.FromZSet(Z(entries), Comparer<int>.Default);

    [Fact]
    public void EveryBatchGetsADistinctPositiveId()
    {
        var ids = new HashSet<long>();
        for (var i = 0; i < 50; i++)
        {
            var id = Batch((i, 1)).Id;
            Assert.True(id > 0, "ids are positive");
            Assert.True(ids.Add(id), $"id {id} was handed out twice");
        }
    }

    [Fact]
    public void IdsAreSharedAcrossGenericInstantiations()
    {
        // Regression guard for the reason SpineBatchId is a non-generic holder: a static field on
        // the generic type would be per closed constructed type, so <int,Z64> and <long,Z64> would
        // each start at 1 and collide.
        var a = ResidentSpineBatch<int, Z64>.FromZSet(Z((1, 1)), Comparer<int>.Default);

        var lb = new ZSetBuilder<long, Z64>();
        lb.Add(1L, new Z64(1));
        var b = ResidentSpineBatch<long, Z64>.FromZSet(lb.Build(), Comparer<long>.Default);

        Assert.NotEqual(a.Id, b.Id);
    }

    [Fact]
    public void Merge_ProducesANewBatchWithANewId()
    {
        // Compaction is not an in-place update: the merged batch has different contents, so a
        // manifest naming the inputs must not silently start naming the output.
        var left = Batch((1, 1), (2, 1));
        var right = Batch((2, 1), (3, 1));

        var merged = SpineBatch<int, Z64>.Merge([left, right], Comparer<int>.Default);

        Assert.NotEqual(left.Id, merged.Id);
        Assert.NotEqual(right.Id, merged.Id);
        Assert.True(merged.Id > Math.Max(left.Id, right.Id), "ids are monotone, so a merge is newer");
    }

    [Fact]
    public void MergeOfOne_StillYieldsTheSameBatch()
    {
        // Merge([x]) short-circuits to Materialise(x), which is a representation change rather
        // than a new batch — so it must NOT burn a new id.
        var only = Batch((1, 1), (2, 1));
        var merged = SpineBatch<int, Z64>.Merge([only], Comparer<int>.Default);
        Assert.Equal(only.Id, merged.Id);
    }

    [Fact]
    public async Task SpilledBatch_KeepsItsId_AndMaterialisingReadsItBackUnderTheSameId()
    {
        // The relocation case, exercised directly on the two representations rather than through
        // the trace's spill plumbing — the invariant belongs to the batch, not to the trace.
        var fs = new InMemoryTableFileSystem();
        var codec = new JsonIntCodec();
        var ctx = new SpillContext(fs);
        await codec.SaveAsync(ctx, "batch_1.arrows", Z((1, 1), (2, 3)));

        const long id = 4242;
        var spilled = new SpilledSpineBatch<int, Z64>(
            fs, "batch_1.arrows", codec, Comparer<int>.Default, bloom: null, count: 2, id: id);

        Assert.Equal(id, spilled.Id);

        // Reading it back on demand is the same batch in memory again, not a new one. A fresh id
        // here would orphan any manifest that referenced the file.
        var resident = spilled.Materialise(Comparer<int>.Default);
        Assert.Equal(id, resident.Id);
        Assert.Equal(2, resident.Count);
    }

    /// <summary>Minimal JSON codec so the spill path has something to serialise with.</summary>
    private sealed class JsonIntCodec : IZSetTraceCodec<int, Z64>
    {
        public string SchemaFingerprint => "test-int-z64";

        public async ValueTask SaveAsync(
            ISnapshotWriter writer, string fileName, ZSet<int, Z64> trace,
            CancellationToken cancellationToken = default)
        {
            var pairs = trace.Select(kv => $"{kv.Key}:{kv.Value.Value}");
            var bytes = System.Text.Encoding.UTF8.GetBytes(string.Join(",", pairs));
            await using var file = await writer.CreateAsync(fileName, cancellationToken);
            await using var stream = file.AsStream();
            await stream.WriteAsync(bytes, cancellationToken);
        }

        public async ValueTask<ZSet<int, Z64>> LoadAsync(
            ISnapshotReader reader, string fileName, CancellationToken cancellationToken = default)
        {
            if (!await reader.ExistsAsync(fileName, cancellationToken))
            {
                return ZSet<int, Z64>.Empty;
            }

            await using var file = await reader.OpenReadAsync(fileName, cancellationToken);
            await using var stream = file.AsStream();
            using var ms = new MemoryStream();
            await stream.CopyToAsync(ms, cancellationToken);
            var text = System.Text.Encoding.UTF8.GetString(ms.ToArray());
            var b = new ZSetBuilder<int, Z64>();
            foreach (var part in text.Split(',', StringSplitOptions.RemoveEmptyEntries))
            {
                var bits = part.Split(':');
                b.Add(
                    int.Parse(bits[0], System.Globalization.CultureInfo.InvariantCulture),
                    new Z64(long.Parse(bits[1], System.Globalization.CultureInfo.InvariantCulture)));
            }

            return b.Build();
        }
    }
}
