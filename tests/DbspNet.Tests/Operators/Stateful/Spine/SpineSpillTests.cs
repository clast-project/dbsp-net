// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Text.Json;
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.IO;
using DbspNet.Core.Operators.Linear;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Core.Operators.Stateful.Spine;
using DbspNet.Persistence.IO;

namespace DbspNet.Tests.Operators.Stateful.Spine;

/// <summary>
/// Behaviour and observability tests for the phase-4 disk-spill path:
/// (1) a spilled spine produces identical outputs to a fully-resident
///     spine on the same input sequence;
/// (2) spill files actually appear on the filesystem at the configured
///     prefix once compactions push a batch past <c>MinSpillLevel</c>;
/// (3) spill files for batches consumed by a later compaction are
///     deleted (no leaks).
/// </summary>
public class SpineSpillTests
{
    [Theory]
    [InlineData(0)]   // every batch spills
    [InlineData(1)]
    [InlineData(2)]
    public void SpilledDistinct_MatchesFlatDistinct(int minSpillLevel)
    {
        var fs = new InMemoryTableFileSystem();
        var spill = new SpineSpillConfig<int, Z64>
        {
            FileSystem = fs,
            Prefix = "spill",
            Codec = new JsonIntZSetCodec(),
            MinSpillLevel = minSpillLevel,
        };

        InputHandle<ZSet<int, Z64>>? flatIn = null;
        OutputHandle<ZSet<int, Z64>>? flatOut = null;
        var flatCircuit = RootCircuit.Build(b =>
        {
            var (h, s) = b.ZSetInput<int, Z64>();
            flatIn = h;
            flatOut = b.Output(b.Distinct(s));
        });

        InputHandle<ZSet<int, Z64>>? spineIn = null;
        OutputHandle<ZSet<int, Z64>>? spineOut = null;
        var spineCircuit = RootCircuit.Build(b =>
        {
            var (h, s) = b.ZSetInput<int, Z64>();
            spineIn = h;
            spineOut = b.Output(b.SpineDistinct(s, spillConfig: spill));
        });

        var rng = new Random(Seed: 101 + minSpillLevel);
        for (var step = 0; step < 200; step++)
        {
            var delta = RandomDelta(rng);
            flatIn!.Push(delta);
            spineIn!.Push(delta);
            flatCircuit.Step();
            spineCircuit.Step();

            Assert.Equal(flatOut!.Current, spineOut!.Current);
        }
    }

    [Fact]
    public async Task SpillFilesAppearOnFilesystem()
    {
        var fs = new InMemoryTableFileSystem();
        var spill = new SpineSpillConfig<int, Z64>
        {
            FileSystem = fs,
            Prefix = "spill",
            Codec = new JsonIntZSetCodec(),
            MinSpillLevel = 1,  // spill aggressively
        };

        InputHandle<ZSet<int, Z64>>? ih = null;
        var circuit = RootCircuit.Build(b =>
        {
            var (h, s) = b.ZSetInput<int, Z64>();
            ih = h;
            b.Output(b.SpineDistinct(s, spillConfig: spill));
        });

        // Push enough singleton ticks to trigger several L0 → L1
        // compactions (tier=4 means every 4 ticks promotes one batch).
        for (var i = 0; i < 32; i++)
        {
            ih!.Push(ZSet.Singleton(i, Z64.One));
            circuit.Step();
        }

        // At least one spill file should be live in the configured prefix.
        var paths = new List<string>();
        await foreach (var entry in fs.ListAsync("spill/"))
        {
            paths.Add(entry.Path);
        }

        Assert.NotEmpty(paths);
        Assert.All(paths, p => Assert.StartsWith("spill/batch_", p));
    }

    [Fact]
    public async Task SpillFilesAreDeletedAfterCompactionConsumesThem()
    {
        var fs = new InMemoryTableFileSystem();
        var spill = new SpineSpillConfig<int, Z64>
        {
            FileSystem = fs,
            Prefix = "spill",
            Codec = new JsonIntZSetCodec(),
            MinSpillLevel = 1,
        };

        InputHandle<ZSet<int, Z64>>? ih = null;
        var circuit = RootCircuit.Build(b =>
        {
            var (h, s) = b.ZSetInput<int, Z64>();
            ih = h;
            b.Output(b.SpineDistinct(s, spillConfig: spill));
        });

        // Push enough singletons to force multiple cascading
        // compactions (L0 → L1 → L2). Each cascading compaction
        // consumes its inputs, which (if spilled) should have their
        // files deleted.
        for (var i = 0; i < 256; i++)
        {
            ih!.Push(ZSet.Singleton(i, Z64.One));
            circuit.Step();
        }

        // Count live spill files vs total ever created.
        var liveFiles = new List<string>();
        await foreach (var entry in fs.ListAsync("spill/"))
        {
            liveFiles.Add(entry.Path);
        }

        // The spill counter increments on every write. We don't have
        // direct access to it, but: with 256 singletons and tier=4
        // compaction, the spine produces many more compactions than
        // surviving batches. The ratio of "live files" to "files ever
        // created" should be tiny.
        //
        // What we can check directly: the live count should be much
        // smaller than 256 (we don't keep one spill per tick — we
        // compact them away).
        Assert.True(liveFiles.Count < 32,
            $"too many live spill files ({liveFiles.Count}) — compaction inputs not being deleted");
    }

    private static ZSet<int, Z64> RandomDelta(Random rng)
    {
        var n = rng.Next(4);
        if (n == 0) return ZSet<int, Z64>.Empty;
        var b = new ZSetBuilder<int, Z64>();
        for (var i = 0; i < n; i++)
        {
            var k = rng.Next(50);
            var w = rng.Next(-2, 3);
            if (w == 0) continue;
            b.Add(k, new Z64(w));
        }
        return b.Build();
    }

    private sealed class JsonIntZSetCodec : IZSetTraceCodec<int, Z64>
    {
        public string SchemaFingerprint => "test-int-z64";

        public async ValueTask SaveAsync(
            ISnapshotWriter writer, string fileName, ZSet<int, Z64> trace,
            CancellationToken cancellationToken = default)
        {
            var entries = new List<long[]>(trace.Count);
            foreach (var (k, w) in trace) entries.Add(new[] { (long)k, w.Value });
            var json = JsonSerializer.SerializeToUtf8Bytes(entries);
            await using var file = await writer.CreateAsync(fileName, cancellationToken);
            await using var stream = file.AsStream();
            await stream.WriteAsync(json, cancellationToken);
        }

        public async ValueTask<ZSet<int, Z64>> LoadAsync(
            ISnapshotReader reader, string fileName,
            CancellationToken cancellationToken = default)
        {
            if (!await reader.ExistsAsync(fileName, cancellationToken)) return ZSet<int, Z64>.Empty;
            await using var file = await reader.OpenReadAsync(fileName, cancellationToken);
            await using var stream = file.AsStream();
            using var ms = new MemoryStream();
            await stream.CopyToAsync(ms, cancellationToken);
            var entries = JsonSerializer.Deserialize<List<long[]>>(ms.ToArray()) ?? new();
            var b = new ZSetBuilder<int, Z64>();
            foreach (var e in entries) b.Add((int)e[0], new Z64(e[1]));
            return b.Build();
        }
    }
}
