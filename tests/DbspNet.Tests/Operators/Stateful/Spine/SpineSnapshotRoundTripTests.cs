// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Text.Json;
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.IO;
using DbspNet.Core.Operators.Linear;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Core.Operators.Stateful.Aggregators;
using DbspNet.Core.Operators.Stateful.Spine;
using DbspNet.Persistence.IO;

namespace DbspNet.Tests.Operators.Stateful.Spine;

/// <summary>
/// End-to-end round-trip tests for the per-batch snapshot path on each
/// wired spine operator. Builds a producer circuit, ticks deltas in,
/// snapshots the spine to an in-memory table filesystem, builds a
/// fresh consumer circuit, restores, and verifies that subsequent ticks
/// produce identical outputs.
/// </summary>
public class SpineSnapshotRoundTripTests
{
    [Fact]
    public async Task SpineDistinct_RoundTrips()
    {
        var fs = new InMemoryTableFileSystem();

        var producer = BuildSpineDistinct();
        producer.Input.Push(ZSet.FromEntries(new[]
        {
            (1, Z64.One), (2, Z64.One), (3, Z64.One),
        }));
        producer.Circuit.Step();
        producer.Input.Push(ZSet.FromEntries(new[] { (4, Z64.One), (5, Z64.One) }));
        producer.Circuit.Step();

        await SaveAsync(producer.Circuit, fs);

        var consumer = BuildSpineDistinct();
        await LoadAsync(consumer.Circuit, fs);

        // Duplicate of an already-seen key → empty output.
        consumer.Input.Push(ZSet.Singleton(3, Z64.One));
        consumer.Circuit.Step();
        Assert.True(consumer.Output.Current.IsEmpty);

        // New key → emit +1.
        consumer.Input.Push(ZSet.Singleton(99, Z64.One));
        consumer.Circuit.Step();
        Assert.Equal(Z64.One, consumer.Output.Current.WeightOf(99));
        Assert.Equal(1, consumer.Output.Current.Count);
    }

    [Fact]
    public async Task SpineAggregate_RoundTrips()
    {
        var fs = new InMemoryTableFileSystem();

        var producer = BuildSpineAggregate();
        producer.Input.Push(BuildIndexed(new[] { (1, 100L), (1, 200L), (2, 50L) }));
        producer.Circuit.Step();
        producer.Input.Push(BuildIndexed(new[] { (1, 300L) }));
        producer.Circuit.Step();

        await SaveAsync(producer.Circuit, fs);

        var consumer = BuildSpineAggregate();
        await LoadAsync(consumer.Circuit, fs);

        // Add another row to group 1; should retract old sum (600) and emit new (650).
        consumer.Input.Push(BuildIndexed(new[] { (1, 50L) }));
        consumer.Circuit.Step();
        Assert.Equal(new Z64(-1), consumer.Output.Current.WeightOf((1, 600L)));
        Assert.Equal(Z64.One, consumer.Output.Current.WeightOf((1, 650L)));
    }

    [Fact]
    public async Task SpineInnerJoin_RoundTrips()
    {
        var fs = new InMemoryTableFileSystem();

        var producer = BuildSpineInnerJoin();
        producer.Left.Push(BuildIndexed(new[] { (1, 10L), (2, 20L) }));
        producer.Right.Push(BuildIndexed(new[] { (1, 100L), (2, 200L) }));
        producer.Circuit.Step();

        await SaveAsync(producer.Circuit, fs);

        var consumer = BuildSpineInnerJoin();
        await LoadAsync(consumer.Circuit, fs);

        // New left row for an existing key → joins against restored right trace.
        consumer.Left.Push(BuildIndexed(new[] { (1, 11L) }));
        consumer.Circuit.Step();
        Assert.Equal(Z64.One, consumer.Output.Current.WeightOf((1, 11L, 100L)));
        Assert.Equal(1, consumer.Output.Current.Count);
    }

    [Fact]
    public async Task SpineLeftJoin_RoundTrips()
    {
        var fs = new InMemoryTableFileSystem();

        var producer = BuildSpineLeftJoin();
        // Tick 1: left-only key 1 → NULL-padded.
        producer.Left.Push(BuildIndexed(new[] { (1, 10L) }));
        producer.Circuit.Step();
        // Tick 2: right arrives for key 1 → gained-match transition.
        producer.Right.Push(BuildIndexed(new[] { (1, 100L) }));
        producer.Circuit.Step();

        await SaveAsync(producer.Circuit, fs);

        var consumer = BuildSpineLeftJoin();
        await LoadAsync(consumer.Circuit, fs);

        // New left key with no right match → NULL-padded.
        consumer.Left.Push(BuildIndexed(new[] { (2, 20L) }));
        consumer.Circuit.Step();
        Assert.Equal(Z64.One, consumer.Output.Current.WeightOf((2, 20L, -1L)));
    }

    // ----- Helpers -----

    private sealed record DistinctRig(
        RootCircuit Circuit,
        InputHandle<ZSet<int, Z64>> Input,
        OutputHandle<ZSet<int, Z64>> Output);

    private static DistinctRig BuildSpineDistinct()
    {
        InputHandle<ZSet<int, Z64>>? ih = null;
        OutputHandle<ZSet<int, Z64>>? oh = null;
        var c = RootCircuit.Build(b =>
        {
            var (h, s) = b.ZSetInput<int, Z64>();
            ih = h;
            oh = b.Output(b.SpineDistinct(s, snapshotCodec: new JsonZSetCodec()));
        });
        return new DistinctRig(c, ih!, oh!);
    }

    private sealed record AggregateRig(
        RootCircuit Circuit,
        InputHandle<IndexedZSet<int, long, Z64>> Input,
        OutputHandle<ZSet<(int Key, long Value), Z64>> Output);

    private static AggregateRig BuildSpineAggregate()
    {
        InputHandle<IndexedZSet<int, long, Z64>>? ih = null;
        OutputHandle<ZSet<(int, long), Z64>>? oh = null;
        var c = RootCircuit.Build(b =>
        {
            var (h, s) = b.Input(IndexedZSet<int, long, Z64>.Empty, (a, x) => a + x);
            ih = h;
            oh = b.Output(b.SpineIncrementalAggregate(
                s,
                new SumAggregator<long>(),
                snapshotCodec: new JsonIndexedZSetCodec()));
        });
        return new AggregateRig(c, ih!, oh!);
    }

    private sealed record JoinRig(
        RootCircuit Circuit,
        InputHandle<IndexedZSet<int, long, Z64>> Left,
        InputHandle<IndexedZSet<int, long, Z64>> Right,
        OutputHandle<ZSet<(int Key, long L, long R), Z64>> Output);

    private static JoinRig BuildSpineInnerJoin()
    {
        InputHandle<IndexedZSet<int, long, Z64>>? li = null;
        InputHandle<IndexedZSet<int, long, Z64>>? ri = null;
        OutputHandle<ZSet<(int, long, long), Z64>>? oh = null;
        var c = RootCircuit.Build(b =>
        {
            var (lh, ls) = b.Input(IndexedZSet<int, long, Z64>.Empty, (a, x) => a + x);
            var (rh, rs) = b.Input(IndexedZSet<int, long, Z64>.Empty, (a, x) => a + x);
            li = lh;
            ri = rh;
            oh = b.Output(b.SpineIncrementalInnerJoin(
                ls, rs,
                (k, l, r) => (k, l, r),
                leftSnapshotCodec: new JsonIndexedZSetCodec(),
                rightSnapshotCodec: new JsonIndexedZSetCodec()));
        });
        return new JoinRig(c, li!, ri!, oh!);
    }

    private static JoinRig BuildSpineLeftJoin()
    {
        InputHandle<IndexedZSet<int, long, Z64>>? li = null;
        InputHandle<IndexedZSet<int, long, Z64>>? ri = null;
        OutputHandle<ZSet<(int, long, long), Z64>>? oh = null;
        var c = RootCircuit.Build(b =>
        {
            var (lh, ls) = b.Input(IndexedZSet<int, long, Z64>.Empty, (a, x) => a + x);
            var (rh, rs) = b.Input(IndexedZSet<int, long, Z64>.Empty, (a, x) => a + x);
            li = lh;
            ri = rh;
            oh = b.Output(b.SpineIncrementalLeftJoin(
                ls, rs,
                joinCombine: (k, l, r) => (k, l, r),
                nullPadCombine: (k, l) => (k, l, -1L),
                leftSnapshotCodec: new JsonIndexedZSetCodec(),
                rightSnapshotCodec: new JsonIndexedZSetCodec()));
        });
        return new JoinRig(c, li!, ri!, oh!);
    }

    private static IndexedZSet<int, long, Z64> BuildIndexed(IEnumerable<(int Key, long Value)> entries)
    {
        var b = new IndexedZSetBuilder<int, long, Z64>();
        foreach (var (k, v) in entries)
        {
            b.Add(k, v, Z64.One);
        }

        return b.Build();
    }

    /// <summary>
    /// Walk the circuit's ISnapshotable operators and Save each via a
    /// per-op-prefixed context. Mirrors the per-op subkey routing that
    /// <c>Snapshot.WriteAsync</c> does without the manifest / pointer /
    /// fingerprint machinery that requires plan compilation.
    /// </summary>
    private static async ValueTask SaveAsync(RootCircuit circuit, InMemoryTableFileSystem fs)
    {
        for (var i = 0; i < circuit.Operators.Count; i++)
        {
            if (circuit.Operators[i] is ISnapshotable s)
            {
                await s.SaveAsync(new PrefixedContext(fs, "op-" + i + "/"));
            }
        }
    }

    private static async ValueTask LoadAsync(RootCircuit circuit, InMemoryTableFileSystem fs)
    {
        for (var i = 0; i < circuit.Operators.Count; i++)
        {
            if (circuit.Operators[i] is ISnapshotable s)
            {
                await s.LoadAsync(new PrefixedContext(fs, "op-" + i + "/"));
            }
        }
    }

    /// <summary>
    /// Thin adapter that turns an <see cref="ITableFileSystem"/> plus a
    /// key prefix into both an <see cref="ISnapshotWriter"/> and an
    /// <see cref="ISnapshotReader"/>. Mirrors the production
    /// <c>TableFileSystemSnapshotContext</c> (which is internal to the
    /// persistence assembly).
    /// </summary>
    private sealed class PrefixedContext : ISnapshotWriter, ISnapshotReader
    {
        private readonly ITableFileSystem _fs;
        private readonly string _prefix;

        public PrefixedContext(ITableFileSystem fs, string prefix)
        {
            _fs = fs;
            _prefix = prefix;
        }

        public ValueTask<ISequentialFile> CreateAsync(string filename, CancellationToken cancellationToken = default)
            => _fs.CreateAsync(_prefix + filename, overwrite: true, cancellationToken);

        public ValueTask<IRandomAccessFile> OpenReadAsync(string filename, CancellationToken cancellationToken = default)
            => _fs.OpenReadAsync(_prefix + filename, cancellationToken);

        public ValueTask<bool> ExistsAsync(string filename, CancellationToken cancellationToken = default)
            => _fs.ExistsAsync(_prefix + filename, cancellationToken);
    }

    // ----- Tiny JSON codecs (test-only) -----

    private sealed class JsonZSetCodec : IZSetTraceCodec<int, Z64>
    {
        public string SchemaFingerprint => "test-int-z64";

        public async ValueTask SaveAsync(
            ISnapshotWriter writer, string fileName, ZSet<int, Z64> trace,
            CancellationToken cancellationToken = default)
        {
            var entries = new List<long[]>(trace.Count);
            foreach (var (k, w) in trace)
            {
                entries.Add(new[] { (long)k, w.Value });
            }

            var json = JsonSerializer.SerializeToUtf8Bytes(entries);
            await using var file = await writer.CreateAsync(fileName, cancellationToken);
            await using var stream = file.AsStream();
            await stream.WriteAsync(json, cancellationToken);
        }

        public async ValueTask<ZSet<int, Z64>> LoadAsync(
            ISnapshotReader reader, string fileName,
            CancellationToken cancellationToken = default)
        {
            if (!await reader.ExistsAsync(fileName, cancellationToken))
            {
                return ZSet<int, Z64>.Empty;
            }

            await using var file = await reader.OpenReadAsync(fileName, cancellationToken);
            await using var stream = file.AsStream();
            using var ms = new MemoryStream();
            await stream.CopyToAsync(ms, cancellationToken);
            var entries = JsonSerializer.Deserialize<List<long[]>>(ms.ToArray()) ?? new();
            var b = new ZSetBuilder<int, Z64>();
            foreach (var e in entries)
            {
                b.Add((int)e[0], new Z64(e[1]));
            }

            return b.Build();
        }
    }

    private sealed class JsonIndexedZSetCodec : IIndexedZSetTraceCodec<int, long, Z64>
    {
        public string SchemaFingerprint => "test-int-long-z64";

        public async ValueTask SaveAsync(
            ISnapshotWriter writer, string fileName,
            IndexedZSet<int, long, Z64> trace,
            CancellationToken cancellationToken = default)
        {
            var entries = new List<long[]>();
            foreach (var (k, group) in trace)
            {
                foreach (var (v, w) in group)
                {
                    entries.Add(new[] { (long)k, v, w.Value });
                }
            }

            var json = JsonSerializer.SerializeToUtf8Bytes(entries);
            await using var file = await writer.CreateAsync(fileName, cancellationToken);
            await using var stream = file.AsStream();
            await stream.WriteAsync(json, cancellationToken);
        }

        public async ValueTask<IndexedZSet<int, long, Z64>> LoadAsync(
            ISnapshotReader reader, string fileName,
            CancellationToken cancellationToken = default)
        {
            if (!await reader.ExistsAsync(fileName, cancellationToken))
            {
                return IndexedZSet<int, long, Z64>.Empty;
            }

            await using var file = await reader.OpenReadAsync(fileName, cancellationToken);
            await using var stream = file.AsStream();
            using var ms = new MemoryStream();
            await stream.CopyToAsync(ms, cancellationToken);
            var entries = JsonSerializer.Deserialize<List<long[]>>(ms.ToArray()) ?? new();
            var b = new IndexedZSetBuilder<int, long, Z64>();
            foreach (var e in entries)
            {
                b.Add((int)e[0], e[1], new Z64(e[2]));
            }

            return b.Build();
        }
    }
}
