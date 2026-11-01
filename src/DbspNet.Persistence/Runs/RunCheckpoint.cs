// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.IO;
using DbspNet.Core.Operators.Stateful;

namespace DbspNet.Persistence.Runs;

/// <summary>
/// Incremental checkpoints of trace state (docs/design-durable-state.md §1f, increment 3). A trace
/// with a chain and a pending change writes the change as one more run; anything else writes a
/// base run of its whole state. Either way the snapshot's run index records the chain, and the
/// trace restarts tracking on it once the checkpoint commits. Restoring sums the chain's runs and
/// marks the result, so the trace that integrates it keeps extending the same chain.
/// </summary>
/// <remarks><c>DBSPNET_RUNS_VERIFY=1</c> reads every chain back as it is written and throws if
/// its sum differs from the state: a missed mutation shows up at the checkpoint that misses it.</remarks>
internal static class RunCheckpoint
{
    internal static bool Verify { get; set; } = Environment.GetEnvironmentVariable("DBSPNET_RUNS_VERIFY") == "1";

    // The row hash every run's footer pins (the trace codecs' order for row-ordered runs).
    private static int RowHash(StructuralRow row) => row.GetHashCode();

    /// <summary>Runs live outside any one snapshot, named after the file that first wrote them.</summary>
    private static string RunPath(TableFileSystemSnapshotContext ctx, string fileName) => "runs/" + ctx.Prefix + fileName;

    internal static async ValueTask SaveZSetAsync(
        ISnapshotWriter writer, string fileName, ZSet<StructuralRow, Z64> state,
        IZSetRunCodec codec, Func<StructuralRow, int> hash, CancellationToken cancellationToken)
    {
        // A partitioned operator orders its runs by partition hash (increment 5), and while it is
        // partly cold its snapshot holds only the partitions in memory: Whole() is everything.
        var trace = CheckpointTracking.TraceOf(state) as IZSetCheckpointTracked<StructuralRow, Z64>;
        var order = trace?.OrderHash;
        var partitionOrder = order is not null;
        var runHash = order ?? hash;
        ZSet<StructuralRow, Z64>? whole = null;
        ZSet<StructuralRow, Z64> Whole() => whole ??= trace?.FullState?.Invoke() ?? state;

        if (writer is not TableFileSystemSnapshotContext { Session: { } session } ctx)
        {
            // Not part of a session-aware checkpoint: one self-contained run in place.
            await RunFile.WriteZSetAsync(writer, fileName, Whole(), codec, hash, cancellationToken: cancellationToken).ConfigureAwait(false);
            return;
        }

        RunChain chain;
        if (trace?.Tracking is { Chain: RunChain prev, Pending: { } pending } && prev.CanExtend(ctx.FileSystem, pending.Count, partitionOrder))
        {
            chain = pending.IsEmpty ? prev : prev.Extend(await WriteAsync(pending), pending.Count);
        }
        else
        {
            var all = Whole();
            chain = RunChain.Base(ctx.FileSystem, await WriteAsync(all), all.Count, partitionOrder);
        }

        // The size: the whole state's, without reading it when only a delta was written.
        var size = whole?.Count ?? trace?.ExactCount ?? (trace?.FullState is null ? state.Count : -1);
        if (size < 0 && trace?.Tracking?.Chain is RunChain tracked && tracked.Count >= 0 && trace.Tracking.Pending is { } p)
        {
            size = tracked.Count + p.Count; // an upper bound (a partitioned operator's: only a heuristic there)
        }

        chain = Adopt(trace, chain).WithCount(size);
        session.Record(ctx.Prefix + fileName, chain);
        if (trace is not null)
        {
            session.OnCommit(() =>
            {
                trace.StartTracking(chain);
                MaybeCompact(trace, chain, (c, path, ct) => RunCompactor.CompactZSetAsync(c, codec, path, ct));
            });
        }

        if (Verify)
        {
            var all = Whole();
            var back = await ReadZSetAsync(chain, codec, runHash, cancellationToken).ConfigureAwait(false);
            if (back.Count != all.Count || all.Any(e => back.WeightOf(e.Key) != e.Value))
            {
                throw new InvalidOperationException(
                    $"run chain for '{ctx.Prefix}{fileName}' sums to {back.Count} rows, the state has {all.Count}");
            }
        }

        async ValueTask<string> WriteAsync(ZSet<StructuralRow, Z64> set)
        {
            var path = RunPath(ctx, fileName);
            await RunFile.WriteZSetAsync(ctx.FileSystem, path, set, codec, runHash, fingerprint: hash, cancellationToken: cancellationToken)
                .ConfigureAwait(false);
            return path;
        }
    }

    internal static async ValueTask SaveIndexedAsync(
        ISnapshotWriter writer, string fileName, IndexedZSet<StructuralRow, StructuralRow, Z64> state,
        IIndexedRunCodec codec, Func<StructuralRow, int> hash, CancellationToken cancellationToken)
    {
        if (writer is not TableFileSystemSnapshotContext { Session: { } session } ctx)
        {
            await RunFile.WriteIndexedAsync(writer, fileName, state, codec, hash, cancellationToken: cancellationToken).ConfigureAwait(false);
            return;
        }

        var trace = CheckpointTracking.TraceOf(state) as IIndexedCheckpointTracked<StructuralRow, StructuralRow, Z64>;
        RunChain chain;
        if (trace?.Tracking is { Chain: RunChain prev, Pending: { } pending } && prev.CanExtend(ctx.FileSystem, Entries(pending)))
        {
            chain = pending.IsEmpty ? prev : prev.Extend(await WriteAsync(pending), Entries(pending));
        }
        else
        {
            chain = RunChain.Base(ctx.FileSystem, await WriteAsync(state), Entries(state));
        }

        chain = Adopt(trace, chain).WithCount(state.GroupCount);
        session.Record(ctx.Prefix + fileName, chain);
        if (trace is not null)
        {
            session.OnCommit(() =>
            {
                trace.StartTracking(chain);
                MaybeCompact(trace, chain, (c, path, ct) => RunCompactor.CompactIndexedAsync(c, codec, path, ct));
            });
        }

        if (Verify)
        {
            var back = await ReadIndexedAsync(chain, codec, hash, cancellationToken).ConfigureAwait(false);
            if (back.GroupCount != state.GroupCount
                || state.Any(g => back.GroupFor(g.Key) is var b && (b.Count != g.Value.Count || g.Value.Any(e => b.WeightOf(e.Key) != e.Value))))
            {
                throw new InvalidOperationException(
                    $"run chain for '{ctx.Prefix}{fileName}' sums to {back.GroupCount} keys, the state has {state.GroupCount}");
            }
        }

        async ValueTask<string> WriteAsync(IndexedZSet<StructuralRow, StructuralRow, Z64> set)
        {
            var path = RunPath(ctx, fileName);
            await RunFile.WriteIndexedAsync(ctx.FileSystem, path, set, codec, hash, cancellationToken: cancellationToken).ConfigureAwait(false);
            return path;
        }
    }

    // ---- Background compaction (increment 7) ----

    private sealed class Job(RunChain input, string outPath, Task<long> merge)
    {
        public RunChain Input { get; } = input;

        public string OutPath { get; } = outPath;

        public Task<long> Merge { get; } = merge;
    }

    // Per tracked state: the merge in flight for its chain, if any. Weak: a dropped program's jobs go.
    private static readonly System.Runtime.CompilerServices.ConditionalWeakTable<object, Job> Jobs = new();

    // Merges share the machine with the steps: at most two at a time.
    private static readonly SemaphoreSlim CompactionSlots = new(2);

    /// <summary>On unless <c>DBSPNET_COMPACTION=off</c> (then chains are only ever rewritten
    /// inline, at <see cref="RunChain.MaxRuns"/>).</summary>
    internal static bool CompactionEnabled { get; set; } =
        !string.Equals(Environment.GetEnvironmentVariable("DBSPNET_COMPACTION"), "off", StringComparison.OrdinalIgnoreCase);

    /// <summary>Every merge in flight (tests and benchmarks wait on them).</summary>
    internal static Task WhenAllCompactedAsync() =>
        Task.WhenAll(Jobs.Select(j => j.Value.Merge).ToArray());

    /// <summary>The merge in flight for <paramref name="trace"/>'s chain (tests wait on it).</summary>
    internal static Task WhenCompactedAsync(object trace) =>
        Jobs.TryGetValue(trace, out var job) ? job.Merge : Task.CompletedTask;

    // After a commit: a long enough chain gets a merge into one run, off the step thread. Its input
    // runs and output are pinned until it is adopted (or the state is dropped), and the output is
    // durable before any checkpoint can name it.
    private static void MaybeCompact(
        object trace, RunChain chain, Func<RunChain, string, CancellationToken, ValueTask<long>> compact)
    {
        if (!CompactionEnabled || chain.Paths.Count < RunChain.CompactAt || Jobs.TryGetValue(trace, out _))
        {
            return;
        }

        var outPath = chain.Paths[^1] + ".c";
        var merge = Task.Run(async () =>
        {
            await CompactionSlots.WaitAsync().ConfigureAwait(false);
            try
            {
                var entries = await compact(chain, outPath, CancellationToken.None).ConfigureAwait(false);
                if (Snapshot.DurableStore(chain.Store) is { } durable)
                {
                    await durable.SyncAsync([outPath]).ConfigureAwait(false);
                }

                return entries;
            }
            finally
            {
                CompactionSlots.Release();
            }
        });
        var job = new Job(chain, outPath, merge);
        RunPins.Add(job, chain.Store, [.. chain.Paths, outPath]);
        Jobs.AddOrUpdate(trace, job);
    }

    // At a checkpoint: a finished merge whose input is still the front of the chain replaces it.
    // A merge that failed, or whose input the chain no longer starts with (a rewrite came first), is
    // dropped; its output, named nowhere and no longer pinned, is swept.
    private static RunChain Adopt(object? trace, RunChain chain)
    {
        if (trace is null || !Jobs.TryGetValue(trace, out var job) || !job.Merge.IsCompleted)
        {
            return chain;
        }

        Jobs.Remove(trace);
        RunPins.Remove(job); // adopted: the new chain names what it needs; dropped: its output goes
        var input = job.Input.Paths;
        if (!job.Merge.IsCompletedSuccessfully
            || !ReferenceEquals(job.Input.Store, chain.Store)
            || chain.Paths.Count < input.Count
            || !chain.Paths.Take(input.Count).SequenceEqual(input, StringComparer.Ordinal))
        {
            return chain;
        }

        return new RunChain(
            chain.Store,
            [job.OutPath, .. chain.Paths.Skip(input.Count)],
            job.Merge.Result,
            Math.Max(0, chain.DeltaEntries - job.Input.DeltaEntries),
            chain.Count,
            chain.PartitionOrder);
    }

    private static long Entries(IndexedZSet<StructuralRow, StructuralRow, Z64> set)
    {
        long n = 0;
        foreach (var g in set)
        {
            n += g.Value.Count;
        }

        return n;
    }

    /// <summary>The chain <paramref name="reader"/>'s run index records for <paramref name="fileName"/>, if any.</summary>
    internal static ValueTask<RunChain?> FindChainAsync(ISnapshotReader reader, string fileName, CancellationToken cancellationToken) =>
        reader is TableFileSystemSnapshotContext ctx ? ctx.ChainAsync(fileName, cancellationToken) : ValueTask.FromResult<RunChain?>(null);

    /// <summary>Sum <paramref name="chain"/> and mark the result as restored from it.</summary>
    internal static async ValueTask<ZSet<StructuralRow, Z64>> LoadZSetAsync(
        RunChain chain, IZSetRunCodec codec, Func<StructuralRow, int> hash, CancellationToken cancellationToken)
    {
        var result = await ReadZSetAsync(chain, codec, hash, cancellationToken).ConfigureAwait(false);
        CheckpointTracking.MarkRestored(result, chain);
        return result;
    }

    internal static async ValueTask<IndexedZSet<StructuralRow, StructuralRow, Z64>> LoadIndexedAsync(
        RunChain chain, IIndexedRunCodec codec, Func<StructuralRow, int> hash, CancellationToken cancellationToken)
    {
        // Lazy (increment 4): open the runs' footers only; groups are read as keys are asked for.
        // The trace that integrates this adopts the cold source instead of reading it.
        var result = RunColdGroups.Enabled && chain.Count >= 0
            ? IndexedZSet<StructuralRow, StructuralRow, Z64>.ColdBacked(
                await RunColdGroups.OpenAsync(chain, codec, hash, cancellationToken).ConfigureAwait(false))
            : await ReadIndexedAsync(chain, codec, hash, cancellationToken).ConfigureAwait(false);
        CheckpointTracking.MarkRestored(result, chain);
        return result;
    }

    private static async ValueTask<ZSet<StructuralRow, Z64>> ReadZSetAsync(
        RunChain chain, IZSetRunCodec codec, Func<StructuralRow, int> hash, CancellationToken cancellationToken)
    {
        var into = new ZSetBuilder<StructuralRow, Z64>((int)Math.Min(int.MaxValue, chain.BaseEntries));
        foreach (var path in chain.Paths)
        {
            // Whatever the order, the footer pins the row hash: open with it as the fingerprint.
            var file = await chain.Store.OpenReadAsync(path, cancellationToken).ConfigureAwait(false);
            await using var run = await RunReader.OpenAsync(file, hash, path, RowHash, cancellationToken).ConfigureAwait(false);
            await run.ReadAllIntoAsync(codec, into, cancellationToken).ConfigureAwait(false);
        }

        return into.Build();
    }

    private static async ValueTask<IndexedZSet<StructuralRow, StructuralRow, Z64>> ReadIndexedAsync(
        RunChain chain, IIndexedRunCodec codec, Func<StructuralRow, int> hash, CancellationToken cancellationToken)
    {
        var into = new IndexedZSetBuilder<StructuralRow, StructuralRow, Z64>();
        foreach (var path in chain.Paths)
        {
            await using var run = await RunReader.OpenAsync(chain.Store, path, hash, cancellationToken).ConfigureAwait(false);
            await run.ReadAllIntoAsync(codec, into, cancellationToken).ConfigureAwait(false);
        }

        return into.Build();
    }
}
