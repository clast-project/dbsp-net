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
/// base run of its whole state. Either way the snapshot gets a run list naming the chain, and the
/// trace restarts tracking on it once the checkpoint commits. Restoring sums the chain's runs and
/// marks the result, so the trace that integrates it keeps extending the same chain.
/// </summary>
/// <remarks><c>DBSPNET_RUNS_VERIFY=1</c> reads every chain back as it is written and throws if
/// its sum differs from the state: a missed mutation shows up at the checkpoint that misses it.</remarks>
internal static class RunCheckpoint
{
    internal static bool Verify { get; set; } = Environment.GetEnvironmentVariable("DBSPNET_RUNS_VERIFY") == "1";

    /// <summary>Runs live outside any one snapshot, named after the file that first wrote them.</summary>
    private static string RunPath(TableFileSystemSnapshotContext ctx, string fileName) => "runs/" + ctx.Prefix + fileName;

    internal static async ValueTask SaveZSetAsync(
        ISnapshotWriter writer, string fileName, ZSet<StructuralRow, Z64> state,
        IZSetRunCodec codec, Func<StructuralRow, int> hash, CancellationToken cancellationToken)
    {
        if (writer is not TableFileSystemSnapshotContext { Session: { } session } ctx)
        {
            // Not part of a session-aware checkpoint: one self-contained run in place.
            await RunFile.WriteZSetAsync(writer, fileName, state, codec, hash, cancellationToken: cancellationToken).ConfigureAwait(false);
            return;
        }

        var trace = CheckpointTracking.TraceOf(state) as IZSetCheckpointTracked<StructuralRow, Z64>;
        RunChain chain;
        if (trace?.Tracking is { Chain: RunChain prev, Pending: { } pending } && prev.CanExtend(ctx.FileSystem, pending.Count))
        {
            chain = pending.IsEmpty ? prev : prev.Extend(await WriteAsync(pending), pending.Count);
        }
        else
        {
            chain = RunChain.Base(ctx.FileSystem, await WriteAsync(state), state.Count);
        }

        await chain.WriteListAsync(ctx, fileName, cancellationToken).ConfigureAwait(false);
        session.Reference(ctx.Prefix, chain.Paths);
        if (trace is not null)
        {
            session.OnCommit(() => trace.StartTracking(chain));
        }

        if (Verify)
        {
            var back = await ReadZSetAsync(chain, codec, hash, cancellationToken).ConfigureAwait(false);
            if (back.Count != state.Count || state.Any(e => back.WeightOf(e.Key) != e.Value))
            {
                throw new InvalidOperationException(
                    $"run chain for '{ctx.Prefix}{fileName}' sums to {back.Count} rows, the state has {state.Count}");
            }
        }

        async ValueTask<string> WriteAsync(ZSet<StructuralRow, Z64> set)
        {
            var path = RunPath(ctx, fileName);
            await RunFile.WriteZSetAsync(ctx.FileSystem, path, set, codec, hash, cancellationToken: cancellationToken).ConfigureAwait(false);
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

        await chain.WriteListAsync(ctx, fileName, cancellationToken).ConfigureAwait(false);
        session.Reference(ctx.Prefix, chain.Paths);
        if (trace is not null)
        {
            session.OnCommit(() => trace.StartTracking(chain));
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

    private static long Entries(IndexedZSet<StructuralRow, StructuralRow, Z64> set)
    {
        long n = 0;
        foreach (var g in set)
        {
            n += g.Value.Count;
        }

        return n;
    }

    /// <summary>A run list opened from <paramref name="reader"/>: sum its chain and mark the result
    /// as restored from it.</summary>
    internal static async ValueTask<ZSet<StructuralRow, Z64>> LoadZSetAsync(
        ISnapshotReader reader, IRandomAccessFile list, string fileName,
        IZSetRunCodec codec, Func<StructuralRow, int> hash, CancellationToken cancellationToken)
    {
        var chain = await OpenChainAsync(reader, list, fileName, cancellationToken).ConfigureAwait(false);
        var result = await ReadZSetAsync(chain, codec, hash, cancellationToken).ConfigureAwait(false);
        CheckpointTracking.MarkRestored(result, chain);
        return result;
    }

    internal static async ValueTask<IndexedZSet<StructuralRow, StructuralRow, Z64>> LoadIndexedAsync(
        ISnapshotReader reader, IRandomAccessFile list, string fileName,
        IIndexedRunCodec codec, Func<StructuralRow, int> hash, CancellationToken cancellationToken)
    {
        var chain = await OpenChainAsync(reader, list, fileName, cancellationToken).ConfigureAwait(false);
        var result = await ReadIndexedAsync(chain, codec, hash, cancellationToken).ConfigureAwait(false);
        CheckpointTracking.MarkRestored(result, chain);
        return result;
    }

    private static async ValueTask<RunChain> OpenChainAsync(
        ISnapshotReader reader, IRandomAccessFile list, string fileName, CancellationToken cancellationToken)
    {
        await using (list)
        {
            if (reader is not TableFileSystemSnapshotContext ctx)
            {
                throw new InvalidDataException($"'{fileName}' is a run list, which only a file-system snapshot reader can resolve");
            }

            return await RunChain.ReadListAsync(list, ctx.FileSystem, ctx.Prefix + fileName, cancellationToken).ConfigureAwait(false);
        }
    }

    private static async ValueTask<ZSet<StructuralRow, Z64>> ReadZSetAsync(
        RunChain chain, IZSetRunCodec codec, Func<StructuralRow, int> hash, CancellationToken cancellationToken)
    {
        var into = new ZSetBuilder<StructuralRow, Z64>((int)Math.Min(int.MaxValue, chain.BaseEntries));
        foreach (var path in chain.Paths)
        {
            await using var run = await RunReader.OpenAsync(chain.Store, path, hash, cancellationToken).ConfigureAwait(false);
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
