// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.IO;

namespace DbspNet.Persistence;

/// <summary>
/// Where a checkpoint's cleanup — pruning old snapshot directories and sweeping unreferenced runs —
/// runs (docs/design-durable-state.md §1f). By default it is <b>deferred</b>: it starts in the
/// background once the pointer has committed, so the checkpoint returns as soon as it is durable,
/// and every later snapshot operation on the same store (write, read, re-shard, listing) waits for
/// it first. Cleanup only ever deletes what no retained snapshot needs, so running it late changes
/// nothing but when the space comes back; a failure in it is ignored, as an inline prune's was, and
/// the next cleanup retries. <c>DBSPNET_CHECKPOINT_CLEANUP=inline</c> (or <see cref="Inline"/>)
/// runs it before the checkpoint returns, as before.
/// </summary>
public static class CheckpointCleanup
{
    private static readonly bool FromEnvironment =
        !string.Equals(Environment.GetEnvironmentVariable("DBSPNET_CHECKPOINT_CLEANUP"), "inline", StringComparison.OrdinalIgnoreCase);

    private static readonly AsyncLocal<bool?> Override = new();

    // Keyed by the store's identity — a local store's root, else the instance — so a second store
    // object over the same directory still waits.
    private static readonly Dictionary<object, Task> Pending = new();

    private static object Key(ITableFileSystem fs) => fs is IO.Local.LocalTableFileSystem local ? "local:" + local.RootPath : fs;

    /// <summary>Whether cleanup runs after the checkpoint returns.</summary>
    public static bool Deferred => Override.Value ?? FromEnvironment;

    /// <summary>Run cleanup inline in the current async context until the result is disposed.</summary>
    public static IDisposable Inline()
    {
        var previous = Override.Value;
        Override.Value = false;
        return new Scope(previous);
    }

    /// <summary>Wait for any cleanup still running against <paramref name="fs"/>.</summary>
    public static async ValueTask WaitAsync(ITableFileSystem fs)
    {
        ArgumentNullException.ThrowIfNull(fs);
        Task? task;
        lock (Pending)
        {
            if (!Pending.Remove(Key(fs), out task))
            {
                return;
            }
        }

        try
        {
            await task.ConfigureAwait(false);
        }
#pragma warning disable CA1031 // best-effort, like the inline prune: the next cleanup retries
        catch (Exception)
#pragma warning restore CA1031
        {
        }
    }

    /// <summary>Run <paramref name="cleanup"/> now, or start it in the background (see the class remarks).</summary>
    internal static async ValueTask RunAsync(ITableFileSystem fs, Func<CancellationToken, ValueTask> cleanup, CancellationToken cancellationToken)
    {
        if (!Deferred)
        {
            await cleanup(cancellationToken).ConfigureAwait(false);
            return;
        }

        var key = Key(fs);
        var task = Task.Run(async () => await cleanup(CancellationToken.None).ConfigureAwait(false), CancellationToken.None);
        lock (Pending)
        {
            Pending[key] = task;
        }

        // Drop the entry once it finishes, unless a later cleanup replaced it.
        _ = task.ContinueWith(
            t =>
            {
                lock (Pending)
                {
                    if (Pending.TryGetValue(key, out var current) && current == t)
                    {
                        Pending.Remove(key);
                    }
                }
            },
            CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
    }

    private sealed class Scope(bool? previous) : IDisposable
    {
        public void Dispose() => Override.Value = previous;
    }
}
