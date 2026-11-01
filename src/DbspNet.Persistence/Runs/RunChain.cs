// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Globalization;
using System.Text;
using DbspNet.Core.Circuit;
using DbspNet.Core.IO;

namespace DbspNet.Persistence.Runs;

/// <summary>
/// The runs whose sum is one trace's state (docs/design-durable-state.md §1f, increment 3): a base
/// run, then one delta run per later checkpoint. A snapshot stores the chain as a small <b>run
/// list</b> file where the trace file would be; the runs themselves live under <c>runs/</c>,
/// named after the snapshot directory that first wrote them, and are shared by every later
/// snapshot whose chain extends it. <see cref="RunSweep"/> deletes runs no retained snapshot lists.
/// </summary>
internal sealed class RunChain
{
    /// <summary>A chain this long is rewritten whole (compaction by rewrite) instead of extended.</summary>
    internal const int MaxRuns = 8;

    private static ReadOnlySpan<byte> Magic => "DBSPRLS1"u8;

    private RunChain(ITableFileSystem store, IReadOnlyList<string> paths, long baseEntries, long deltaEntries)
    {
        Store = store;
        Paths = paths;
        BaseEntries = baseEntries;
        DeltaEntries = deltaEntries;
    }

    /// <summary>The store the paths are in: a chain never extends across stores.</summary>
    public ITableFileSystem Store { get; }

    /// <summary>Store-absolute run paths, base first.</summary>
    public IReadOnlyList<string> Paths { get; }

    public long BaseEntries { get; }

    /// <summary>Entries across the delta runs (a bound on how far the chain has drifted from its base).</summary>
    public long DeltaEntries { get; }

    internal static RunChain Base(ITableFileSystem store, string path, long entries) => new(store, [path], entries, 0);

    /// <summary>Whether a delta of <paramref name="pendingEntries"/> may extend this chain in
    /// <paramref name="store"/>, or the trace should be rewritten whole.</summary>
    internal bool CanExtend(ITableFileSystem store, long pendingEntries) =>
        ReferenceEquals(store, Store)
        && Paths.Count < MaxRuns
        && DeltaEntries + pendingEntries <= Math.Max(4096, BaseEntries / 2);

    internal RunChain Extend(string path, long entries) => new(Store, [.. Paths, path], BaseEntries, DeltaEntries + entries);

    internal async ValueTask WriteListAsync(ISnapshotWriter writer, string fileName, CancellationToken cancellationToken)
    {
        var text = new StringBuilder();
        text.Append(CultureInfo.InvariantCulture, $"{BaseEntries}\n{DeltaEntries}\n");
        foreach (var p in Paths)
        {
            text.Append(p).Append('\n');
        }

        var body = Encoding.UTF8.GetBytes(text.ToString());
        var bytes = new byte[Magic.Length + body.Length];
        Magic.CopyTo(bytes);
        body.CopyTo(bytes, Magic.Length);
        await using var file = await writer.CreateAsync(fileName, cancellationToken).ConfigureAwait(false);
        await file.WriteAsync(bytes, cancellationToken).ConfigureAwait(false);
        await file.FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>True if <paramref name="file"/> starts like a run list.</summary>
    internal static async ValueTask<bool> IsListAsync(IRandomAccessFile file, CancellationToken cancellationToken)
    {
        var length = await file.GetLengthAsync(cancellationToken).ConfigureAwait(false);
        if (length < Magic.Length)
        {
            return false;
        }

        using var head = await file.ReadAsync(new FileRange(0, Magic.Length), cancellationToken).ConfigureAwait(false);
        return head.Memory.Span[..Magic.Length].SequenceEqual(Magic);
    }

    /// <summary>Parse a run list; <paramref name="store"/> is where its paths resolve.</summary>
    internal static async ValueTask<RunChain> ReadListAsync(
        IRandomAccessFile file, ITableFileSystem store, string path, CancellationToken cancellationToken)
    {
        var length = await file.GetLengthAsync(cancellationToken).ConfigureAwait(false);
        using var bytes = await file.ReadAsync(new FileRange(0, length), cancellationToken).ConfigureAwait(false);
        var span = bytes.Memory.Span[..(int)length];
        if (!span[..Magic.Length].SequenceEqual(Magic))
        {
            throw new InvalidDataException($"'{path}' is not a run list");
        }

        var lines = Encoding.UTF8.GetString(span[Magic.Length..]).Split('\n', StringSplitOptions.RemoveEmptyEntries);
        if (lines.Length < 3
            || !long.TryParse(lines[0], NumberStyles.None, CultureInfo.InvariantCulture, out var baseEntries)
            || !long.TryParse(lines[1], NumberStyles.None, CultureInfo.InvariantCulture, out var deltaEntries))
        {
            throw new InvalidDataException($"run list '{path}' is malformed");
        }

        return new RunChain(store, lines[2..], baseEntries, deltaEntries);
    }
}
