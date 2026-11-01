// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Globalization;
using System.Text;
using DbspNet.Core.IO;

namespace DbspNet.Persistence.Runs;

/// <summary>
/// The runs whose sum is one trace's state (docs/design-durable-state.md §1f, increment 3): a base
/// run, then one delta run per later checkpoint. Runs live under <c>runs/</c>, named after the
/// snapshot directory that first wrote them, and are shared by every later snapshot whose chain
/// extends it. A snapshot directory records its chains in one <see cref="RunIndex"/> file rather than
/// a file per trace; <see cref="RunSweep"/> deletes runs no retained snapshot's index names.
/// </summary>
internal sealed class RunChain
{
    /// <summary>A chain this long is rewritten whole (compaction by rewrite) instead of extended.</summary>
    internal const int MaxRuns = 8;

    internal RunChain(ITableFileSystem store, IReadOnlyList<string> paths, long baseEntries, long deltaEntries, long count = -1)
    {
        Store = store;
        Paths = paths;
        BaseEntries = baseEntries;
        DeltaEntries = deltaEntries;
        Count = count;
    }

    /// <summary>The state's exact size when the chain was written — its group count (indexed) or
    /// row count — so a lazily restored collection knows its size without reading (increment 4).
    /// -1 until <see cref="WithCount"/>.</summary>
    public long Count { get; }

    internal RunChain WithCount(long count) => new(Store, Paths, BaseEntries, DeltaEntries, count);

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
}

/// <summary>
/// One snapshot directory's chains (<c>{dir}/runs.txt</c>): a line per trace file,
/// <c>op-{i}/{file}\t{base entries}\t{delta entries}\t{count}\t{run}\t{run}…</c>. It replaces the trace's own
/// file, so a checkpoint creates, syncs and later deletes one file per directory instead of one per
/// trace. Written for every directory a session-aware checkpoint writes, even with no chains, so
/// its absence means a directory whose trace files are self-contained.
/// </summary>
internal sealed class RunIndex
{
    internal const string FileName = "runs.txt";

    private readonly Dictionary<string, (long Base, long Delta, long Count, string[] Paths)> _entries;

    private RunIndex(Dictionary<string, (long, long, long, string[])> entries) => _entries = entries;

    /// <summary>Every run any chain in this index names.</summary>
    internal IEnumerable<string> AllPaths => _entries.Values.SelectMany(e => e.Paths);

    /// <summary>The chain recorded for <paramref name="key"/> (<c>op-{i}/{file}</c>), resolved in <paramref name="store"/>.</summary>
    internal RunChain? Find(string key, ITableFileSystem store) =>
        _entries.TryGetValue(key, out var e) ? new RunChain(store, e.Paths, e.Base, e.Delta, e.Count) : null;

    internal static byte[] Encode(IEnumerable<(string Key, RunChain Chain)> chains)
    {
        var text = new StringBuilder();
        foreach (var (key, chain) in chains.OrderBy(c => c.Key, StringComparer.Ordinal))
        {
            text.Append(key).Append(CultureInfo.InvariantCulture, $"\t{chain.BaseEntries}\t{chain.DeltaEntries}\t{chain.Count}");
            foreach (var p in chain.Paths)
            {
                text.Append('\t').Append(p);
            }

            text.Append('\n');
        }

        return Encoding.UTF8.GetBytes(text.ToString());
    }

    /// <summary>The index of snapshot directory <paramref name="dir"/>, or null if it has none.</summary>
    internal static async ValueTask<RunIndex?> ReadAsync(ITableFileSystem fs, string dir, CancellationToken cancellationToken)
    {
        var path = dir.TrimEnd('/') + "/" + FileName;
        if (!await fs.ExistsAsync(path, cancellationToken).ConfigureAwait(false))
        {
            return null;
        }

        var bytes = await fs.ReadAllBytesAsync(path, cancellationToken).ConfigureAwait(false);
        var entries = new Dictionary<string, (long, long, long, string[])>(StringComparer.Ordinal);
        foreach (var line in Encoding.UTF8.GetString(bytes).Split('\n', StringSplitOptions.RemoveEmptyEntries))
        {
            var f = line.Split('\t');
            if (f.Length < 5
                || !long.TryParse(f[1], NumberStyles.None, CultureInfo.InvariantCulture, out var baseEntries)
                || !long.TryParse(f[2], NumberStyles.None, CultureInfo.InvariantCulture, out var deltaEntries)
                || !long.TryParse(f[3], NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out var count))
            {
                throw new InvalidDataException($"run index '{path}' has a malformed line");
            }

            entries[f[0]] = (baseEntries, deltaEntries, count, f[4..]);
        }

        return new RunIndex(entries);
    }
}
