// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Text.Json;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;

namespace DbspNet.Connectors.EngineeredWood;

/// <summary>
/// Reads a Delta table's current row count from its transaction-log statistics —
/// the sum of the per-file <c>numRecords</c> the writer records in each add
/// action's <c>stats</c> — with no table scan. Feeds
/// <see cref="DbspNet.Sql.Compiler.CompileOptions.RelationRowCounts"/> so the
/// compiler's broadcast-join size gate can tell a small dimension from a large
/// fact from metadata alone.
/// </summary>
/// <remarks>
/// Returns <c>null</c> when <em>any</em> active file is missing the
/// <c>numRecords</c> stat: a partial sum would undercount, and the broadcast gate
/// must never be handed a too-small number (it would replicate a large table to
/// every worker). An unknown count is treated by the gate as "large", i.e. the
/// safe hash-join default — so a table whose stats are incomplete simply is not
/// broadcast. Uses only the published engineered-wood snapshot API
/// (<see cref="EngineeredWood.DeltaLake.Snapshot.Snapshot.ActiveFiles"/> and the
/// add action's <c>Stats</c>); no changes to engineered-wood are required.
/// </remarks>
public static class DeltaRowCounts
{
    /// <summary>
    /// The current row count of the Delta table at <paramref name="tableDirectory"/>,
    /// or <c>null</c> if it cannot be determined exactly from file statistics.
    /// </summary>
    public static ValueTask<long?> TryReadAsync(string tableDirectory, CancellationToken cancellationToken = default) =>
        TryReadAsync(new LocalTableFileSystem(tableDirectory), cancellationToken);

    /// <summary>
    /// The current row count of the Delta table on <paramref name="fs"/>, or
    /// <c>null</c> if it cannot be determined exactly from file statistics.
    /// </summary>
    public static async ValueTask<long?> TryReadAsync(ITableFileSystem fs, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(fs);
        using var table = await DeltaTable.OpenAsync(fs, cancellationToken: cancellationToken).ConfigureAwait(false);

        long total = 0;
        foreach (var add in table.CurrentSnapshot.ActiveFiles.Values)
        {
            if (TryNumRecords(add.Stats) is not { } n)
            {
                return null; // an unquantifiable file ⇒ unknown table size (safe: not broadcast).
            }

            total += n;
        }

        return total;
    }

    /// <summary>
    /// Build a <c>relation name → row count</c> map for the named Delta tables,
    /// suitable for <see cref="DbspNet.Sql.Compiler.CompileOptions.RelationRowCounts"/>.
    /// Tables whose count cannot be determined are omitted (⇒ the gate treats them
    /// as large / not broadcast). Reads run concurrently.
    /// </summary>
    public static async ValueTask<IReadOnlyDictionary<string, long>> ReadAsync(
        IEnumerable<(string Name, string Directory)> tables,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(tables);
        var list = tables.ToList();
        var counts = await Task.WhenAll(list.Select(async t =>
            (t.Name, Count: await TryReadAsync(t.Directory, cancellationToken).ConfigureAwait(false))))
            .ConfigureAwait(false);

        var map = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach (var (name, count) in counts)
        {
            if (count is { } n)
            {
                map[name] = n;
            }
        }

        return map;
    }

    /// <summary>Extract <c>numRecords</c> from a Delta add action's stats JSON.</summary>
    private static long? TryNumRecords(string? statsJson)
    {
        if (string.IsNullOrEmpty(statsJson))
        {
            return null;
        }

        using var doc = JsonDocument.Parse(statsJson);
        return doc.RootElement.TryGetProperty("numRecords", out var el)
            && el.TryGetInt64(out var n)
            ? n
            : null;
    }
}
