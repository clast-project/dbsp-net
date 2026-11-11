// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;
using DbspNet.Connectors.Abstractions;

namespace DbspNet.Server;

/// <summary>
/// The broadcast-join choice for a parallel deploy (docs/design-tpcdi-batch1.md §4a). A join whose right
/// side is a small dimension is compiled to replicate that side on every worker instead of hash-sharding
/// both sides on the join key. A key with a handful of values would otherwise put the whole fact side on
/// a handful of workers (ivm-bench's trades_history: 982k trades on 5- and 6-value codes, −6.8% batch 1).
/// <para>Sizes come from the inputs' Delta-log row counts at deploy (<see cref="InputOrder.ReadRowCountsAsync"/>). An
/// unknown or zero count counts as large: an empty table has nothing to skew, and a fact table staged
/// after deploy must not be mistaken for a dimension. <c>DBSPNET_BROADCAST_MAX_ROWS</c> sets the
/// threshold (default 1,000; <c>0</c> or <c>off</c> disables).</para>
/// <para>The choice decides where a broadcast side's state lives (replicated, not sharded), so it is
/// part of the plan a snapshot holds. A persistent deploy records it in every checkpoint under
/// <see cref="MetadataKey"/>, and a restore compiles with the recorded choice, never fresh counts. A
/// snapshot without the key was written with no broadcast, and restores that way.</para>
/// </summary>
internal sealed record BroadcastJoins(
    [property: JsonPropertyName("max_rows")] long MaxRows,
    [property: JsonPropertyName("row_counts")] IReadOnlyDictionary<string, long> RowCounts)
{
    /// <summary>The checkpoint-metadata key the choice is recorded under.</summary>
    public const string MetadataKey = "compile.broadcast";

    /// <summary>The default threshold: dimensions of up to this many rows are broadcast.</summary>
    public const long DefaultMaxRows = 1000;

    /// <summary><c>DBSPNET_BROADCAST_MAX_ROWS</c>: unset is <see cref="DefaultMaxRows"/>; <c>0</c> or
    /// <c>off</c> is 0 (no broadcast).</summary>
    public static long ConfiguredMaxRows(string? setting)
    {
        var s = setting?.Trim();
        if (string.IsNullOrEmpty(s))
        {
            return DefaultMaxRows;
        }

        if (s.Equals("off", StringComparison.OrdinalIgnoreCase))
        {
            return 0;
        }

        return long.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out var n) && n >= 0
            ? n
            : throw new FormatException($"DBSPNET_BROADCAST_MAX_ROWS must be a non-negative row count or 'off', not '{setting}'");
    }

    /// <summary>The count a hinted view is recorded with: small enough for any threshold.</summary>
    public const long HintedRows = 1;

    /// <summary>
    /// <c>DBSPNET_BROADCAST_VIEWS</c>: comma-separated relations (normally views, whose size the
    /// compiler cannot know at deploy) to broadcast as small dimensions. Unset or blank is none.
    /// </summary>
    public static IReadOnlyList<string> ConfiguredViews(string? setting) =>
        string.IsNullOrWhiteSpace(setting)
            ? []
            : [.. setting.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).Distinct(StringComparer.Ordinal)];

    /// <summary>
    /// <paramref name="rowCounts"/> with each of <paramref name="views"/> counted as
    /// <see cref="HintedRows"/>: the compiler keeps a supplied view count instead of estimating it,
    /// so a hinted view is broadcast where it is a join's right side, and the hint is recorded with
    /// the choice like any count.
    /// </summary>
    public static IReadOnlyDictionary<string, long> WithHints(IReadOnlyDictionary<string, long> rowCounts, IReadOnlyList<string> views)
    {
        if (views.Count == 0)
        {
            return rowCounts;
        }

        var merged = new Dictionary<string, long>(rowCounts, StringComparer.Ordinal);
        foreach (var v in views)
        {
            merged[v] = HintedRows;
        }

        return merged;
    }

    /// <summary>
    /// The choice for this deploy: the one <paramref name="checkpoint"/>'s current parallel snapshot
    /// recorded (null if it recorded none), or, on a fresh start, one made from
    /// <paramref name="rowCounts"/> (the inputs' known, non-zero counts:
    /// <see cref="InputOrder.ReadRowCountsAsync"/>) at <paramref name="maxRows"/> (null if
    /// <paramref name="maxRows"/> is 0). Records the result in <paramref name="checkpoint"/> so every
    /// later snapshot carries it.
    /// </summary>
    public static async Task<BroadcastJoins?> ChooseAsync(
        IReadOnlyDictionary<string, long> rowCounts,
        long maxRows,
        SnapshotCheckpointStore? checkpoint,
        CancellationToken cancellationToken)
    {
        BroadcastJoins? choice;
        if (checkpoint is not null
            && await checkpoint.RecordedParallelMetadataAsync(cancellationToken).ConfigureAwait(false) is { } recorded)
        {
            choice = recorded.TryGetValue(MetadataKey, out var json) ? Parse(json) : null;
        }
        else if (maxRows > 0)
        {
            choice = new BroadcastJoins(maxRows, new Dictionary<string, long>(rowCounts, StringComparer.Ordinal));
        }
        else
        {
            choice = null;
        }

        if (checkpoint is not null)
        {
            if (choice is null)
            {
                checkpoint.ProgramMetadata.Remove(MetadataKey);
            }
            else
            {
                checkpoint.ProgramMetadata[MetadataKey] = choice.ToJson();
            }
        }

        return choice;
    }

    /// <summary>The recorded form.</summary>
    public string ToJson() => JsonSerializer.Serialize(this);

    /// <summary>Read a recorded choice.</summary>
    public static BroadcastJoins Parse(string json) =>
        JsonSerializer.Deserialize<BroadcastJoins>(json)
        ?? throw new InvalidDataException($"{MetadataKey} metadata deserialized to null");

    /// <summary>The relations (inputs, and hinted views) at or under the threshold, smallest first
    /// (for the deploy log).</summary>
    public IEnumerable<KeyValuePair<string, long>> Small() =>
        RowCounts.Where(kv => kv.Value <= MaxRows).OrderBy(kv => kv.Value).ThenBy(kv => kv.Key, StringComparer.Ordinal);
}
