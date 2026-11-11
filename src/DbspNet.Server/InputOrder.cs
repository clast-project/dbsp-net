// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Connectors.EngineeredWood;

namespace DbspNet.Server;

/// <summary>
/// The order a deploy reads its inputs in (docs/design-tpcdi-batch1.md §4c). Each source's new
/// version is one engine tick, taken in input order, so the order decides what every tick joins
/// against. Smallest first means the dimensions are loaded before the facts that join to them. In
/// the other order, a left join pads every fact row while its dimension is empty and retracts it
/// all a few ticks later; an inner join builds a fact-sized state and then re-probes all of it; and
/// everything downstream (windows, aggregates) runs twice. ivm-bench's TPC-DI spec lists its
/// five-row <c>trade_type</c> next to last, after 2.3M fact rows; smallest first cuts batch 1's
/// drain by ~8% and its allocation by ~14%, with the same views.
/// <para>Sizes are the inputs' Delta-log row counts at deploy (<see cref="DeltaRowCounts"/>).
/// Inputs with a known count come first, smallest first; the rest (unreadable, or empty at deploy
/// and so likely staged later) follow in spec order. The order is fixed per deploy. It changes no
/// result and no state layout, so nothing is recorded with a checkpoint: a restore may order its
/// inputs differently, and the tick journal replays by source name. <c>DBSPNET_INPUT_ORDER</c>:
/// <c>size</c> (default) or <c>spec</c>, which keeps the spec's order.</para>
/// </summary>
internal static class InputOrder
{
    /// <summary><c>DBSPNET_INPUT_ORDER</c>: true (order by size) when unset or <c>size</c>; false
    /// for <c>spec</c>.</summary>
    public static bool BySize(string? setting)
    {
        var s = setting?.Trim();
        if (string.IsNullOrEmpty(s) || s.Equals("size", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        return s.Equals("spec", StringComparison.OrdinalIgnoreCase)
            ? false
            : throw new FormatException($"DBSPNET_INPUT_ORDER must be 'size' or 'spec', not '{setting}'");
    }

    /// <summary>
    /// <paramref name="inputs"/> with every input of known size first, smallest first, then the rest;
    /// ties and unknowns keep their order.
    /// </summary>
    public static List<T> Order<T>(IEnumerable<T> inputs, Func<T, string> table, IReadOnlyDictionary<string, long> rowCounts) =>
        inputs
            .Select(i => (Input: i, Known: rowCounts.TryGetValue(table(i), out var n), Rows: n))
            .OrderBy(e => e.Known ? 0 : 1)
            .ThenBy(e => e.Rows)
            .Select(e => e.Input)
            .ToList(); // OrderBy is stable

    /// <summary>
    /// The inputs' current row counts, where known and above zero. An empty table, or one that cannot
    /// be read yet, is left out: its size is unknown, which every caller treats as large.
    /// </summary>
    public static async Task<IReadOnlyDictionary<string, long>> ReadRowCountsAsync(
        IEnumerable<(string Table, string Uri)> inputs, CancellationToken cancellationToken)
    {
        var counts = await Task.WhenAll(inputs.Select(async i =>
            (i.Table, Rows: await TryCountAsync(i.Uri, cancellationToken).ConfigureAwait(false))))
            .ConfigureAwait(false);
        var map = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach (var (table, rows) in counts)
        {
            if (rows is { } n and > 0)
            {
                map[table] = n;
            }
        }

        return map;
    }

    private static async Task<long?> TryCountAsync(string uri, CancellationToken cancellationToken)
    {
        try
        {
            return await DeltaRowCounts.TryReadAsync(uri, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            // Unreadable here (missing, not yet a table): unknown.
            return null;
        }
    }
}
