// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
namespace DbspNet.Core.Operators.Stateful.Blocks;

/// <summary>
/// Which traces the compiler holds as reference-free blocks (docs/design-flat-trace-state.md §3).
/// <c>DBSPNET_TRACE_BLOCKS</c> is a comma list: <c>join</c> (structural inner and outer joins),
/// <c>agg</c> (incremental aggregates' input traces), <c>distinct</c> (DISTINCT's trace),
/// <c>view</c> (stored output views, and with them an incremental output connector's row index, §3.9–§3.10),
/// <c>window</c> (the window aggregate's large partitions as ordered chunks, §3.15–§3.18),
/// or <c>all</c> (all five); flat trace family and block-encodable columns only; off by default.
/// <c>DBSPNET_TRACE_BLOCKS_FREEZE</c> and <c>DBSPNET_TRACE_BLOCKS_BLOCK</c> set the freeze threshold and
/// block size (defaults 65,536 and 1,024; small values make a test suite exercise runs and merges).
/// <see cref="Use"/> overrides all three in an async context (tests). Read at compile time.
/// </summary>
internal static class TraceBlocksMode
{
    private static readonly string[] Kinds =
        (Environment.GetEnvironmentVariable("DBSPNET_TRACE_BLOCKS") ?? string.Empty).ToLowerInvariant()
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

    private static readonly bool Default = Kinds.Contains("join") || Kinds.Contains("all");

    private static readonly bool DefaultAggregate = Kinds.Contains("agg") || Kinds.Contains("all");

    private static readonly bool DefaultView = Kinds.Contains("view") || Kinds.Contains("all");

    private static readonly bool DefaultDistinct = Kinds.Contains("distinct") || Kinds.Contains("all");

    // In `all` since §3.18: small partitions managed, large ones as ordered chunks.
    private static readonly bool DefaultWindow = Kinds.Contains("window") || Kinds.Contains("all");

    private static readonly int DefaultFreeze = Positive("DBSPNET_TRACE_BLOCKS_FREEZE", 1 << 16);

    private static readonly int DefaultBlock = Positive("DBSPNET_TRACE_BLOCKS_BLOCK", 1024);

    private static readonly AsyncLocal<(bool Join, int Freeze, int Block, bool Aggregate, bool View, bool Distinct, bool Window)?> Override = new();

    /// <summary>Whether join traces are compiled as blocks.</summary>
    public static bool Join => Override.Value?.Join ?? Default;

    /// <summary>Whether aggregate input traces are compiled as blocks.</summary>
    public static bool Aggregate => Override.Value?.Aggregate ?? DefaultAggregate;

    /// <summary>Whether stored output views are compiled as blocks.</summary>
    public static bool View => Override.Value?.View ?? DefaultView;

    /// <summary>Whether DISTINCT's traces are compiled as blocks.</summary>
    public static bool Distinct => Override.Value?.Distinct ?? DefaultDistinct;

    /// <summary>Whether the window aggregate's per-partition input is compiled as blocks (§3.13).</summary>
    public static bool Window => Override.Value?.Window ?? DefaultWindow;

    /// <summary>Rows at which a blocked window partition moves to ordered chunks (§3.15);
    /// <c>DBSPNET_TRACE_BLOCKS_PROMOTE</c>, default 512.</summary>
    public static int PromoteRows => PromoteOverride.Value ?? DefaultPromote;

    /// <summary>Rows per ordered chunk (§3.15); <c>DBSPNET_TRACE_BLOCKS_CHUNK</c>, default 512.</summary>
    public static int ChunkRows => ChunkOverride.Value ?? DefaultChunk;

    private static readonly int DefaultPromote = Positive("DBSPNET_TRACE_BLOCKS_PROMOTE", 512);

    private static readonly int DefaultChunk = Positive("DBSPNET_TRACE_BLOCKS_CHUNK", 512);

    private static readonly AsyncLocal<int?> PromoteOverride = new();

    private static readonly AsyncLocal<int?> ChunkOverride = new();

    /// <summary><see cref="PromoteRows"/> and <see cref="ChunkRows"/> in the current async context
    /// until the result is disposed (tests).</summary>
    public static IDisposable UseChunks(int promoteRows, int chunkRows)
    {
        var (p, c) = (PromoteOverride.Value, ChunkOverride.Value);
        PromoteOverride.Value = promoteRows;
        ChunkOverride.Value = chunkRows;
        return new Restore(() => (PromoteOverride.Value, ChunkOverride.Value) = (p, c));
    }

    /// <summary>Managed entries that make a blocked trace freeze.</summary>
    public static int FreezeEntries => Override.Value?.Freeze ?? DefaultFreeze;

    /// <summary>Entries per block.</summary>
    public static int BlockEntries => Override.Value?.Block ?? DefaultBlock;

    /// <summary>These settings in the current async context until the result is disposed;
    /// <paramref name="aggregate"/>, <paramref name="view"/> and <paramref name="distinct"/> default to
    /// <paramref name="join"/> (as <c>all</c>), and <paramref name="window"/> too.</summary>
    public static IDisposable Use(
        bool join, int? freezeEntries = null, int? blockEntries = null, bool? aggregate = null, bool? view = null, bool? distinct = null,
        bool? window = null)
    {
        var previous = Override.Value;
        Override.Value = (join, freezeEntries ?? FreezeEntries, blockEntries ?? BlockEntries, aggregate ?? join, view ?? join, distinct ?? join, window ?? join);
        return new Restore(() => Override.Value = previous);
    }

    private static int Positive(string name, int fallback) =>
        int.TryParse(Environment.GetEnvironmentVariable(name), out var v) && v > 0 ? v : fallback;

    private sealed class Restore(Action undo) : IDisposable
    {
        public void Dispose() => undo();
    }
}
