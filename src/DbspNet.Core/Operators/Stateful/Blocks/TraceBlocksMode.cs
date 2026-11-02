// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
namespace DbspNet.Core.Operators.Stateful.Blocks;

/// <summary>
/// Which traces the compiler holds as reference-free blocks (docs/design-flat-trace-state.md §3).
/// <c>DBSPNET_TRACE_BLOCKS</c> is a comma list: <c>join</c> (structural inner and outer joins),
/// <c>agg</c> (incremental aggregates' input traces), or <c>all</c>; flat trace family and
/// block-encodable columns only; off by default.
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

    private static readonly int DefaultFreeze = Positive("DBSPNET_TRACE_BLOCKS_FREEZE", 1 << 16);

    private static readonly int DefaultBlock = Positive("DBSPNET_TRACE_BLOCKS_BLOCK", 1024);

    private static readonly AsyncLocal<(bool Join, int Freeze, int Block, bool Aggregate)?> Override = new();

    /// <summary>Whether join traces are compiled as blocks.</summary>
    public static bool Join => Override.Value?.Join ?? Default;

    /// <summary>Whether aggregate input traces are compiled as blocks.</summary>
    public static bool Aggregate => Override.Value?.Aggregate ?? DefaultAggregate;

    /// <summary>Managed entries that make a blocked trace freeze.</summary>
    public static int FreezeEntries => Override.Value?.Freeze ?? DefaultFreeze;

    /// <summary>Entries per block.</summary>
    public static int BlockEntries => Override.Value?.Block ?? DefaultBlock;

    /// <summary>These settings in the current async context until the result is disposed;
    /// <paramref name="aggregate"/> defaults to <paramref name="join"/>.</summary>
    public static IDisposable Use(bool join, int? freezeEntries = null, int? blockEntries = null, bool? aggregate = null)
    {
        var previous = Override.Value;
        Override.Value = (join, freezeEntries ?? FreezeEntries, blockEntries ?? BlockEntries, aggregate ?? join);
        return new Restore(() => Override.Value = previous);
    }

    private static int Positive(string name, int fallback) =>
        int.TryParse(Environment.GetEnvironmentVariable(name), out var v) && v > 0 ? v : fallback;

    private sealed class Restore(Action undo) : IDisposable
    {
        public void Dispose() => undo();
    }
}
