// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
namespace DbspNet.Sql.Compiler;

/// <summary>
/// Plain-exchange fusion (docs/design-bounded-memory.md §5.56): at W&gt;1 the Filter/Project chain in
/// front of a DISTINCT's or a partitioned TOP-K's plain (unindexed) exchange runs in the exchange's
/// sender, as §5.55 does for indexed exchanges. On by default; <c>DBSPNET_PLAIN_EXCHANGE_FUSION=off</c>,
/// or the thread-static <see cref="Enabled"/> setter (tests). Needs <see cref="ExchangeFusionMode"/>
/// (and so <see cref="IndexFusionMode"/>) on too.
/// </summary>
internal static class PlainExchangeFusionMode
{
    private static readonly bool EnvironmentOff = string.Equals(
        Environment.GetEnvironmentVariable("DBSPNET_PLAIN_EXCHANGE_FUSION")?.Trim(), "off", StringComparison.OrdinalIgnoreCase);

    // Null = the default ([ThreadStatic] fields start at their zero value on every thread).
    [ThreadStatic]
    private static bool? _enabled;

    internal static bool Enabled
    {
        get => _enabled ?? !EnvironmentOff;
        set => _enabled = value;
    }

    /// <summary>Clear the override (tests).</summary>
    internal static void Reset() => _enabled = null;
}
