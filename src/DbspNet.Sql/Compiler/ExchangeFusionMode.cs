// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
namespace DbspNet.Sql.Compiler;

/// <summary>
/// Exchange fusion (docs/design-bounded-memory.md §5.55): at W&gt;1 an input about to be exchanged and
/// indexed (an aggregate, an inner join's sides, a LEFT join's two sides) runs its own Filter/Project
/// chain and NULL-key check inside the exchange's sender pass, where each row is decoded anyway, instead
/// of as operators that each decode, rebuild and encode the delta; a hashed LEFT join's preserved side
/// is exchanged and indexed in one operator, NULL keys indexed and padded by the join (§5.46's form).
/// On by default; <c>DBSPNET_EXCHANGE_FUSION=off</c>, or the thread-static <see cref="Enabled"/>
/// setter (tests). Needs <see cref="IndexFusionMode"/> on too.
/// </summary>
internal static class ExchangeFusionMode
{
    private static readonly bool EnvironmentOff = string.Equals(
        Environment.GetEnvironmentVariable("DBSPNET_EXCHANGE_FUSION")?.Trim(), "off", StringComparison.OrdinalIgnoreCase);

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
