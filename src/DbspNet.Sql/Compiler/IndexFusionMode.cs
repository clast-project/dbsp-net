// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
namespace DbspNet.Sql.Compiler;

/// <summary>
/// Whether the structural compiler fuses a join input's pointwise work (its Filter/Project
/// chain and the NULL-key check) into the pass that indexes it by the join key, instead of
/// emitting separate operators that each build a throwaway Z-set
/// (docs/design-state-representation.md §9.9). Exact for arbitrary signed Z-sets; W≤1 only.
/// </summary>
/// <remarks>
/// Default on. <c>DBSPNET_INDEX_FUSION=off</c> or the thread-static <see cref="Enabled"/>
/// selects the unfused layout for A/B runs and differential tests. Read at compile time.
/// </remarks>
internal static class IndexFusionMode
{
    private static readonly bool EnvironmentOff = string.Equals(
        Environment.GetEnvironmentVariable("DBSPNET_INDEX_FUSION")?.Trim(), "off", StringComparison.OrdinalIgnoreCase);

    // Null = the default. A [ThreadStatic] field's zero value is what every thread sees,
    // so a default of "on" has to live behind a nullable.
    [ThreadStatic]
    private static bool? _enabled;

    internal static bool Enabled
    {
        get => _enabled ?? !EnvironmentOff;
        set => _enabled = value;
    }
}
