// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
namespace DbspNet.Sql.Compiler;

/// <summary>
/// Whether the structural compiler compiles a CTE referenced exactly once, and (in a
/// program) a non-output view referenced exactly once, <b>inline</b> at its reference
/// instead of as a separately cached stream. Inline, the body's trailing Filter/Project
/// chain fuses with its consumer's (docs/design-state-representation.md §9.9 lever 3).
/// </summary>
/// <remarks>
/// Default on. <c>DBSPNET_INLINE_SINGLE_USE=off</c> or the thread-static
/// <see cref="Enabled"/> keeps every CTE/view as its own stream. Read at compile time.
/// </remarks>
internal static class InlineSingleUseMode
{
    private static readonly bool EnvironmentOff = string.Equals(
        Environment.GetEnvironmentVariable("DBSPNET_INLINE_SINGLE_USE")?.Trim(), "off", StringComparison.OrdinalIgnoreCase);

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
