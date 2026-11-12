// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
namespace DbspNet.Sql.Compiler;

/// <summary>
/// Whether a LEFT join whose only consumer is a projection builds the projected row straight
/// from its two input rows, instead of building the joined row and projecting it after
/// (docs/design-bounded-memory.md §5.47). On by default; <c>DBSPNET_JOIN_PROJECTION_FOLD=off</c>
/// or setting <see cref="Enabled"/> restores the two-row build.
/// </summary>
internal static class JoinProjectionFoldMode
{
    private static readonly bool EnvironmentOff = string.Equals(
        Environment.GetEnvironmentVariable("DBSPNET_JOIN_PROJECTION_FOLD")?.Trim(), "off", StringComparison.OrdinalIgnoreCase);

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
