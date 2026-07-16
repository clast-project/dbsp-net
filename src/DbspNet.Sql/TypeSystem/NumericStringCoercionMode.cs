// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
namespace DbspNet.Sql.TypeSystem;

/// <summary>
/// Opt-in for <b>implicit numeric ↔ string comparison coercion</b> — when a numeric
/// operand is compared to a string one (<c>BIGINT = VARCHAR</c>), coerce the string to
/// the numeric type (parse it) rather than rejecting the pair.
/// </summary>
/// <remarks>
/// <para><b>Off by default</b>, because the engines split on this and DbspNet's default
/// tracks PostgreSQL, which <i>rejects</i> <c>numeric = string</c> for a column pair
/// (only string literals coerce). The permissive side is nonetheless the majority: SQL
/// Server (data-type precedence), Oracle (char→number), MySQL, DuckDB (`'01' = 1` → true),
/// Spark, and Calcite (Feldera) all coerce the string to the numeric type. All three
/// ivm-bench engines (Spark, DuckDB, Feldera) are on that side, so their shared models —
/// e.g. <c>dim_account</c>'s <c>USING (broker_id)</c> joining a BIGINT broker_id against a
/// VARCHAR one — are written assuming coercion.</para>
/// <para>Enable it (before resolving) to run those models. The coercion casts the string
/// to the numeric peer type; a non-numeric string then fails at runtime on the parse — the
/// correct outcome (a non-numeric string cannot equal a number), and unlike MySQL's silent
/// "non-numeric → 0" it does not fabricate matches. Read at resolve time in
/// <see cref="TypeInference.CommonComparableType"/>; <see cref="ThreadStaticAttribute"/> so
/// concurrent resolves cannot observe each other's value.</para>
/// </remarks>
internal static class NumericStringCoercionMode
{
    [ThreadStatic]
    private static bool _enabled;

    /// <summary>
    /// When true, a numeric-vs-string comparison coerces the string operand to the numeric
    /// type instead of throwing. Default false (PostgreSQL-faithful). Thread-static.
    /// </summary>
    internal static bool Enabled
    {
        get => _enabled;
        set => _enabled = value;
    }
}
