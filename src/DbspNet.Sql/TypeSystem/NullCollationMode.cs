// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
namespace DbspNet.Sql.TypeSystem;

/// <summary>
/// How NULL sorts under <c>ORDER BY</c> / windowed <c>RANK</c> when the query does
/// <b>not</b> spell out <c>NULLS FIRST</c>/<c>NULLS LAST</c>. Mirrors Apache Calcite's
/// <c>NullCollation</c> (the enum Feldera configures), so DbspNet can match a target
/// engine's default null placement.
/// </summary>
public enum NullCollation
{
    /// <summary>
    /// NULL is the largest value: sorts <b>last under ASC, first under DESC</b>.
    /// PostgreSQL's / DuckDB's default — DbspNet's default too.
    /// </summary>
    High,

    /// <summary>
    /// NULL is the smallest value: sorts <b>first under ASC, last under DESC</b>.
    /// This is <c>NullCollation.LOW</c> — the default Feldera (Calcite) uses.
    /// </summary>
    Low,

    /// <summary>NULL always sorts first, regardless of ASC/DESC.</summary>
    First,

    /// <summary>NULL always sorts last, regardless of ASC/DESC.</summary>
    Last,
}

/// <summary>
/// Selects the <see cref="NullCollation"/> applied while resolving a program — the
/// default NULL placement for an <c>ORDER BY</c> key with no explicit <c>NULLS</c>
/// clause.
/// </summary>
/// <remarks>
/// <para><b>Defaults to <see cref="NullCollation.High"/></b> (PostgreSQL-faithful, the
/// long-standing DbspNet behaviour). Engines split on this: PostgreSQL / DuckDB sort
/// nulls high, while Calcite (Feldera), SQL Server, and MySQL sort them low — so a bare
/// <c>ORDER BY x DESC</c> puts NULL at opposite ends. The ivm-bench models are written
/// against Feldera, whose ranking windows (<c>DENSE_RANK() OVER (ORDER BY total_commission
/// DESC)</c>) place an all-NULL group last; under the High default DbspNet placed it first,
/// shifting every rank below it.</para>
/// <para>Set it (before resolving) to match the target engine; read at resolve time via
/// <see cref="DefaultNullsFirst"/>. <see cref="System.ThreadStaticAttribute"/> so concurrent
/// resolves cannot observe each other's value.</para>
/// </remarks>
internal static class NullCollationMode
{
    [ThreadStatic]
    private static NullCollation _collation;

    /// <summary>
    /// The active null collation. Default <see cref="NullCollation.High"/>
    /// (the enum's zero value, so an unset thread is PostgreSQL-faithful). Thread-static.
    /// </summary>
    internal static NullCollation Collation
    {
        get => _collation;
        set => _collation = value;
    }

    /// <summary>
    /// The default <c>NULLS FIRST?</c> flag for an <c>ORDER BY</c> key with the given
    /// direction and no explicit <c>NULLS</c> clause, under the active collation.
    /// </summary>
    internal static bool DefaultNullsFirst(bool descending) => _collation switch
    {
        // Nulls largest: last under ASC (!descending → false), first under DESC.
        NullCollation.High => descending,
        // Nulls smallest: first under ASC, last under DESC.
        NullCollation.Low => !descending,
        NullCollation.First => true,
        NullCollation.Last => false,
        _ => descending,
    };
}
