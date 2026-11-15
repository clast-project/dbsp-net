// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
namespace DbspNet.Sql.TypeSystem;

/// <summary>
/// The numeric behaviours SQL leaves implementation-defined, as a unit: pick the target
/// engine whose results a program must reproduce.
/// </summary>
/// <remarks>
/// <para>What a dialect decides:</para>
/// <list type="bullet">
/// <item><description><b><c>CAST(numeric AS TIMESTAMP)</c>:</b> the unit of the number.
/// <see cref="Default"/> reads microseconds since the epoch (<c>Timestamp</c>'s native
/// unit). <see cref="Feldera"/> reads milliseconds. Integers convert exactly, and
/// floating-point values round half-even to the microsecond, as Feldera's sqllib does.</description></item>
/// <item><description><b>DECIMAL division:</b> how the quotient is cut to the result
/// scale. <see cref="Default"/> rounds half-even. <see cref="Feldera"/> rounds toward
/// zero, so a later <c>ROUND(…, n)</c> sees the truncated digits rather than an
/// already-rounded quotient. With half-even, <c>ROUND(0.05844998…, 4)</c> computed through
/// a scale-6 quotient gives 0.0585. Feldera, and the exact value, give 0.0584.</description></item>
/// </list>
/// <para>Found by the TPC-DI re-validation against Feldera (issue #6). The ivm-bench models
/// are written against Feldera, so <c>DbspNetEngine</c> compiles with <see cref="Feldera"/>.
/// It sits beside <see cref="NullCollation"/> and numeric-string coercion, the other two
/// Feldera-compatibility switches.</para>
/// </remarks>
public enum NumericDialect
{
    /// <summary>DbspNet's long-standing behaviour: microsecond epoch casts, half-even decimal division.</summary>
    Default,

    /// <summary>Feldera's (Calcite/sqllib) behaviour: millisecond epoch casts, toward-zero decimal division.</summary>
    Feldera,
}

/// <summary>
/// The <see cref="NumericDialect"/> in force while a program is resolved. The resolver
/// stamps it onto the nodes it affects (<c>ResolvedCast.NumericEpochMillis</c>,
/// <c>ResolvedBinary.DecimalDivisionTowardZero</c>), so both expression compilers read it
/// from the plan and never from this thread-static.
/// </summary>
internal static class NumericDialectMode
{
    [ThreadStatic]
    private static NumericDialect _dialect;

    /// <summary>The active dialect; <see cref="NumericDialect.Default"/> on an unset thread.</summary>
    internal static NumericDialect Dialect
    {
        get => _dialect;
        set => _dialect = value;
    }

    internal static bool IsFeldera => _dialect == NumericDialect.Feldera;
}
