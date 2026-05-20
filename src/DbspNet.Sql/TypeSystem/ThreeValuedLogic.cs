// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
namespace DbspNet.Sql.TypeSystem;

/// <summary>
/// SQL three-valued logic operators for the expression compiler. NULL is
/// represented as <c>null</c>, TRUE as <c>true</c>, FALSE as <c>false</c>.
/// </summary>
/// <remarks>
/// Truth tables:
/// <code>
/// A     AND B  | T N F     OR B   | T N F     NOT A | T = F, F = T, N = N
/// T            | T N F            | T T T
/// N            | N N F            | T N N
/// F            | F F F            | T N F
/// </code>
/// Arithmetic and comparison operators propagate NULL: any NULL operand
/// yields a NULL result.
/// </remarks>
public static class ThreeValuedLogic
{
    // Logical AND: short-circuits only on definite FALSE; NULL spreads.
    public static bool? And(bool? a, bool? b)
    {
        if (a == false || b == false)
        {
            return false;
        }

        if (a is null || b is null)
        {
            return null;
        }

        return a.Value && b.Value;
    }

    public static bool? Or(bool? a, bool? b)
    {
        if (a == true || b == true)
        {
            return true;
        }

        if (a is null || b is null)
        {
            return null;
        }

        return a.Value || b.Value;
    }

    public static bool? Not(bool? a) => a is null ? null : !a.Value;

    /// <summary>
    /// In expressions that ultimately feed a WHERE or HAVING clause, SQL
    /// treats NULL as FALSE: rows where the predicate is NULL are filtered
    /// out. This helper materialises that conversion at the edge of the
    /// predicate pipeline.
    /// </summary>
    public static bool NullIsFalse(bool? a) => a == true;
}
