// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections.Generic;

namespace DbspNet.Sql.Parser.Ast;

public abstract record Expression : SqlNode;

public enum LiteralKind
{
    Integer,
    Decimal,
    Float,
    String,
    Boolean,
    Null,
}

public sealed record LiteralExpression(LiteralKind Kind, object? Value) : Expression
{
    /// <summary>
    /// Source-text scale for <see cref="LiteralKind.Decimal"/> literals only.
    /// "1.5" → 1; "1.50" → 2; integer-overflow fallback → 0. Ignored for
    /// non-decimal kinds. Lets the resolver type a decimal literal as
    /// <c>DECIMAL(38, scale)</c> without re-parsing the source.
    /// </summary>
    public byte DecimalScale { get; init; }
}

public sealed record ColumnReference(string? Qualifier, string Name) : Expression;

public enum BinaryOperator
{
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    Equal,
    NotEqual,
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
    And,
    Or,
}

public sealed record BinaryExpression(
    BinaryOperator Operator,
    Expression Left,
    Expression Right) : Expression;

public enum UnaryOperator
{
    Negate,
    Not,
}

public sealed record UnaryExpression(UnaryOperator Operator, Expression Operand) : Expression;

public sealed record IsNullExpression(Expression Operand, bool Negated) : Expression;

public sealed record CastExpression(Expression Operand, SqlTypeSpec TargetType) : Expression;

/// <summary>
/// A function or aggregate call. <see cref="IsStar"/> is set for
/// <c>COUNT(*)</c> (and only there in v1) and forces <see cref="Arguments"/>
/// to be empty.
/// </summary>
public sealed record FunctionCallExpression(
    string FunctionName,
    IReadOnlyList<Expression> Arguments,
    bool IsStar) : Expression;

/// <summary>
/// A parenthesised query appearing in expression position — a scalar
/// subquery. The inner query may be a bare <c>SELECT</c> or a
/// <c>UNION ALL</c>. Resolution must confirm it returns exactly one column;
/// runtime must return at most one row (empty subquery → <c>NULL</c>, &gt;1 row
/// → SQL runtime error — undefined in v1).
/// </summary>
public sealed record SubqueryExpression(SqlQuery Query) : Expression;

/// <summary>
/// <c>probe [NOT] IN (v1, v2, ..., vN)</c> — the literal-list form of <c>IN</c>.
/// Modeled as a single AST node with a flat <see cref="Values"/> list so the
/// recursive walkers (resolver, expression compiler, monotonicity analyzer)
/// don't blow the C# stack on large lists. A naïve desugar to
/// <c>(probe = v1) OR (probe = v2) OR ...</c> would build an O(N)-depth tree.
/// SQL three-valued NULL semantics are honoured at evaluation: NULL probe →
/// NULL; match on a non-NULL value → TRUE (or FALSE if negated); no match
/// with a NULL among the values → NULL; no match and no NULLs → FALSE (or TRUE
/// if negated).
/// </summary>
public sealed record InListExpression(
    Expression Probe,
    IReadOnlyList<Expression> Values,
    bool IsNegated) : Expression;
