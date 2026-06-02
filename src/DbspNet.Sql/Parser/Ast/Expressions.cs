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

/// <summary>Which spelling of the advancing logical clock the user wrote.</summary>
public enum NowFunction
{
    /// <summary><c>NOW()</c> — TIMESTAMP-valued (microseconds since epoch).</summary>
    Now,

    /// <summary><c>CURRENT_TIMESTAMP</c> — TIMESTAMP-valued; synonym of <see cref="Now"/>.</summary>
    CurrentTimestamp,

    /// <summary><c>CURRENT_DATE</c> — DATE-valued: the clock truncated to its day.
    /// Monotone, so it has sound advancing-clock semantics, but lives in day units,
    /// not microseconds (see <c>docs/now-and-temporal-filters.md</c>).</summary>
    CurrentDate,

    /// <summary><c>CURRENT_TIME</c> — TIME-valued: the clock modulo one day. Cyclic
    /// (wraps every midnight), hence <b>not</b> monotone, so it has no sound
    /// advancing-clock semantics and is rejected in a temporal filter.</summary>
    CurrentTime,
}

/// <summary>
/// The advancing logical clock — <c>NOW()</c> / <c>CURRENT_TIMESTAMP</c> /
/// <c>CURRENT_DATE</c> / <c>CURRENT_TIME</c>.
/// Deliberately <b>not</b> a <see cref="FunctionCallExpression"/>: the clock is
/// not a pure function of the row (its value is the logical time, not the row),
/// so it must never route through the scalar-function registry, whose contract
/// is purity. <see cref="Function"/> records both the spelling (for diagnostics)
/// and the value space: <see cref="NowFunction.Now"/> /
/// <see cref="NowFunction.CurrentTimestamp"/> are <c>TIMESTAMP</c>,
/// <see cref="NowFunction.CurrentDate"/> is <c>DATE</c>, and
/// <see cref="NowFunction.CurrentTime"/> is <c>TIME</c>.
/// </summary>
/// <remarks>
/// Under the temporal-filter model (option B in
/// <c>docs/now-and-temporal-filters.md</c>) this node is legal <i>only</i>
/// inside a sanctioned temporal-filter predicate — a comparison of a
/// <c>TIMESTAMP</c> (or, for <c>CURRENT_DATE</c>, a <c>DATE</c>) expression
/// against the clock, optionally shifted by a constant day-time <c>INTERVAL</c>.
/// The resolver folds such predicates into a <see cref="Plan.TemporalFilterPlan"/>
/// and rejects the clock in every other position. <c>CURRENT_TIME</c> is cyclic
/// and so is rejected even inside a temporal filter.
/// </remarks>
public sealed record NowExpression(NowFunction Function) : Expression;

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

/// <summary>The framing mode of a window <see cref="WindowFrame"/>. Only
/// <see cref="Range"/> is supported by the resolver in v1; <see cref="Rows"/> /
/// <see cref="Groups"/> parse so the resolver can reject them with a precise
/// message.</summary>
public enum WindowFrameMode
{
    Range,
    Rows,
    Groups,
}

/// <summary>Which kind of window-frame bound.</summary>
public enum FrameBoundKind
{
    UnboundedPreceding,
    Preceding,
    CurrentRow,
    Following,
    UnboundedFollowing,
}

/// <summary>
/// One bound of a window frame. <see cref="Offset"/> carries the
/// <c>PRECEDING</c> / <c>FOLLOWING</c> distance (a constant or <c>INTERVAL</c>
/// literal) and is <c>null</c> for the <c>UNBOUNDED</c> and <c>CURRENT ROW</c>
/// kinds.
/// </summary>
public sealed record FrameBound(FrameBoundKind Kind, Expression? Offset);

/// <summary>
/// A window frame clause — <c>{RANGE|ROWS|GROUPS} BETWEEN start AND end</c> (or
/// the single-bound <c>RANGE start</c> shorthand, where <see cref="End"/> is
/// <c>CURRENT ROW</c>).
/// </summary>
public sealed record WindowFrame(WindowFrameMode Mode, FrameBound Start, FrameBound End);

/// <summary>
/// The <c>OVER (...)</c> clause of a <see cref="WindowFunctionExpression"/>.
/// <see cref="PartitionBy"/> may be empty (a single global partition);
/// <see cref="OrderBy"/> reuses the same <see cref="SortItem"/> shape as a
/// query-level <c>ORDER BY</c>; <see cref="Frame"/> is the optional frame clause
/// (only meaningful with an <c>ORDER BY</c>).
/// </summary>
public sealed record WindowSpec(
    IReadOnlyList<Expression> PartitionBy,
    IReadOnlyList<SortItem> OrderBy,
    WindowFrame? Frame = null);

/// <summary>
/// A window-function call — <c>fn(args) OVER (PARTITION BY … ORDER BY … frame)</c>.
/// </summary>
/// <remarks>
/// Two shapes are supported, each by its own resolver path:
/// <list type="bullet">
/// <item>The ranking functions <c>ROW_NUMBER</c> / <c>RANK</c> / <c>DENSE_RANK</c>
/// (no arguments) — supported <b>only</b> in the incremental partitioned TOP-K
/// filter pattern (a derived table's select list filtered with <c>&lt;= k</c> by
/// the enclosing query), lowered to a <c>PartitionedTopKPlan</c>.</item>
/// <item>The aggregate functions <c>SUM</c> / <c>COUNT</c> / <c>AVG</c> /
/// <c>MIN</c> / <c>MAX</c> (one argument, or <c>COUNT(*)</c>) — supported as a
/// new output column, lowered to a <c>WindowAggregatePlan</c>.</item>
/// </list>
/// A window function used anywhere else (nested in an expression, an unsupported
/// function, or a rank function with no qualifying filter) is rejected with an
/// explicit error.
/// </remarks>
public sealed record WindowFunctionExpression(
    string FunctionName,
    IReadOnlyList<Expression> Arguments,
    bool IsStar,
    WindowSpec Over) : Expression;

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

/// <summary>
/// <c>probe [NOT] IN (subquery)</c> — the subquery form of <c>IN</c>.
/// Uncorrelated only; the subquery body has no access to outer-scope
/// columns. Resolves to a <see cref="Plan.SemiJoinPlan"/> when used as a
/// (top-level conjunct of a) WHERE predicate; rejects with a clear
/// "deferred" error in other expression positions (SELECT / HAVING /
/// nested boolean).
/// </summary>
public sealed record InSubqueryExpression(
    Expression Probe,
    SubqueryExpression Subquery,
    bool IsNegated) : Expression;

/// <summary>
/// <c>EXISTS (subquery)</c> — the membership-test form. The resolver
/// routes uncorrelated cases via a synthesised
/// <c>COALESCE((SELECT COUNT(*) FROM (sq)), 0) &gt; 0</c> desugar (the
/// shape the parser used to produce before this node existed), and
/// correlated cases via a <see cref="Plan.SemiJoinPlan"/> lift with the
/// correlation columns as equi-keys. <c>NOT EXISTS</c> is just
/// <see cref="UnaryExpression"/> wrapping this node — no dedicated
/// negation field.
/// </summary>
/// <param name="Subquery">The original subquery the user wrote.</param>
/// <param name="CountSubquery">
/// Pre-cached <c>(SELECT COUNT(*) FROM (Subquery) AS __exists_inner)</c>
/// subquery — the scalar value used by the COALESCE-desugar. Computed
/// once by the parser so reference-equality dedup works across
/// <c>CollectSubqueriesInto</c> and the resolver's synthesised expression.
/// </param>
public sealed record ExistsExpression(
    SubqueryExpression Subquery,
    SubqueryExpression CountSubquery) : Expression;

/// <summary>
/// A single <c>WHEN <see cref="Condition"/> THEN <see cref="Result"/></c>
/// arm of a <see cref="CaseExpression"/>. Not an <see cref="Expression"/>
/// itself — just a pair the recursive walkers descend into.
/// </summary>
public sealed record CaseWhenClause(Expression Condition, Expression Result);

/// <summary>
/// <c>CASE WHEN c1 THEN r1 [WHEN c2 THEN r2 …] [ELSE rN] END</c> — the
/// searched form. The <i>simple</i> form
/// (<c>CASE operand WHEN v1 THEN r1 …</c>) is desugared by the parser into
/// this node, with each arm's condition rewritten to
/// <c>operand = vK</c>, so the resolver and compilers only ever see the
/// searched shape.
/// </summary>
/// <remarks>
/// Modeled as a flat list of <see cref="CaseWhenClause"/>s (not a nested
/// chain) so every recursive walker — resolver, expression compilers,
/// optimizer rewriters — contributes constant stack depth regardless of
/// the arm count. A WHEN arm is taken iff its condition evaluates to a
/// <i>definite</i> TRUE; NULL or FALSE falls through (SQL three-valued
/// semantics). An absent <see cref="ElseResult"/> yields NULL when no arm
/// matches, so the result type is then nullable. Branch evaluation is
/// lazy: a non-taken arm's <see cref="CaseWhenClause.Result"/> is never
/// evaluated, so e.g. <c>CASE WHEN x &lt;&gt; 0 THEN 1/x ELSE 0 END</c> is
/// safe.
/// </remarks>
public sealed record CaseExpression(
    IReadOnlyList<CaseWhenClause> Whens,
    Expression? ElseResult) : Expression;
