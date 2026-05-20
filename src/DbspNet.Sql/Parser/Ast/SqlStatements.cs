// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections.Generic;

namespace DbspNet.Sql.Parser.Ast;

/// <summary>
/// Base of every AST node produced by <see cref="Parser"/>. Nodes are
/// immutable records; equality is structural (by-value), which is useful in
/// golden-AST tests.
/// </summary>
public abstract record SqlNode;

public abstract record SqlStatement : SqlNode;

public sealed record CreateTableStatement(
    string TableName,
    IReadOnlyList<ColumnDefinition> Columns) : SqlStatement;

public sealed record ColumnDefinition(
    string Name,
    SqlTypeSpec Type,
    bool NotNull,
    bool PrimaryKey);

/// <summary>
/// The unresolved, parser-level spelling of a type. The resolver converts
/// this into a concrete <see cref="DbspNet.Sql.TypeSystem.SqlType"/>. Keeping
/// the AST type "string-ish" lets the parser stay dumb about the type system.
/// </summary>
public sealed record SqlTypeSpec(
    string Name,
    int? Parameter1 = null,
    int? Parameter2 = null);

public sealed record CreateViewStatement(
    string ViewName,
    SqlQuery Query) : SqlStatement;

/// <summary>
/// A "query expression": anything that yields a relation. Includes a
/// single <see cref="SelectStatement"/> as well as <see cref="SetOpQuery"/>
/// concatenations. Holds an optional <c>WITH</c>-clause CTE list that
/// applies to the query as a whole (including every set-op branch).
/// </summary>
public abstract record SqlQuery(IReadOnlyList<CteDefinition> Ctes) : SqlStatement;

public sealed record SelectStatement(
    IReadOnlyList<SelectItem> Items,
    FromClause From,
    Expression? Where,
    IReadOnlyList<Expression> GroupBy,
    Expression? Having,
    IReadOnlyList<CteDefinition> Ctes) : SqlQuery(Ctes);

public enum SetOpKind
{
    /// <summary>Bag semantics: weights add (duplicates preserved).</summary>
    UnionAll,

    /// <summary>Set semantics: bag union followed by dedup.</summary>
    Union,

    /// <summary>Set semantics: rows present in every branch (dedup'd).</summary>
    Intersect,

    /// <summary>Set semantics: rows in the first branch not in any subsequent branch (dedup'd).</summary>
    Except,
}

/// <summary>
/// A set-operation query: <c>q₁ OP q₂ [OP q₃ …]</c> where OP is
/// <c>UNION [ALL]</c>, <c>INTERSECT</c>, or <c>EXCEPT</c>. Branches are
/// flat (same-kind chains collapse to a single N-branch node). Every
/// branch must have the same arity; per-column types are unified at
/// resolution. UNION-ALL uses bag semantics; the other kinds use set
/// semantics (dedup applied).
/// </summary>
public sealed record SetOpQuery(
    SetOpKind Kind,
    IReadOnlyList<SqlQuery> Branches,
    IReadOnlyList<CteDefinition> Ctes) : SqlQuery(Ctes);

/// <summary>
/// A single CTE. When <see cref="IsRecursive"/> is true, the CTE body may
/// reference itself by <see cref="Name"/>; the resolver partitions the
/// query's UNION ALL branches into base cases (non-self-referencing) and
/// recursive cases and produces a <c>RecursiveCtePlan</c>. A
/// <c>WITH RECURSIVE</c> clause sets this flag on every CTE in the clause
/// (self-reference is then allowed but not required for each).
/// </summary>
public sealed record CteDefinition(string Name, SqlQuery Query, bool IsRecursive = false);

public abstract record SelectItem;

public sealed record ExpressionSelectItem(Expression Expression, string? Alias) : SelectItem;

/// <summary>
/// <c>*</c> (unqualified) when <see cref="TableQualifier"/> is null, or
/// <c>t.*</c> when set.
/// </summary>
public sealed record StarSelectItem(string? TableQualifier) : SelectItem;

public abstract record FromClause;

public sealed record TableReference(string TableName, string? Alias) : FromClause;

/// <summary>
/// A subquery appearing in <c>FROM</c>: <c>FROM (SELECT …) AS t</c>. The
/// alias is required (SQL standard — without it the inner columns have no
/// referenceable qualifier). Inside the query body every column of the
/// subquery is exposed qualified by <see cref="Alias"/>.
/// </summary>
public sealed record DerivedTableReference(SqlQuery Query, string Alias) : FromClause;

public sealed record JoinClause(
    FromClause Left,
    FromClause Right,
    JoinType Type,
    Expression OnCondition) : FromClause;

public enum JoinType
{
    Inner,
    LeftOuter,
    RightOuter,
}
