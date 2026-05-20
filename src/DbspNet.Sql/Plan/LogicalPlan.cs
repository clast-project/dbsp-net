// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections.Generic;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Sql.Plan;

/// <summary>
/// Root of the logical-plan tree. Every plan carries its output
/// <see cref="Schema"/>; the plan→circuit compiler
/// (<c>DbspNet.Sql.Compiler.PlanToCircuit</c>) walks these records to build
/// the DBSP runtime graph.
/// </summary>
public abstract record LogicalPlan(Schema Schema);

public sealed record ScanPlan(string TableName, Schema Schema) : LogicalPlan(Schema);

/// <summary>
/// Reference to a <c>WITH name AS (...)</c> CTE definition. Reference
/// equality (not structural) drives the plan→circuit compiler's deduplication:
/// two <see cref="CteScanPlan"/>s that share a <see cref="CteRef"/> instance
/// compile to a single shared subcircuit stream.
/// </summary>
/// <remarks>
/// <see cref="Plan"/> is settable so recursive-CTE resolution can create the
/// ref before the plan exists (the recursive body references itself, so the
/// ref must be in scope while the body is resolved, and the body becomes the
/// ref's plan once constructed).
/// </remarks>
public sealed class CteRef
{
    public CteRef(string name, LogicalPlan plan)
    {
        ArgumentNullException.ThrowIfNull(name);
        ArgumentNullException.ThrowIfNull(plan);
        Name = name;
        Plan = plan;
    }

    public string Name { get; }

    public LogicalPlan Plan { get; internal set; }
}

/// <summary>
/// Reads rows produced by a CTE. The runtime stream is identical for every
/// scan of the same <see cref="CteRef"/>; the <see cref="Schema"/> may differ
/// per scan only in column qualifier (<c>FROM cte AS c</c> vs <c>FROM cte</c>).
/// </summary>
public sealed record CteScanPlan(CteRef Cte, Schema Schema) : LogicalPlan(Schema);

/// <summary>
/// A recursive CTE: <c>WITH RECURSIVE r AS (base UNION ALL step(r))</c>.
/// <see cref="BasePlan"/> is the union of the non-self-referencing branches;
/// <see cref="RecursivePlan"/> is the union of the branches that reference
/// <see cref="SelfRef"/>. The circuit runtime evaluates this by a per-tick
/// batch fixed-point (see <c>RecursiveCteOp</c>).
/// </summary>
/// <remarks>
/// Equality on this record is reference-based to avoid cycles (the plan
/// contains a CteRef whose Plan points back at this record, and inside
/// RecursivePlan there can be CteScanPlans referencing that CteRef).
/// </remarks>
public sealed record RecursiveCtePlan(
    LogicalPlan BasePlan,
    LogicalPlan RecursivePlan,
    CteRef SelfRef,
    Schema Schema) : LogicalPlan(Schema)
{
    // Override structural equality — records' default walks into every field,
    // which would infinite-loop across the CteRef.Plan back-edge.
    public bool Equals(RecursiveCtePlan? other) => ReferenceEquals(this, other);

    public override int GetHashCode() => System.Runtime.CompilerServices.RuntimeHelpers.GetHashCode(this);
}

public sealed record FilterPlan(LogicalPlan Input, ResolvedExpression Predicate)
    : LogicalPlan(Input.Schema);

/// <summary>
/// <c>UNION ALL</c> of two or more branches. Every branch has been
/// projection-aligned to <see cref="Schema"/> (same arity and per-column
/// types). Compiles to successive Z-set additions in the runtime.
/// </summary>
public sealed record UnionAllPlan(
    IReadOnlyList<LogicalPlan> Branches,
    Schema Schema) : LogicalPlan(Schema);

/// <summary>
/// Dedup a Z-set to set semantics — every row with strictly-positive
/// accumulated weight emits as weight 1; transitions to non-positive
/// weight emit retractions. Compiles to the Core <c>Distinct</c> operator.
/// Used by <c>UNION</c> / <c>INTERSECT</c> / <c>EXCEPT</c> rewrites.
/// </summary>
public sealed record DistinctPlan(LogicalPlan Input)
    : LogicalPlan(Input.Schema);

/// <summary>
/// Pointwise Z-set difference — <c>A − B</c>. Same-schema inputs.
/// Used by <c>EXCEPT</c> rewrites: <c>Distinct(a) − Intersect(a, b)</c>.
/// </summary>
public sealed record DifferencePlan(LogicalPlan Left, LogicalPlan Right)
    : LogicalPlan(Left.Schema);

/// <summary>
/// Appends one "hidden" column per (uncorrelated) scalar subquery to the
/// input schema, each computed by cross-joining the input with the
/// subquery's output on a unit key. An empty subquery result contributes
/// <c>NULL</c>; a non-empty result contributes its single value. &gt;1 rows in
/// the subquery at runtime is undefined (v1 does not validate).
/// </summary>
public sealed record ScalarSubqueryJoinPlan(
    LogicalPlan Input,
    IReadOnlyList<LogicalPlan> Subqueries,
    Schema Schema) : LogicalPlan(Schema);

public sealed record ProjectionItem(ResolvedExpression Expression, string Name, string? Qualifier = null);

public sealed record ProjectPlan(
    LogicalPlan Input,
    IReadOnlyList<ProjectionItem> Projections,
    Schema Schema) : LogicalPlan(Schema);

public sealed record JoinPlan(
    LogicalPlan Left,
    LogicalPlan Right,
    DbspNet.Sql.Parser.Ast.JoinType JoinType,
    IReadOnlyList<JoinEquality> EquiKeys,
    ResolvedExpression? Residual,
    Schema Schema,
    bool AllowNullKeys = false) : LogicalPlan(Schema);

/// <summary>One equi-join conjunct: <c>left[LeftIndex] = right[RightIndex]</c>.</summary>
public sealed record JoinEquality(int LeftIndex, int RightIndex, SqlType KeyType);

public enum AggregateKind
{
    CountStar,
    Count,
    Sum,
    Min,
    Max,
    Avg,
}

/// <summary>
/// One aggregate call inside an <see cref="AggregatePlan"/>. For
/// <see cref="AggregateKind.CountStar"/>, <see cref="Argument"/> is null.
/// </summary>
public sealed record AggregateCall(AggregateKind Kind, ResolvedExpression? Argument, SqlType ResultType);

/// <summary>
/// Group-by + aggregation. Output schema is
/// <c>[group-key columns..., aggregate-result columns...]</c>; the outer
/// <see cref="ProjectPlan"/> remaps and aliases these for the user's
/// <c>SELECT</c> list.
/// </summary>
public sealed record AggregatePlan(
    LogicalPlan Input,
    IReadOnlyList<ResolvedExpression> GroupKeys,
    IReadOnlyList<AggregateCall> Aggregates,
    Schema Schema) : LogicalPlan(Schema);

// Statement-level plans (sit outside the relational algebra tree).
public abstract record PlanStatement;

public sealed record CreateTablePlan(string TableName, Schema Schema) : PlanStatement;

public sealed record CreateViewPlan(string ViewName, LogicalPlan Query) : PlanStatement;

public sealed record SelectPlan(LogicalPlan Query) : PlanStatement;
