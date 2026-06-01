// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections.Generic;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Sql.Plan;

/// <summary>
/// Root of the logical-plan tree. Every plan carries its output
/// <see cref="Schema"/>; the plan→circuit compiler
/// (<c>DbspNet.Sql.Compiler.PlanToCircuit</c>) walks these records to build
/// the DBSP runtime graph.
/// </summary>
public abstract record LogicalPlan(Schema Schema);

/// <summary>
/// Scan of a base table. <see cref="ColumnLateness"/> maps a column index to
/// its declared <c>LATENESS</c> bound in the column's native units (microseconds
/// for temporal columns, a raw offset for integer logical-time columns); absent
/// keys carry no lateness. The monotonicity analyzer seeds from these, and the
/// compiler uses the bound to build the column's frontier.
/// </summary>
public sealed record ScanPlan(
    string TableName,
    Schema Schema,
    IReadOnlyDictionary<int, long>? ColumnLateness = null) : LogicalPlan(Schema);

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
/// One <c>ORDER BY</c> sort key for a <see cref="TopKPlan"/>: an expression
/// evaluated over the input row, a direction, and where NULLs sort.
/// <see cref="NullsFirst"/> is already resolved from the SQL default
/// (<c>ASC</c> ⇒ NULLS LAST, <c>DESC</c> ⇒ NULLS FIRST) or the explicit
/// <c>NULLS FIRST|LAST</c> the user wrote.
/// </summary>
public sealed record SortKey(ResolvedExpression Expression, bool Descending, bool NullsFirst);

/// <summary>
/// Incremental TOP-K: keep the rows of <see cref="Input"/> occupying sort
/// positions <c>[Offset, Offset + Limit)</c> under the <see cref="SortKeys"/>
/// order (a total order — ties broken by the full row). Maintained as rows
/// enter/leave that window under retraction. <see cref="Limit"/> null means
/// "to the end" (an <c>OFFSET</c> with no <c>LIMIT</c>); <see cref="Offset"/>
/// null means 0. Schema is the input's, unchanged — TOP-K only restricts
/// which rows survive, never their shape.
/// </summary>
/// <remarks>
/// Row order is unobservable in the output Z-set, so a bare <c>ORDER BY</c>
/// (no limit/offset) never produces this node — the resolver returns the input
/// plan directly. This node is therefore only built when a bound is present.
/// </remarks>
public sealed record TopKPlan(
    LogicalPlan Input,
    IReadOnlyList<SortKey> SortKeys,
    long? Limit,
    long? Offset) : LogicalPlan(Input.Schema);

/// <summary>
/// Incremental partitioned TOP-K — the <c>ROW_NUMBER</c> / <c>RANK</c> /
/// <c>DENSE_RANK</c> window functions restricted to the SQL filter pattern
/// <c>… OVER (PARTITION BY p ORDER BY o) &lt;= Limit</c>. Within each partition
/// (the <see cref="PartitionKeys"/> grouping) it keeps the rows whose rank under
/// <see cref="SortKeys"/> is <c>&lt;= Limit</c>, maintained as rows enter and
/// leave that window under retraction.
/// </summary>
/// <remarks>
/// The rank value is never materialised — it only drives the cut at
/// <see cref="Limit"/>, so the schema is the input's, unchanged (rows are
/// filtered, never widened). <see cref="PartitionKeys"/> and the
/// <see cref="SortKey.Expression"/>s are resolved against <see cref="Input"/>;
/// any that reference a column not in the inner select list are carried as
/// hidden trailing columns on <see cref="Input"/> and stripped by a projection
/// the resolver places above this node.
/// </remarks>
public sealed record PartitionedTopKPlan(
    LogicalPlan Input,
    IReadOnlyList<ResolvedExpression> PartitionKeys,
    IReadOnlyList<SortKey> SortKeys,
    RankFunction Function,
    long Limit) : LogicalPlan(Input.Schema);

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

/// <summary>One equi-join key for a <see cref="SemiJoinPlan"/>: an
/// outer-side expression matched against a fixed inner-side column index.</summary>
public sealed record SemiJoinEqui(
    ResolvedExpression OuterKey,
    int InnerColumnIndex,
    SqlType Type);

/// <summary>
/// Semi-join (equi-keyed): keep every row of <see cref="Input"/> whose
/// projection of <see cref="EquiKeys"/>' outer-side expressions matches at
/// least one row in <see cref="Subquery"/> on the corresponding inner-column
/// indices. Output schema is <see cref="Input"/>'s schema unchanged (the
/// subquery side is consumed by the match check). Compiles to
/// <c>Distinct(Subquery) ⋈ Input</c> on the equi-key tuple + project outer
/// columns. Used for both the WHERE-level <c>x IN (subquery)</c> rewrite
/// (uncorrelated → one equi-key) and the correlated-IN decorrelation
/// (correlated → N+1 equi-keys covering the IN-probe plus each correlation
/// column).
/// </summary>
/// <remarks>
/// <para>NULL semantics match the SQL WHERE shape: <c>NULL</c> outer-key
/// rows are dropped (equi-join filters NULL keys); <c>NULL</c> values in the
/// subquery never match (same).</para>
/// <para><see cref="IsAnti"/> is reserved for future <c>NOT IN (subquery)</c>
/// (anti-semi-join). Always <c>false</c> in v1; resolver rejects the
/// negated form with an explicit "deferred" message.</para>
/// </remarks>
public sealed record SemiJoinPlan(
    LogicalPlan Input,
    LogicalPlan Subquery,
    IReadOnlyList<SemiJoinEqui> EquiKeys,
    bool IsAnti = false) : LogicalPlan(Input.Schema);

/// <summary>
/// Correlated scalar-subquery LEFT JOIN: appends one nullable column to
/// every row of <see cref="Input"/>, computed as the
/// <see cref="ScalarColumnIndex"/> column of the inner subquery for the
/// row whose correlation tuple (per <see cref="CorrelationKeys"/>)
/// matches the outer row, or NULL when no inner row matches.
/// </summary>
/// <remarks>
/// <para><b>Why a separate node from <see cref="ScalarSubqueryJoinPlan"/>:</b>
/// uncorrelated scalar subqueries are batched in a single node (one
/// hidden column per subquery) using a 0-column unit key. Correlated
/// scalar subqueries each need their own key tuple, so they're layered
/// individually — mirrors the IN/EXISTS separation between
/// <see cref="SemiJoinPlan"/> and the uncorrelated batched form.</para>
/// <para><b>Subquery shape (post-decorrelation):</b>
/// <c>[__corr_0, ..., __corr_N, scalar_value]</c>. The first N columns
/// are projected from the inner's aggregate GROUP BY (the correlation
/// columns); the last column is the original scalar value the user
/// referenced.</para>
/// </remarks>
public sealed record CorrelatedScalarSubqueryJoinPlan(
    LogicalPlan Input,
    LogicalPlan Subquery,
    IReadOnlyList<SemiJoinEqui> CorrelationKeys,
    int ScalarColumnIndex,
    Schema Schema) : LogicalPlan(Schema);

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
