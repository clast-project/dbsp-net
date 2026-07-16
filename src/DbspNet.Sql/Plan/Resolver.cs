// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections.Generic;
using System.Linq;
using Clast.DatabaseDecimal.Values;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Sql.Expressions;
using DbspNet.Sql.Optimizer;
using DbspNet.Sql.Parser.Ast;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Sql.Plan;

/// <summary>
/// Walks a parsed <see cref="SqlStatement"/> and produces a
/// <see cref="PlanStatement"/> with fully-typed scalar expressions and
/// positional column references. The resolver is the sole source of
/// truth for v1 SQL semantic rules: type coercion, name resolution,
/// <c>GROUP BY</c> well-formedness, and equi-join extraction.
/// </summary>
public sealed class Resolver
{
    private readonly Catalog _catalog;

    public Resolver(Catalog catalog)
    {
        ArgumentNullException.ThrowIfNull(catalog);
        _catalog = catalog;
    }

    public PlanStatement Resolve(SqlStatement statement) => statement switch
    {
        CreateTableStatement ct => ResolveCreateTable(ct),
        CreateViewStatement cv => ResolveCreateView(cv),
        SqlQuery q => new SelectPlan(ResolveQuery(q, EmptyCteScope)),
        _ => throw new ResolveException($"unsupported statement kind: {statement.GetType().Name}"),
    };

    private LogicalPlan ResolveQuery(
        SqlQuery query,
        IReadOnlyDictionary<string, CteRef> outerCteScope,
        Schema? outerSchema = null) => query switch
    {
        SelectStatement s => ResolveSelect(s, outerCteScope, outerSchema),
        SetOpQuery u => ResolveSetOp(u, outerCteScope),
        OrderLimitQuery o => ResolveOrderLimit(o, outerCteScope, outerSchema),
        _ => throw new ResolveException($"unsupported query kind: {query.GetType().Name}"),
    };

    private static readonly IReadOnlyDictionary<string, CteRef> EmptyCteScope =
        new Dictionary<string, CteRef>(StringComparer.Ordinal);

    /// <summary>
    /// Maps each scalar-subquery AST node (by structural equality) to the
    /// hidden column index and type assigned to it in the augmented schema.
    /// When non-empty, the "current plan" has been wrapped in a
    /// <see cref="ScalarSubqueryJoinPlan"/> that materialises those columns.
    /// </summary>
    private readonly record struct SubqueryBinding(int ColumnIndex, SqlType Type);

    /// <summary>
    /// Maps an <see cref="ExistsExpression"/> or
    /// <see cref="InSubqueryExpression"/> (by reference identity) to the
    /// hidden match-count column the non-WHERE pre-pass layered onto the
    /// running plan. The expression resolves to
    /// <c>COALESCE(match_count, 0) &gt; 0</c> (or the negated form for
    /// <c>NOT IN</c> / a wrapping <c>NOT EXISTS</c>).
    /// </summary>
    private readonly record struct BooleanSubqueryBinding(int CountColumnIndex, bool IsNegated)
    {
        /// <summary>
        /// Set for the nullable-operand <c>IN</c> / <c>NOT IN</c> 3VL path in
        /// non-WHERE positions. When non-<c>null</c>,
        /// <see cref="BuildBooleanSubqueryRef"/> emits a full <c>CASE</c> using
        /// <see cref="CountColumnIndex"/> as the match count plus the total /
        /// null counts below; the probe expression is needed for the
        /// <c>probe IS NULL → NULL</c> arm. <c>null</c> on the EXISTS and
        /// NOT-NULL-operand fast paths, which use the plain
        /// <c>COALESCE(count, 0) &gt; 0</c> comparison.
        /// </summary>
        public ResolvedExpression? NullableProbe { get; init; }

        /// <summary>Per-group total row count (empty-subquery detection). Nullable path only.</summary>
        public int TotalCountColumnIndex { get; init; }

        /// <summary>Per-group count of NULL subquery values. Nullable path only.</summary>
        public int NullCountColumnIndex { get; init; }
    }

    // ---------- DDL ----------

    private CreateTablePlan ResolveCreateTable(CreateTableStatement stmt)
    {
        var cols = new List<SchemaColumn>(stmt.Columns.Count);
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var c in stmt.Columns)
        {
            if (!seen.Add(c.Name))
            {
                throw new ResolveException($"duplicate column '{c.Name}' in table '{stmt.TableName}'");
            }

            var type = TypeInference.FromSpec(c.Type, nullable: !c.NotNull);
            if (c.Lateness is { } bound)
            {
                if (!c.NotNull)
                {
                    throw new ResolveException($"LATENESS column '{c.Name}' must be NOT NULL");
                }

                if (bound < 0)
                {
                    throw new ResolveException($"LATENESS for column '{c.Name}' must be non-negative");
                }

                if (!IsLatenessOrderedType(type))
                {
                    throw new ResolveException(
                        $"LATENESS column '{c.Name}' must be an integer or temporal type");
                }
            }

            cols.Add(new SchemaColumn(c.Name, type, Qualifier: stmt.TableName, Lateness: c.Lateness));
        }

        var schema = new Schema(cols);
        _catalog.Register(stmt.TableName, schema);
        return new CreateTablePlan(stmt.TableName, schema);
    }

    // LATENESS requires a totally-ordered column whose values map to an Int64
    // frontier key: the integer and temporal types.
    private static bool IsLatenessOrderedType(SqlType type) =>
        type is SqlIntegerType or SqlBigintType or SqlTimestampType or SqlDateType or SqlTimeType;

    private CreateViewPlan ResolveCreateView(CreateViewStatement stmt)
    {
        var query = ResolveQuery(stmt.Query, EmptyCteScope);
        return new CreateViewPlan(stmt.ViewName, query);
    }

    // ---------- SELECT ----------

    private LogicalPlan ResolveSelect(
        SelectStatement stmt,
        IReadOnlyDictionary<string, CteRef> outerCteScope,
        Schema? outerSchema = null)
    {
        // Build the local CTE scope — inherits from the outer scope and adds
        // any WITH-defined CTEs on this SELECT. CTEs are non-recursive in v1
        // (a definition may reference previously-defined CTEs but not itself
        // or later ones), so we resolve them one at a time in declaration
        // order. Shadowing an outer CTE name is an error.
        Dictionary<string, CteRef>? cteScope = null;
        foreach (var cte in stmt.Ctes)
        {
            cteScope ??= new Dictionary<string, CteRef>(outerCteScope, StringComparer.Ordinal);
            if (cteScope.ContainsKey(cte.Name))
            {
                throw new ResolveException($"duplicate CTE name '{cte.Name}'");
            }

            cteScope[cte.Name] = cte.IsRecursive
                ? ResolveRecursiveCte(cte, cteScope)
                : new CteRef(cte.Name, ResolveQuery(cte.Query, cteScope));
        }

        IReadOnlyDictionary<string, CteRef> scope = cteScope ?? outerCteScope;

        // Partitioned TOP-K (ROW_NUMBER/RANK/DENSE_RANK in the `… OVER (…) <= k`
        // filter pattern) is recognised structurally before normal resolution,
        // because there is no general window-function plan node — only this
        // shape is supported.
        if (TryResolvePartitionedTopK(stmt, scope, outerSchema) is { } partitioned)
        {
            return partitioned;
        }

        // Window aggregates (SUM/COUNT/AVG/MIN/MAX OVER (...) emitted as output
        // columns). Recognised before normal resolution: the pre-window relation
        // (FROM + WHERE) is resolved as a star query, this node widens each row
        // with the aggregate result(s), and a projection maps the select list.
        if (TryResolveWindowAggregate(stmt, scope, outerSchema) is { } windowed)
        {
            return windowed;
        }

        // FROM
        var plan = ResolveFrom(stmt.From, scope, outerSchema);

        // WHERE — pre-pass for scalar subqueries referenced in the predicate
        // (each adds a hidden column via ScalarSubqueryJoinPlan), plus
        // semi-join lifts for `x IN (subquery)` and `EXISTS (subquery)`
        // conjuncts.
        if (stmt.Where is not null)
        {
            // Split at top-level AND so IN-subquery and EXISTS terms can be
            // peeled off and lifted to SemiJoinPlan; the remaining scalar
            // predicates stay as a FilterPlan over the result.
            var conjuncts = SplitAndConjuncts(stmt.Where);
            var inSubqueries = new List<InSubqueryExpression>();
            var existsConjuncts = new List<(ExistsExpression Sq, bool IsNegated)>();
            var temporalBounds = new List<TemporalHalfBound>();
            var scalarConjuncts = new List<Expression>();

            // Temporal filters resolve their time-key against the FROM schema;
            // IN / EXISTS lifts below preserve that schema (semi-join / anti-
            // join keep the input's columns), so the resolved column indices
            // stay valid no matter the order the filters are stacked.
            var fromSchema = plan.Schema;
            foreach (var c in conjuncts)
            {
                if (MentionsNow(c))
                {
                    // A conjunct that mentions NOW() must be a sanctioned
                    // temporal-filter predicate; MatchTemporalConjunct throws a
                    // precise error otherwise (rather than letting the generic
                    // NowExpression guard fire later).
                    temporalBounds.Add(MatchTemporalConjunct(c, fromSchema));
                }
                else if (c is InSubqueryExpression isq)
                {
                    // Both IN and NOT IN go through the same lift; the lift
                    // builds a SemiJoinPlan with IsAnti=isNegated, and
                    // (for NOT IN only) rejects nullable operands.
                    inSubqueries.Add(isq);
                }
                else if (c is ExistsExpression e)
                {
                    existsConjuncts.Add((e, false));
                }
                else if (c is UnaryExpression { Operator: UnaryOperator.Not, Operand: ExistsExpression eNeg })
                {
                    existsConjuncts.Add((eNeg, true));
                }
                else
                {
                    scalarConjuncts.Add(c);
                }
            }

            foreach (var isq in inSubqueries)
            {
                plan = LiftInSubqueryToSemiJoin(plan, isq, scope);
            }

            foreach (var (e, isNegated) in existsConjuncts)
            {
                plan = LiftExistsOrDesugar(plan, e, isNegated, scope, scalarConjuncts);
            }

            plan = ApplyTemporalFilters(plan, temporalBounds);

            if (scalarConjuncts.Count > 0)
            {
                var remaining = JoinAnd(scalarConjuncts);
                plan = WrapWithScalarSubqueries(plan, scope, CollectSubqueries(remaining), out var whereSubMap);
                var pred = ResolveScalarExpression(remaining, plan.Schema, whereSubMap, outerSchema);
                EnsureBooleanCoercible(pred, "WHERE");
                plan = new FilterPlan(plan, pred);
            }
        }

        // Aggregation?
        var hasGroupBy = stmt.GroupBy.Count > 0;
        var hasAggregates = HasAggregate(stmt.Items) || (stmt.Having is not null && HasAggregate(stmt.Having));

        if (!hasGroupBy && !hasAggregates)
        {
            if (stmt.Having is not null)
            {
                throw new ResolveException("HAVING without GROUP BY or aggregate");
            }

            // Non-aggregate SELECT: lift any non-WHERE boolean subqueries
            // (correlated/uncorrelated EXISTS, IN, NOT IN, NOT EXISTS in
            // SELECT items) to hidden match-count columns FIRST so that the
            // scalar-resolve below sees their bindings. Then collect any
            // remaining SubqueryExpressions (regular scalar subqueries) and
            // wrap as today.
            var selectExists = new List<ExistsExpression>();
            var selectInSubs = new List<InSubqueryExpression>();
            foreach (var item in stmt.Items)
            {
                if (item is ExpressionSelectItem esi)
                {
                    CollectNonWhereBooleanSubqueries(esi.Expression, selectExists, selectInSubs);
                }
            }

            // Lift ALL non-WHERE EXISTS/IN via the pre-pass for uniformity.
            // The path works for both correlated and uncorrelated forms;
            // uncorrelated subqueries produce a no-GroupBy aggregate layered
            // via ScalarSubqueryJoinPlan (same semantic shape as the
            // parser-time CountSubquery path).
            var (planAfterBool, existsMap, inMap) = WrapWithNonWhereBooleanSubqueries(
                plan, scope, selectExists, selectInSubs);
            plan = planAfterBool;

            var selectSubs = new List<SubqueryExpression>();
            foreach (var item in stmt.Items)
            {
                if (item is ExpressionSelectItem esi)
                {
                    CollectSubqueriesIntoExcludingBound(esi.Expression, selectSubs, existsMap, inMap);
                }
            }

            plan = WrapWithScalarSubqueries(plan, scope, selectSubs, out var selectSubMap);
            var preBound = BuildPreBoundFromBoolMaps(existsMap, inMap);
            var projections = ResolveProjections(stmt.Items, plan.Schema, selectSubMap, preBound);
            LogicalPlan projected = new ProjectPlan(plan, projections, BuildProjectSchema(projections));
            return stmt.Distinct ? new DistinctPlan(projected) : projected;
        }

        // Aggregation path. Resolve GROUP BY keys against the pre-aggregate
        // schema. A key may be any scalar expression (e.g. CAST(ts AS DATE),
        // a + b); a bare column keeps its name/qualifier so SELECT/HAVING can
        // still reference it by name and the output column reads naturally.
        var groupKeyExprs = new List<ResolvedExpression>();
        var groupKeyAstItems = new List<(Expression Ast, int OutputIndex)>();
        var groupCols = new List<SchemaColumn>();
        var seenKeys = new HashSet<ResolvedExpression>();
        for (var i = 0; i < stmt.GroupBy.Count; i++)
        {
            var gb = stmt.GroupBy[i];
            if (HasAggregate(gb))
            {
                throw new ResolveException("aggregate functions are not allowed in a GROUP BY key");
            }

            ResolvedExpression resolved;
            SchemaColumn col;
            if (gb is ColumnReference cref)
            {
                var idx = plan.Schema.Resolve(cref.Qualifier, cref.Name);
                var src = plan.Schema[idx];
                resolved = new ResolvedColumn(idx, src.Type);
                col = new SchemaColumn(src.Name, src.Type, src.Qualifier);
            }
            else
            {
                resolved = ResolveScalarExpression(gb, plan.Schema);
                var (name, _) = DeriveProjectionName(gb, null);
                col = new SchemaColumn(name, resolved.Type);
            }

            if (!seenKeys.Add(resolved))
            {
                throw new ResolveException("duplicate GROUP BY key");
            }

            groupKeyExprs.Add(resolved);
            groupKeyAstItems.Add((gb, i));
            groupCols.Add(col);
        }

        // Collect aggregates by a light pre-walk of SELECT + HAVING. Final
        // ResolvedExpressions for these contexts are built later (once we
        // know the post-aggregate / post-subquery-join schema).
        var aggregates = new List<AggregateCall>();
        var aggIndex = new Dictionary<AggregateKey, int>();
        var aggStart = groupCols.Count;

        foreach (var item in stmt.Items)
        {
            if (item is StarSelectItem)
            {
                throw new ResolveException("SELECT * is not allowed with GROUP BY / aggregates");
            }

            var exprItem = (ExpressionSelectItem)item;
            CollectAggregatesInto(exprItem.Expression, plan.Schema, groupKeyAstItems, aggregates, aggIndex);
        }

        if (stmt.Having is not null)
        {
            CollectAggregatesInto(stmt.Having, plan.Schema, groupKeyAstItems, aggregates, aggIndex);
        }

        var resolvedItems = new List<(ResolvedExpression Expr, string Name, string? Qualifier)>();

        // Build aggregated-schema columns for aggregates.
        var aggCols = new List<SchemaColumn>(aggregates.Count);
        for (var i = 0; i < aggregates.Count; i++)
        {
            aggCols.Add(new SchemaColumn("$agg" + i, aggregates[i].ResultType));
        }

        var groupedSchema = new Schema([.. groupCols, .. aggCols]);
        LogicalPlan aggPlan = new AggregatePlan(plan, groupKeyExprs, aggregates, groupedSchema);

        // HAVING — non-WHERE boolean pre-pass (EXISTS / IN lift), then
        // scalar-subquery pre-pass, then resolve the filter predicate.
        LogicalPlan withHaving = aggPlan;
        if (stmt.Having is not null)
        {
            var havingExists = new List<ExistsExpression>();
            var havingIns = new List<InSubqueryExpression>();
            CollectNonWhereBooleanSubqueries(stmt.Having, havingExists, havingIns);
            var (planAfterBoolH, havingExistsMap, havingInMap) =
                WrapWithNonWhereBooleanSubqueries(withHaving, scope, havingExists, havingIns);
            withHaving = planAfterBoolH;

            var havingScalarSubs = new List<SubqueryExpression>();
            CollectSubqueriesIntoExcludingBound(stmt.Having, havingScalarSubs, havingExistsMap, havingInMap);
            withHaving = WrapWithScalarSubqueries(withHaving, scope, havingScalarSubs, out var havingSubMap);

            var havingPreBound = BuildPreBoundFromBoolMaps(havingExistsMap, havingInMap);
            var havingPred = ResolvePostAggregateExpression(
                stmt.Having, plan.Schema, groupKeyAstItems, aggregates, aggIndex, aggStart,
                havingSubMap, withHaving.Schema, havingPreBound);
            EnsureBooleanCoercible(havingPred, "HAVING");
            withHaving = new FilterPlan(withHaving, havingPred);
        }

        // SELECT (aggregate path) — non-WHERE boolean pre-pass for the
        // projection list, then scalar-subquery pre-pass.
        var selectExistsAgg = new List<ExistsExpression>();
        var selectInsAgg = new List<InSubqueryExpression>();
        foreach (var item in stmt.Items)
        {
            if (item is ExpressionSelectItem esi)
            {
                CollectNonWhereBooleanSubqueries(esi.Expression, selectExistsAgg, selectInsAgg);
            }
        }

        var (withHavingPlusBool, existsMapAgg, inMapAgg) =
            WrapWithNonWhereBooleanSubqueries(withHaving, scope, selectExistsAgg, selectInsAgg);
        withHaving = withHavingPlusBool;

        var selectSubsAgg = new List<SubqueryExpression>();
        foreach (var item in stmt.Items)
        {
            if (item is ExpressionSelectItem esi)
            {
                CollectSubqueriesIntoExcludingBound(esi.Expression, selectSubsAgg, existsMapAgg, inMapAgg);
            }
        }

        withHaving = WrapWithScalarSubqueries(withHaving, scope, selectSubsAgg, out var selectSubMapAgg);
        var selectPreBoundAgg = BuildPreBoundFromBoolMaps(existsMapAgg, inMapAgg);

        resolvedItems.Clear();
        foreach (var item in stmt.Items)
        {
            var exprItem = (ExpressionSelectItem)item;
            var resolved = ResolvePostAggregateExpression(
                exprItem.Expression, plan.Schema, groupKeyAstItems, aggregates, aggIndex, aggStart,
                selectSubMapAgg, withHaving.Schema, selectPreBoundAgg);

            var (name, qualifier) = DeriveProjectionName(exprItem.Expression, exprItem.Alias);
            resolvedItems.Add((resolved, name, qualifier));
        }

        var projItems = new List<ProjectionItem>(resolvedItems.Count);
        foreach (var (e, n, q) in resolvedItems)
        {
            projItems.Add(new ProjectionItem(e, n, q));
        }

        LogicalPlan aggProjected = new ProjectPlan(withHaving, projItems, BuildProjectSchema(projItems));
        return stmt.Distinct ? new DistinctPlan(aggProjected) : aggProjected;
    }

    // ---------- Recursive CTE ----------

    /// <summary>
    /// Resolve a <c>WITH RECURSIVE</c> CTE. The body must be a
    /// <c>UNION ALL</c> query; branches are partitioned at the AST level by
    /// whether they reference the CTE's name. Base-case branches are
    /// resolved first (without the CTE in scope) to derive the output
    /// schema; the recursive-step branches are then resolved in a scope
    /// that includes a stub CteRef with that schema.
    /// </summary>
    private CteRef ResolveRecursiveCte(CteDefinition cte, IReadOnlyDictionary<string, CteRef> outerScope)
    {
        if (cte.Query is not SetOpQuery { Kind: SetOpKind.UnionAll } unionBody)
        {
            throw new ResolveException(
                $"recursive CTE '{cte.Name}' must be a UNION ALL of a base case and a recursive step");
        }

        // AST-level partition: which branches mention the CTE's name?
        var baseAst = new List<SqlQuery>();
        var recAst = new List<SqlQuery>();
        foreach (var branch in unionBody.Branches)
        {
            if (QueryReferencesName(branch, cte.Name))
            {
                recAst.Add(branch);
            }
            else
            {
                baseAst.Add(branch);
            }
        }

        if (baseAst.Count == 0)
        {
            throw new ResolveException(
                $"recursive CTE '{cte.Name}' needs at least one non-self-referencing branch (base case)");
        }

        if (recAst.Count == 0)
        {
            throw new ResolveException(
                $"recursive CTE '{cte.Name}' is declared RECURSIVE but never references itself");
        }

        // Resolve base branches in the outer scope (without the stub) and
        // unify their schemas to derive the CTE's declared shape.
        var baseResolved = new List<LogicalPlan>(baseAst.Count);
        foreach (var b in baseAst)
        {
            baseResolved.Add(ResolveQuery(b, outerScope));
        }

        var unifiedSchema = UnifyBranchSchemas(baseResolved, $"recursive CTE '{cte.Name}' base case");

        var baseAligned = new List<LogicalPlan>(baseResolved.Count);
        foreach (var b in baseResolved)
        {
            baseAligned.Add(AlignBranchToUnion(b, unifiedSchema));
        }

        var basePlan = CombineBranches(baseAligned, unifiedSchema);

        // Create the self-ref with the base plan; this lets CteScanPlan
        // inside the recursive branches see the correct schema. We'll
        // overwrite selfRef.Plan with the final RecursiveCtePlan below.
        var selfRef = new CteRef(cte.Name, basePlan);
        var extendedScope = new Dictionary<string, CteRef>(outerScope, StringComparer.Ordinal)
        {
            [cte.Name] = selfRef,
        };

        // Resolve recursive branches in the extended scope and re-align.
        var recResolved = new List<LogicalPlan>(recAst.Count);
        foreach (var b in recAst)
        {
            recResolved.Add(ResolveQuery(b, extendedScope));
        }

        var recAligned = new List<LogicalPlan>(recResolved.Count);
        foreach (var b in recResolved)
        {
            recAligned.Add(AlignBranchToUnion(b, unifiedSchema));
        }

        var recPlan = CombineBranches(recAligned, unifiedSchema);

        var rcp = new RecursiveCtePlan(basePlan, recPlan, selfRef, unifiedSchema);
        selfRef.Plan = rcp; // close the cycle
        return selfRef;
    }

    private static Schema UnifyBranchSchemas(IReadOnlyList<LogicalPlan> branches, string contextForErrors)
    {
        var arity = branches[0].Schema.Count;
        for (var i = 1; i < branches.Count; i++)
        {
            if (branches[i].Schema.Count != arity)
            {
                throw new ResolveException(
                    $"{contextForErrors}: branches must have the same column count");
            }
        }

        var cols = new List<SchemaColumn>(arity);
        for (var c = 0; c < arity; c++)
        {
            var t = branches[0].Schema[c].Type;
            for (var i = 1; i < branches.Count; i++)
            {
                t = TypeInference.CommonComparableType(t, branches[i].Schema[c].Type);
            }

            cols.Add(new SchemaColumn(branches[0].Schema[c].Name, t, Qualifier: null));
        }

        return new Schema(cols);
    }

    private static LogicalPlan CombineBranches(List<LogicalPlan> branches, Schema schema)
    {
        if (branches.Count == 1)
        {
            return branches[0];
        }

        return new UnionAllPlan(branches, schema);
    }

    /// <summary>
    /// AST-level check for whether a query (recursively, through its
    /// subqueries and FROM joins) mentions <paramref name="name"/> as a
    /// table reference. Used to partition UNION ALL branches for a
    /// recursive CTE. Over-approximates when a same-named table or inner
    /// CTE shadows the outer recursive CTE — good enough for v1.
    /// </summary>
    private static bool QueryReferencesName(SqlQuery q, string name) => q switch
    {
        SelectStatement s =>
            FromReferencesName(s.From, name)
            || (s.Where is not null && ExpressionReferencesName(s.Where, name))
            || s.GroupBy.Any(g => ExpressionReferencesName(g, name))
            || (s.Having is not null && ExpressionReferencesName(s.Having, name))
            || s.Items.Any(i => SelectItemReferencesName(i, name))
            || s.Ctes.Any(c => QueryReferencesName(c.Query, name)),
        SetOpQuery u =>
            u.Branches.Any(b => QueryReferencesName(b, name))
            || u.Ctes.Any(c => QueryReferencesName(c.Query, name)),
        _ => false,
    };

    private static bool FromReferencesName(FromClause from, string name) => from switch
    {
        TableReference tr => string.Equals(tr.TableName, name, StringComparison.Ordinal),
        JoinClause jc =>
            FromReferencesName(jc.Left, name)
            || FromReferencesName(jc.Right, name)
            || (jc.OnCondition is not null && ExpressionReferencesName(jc.OnCondition, name)),
        _ => false,
    };

    private static bool SelectItemReferencesName(SelectItem item, string name) => item switch
    {
        ExpressionSelectItem e => ExpressionReferencesName(e.Expression, name),
        _ => false,
    };

    private static bool ExpressionReferencesName(Expression expr, string name) => expr switch
    {
        BinaryExpression b => ExpressionReferencesName(b.Left, name) || ExpressionReferencesName(b.Right, name),
        UnaryExpression u => ExpressionReferencesName(u.Operand, name),
        IsNullExpression isn => ExpressionReferencesName(isn.Operand, name),
        CastExpression c => ExpressionReferencesName(c.Operand, name),
        FunctionCallExpression fn => fn.Arguments.Any(a => ExpressionReferencesName(a, name)),
        SubqueryExpression sq => QueryReferencesName(sq.Query, name),
        InListExpression il => ExpressionReferencesName(il.Probe, name)
            || il.Values.Any(v => ExpressionReferencesName(v, name)),
        _ => false,
    };

    // ---------- Set operations (UNION [ALL] / INTERSECT / EXCEPT) ----------

    private LogicalPlan ResolveSetOp(
        SetOpQuery query,
        IReadOnlyDictionary<string, CteRef> outerCteScope)
    {
        if (query.Branches.Count < 2)
        {
            throw new ResolveException($"{KindName(query.Kind)} requires at least two branches");
        }

        // Query-level CTEs visible to every branch.
        Dictionary<string, CteRef>? cteScope = null;
        foreach (var cte in query.Ctes)
        {
            cteScope ??= new Dictionary<string, CteRef>(outerCteScope, StringComparer.Ordinal);
            if (cteScope.ContainsKey(cte.Name))
            {
                throw new ResolveException($"duplicate CTE name '{cte.Name}'");
            }

            cteScope[cte.Name] = cte.IsRecursive
                ? ResolveRecursiveCte(cte, cteScope)
                : new CteRef(cte.Name, ResolveQuery(cte.Query, cteScope));
        }

        IReadOnlyDictionary<string, CteRef> scope = cteScope ?? outerCteScope;

        // Resolve each branch and compute a shared unified schema (same rules
        // as UNION ALL — common comparable type per column, nullable if any
        // branch is nullable, names from the first branch).
        var branches = new List<LogicalPlan>(query.Branches.Count);
        foreach (var b in query.Branches)
        {
            branches.Add(ResolveQuery(b, scope));
        }

        var arity = branches[0].Schema.Count;
        for (var i = 1; i < branches.Count; i++)
        {
            if (branches[i].Schema.Count != arity)
            {
                throw new ResolveException(
                    $"{KindName(query.Kind)} branches must have the same column count (got {arity} vs {branches[i].Schema.Count})");
            }
        }

        var unifiedCols = new List<SchemaColumn>(arity);
        for (var col = 0; col < arity; col++)
        {
            var common = branches[0].Schema[col].Type;
            for (var b = 1; b < branches.Count; b++)
            {
                common = TypeInference.CommonComparableType(common, branches[b].Schema[col].Type);
            }

            unifiedCols.Add(new SchemaColumn(branches[0].Schema[col].Name, common, Qualifier: null));
        }

        var unifiedSchema = new Schema(unifiedCols);
        var aligned = new List<LogicalPlan>(branches.Count);
        foreach (var b in branches)
        {
            aligned.Add(AlignBranchToUnion(b, unifiedSchema));
        }

        return query.Kind switch
        {
            SetOpKind.UnionAll => new UnionAllPlan(aligned, unifiedSchema),
            SetOpKind.Union => new DistinctPlan(new UnionAllPlan(aligned, unifiedSchema)),
            SetOpKind.Intersect => BuildIntersect(aligned, unifiedSchema),
            SetOpKind.Except => BuildExcept(aligned, unifiedSchema),
            _ => throw new ResolveException($"unsupported set-op kind {query.Kind}"),
        };
    }

    /// <summary>A classified <c>ORDER BY</c> term. Exactly one of
    /// <see cref="OutputExpr"/> (resolved against the inner query's output —
    /// an ordinal, alias, or expression over selected columns) or
    /// <see cref="HiddenPos"/> ≥ 0 (a reference to a non-selected column,
    /// carried as a hidden projection at that position) is set.</summary>
    private readonly record struct OrderTerm(
        ResolvedExpression? OutputExpr, int HiddenPos, bool Descending, bool NullsFirst);

    /// <summary>
    /// Lower an <c>ORDER BY … LIMIT/OFFSET</c> wrapper to a <see cref="TopKPlan"/>.
    /// Each sort key resolves against the inner query's <b>output</b> first (a
    /// 1-based ordinal into the select list, an alias, or an expression over
    /// selected columns). A term that references a column <em>not</em> in the
    /// select list is carried as a hidden projection: the inner <c>SELECT</c> is
    /// re-resolved with the ordering expression appended (so it resolves against
    /// the FROM scope, with the resolver's normal aggregate / non-grouped-column
    /// rules), TOP-K orders by the hidden column, and a final projection strips
    /// it. A bare <c>ORDER BY</c> (no bound) is validated then discarded — row
    /// order is unobservable in the output Z-set, so it cannot affect the result.
    /// </summary>
    private LogicalPlan ResolveOrderLimit(
        OrderLimitQuery query,
        IReadOnlyDictionary<string, CteRef> outerCteScope,
        Schema? outerSchema)
    {
        // Query-level CTEs declared on this wrapper are visible to the inner query.
        Dictionary<string, CteRef>? cteScope = null;
        foreach (var cte in query.Ctes)
        {
            cteScope ??= new Dictionary<string, CteRef>(outerCteScope, StringComparer.Ordinal);
            if (cteScope.ContainsKey(cte.Name))
            {
                throw new ResolveException($"duplicate CTE name '{cte.Name}'");
            }

            cteScope[cte.Name] = cte.IsRecursive
                ? ResolveRecursiveCte(cte, cteScope)
                : new CteRef(cte.Name, ResolveQuery(cte.Query, cteScope));
        }

        IReadOnlyDictionary<string, CteRef> scope = cteScope ?? outerCteScope;
        var inner = ResolveQuery(query.Input, scope, outerSchema);
        var outputSchema = inner.Schema;

        // Classify each term against the output scope; collect the ASTs of the
        // ones that need a hidden column (resolved later against the FROM scope).
        var terms = new OrderTerm[query.OrderBy.Count];
        var hiddenAsts = new List<Parser.Ast.Expression>();
        for (var i = 0; i < query.OrderBy.Count; i++)
        {
            var item = query.OrderBy[i];
            var descending = item.Direction == SortDirection.Descending;
            var nullsFirst = item.Nulls switch
            {
                NullOrdering.NullsFirst => true,
                NullOrdering.NullsLast => false,
                // SQL default (PostgreSQL): NULL is the "largest" value — so it
                // sorts last under ASC and first under DESC.
                _ => descending,
            };

            if (item.Expression is LiteralExpression { Kind: LiteralKind.Integer, Value: long ordinal })
            {
                if (ordinal < 1 || ordinal > outputSchema.Count)
                {
                    throw new ResolveException(
                        $"ORDER BY position {ordinal} is out of range (1..{outputSchema.Count})");
                }

                var idx = (int)(ordinal - 1);
                terms[i] = new OrderTerm(
                    new ResolvedColumn(idx, outputSchema[idx].Type), -1, descending, nullsFirst);
                continue;
            }

            // Output scope first (alias / selected column / expression over them);
            // SQL resolves an ORDER BY name as an output alias ahead of a table
            // column. A term that fails here references a non-selected column.
            var outExpr = TryResolveAgainstOutput(item.Expression, outputSchema);
            if (outExpr is not null)
            {
                terms[i] = new OrderTerm(outExpr, -1, descending, nullsFirst);
            }
            else
            {
                terms[i] = new OrderTerm(null, hiddenAsts.Count, descending, nullsFirst);
                hiddenAsts.Add(item.Expression);
            }
        }

        // The plan TOP-K orders over: the inner plan, or — when some term needs a
        // non-selected column — the inner SELECT re-resolved with the ordering
        // expressions appended as hidden trailing columns.
        var basePlan = hiddenAsts.Count == 0
            ? inner
            : ResolveWithHiddenOrderColumns(query.Input, hiddenAsts, scope, outerSchema, outputSchema);
        var baseSchema = basePlan.Schema;

        var sortKeys = new List<SortKey>(terms.Length);
        foreach (var term in terms)
        {
            var keyExpr = term.OutputExpr;
            if (keyExpr is null)
            {
                var hiddenIdx = outputSchema.Count + term.HiddenPos;
                keyExpr = new ResolvedColumn(hiddenIdx, baseSchema[hiddenIdx].Type);
            }

            sortKeys.Add(new SortKey(keyExpr, term.Descending, term.NullsFirst));
        }

        // Bare ORDER BY (no LIMIT/OFFSET): the result set is identical to the
        // inner query — return it directly (the hidden columns, if any, were
        // built only to validate the ordering expressions and are discarded).
        if (query.Limit is null && query.Offset is null)
        {
            return inner;
        }

        var topK = new TopKPlan(basePlan, sortKeys, query.Limit, query.Offset);
        if (hiddenAsts.Count == 0)
        {
            return topK;
        }

        // Strip the hidden ORDER BY columns back to the user's select list.
        var projections = new List<ProjectionItem>(outputSchema.Count);
        for (var j = 0; j < outputSchema.Count; j++)
        {
            var col = outputSchema[j];
            projections.Add(new ProjectionItem(new ResolvedColumn(j, col.Type), col.Name, col.Qualifier));
        }

        return new ProjectPlan(topK, projections, outputSchema);
    }

    /// <summary>A FROM relation that has already been resolved to a plan. Used
    /// internally by <see cref="TryResolvePartitionedTopK"/> to re-enter
    /// <see cref="ResolveSelect"/> for the enclosing query's remaining clauses
    /// (projection, residual WHERE, GROUP BY, …) without re-resolving the
    /// rewritten FROM.</summary>
    private sealed record PreResolvedRelation(LogicalPlan Plan) : FromClause;

    /// <summary>
    /// Recognise the incremental partitioned TOP-K pattern —
    /// <c>SELECT … FROM (SELECT …, {ROW_NUMBER|RANK|DENSE_RANK}() OVER
    /// (PARTITION BY … ORDER BY …) AS rn FROM …) WHERE rn &lt;= k</c> — and lower
    /// it to a <see cref="PartitionedTopKPlan"/>. Returns <c>null</c> when
    /// <paramref name="stmt"/> is not a window query at all (ordinary resolution
    /// proceeds); throws a <see cref="ResolveException"/> when a window function
    /// is present but used outside the supported pattern.
    /// </summary>
    /// <remarks>
    /// The rank value is never materialised: the derived table exposes only its
    /// non-window columns, the rank alias is consumed by the <c>&lt;= k</c>
    /// filter, and the rest of the enclosing query is resolved normally over the
    /// TOP-K result via a <see cref="PreResolvedRelation"/>. Standard SQL forbids
    /// referencing a window alias in the same query's WHERE, so the derived-table
    /// spelling is the portable one; <c>QUALIFY</c> and selecting the rank value
    /// are deferred (see <c>docs/skipped.md</c>).
    /// </remarks>
    private LogicalPlan? TryResolvePartitionedTopK(
        SelectStatement stmt,
        IReadOnlyDictionary<string, CteRef> scope,
        Schema? outerSchema)
    {
        if (stmt.From is not DerivedTableReference { Query: SelectStatement innerSel } dt)
        {
            return null;
        }

        var windowItems = innerSel.Items
            .OfType<ExpressionSelectItem>()
            .Where(it => it.Expression is WindowFunctionExpression)
            .ToList();
        if (windowItems.Count == 0)
        {
            return null; // ordinary derived table.
        }

        if (windowItems.Count > 1)
        {
            throw new ResolveException("at most one window function per query is supported in v1");
        }

        var windowItem = windowItems[0];
        var win = (WindowFunctionExpression)windowItem.Expression;
        var rankAlias = windowItem.Alias ?? throw new ResolveException(
            "a window function must be aliased so the TOP-K filter can reference it " +
            "(e.g. ROW_NUMBER() OVER (...) AS rn)");

        var func = win.FunctionName switch
        {
            "row_number" => RankFunction.RowNumber,
            "rank" => RankFunction.Rank,
            "dense_rank" => RankFunction.DenseRank,
            _ => throw new ResolveException(
                $"unsupported window function '{win.FunctionName}'; only ROW_NUMBER, RANK, " +
                "and DENSE_RANK are supported (window aggregates, LAG/LEAD, etc. are deferred)"),
        };

        if (win.Over.OrderBy.Count == 0)
        {
            throw new ResolveException(
                $"{win.FunctionName.ToUpperInvariant()}() OVER (...) requires an ORDER BY");
        }

        // Build the inner SELECT without the window item; everything below treats
        // it as the derived table's real (rank-free) shape.
        var innerItems = innerSel.Items.Where(it => !ReferenceEquals(it, windowItem)).ToList();
        if (innerItems.Count == 0)
        {
            throw new ResolveException(
                "the windowed derived table must select at least one column besides the ranking function");
        }

        var innerNoWindow = innerSel with { Items = innerItems };
        if (innerSel.Distinct)
        {
            throw new ResolveException("a window function over SELECT DISTINCT is not supported in v1");
        }

        if (innerSel.GroupBy.Count > 0 || HasAggregate(innerNoWindow.Items)
            || (innerSel.Having is not null && HasAggregate(innerSel.Having)))
        {
            throw new ResolveException(
                "a window function over a grouped/aggregated query is not supported in v1");
        }

        // The enclosing query must filter the rank alias with `< k` / `<= k`
        // (or the reversed `k >= rn` / `k > rn`) and use it nowhere else.
        if (stmt.Where is null)
        {
            throw new ResolveException(
                "a ranking window function requires a `" + rankAlias + " <= k` filter in the " +
                "enclosing query (the incremental TOP-K pattern)");
        }

        long? limit = null;
        var residual = new List<Expression>();
        foreach (var c in SplitAndConjuncts(stmt.Where))
        {
            if (limit is null && TryMatchRankFilter(c, rankAlias, dt.Alias, out var k))
            {
                limit = k;
            }
            else
            {
                residual.Add(c);
            }
        }

        if (limit is null)
        {
            throw new ResolveException(
                "a ranking window function requires a `" + rankAlias + " <= k` (or `< k`) filter in " +
                "the enclosing query; other comparisons on the rank are not the TOP-K pattern");
        }

        if (residual.Any(c => ExprReferencesRank(c, rankAlias, dt.Alias))
            || stmt.Items.Any(it => it is ExpressionSelectItem esi && ExprReferencesRank(esi.Expression, rankAlias, dt.Alias))
            || stmt.GroupBy.Any(g => ExprReferencesRank(g, rankAlias, dt.Alias))
            || (stmt.Having is not null && ExprReferencesRank(stmt.Having, rankAlias, dt.Alias)))
        {
            throw new ResolveException(
                "the window rank column may only be used in the TOP-K filter; selecting or otherwise " +
                "referencing it is not supported in v1");
        }

        // Resolve the rank-free inner query (the derived table's visible schema),
        // then re-resolve it with the PARTITION BY / ORDER BY expressions appended
        // as hidden trailing columns so they resolve against the inner FROM scope.
        var visiblePlan = ResolveQuery(innerNoWindow, scope, outerSchema);
        var visibleSchema = visiblePlan.Schema;

        var hiddenAsts = new List<Expression>(win.Over.PartitionBy.Count + win.Over.OrderBy.Count);
        hiddenAsts.AddRange(win.Over.PartitionBy);
        foreach (var s in win.Over.OrderBy)
        {
            hiddenAsts.Add(s.Expression);
        }

        var basePlan = ResolveWithHiddenOrderColumns(
            innerNoWindow, hiddenAsts, scope, outerSchema, visibleSchema);
        var baseSchema = basePlan.Schema;

        var partitionKeys = new List<ResolvedExpression>(win.Over.PartitionBy.Count);
        for (var j = 0; j < win.Over.PartitionBy.Count; j++)
        {
            var idx = visibleSchema.Count + j;
            partitionKeys.Add(new ResolvedColumn(idx, baseSchema[idx].Type));
        }

        var sortKeys = new List<SortKey>(win.Over.OrderBy.Count);
        for (var j = 0; j < win.Over.OrderBy.Count; j++)
        {
            var item = win.Over.OrderBy[j];
            var idx = visibleSchema.Count + win.Over.PartitionBy.Count + j;
            var descending = item.Direction == SortDirection.Descending;
            var nullsFirst = item.Nulls switch
            {
                NullOrdering.NullsFirst => true,
                NullOrdering.NullsLast => false,
                _ => descending,
            };
            sortKeys.Add(new SortKey(new ResolvedColumn(idx, baseSchema[idx].Type), descending, nullsFirst));
        }

        var topk = new PartitionedTopKPlan(basePlan, partitionKeys, sortKeys, func, limit.Value);

        // Strip the hidden columns back to the derived table's visible schema,
        // re-qualified with the derived alias (mirrors ResolveDerivedTable).
        var stripCols = new List<SchemaColumn>(visibleSchema.Count);
        var stripProj = new List<ProjectionItem>(visibleSchema.Count);
        for (var i = 0; i < visibleSchema.Count; i++)
        {
            var col = visibleSchema[i];
            stripCols.Add(new SchemaColumn(col.Name, col.Type, dt.Alias));
            stripProj.Add(new ProjectionItem(new ResolvedColumn(i, col.Type), col.Name, dt.Alias));
        }

        var topkRelation = new ProjectPlan(topk, stripProj, new Schema(stripCols));

        // Re-enter ResolveSelect for the enclosing query's remaining clauses over
        // the TOP-K result. CTEs were already folded into `scope`, so clear them
        // and pass `scope` as the outer scope to avoid re-resolving them.
        var rewritten = stmt with
        {
            From = new PreResolvedRelation(topkRelation),
            Where = residual.Count == 0 ? null : JoinAnd(residual),
            Ctes = System.Array.Empty<CteDefinition>(),
        };

        return ResolveSelect(rewritten, scope, outerSchema);
    }

    /// <summary>
    /// Match a rank-filter conjunct <c>rank &lt;= k</c> / <c>rank &lt; k</c> / <c>rank = 1</c>
    /// (or the reversed <c>k &gt;= rank</c> / <c>k &gt; rank</c> / <c>1 = rank</c>) where
    /// <c>rank</c> is a column reference to <paramref name="rankAlias"/> (optionally
    /// qualified by the derived alias) and <c>k</c> is an integer literal. Yields the
    /// TOP-K <paramref name="limit"/> (<c>&lt; k</c> ⇒ <c>k − 1</c>).
    ///
    /// Equality matches only at <c>k = 1</c>. Ranks start at 1, so <c>rank = 1</c> is
    /// exactly <c>rank &lt;= 1</c> (true for RANK/DENSE_RANK under ties as well). For
    /// <c>k &gt; 1</c> the two diverge — <c>rank = 3</c> selects the third rank alone,
    /// which TOP-K cannot express — so those fall through unmatched and are reported
    /// as an unsupported rank use.
    /// </summary>
    private static bool TryMatchRankFilter(Expression c, string rankAlias, string dtAlias, out long limit)
    {
        limit = 0;
        if (c is not BinaryExpression be)
        {
            return false;
        }

        var leftIsRank = IsRankRef(be.Left, rankAlias, dtAlias);
        var rightIsRank = IsRankRef(be.Right, rankAlias, dtAlias);
        if (leftIsRank == rightIsRank)
        {
            return false; // need exactly one side to be the rank column.
        }

        // Normalise to `rank <op> kExpr`.
        var op = leftIsRank ? be.Operator : FlipComparison(be.Operator);
        var kExpr = leftIsRank ? be.Right : be.Left;
        if (kExpr is not LiteralExpression { Kind: LiteralKind.Integer, Value: long k })
        {
            return false;
        }

        switch (op)
        {
            case BinaryOperator.LessEqual:
                limit = k;
                return true;
            case BinaryOperator.Less:
                limit = k - 1;
                return true;
            case BinaryOperator.Equal when k == 1:
                limit = 1;
                return true;
            default:
                return false;
        }
    }

    private static bool IsRankRef(Expression e, string rankAlias, string dtAlias) =>
        e is ColumnReference cr && cr.Name == rankAlias
        && (cr.Qualifier is null || cr.Qualifier == dtAlias);

    private static BinaryOperator FlipComparison(BinaryOperator op) => op switch
    {
        BinaryOperator.Less => BinaryOperator.Greater,
        BinaryOperator.LessEqual => BinaryOperator.GreaterEqual,
        BinaryOperator.Greater => BinaryOperator.Less,
        BinaryOperator.GreaterEqual => BinaryOperator.LessEqual,
        _ => op,
    };

    /// <summary>Conservative scan for a reference to the rank alias. A missed
    /// node type simply falls through to ordinary resolution, which then fails
    /// with "unknown column" (the rank column isn't in the visible schema) — so
    /// this only needs to be complete enough for a friendlier error.</summary>
    private static bool ExprReferencesRank(Expression e, string rankAlias, string dtAlias) => e switch
    {
        ColumnReference => IsRankRef(e, rankAlias, dtAlias),
        BinaryExpression be => ExprReferencesRank(be.Left, rankAlias, dtAlias) || ExprReferencesRank(be.Right, rankAlias, dtAlias),
        UnaryExpression u => ExprReferencesRank(u.Operand, rankAlias, dtAlias),
        IsNullExpression isn => ExprReferencesRank(isn.Operand, rankAlias, dtAlias),
        CastExpression cast => ExprReferencesRank(cast.Operand, rankAlias, dtAlias),
        FunctionCallExpression fn => fn.Arguments.Any(a => ExprReferencesRank(a, rankAlias, dtAlias)),
        InListExpression il => ExprReferencesRank(il.Probe, rankAlias, dtAlias) || il.Values.Any(v => ExprReferencesRank(v, rankAlias, dtAlias)),
        CaseExpression ce => ce.Whens.Any(w => ExprReferencesRank(w.Condition, rankAlias, dtAlias) || ExprReferencesRank(w.Result, rankAlias, dtAlias))
            || (ce.ElseResult is not null && ExprReferencesRank(ce.ElseResult, rankAlias, dtAlias)),
        _ => false,
    };

    private static ResolvedExpression? TryResolveAgainstOutput(
        Parser.Ast.Expression expr, Schema outputSchema)
    {
        try
        {
            return ResolveScalarExpression(expr, outputSchema);
        }
        catch (ResolveException)
        {
            // Not resolvable against the output — caller pushes it as a hidden
            // column resolved against the inner query's FROM scope instead.
            return null;
        }
    }

    /// <summary>
    /// Re-resolve the inner <c>SELECT</c> with the <paramref name="hiddenAsts"/>
    /// ordering expressions appended as trailing <c>__orderby_k</c> columns, so
    /// they resolve against the FROM scope. Only a single (non-<c>DISTINCT</c>)
    /// <c>SELECT</c> can carry hidden columns: <c>DISTINCT</c> forbids ordering by
    /// a non-selected column (the value is ambiguous after dedup), and a set
    /// operation's <c>ORDER BY</c> may reference only output columns. Aggregate /
    /// non-grouped-column rules are enforced naturally by the re-resolution.
    /// </summary>
    private LogicalPlan ResolveWithHiddenOrderColumns(
        SqlQuery input,
        List<Parser.Ast.Expression> hiddenAsts,
        IReadOnlyDictionary<string, CteRef> scope,
        Schema? outerSchema,
        Schema outputSchema)
    {
        if (input is not SelectStatement select)
        {
            throw new ResolveException(
                "ORDER BY of a set operation may reference only its output columns " +
                "(a selected column, alias, or ordinal)");
        }

        if (select.Distinct)
        {
            throw new ResolveException(
                "for SELECT DISTINCT, ORDER BY items must appear in the select list");
        }

        var items = new List<SelectItem>(select.Items.Count + hiddenAsts.Count);
        items.AddRange(select.Items);
        for (var k = 0; k < hiddenAsts.Count; k++)
        {
            items.Add(new ExpressionSelectItem(hiddenAsts[k], $"__orderby_{k}"));
        }

        var augmented = ResolveQuery(select with { Items = items }, scope, outerSchema);

        // The hidden columns must land immediately after the original output so
        // the strip projection and hidden-index math hold.
        if (augmented.Schema.Count != outputSchema.Count + hiddenAsts.Count)
        {
            throw new ResolveException(
                "ORDER BY over non-selected columns is not supported for this query shape");
        }

        return augmented;
    }

    /// <summary>
    /// Recognise window aggregates — <c>SUM/COUNT/AVG/MIN/MAX(x) OVER
    /// (PARTITION BY p [ORDER BY o [RANGE …]])</c> appearing as top-level select
    /// items — and lower them to a <see cref="WindowAggregatePlan"/> followed by a
    /// projection that maps the user's select list. Returns <c>null</c> when the
    /// query has no window function at all (ordinary resolution proceeds); throws
    /// a <see cref="ResolveException"/> when a window function is present but used
    /// outside the supported shape.
    /// </summary>
    /// <remarks>
    /// The pre-window relation (<c>FROM</c> + <c>WHERE</c>) is resolved as a star
    /// query, so partition / order / argument expressions resolve against the full
    /// source schema and the operator can widen each row. Standard SQL evaluates
    /// window functions after <c>WHERE</c> and before <c>ORDER BY</c> / <c>LIMIT</c>
    /// (the latter ride the enclosing <c>OrderLimitQuery</c>, so they wrap this
    /// result naturally).
    /// </remarks>
    private LogicalPlan? TryResolveWindowAggregate(
        SelectStatement stmt,
        IReadOnlyDictionary<string, CteRef> scope,
        Schema? outerSchema)
    {
        var windowItems = stmt.Items
            .OfType<ExpressionSelectItem>()
            .Where(it => it.Expression is WindowFunctionExpression)
            .ToList();

        var mentionedElsewhere =
            (stmt.Where is not null && MentionsWindowFunction(stmt.Where))
            || stmt.GroupBy.Any(MentionsWindowFunction)
            || (stmt.Having is not null && MentionsWindowFunction(stmt.Having))
            || stmt.Items.Any(it => it is ExpressionSelectItem esi
                && esi.Expression is not WindowFunctionExpression
                && MentionsWindowFunction(esi.Expression));

        if (windowItems.Count == 0 && !mentionedElsewhere)
        {
            return null; // ordinary query — no window function anywhere.
        }

        if ((stmt.Where is not null && MentionsWindowFunction(stmt.Where))
            || stmt.GroupBy.Any(MentionsWindowFunction)
            || (stmt.Having is not null && MentionsWindowFunction(stmt.Having)))
        {
            throw new ResolveException("window functions are not allowed in WHERE / GROUP BY / HAVING");
        }

        if (stmt.Items.Any(it => it is ExpressionSelectItem esi
            && esi.Expression is not WindowFunctionExpression
            && MentionsWindowFunction(esi.Expression)))
        {
            throw new ResolveException(
                "a window function must be a top-level select item in v1 (it cannot be nested in an expression)");
        }

        // Every top-level window item must be a supported window function — a
        // window aggregate (SUM/COUNT/AVG/MIN/MAX) or an offset function
        // (LAG/LEAD/FIRST_VALUE/LAST_VALUE). Ranking functions ride the TOP-K
        // pattern. The items are grouped below by family and OVER specification;
        // each distinct group becomes its own operator, so a single query may
        // carry several different OVER specs and freely mix aggregates with
        // LAG / LEAD.
        foreach (var wi in windowItems)
        {
            var w = (WindowFunctionExpression)wi.Expression;
            if (!IsAggregateName(w.FunctionName, w.IsStar) && !IsOffsetFunctionName(w.FunctionName))
            {
                throw new ResolveException(
                    $"window function '{w.FunctionName}' is not supported as an output column; only the " +
                    "window aggregates SUM / COUNT / AVG / MIN / MAX and the offset functions LAG / LEAD / " +
                    "FIRST_VALUE / LAST_VALUE are (ranking functions ROW_NUMBER / RANK / DENSE_RANK are " +
                    "supported only in the TOP-K filter pattern)");
            }
        }

        if (stmt.GroupBy.Count > 0 || stmt.Having is not null
            || stmt.Items.Any(it => it is ExpressionSelectItem esi
                && esi.Expression is not WindowFunctionExpression && HasAggregate(esi.Expression)))
        {
            throw new ResolveException(
                "a window function over a grouped / aggregated query is not supported in v1");
        }

        if (stmt.Items.Any(it => it is StarSelectItem))
        {
            throw new ResolveException(
                "SELECT * is not allowed with a window function; list the output columns explicitly");
        }

        // Resolve the pre-window relation (FROM + WHERE) as a star query so the
        // partition / order / argument expressions resolve against the full
        // source schema and the operator can compute them per row.
        var baseQuery = new SelectStatement(
            Items: new SelectItem[] { new StarSelectItem(null) },
            From: stmt.From,
            Where: stmt.Where,
            GroupBy: System.Array.Empty<Expression>(),
            Having: null,
            Ctes: System.Array.Empty<CteDefinition>(),
            Distinct: false);
        var basePlan = ResolveQuery(baseQuery, scope, outerSchema);
        var baseSchema = basePlan.Schema;

        // Group window items by (family, OVER spec), preserving first-occurrence
        // order. Items in one group share a single operator (multiple result
        // columns); each distinct group is chained on top of the previous,
        // widening every row it receives.
        var groups = new List<List<ExpressionSelectItem>>();
        foreach (var wi in windowItems)
        {
            var w = (WindowFunctionExpression)wi.Expression;
            var off = IsOffsetFunctionName(w.FunctionName);
            var group = groups.FirstOrDefault(g =>
            {
                var head = (WindowFunctionExpression)g[0].Expression;
                return IsOffsetFunctionName(head.FunctionName) == off
                    && SameWindowSpec(head.Over, w.Over);
            });

            if (group is null)
            {
                group = new List<ExpressionSelectItem>();
                groups.Add(group);
            }

            group.Add(wi);
        }

        // Chain one window operator per group. `preBound` maps each window
        // expression to the (absolute) result column it lands in; `synth` keeps
        // synthetic column names unique across the chain.
        var preBound = new Dictionary<Expression, ResolvedExpression>(ReferenceEqualityComparer.Instance);
        var synth = 0;
        LogicalPlan windowPlan = basePlan;
        foreach (var group in groups)
        {
            windowPlan = BuildWindowGroup(group, windowPlan, baseSchema, preBound, ref synth);
        }

        // Map the user's select list over the widened rows: non-window items
        // resolve against the base columns (the schema prefix); window items
        // resolve to their result column via `preBound`.
        var projections = ResolveProjections(stmt.Items, windowPlan.Schema, subqueryMap: null, preBound);
        LogicalPlan projected = new ProjectPlan(windowPlan, projections, BuildProjectSchema(projections));
        return stmt.Distinct ? new DistinctPlan(projected) : projected;
    }

    /// <summary>
    /// Lower one group of window items — all sharing a family (aggregate vs
    /// offset) and OVER specification — into a single <see cref="WindowAggregatePlan"/>
    /// or <see cref="WindowOffsetPlan"/> chained on <paramref name="inputPlan"/>.
    /// Partition / order / argument expressions resolve against
    /// <paramref name="baseSchema"/> (the pre-window columns, which remain an
    /// unchanging prefix of every widened row), so chained groups all see the same
    /// base column indices. Each item's result column is recorded in
    /// <paramref name="preBound"/> at its absolute index in the widened schema, and
    /// <paramref name="synth"/> advances so synthetic column names stay unique.
    /// </summary>
    private LogicalPlan BuildWindowGroup(
        List<ExpressionSelectItem> group,
        LogicalPlan inputPlan,
        Schema baseSchema,
        Dictionary<Expression, ResolvedExpression> preBound,
        ref int synth)
    {
        var firstWin = (WindowFunctionExpression)group[0].Expression;
        var isOffset = IsOffsetFunctionName(firstWin.FunctionName);

        // PARTITION BY.
        var partitionKeys = new List<ResolvedExpression>(firstWin.Over.PartitionBy.Count);
        foreach (var p in firstWin.Over.PartitionBy)
        {
            partitionKeys.Add(ResolveScalarExpression(p, baseSchema));
        }

        // ORDER BY. The offset family (LAG / LEAD / FIRST_VALUE / LAST_VALUE) takes
        // any number of keys — it only ever *compares* rows. Window aggregates are
        // held to one key by the aggregate branch below, because a bounded RANGE
        // frame does arithmetic (`value - preceding`) on a single scalar.
        var orderKeys = new List<SortKey>(firstWin.Over.OrderBy.Count);
        foreach (var item in firstWin.Over.OrderBy)
        {
            var resolved = ResolveScalarExpression(item.Expression, baseSchema);
            var descending = item.Direction == SortDirection.Descending;
            var nullsFirst = item.Nulls switch
            {
                NullOrdering.NullsFirst => true,
                NullOrdering.NullsLast => false,
                _ => descending,
            };
            orderKeys.Add(new SortKey(resolved, descending, nullsFirst));
        }

        var inputSchema = inputPlan.Schema;
        var resultCols = new List<SchemaColumn>(group.Count);

        if (isOffset)
        {
            if (orderKeys.Count == 0)
            {
                throw new ResolveException("LAG / LEAD / FIRST_VALUE / LAST_VALUE requires an ORDER BY");
            }

            if (firstWin.Over.Frame is not null)
            {
                throw new ResolveException(
                    "LAG / LEAD / FIRST_VALUE / LAST_VALUE does not take a frame clause in v1 " +
                    "(FIRST_VALUE / LAST_VALUE span the whole partition — UNLIMITED RANGE)");
            }

            var functions = new List<OffsetFunctionCall>(group.Count);
            for (var k = 0; k < group.Count; k++)
            {
                var wi = group[k];
                var w = (WindowFunctionExpression)wi.Expression;
                var (fn, resultType) = ResolveOffsetFunction(w, baseSchema);
                functions.Add(fn);

                var (name, _) = DeriveProjectionName(w, wi.Alias);
                resultCols.Add(new SchemaColumn(wi.Alias ?? (name == "$col" ? $"$woff{synth}" : name), resultType));
                preBound[wi.Expression] = new ResolvedColumn(inputSchema.Count + k, resultType);
                synth++;
            }

            var offsetSchema = new Schema([.. inputSchema.Columns, .. resultCols]);
            return new WindowOffsetPlan(inputPlan, partitionKeys, orderKeys, functions, offsetSchema);
        }
        else
        {
            WindowFrameBounds? frame = null;
            SortKey? orderKey = null;
            if (orderKeys.Count > 0)
            {
                if (orderKeys.Count > 1)
                {
                    throw new ResolveException(
                        "a window aggregate supports a single ORDER BY key in v1 (a bounded RANGE " +
                        "frame measures distance along one scalar key); LAG / LEAD / FIRST_VALUE / " +
                        "LAST_VALUE accept multiple keys");
                }

                orderKey = orderKeys[0];
                if (orderKey.Expression.Type is not (SqlIntegerType or SqlBigintType
                    or SqlDateType or SqlTimeType or SqlTimestampType))
                {
                    throw new ResolveException(
                        "an ordered window aggregate requires an integer (INT/BIGINT) or temporal " +
                        "(DATE/TIME/TIMESTAMP) ORDER BY key in v1");
                }

                frame = ResolveWindowFrame(firstWin.Over.Frame, orderKey.Expression.Type, baseSchema);
            }
            else if (firstWin.Over.Frame is not null)
            {
                throw new ResolveException("a window frame (RANGE) requires an ORDER BY");
            }

            var aggregates = new List<AggregateCall>(group.Count);
            for (var k = 0; k < group.Count; k++)
            {
                var wi = group[k];
                var w = (WindowFunctionExpression)wi.Expression;
                var asCall = new FunctionCallExpression(w.FunctionName, w.Arguments, w.IsStar);
                var kind = ToAggregateKind(asCall);
                if (kind == AggregateKind.ApproxPercentile)
                {
                    throw new ResolveException(
                        $"{w.FunctionName.ToUpperInvariant()} is not supported as a window function");
                }

                ResolvedExpression? arg = null;
                if (kind != AggregateKind.CountStar)
                {
                    if (w.Arguments.Count != 1)
                    {
                        throw new ResolveException(
                            $"{w.FunctionName.ToUpperInvariant()} OVER takes exactly one argument");
                    }

                    arg = ResolveScalarExpression(w.Arguments[0], baseSchema);
                }

                var resultType = ComputeAggregateResultType(kind, arg?.Type);
                aggregates.Add(new AggregateCall(kind, arg, resultType));

                var (name, _) = DeriveProjectionName(w, wi.Alias);
                resultCols.Add(new SchemaColumn(wi.Alias ?? (name == "$col" ? $"$wagg{synth}" : name), resultType));
                preBound[wi.Expression] = new ResolvedColumn(inputSchema.Count + k, resultType);
                synth++;
            }

            var windowSchema = new Schema([.. inputSchema.Columns, .. resultCols]);
            return new WindowAggregatePlan(inputPlan, partitionKeys, orderKey, frame, aggregates, windowSchema);
        }
    }

    /// <summary>Resolve a parsed window <see cref="WindowFrame"/> into
    /// <see cref="WindowFrameBounds"/> (the lower bound in the ORDER BY key's
    /// native units; the upper bound is always <c>CURRENT ROW</c> in v1). A
    /// <c>null</c> frame defaults to <c>RANGE UNBOUNDED PRECEDING AND CURRENT
    /// ROW</c> (a running aggregate).</summary>
    private static WindowFrameBounds ResolveWindowFrame(WindowFrame? frame, SqlType orderType, Schema schema)
    {
        if (frame is null)
        {
            return new WindowFrameBounds(Preceding: null); // running (UNBOUNDED PRECEDING .. CURRENT ROW).
        }

        if (frame.Mode != WindowFrameMode.Range)
        {
            throw new ResolveException(
                $"{frame.Mode.ToString().ToUpperInvariant()} frames are not supported in v1; only RANGE is");
        }

        if (frame.End.Kind != FrameBoundKind.CurrentRow)
        {
            throw new ResolveException(
                "a window frame must end at CURRENT ROW in v1 (FOLLOWING / UNBOUNDED FOLLOWING bounds are deferred)");
        }

        return frame.Start.Kind switch
        {
            FrameBoundKind.UnboundedPreceding => new WindowFrameBounds(Preceding: null),
            FrameBoundKind.CurrentRow => new WindowFrameBounds(Preceding: 0),
            FrameBoundKind.Preceding => new WindowFrameBounds(
                ResolveRangeOffset(frame.Start.Offset!, orderType, schema)),
            _ => throw new ResolveException(
                "a window frame start must be UNBOUNDED PRECEDING, <n> PRECEDING, or CURRENT ROW in v1"),
        };
    }

    /// <summary>Evaluate a constant <c>PRECEDING</c> offset to a non-negative
    /// <c>long</c> in <paramref name="orderType"/>'s native units — an integer for
    /// <c>INT</c>/<c>BIGINT</c> keys, a day-time <c>INTERVAL</c> (µs, or whole days
    /// for a <c>DATE</c> key) for temporal keys.</summary>
    private static long ResolveRangeOffset(Expression offsetAst, SqlType orderType, Schema schema)
    {
        var resolved = ResolveScalarExpression(offsetAst, schema);
        if (resolved is not ResolvedLiteral lit)
        {
            throw new ResolveException("a RANGE frame offset must be a constant");
        }

        switch (orderType)
        {
            case SqlDateType:
                {
                    if (lit.Value is not Interval iv || iv.Months != 0)
                    {
                        throw new ResolveException(
                            "a RANGE frame over a DATE ORDER BY key requires a day-time INTERVAL offset " +
                            "(month/year intervals are not a constant size)");
                    }

                    if (iv.Micros % Interval.MicrosPerDay != 0)
                    {
                        throw new ResolveException(
                            "a RANGE frame offset over a DATE key must be a whole number of days");
                    }

                    return NonNegativeOffset(iv.Micros / Interval.MicrosPerDay);
                }

            case SqlTimestampType:
            case SqlTimeType:
                {
                    if (lit.Value is not Interval iv || iv.Months != 0)
                    {
                        throw new ResolveException(
                            "a RANGE frame over a TIMESTAMP/TIME ORDER BY key requires a day-time INTERVAL " +
                            "offset (DAY / HOUR / MINUTE / SECOND)");
                    }

                    return NonNegativeOffset(iv.Micros);
                }

            case SqlIntegerType:
            case SqlBigintType:
                return lit.Value switch
                {
                    long n => NonNegativeOffset(n),
                    int ni => NonNegativeOffset(ni),
                    _ => throw new ResolveException(
                        "a RANGE frame offset over an integer ORDER BY key must be an integer constant"),
                };

            default:
                throw new ResolveException(
                    "a bounded RANGE frame requires an integer (INT/BIGINT) or temporal (DATE/TIME/TIMESTAMP) " +
                    "ORDER BY key");
        }
    }

    private static long NonNegativeOffset(long v) =>
        v >= 0 ? v : throw new ResolveException("a RANGE frame PRECEDING offset must be non-negative");

    private static bool IsOffsetFunctionName(string name) =>
        name is "lag" or "lead" or "first_value" or "last_value";

    /// <summary>Fold a resolved constant — a literal or a unary negation of a
    /// numeric literal — to its value. Used for LAG/LEAD constant offsets and
    /// defaults (a bare <c>-1</c> resolves to <c>Negate(1)</c>, not a literal).</summary>
    private static bool TryFoldConstant(ResolvedExpression e, out object? value)
    {
        switch (e)
        {
            case ResolvedLiteral lit:
                value = lit.Value;
                return true;
            case ResolvedUnary { Operator: UnaryOperator.Negate, Operand: ResolvedLiteral lit }:
                value = lit.Value switch
                {
                    int i => -i,
                    long l => -l,
                    double d => -d,
                    _ => null,
                };
                return value is not null;
            default:
                value = null;
                return false;
        }
    }

    /// <summary>Resolve a <c>LAG</c> / <c>LEAD</c> call —
    /// <c>fn(value [, offset [, default]])</c> — against the pre-window schema.
    /// The offset is a non-negative integer constant (default 1); the default is a
    /// constant returned when the offset row falls outside the partition (default
    /// NULL). The result type is the value's type, made nullable.</summary>
    private static (OffsetFunctionCall Call, SqlType ResultType) ResolveOffsetFunction(
        WindowFunctionExpression w, Schema baseSchema)
    {
        var fnUpper = w.FunctionName.ToUpperInvariant();
        var kind = w.FunctionName switch
        {
            "lag" => OffsetKind.Lag,
            "lead" => OffsetKind.Lead,
            "first_value" => OffsetKind.FirstValue,
            "last_value" => OffsetKind.LastValue,
            _ => throw new ResolveException($"unsupported offset function '{w.FunctionName}'"),
        };

        long offset = 1;
        object? def = null;

        if (kind is OffsetKind.Lag or OffsetKind.Lead)
        {
            if (w.IsStar || w.Arguments.Count is < 1 or > 3)
            {
                throw new ResolveException(
                    $"{fnUpper} takes 1 to 3 arguments (value [, offset [, default]])");
            }

            if (w.Arguments.Count >= 2)
            {
                if (!TryFoldConstant(ResolveScalarExpression(w.Arguments[1], baseSchema), out var ov)
                    || ov is not (int or long))
                {
                    throw new ResolveException($"the {fnUpper} offset must be a constant integer");
                }

                offset = ov is int oi ? oi : (long)ov;
                if (offset < 0)
                {
                    throw new ResolveException($"the {fnUpper} offset must be non-negative");
                }
            }

            if (w.Arguments.Count == 3
                && !TryFoldConstant(ResolveScalarExpression(w.Arguments[2], baseSchema), out def))
            {
                throw new ResolveException($"the {fnUpper} default must be a constant");
            }
        }
        else
        {
            // FIRST_VALUE / LAST_VALUE — exactly one argument; the partition's
            // first / last row always exists, so there is no offset or default.
            if (w.IsStar || w.Arguments.Count != 1)
            {
                throw new ResolveException($"{fnUpper} takes exactly one argument");
            }
        }

        var value = ResolveScalarExpression(w.Arguments[0], baseSchema);
        var resultType = value.Type.WithNullable(true);
        return (new OffsetFunctionCall(value, offset, kind, def, resultType), resultType);
    }

    private static bool MentionsWindowFunction(Expression e) => e switch
    {
        WindowFunctionExpression => true,
        BinaryExpression b => MentionsWindowFunction(b.Left) || MentionsWindowFunction(b.Right),
        UnaryExpression u => MentionsWindowFunction(u.Operand),
        IsNullExpression isn => MentionsWindowFunction(isn.Operand),
        CastExpression c => MentionsWindowFunction(c.Operand),
        FunctionCallExpression f => f.Arguments.Any(MentionsWindowFunction),
        InListExpression il => MentionsWindowFunction(il.Probe) || il.Values.Any(MentionsWindowFunction),
        CaseExpression ce =>
            ce.Whens.Any(w => MentionsWindowFunction(w.Condition) || MentionsWindowFunction(w.Result))
            || (ce.ElseResult is not null && MentionsWindowFunction(ce.ElseResult)),
        _ => false,
    };

    /// <summary>Structural equality of two <see cref="WindowSpec"/>s — used to
    /// require that all window aggregates in one query share a single OVER
    /// specification (v1 emits one operator per query).</summary>
    private static bool SameWindowSpec(WindowSpec a, WindowSpec b)
    {
        if (a.PartitionBy.Count != b.PartitionBy.Count || a.OrderBy.Count != b.OrderBy.Count)
        {
            return false;
        }

        for (var i = 0; i < a.PartitionBy.Count; i++)
        {
            if (!AstEqual(a.PartitionBy[i], b.PartitionBy[i]))
            {
                return false;
            }
        }

        for (var i = 0; i < a.OrderBy.Count; i++)
        {
            if (a.OrderBy[i].Direction != b.OrderBy[i].Direction
                || a.OrderBy[i].Nulls != b.OrderBy[i].Nulls
                || !AstEqual(a.OrderBy[i].Expression, b.OrderBy[i].Expression))
            {
                return false;
            }
        }

        return SameFrame(a.Frame, b.Frame);
    }

    private static bool SameFrame(WindowFrame? a, WindowFrame? b)
    {
        if (a is null || b is null)
        {
            return a is null && b is null;
        }

        return a.Mode == b.Mode
            && SameBound(a.Start, b.Start)
            && SameBound(a.End, b.End);
    }

    private static bool SameBound(FrameBound a, FrameBound b) =>
        a.Kind == b.Kind
        && ((a.Offset is null && b.Offset is null) || (a.Offset is not null && b.Offset is not null && AstEqual(a.Offset, b.Offset)));

    private static string KindName(SetOpKind k) => k switch
    {
        SetOpKind.UnionAll => "UNION ALL",
        SetOpKind.Union => "UNION",
        SetOpKind.Intersect => "INTERSECT",
        SetOpKind.Except => "EXCEPT",
        _ => k.ToString(),
    };

    /// <summary>
    /// Build <c>a INTERSECT b [INTERSECT c …]</c> via left-fold: each pair
    /// becomes an all-column equi-join of the two sides' Distinct
    /// projections, projecting back to a single copy of the shared schema.
    /// </summary>
    private static LogicalPlan BuildIntersect(List<LogicalPlan> aligned, Schema schema)
    {
        var result = new DistinctPlan(aligned[0]);
        for (var i = 1; i < aligned.Count; i++)
        {
            result = (DistinctPlan)WrapPairIntersect(result, new DistinctPlan(aligned[i]), schema);
        }

        return result;
    }

    // INTERSECT(leftDistinct, rightDistinct) =
    //   Project(Join(leftDistinct, rightDistinct, all-col-equi, allowNulls=true),
    //           first-half-cols)
    // The result is already distinct (each pair appears once), but wrap in
    // DistinctPlan anyway so the output is set-shape for chaining.
    private static LogicalPlan WrapPairIntersect(LogicalPlan left, LogicalPlan right, Schema schema)
    {
        var equiKeys = new List<JoinEquality>(schema.Count);
        for (var i = 0; i < schema.Count; i++)
        {
            equiKeys.Add(new JoinEquality(i, i, schema[i].Type));
        }

        var joinedSchema = schema.Concat(schema);
        var joined = new JoinPlan(
            left,
            right,
            DbspNet.Sql.Parser.Ast.JoinType.Inner,
            equiKeys,
            Residual: null,
            Schema: joinedSchema,
            AllowNullKeys: true);

        var projections = new List<ProjectionItem>(schema.Count);
        for (var i = 0; i < schema.Count; i++)
        {
            projections.Add(new ProjectionItem(
                new ResolvedColumn(i, schema[i].Type), schema[i].Name, Qualifier: null));
        }

        return new DistinctPlan(new ProjectPlan(joined, projections, schema));
    }

    /// <summary>
    /// Build <c>a EXCEPT b [EXCEPT c …]</c> via left-fold:
    /// <c>(a EXCEPT b) = Distinct(a) − Intersect(a, b)</c>. For chains,
    /// feed the previous result as the new "a" side.
    /// </summary>
    private static LogicalPlan BuildExcept(List<LogicalPlan> aligned, Schema schema)
    {
        var result = (LogicalPlan)new DistinctPlan(aligned[0]);
        for (var i = 1; i < aligned.Count; i++)
        {
            var rightDistinct = new DistinctPlan(aligned[i]);
            var intersection = WrapPairIntersect(result, rightDistinct, schema);
            result = new DifferencePlan(result, intersection);
        }

        return result;
    }

    private static LogicalPlan AlignBranchToUnion(LogicalPlan branch, Schema unified)
    {
        // Fast path: if the branch schema already matches by type, skip the
        // wrapping projection to avoid pointless MapRows at runtime.
        var identical = branch.Schema.Count == unified.Count;
        if (identical)
        {
            for (var i = 0; i < unified.Count; i++)
            {
                if (!SameTypeIgnoringNullable(branch.Schema[i].Type, unified[i].Type))
                {
                    identical = false;
                    break;
                }
            }
        }

        if (identical)
        {
            return branch;
        }

        var projections = new List<ProjectionItem>(unified.Count);
        for (var i = 0; i < unified.Count; i++)
        {
            ResolvedExpression expr = new ResolvedColumn(i, branch.Schema[i].Type);
            if (!SameTypeIgnoringNullable(expr.Type, unified[i].Type))
            {
                expr = new ResolvedCast(expr, unified[i].Type);
            }

            projections.Add(new ProjectionItem(expr, unified[i].Name));
        }

        return new ProjectPlan(branch, projections, unified);
    }

    // ---------- FROM ----------

    private LogicalPlan ResolveFrom(
        FromClause from,
        IReadOnlyDictionary<string, CteRef> cteScope,
        Schema? outerSchema = null) => from switch
    {
        TableReference tr => ResolveTableReference(tr, cteScope),
        JoinClause jc => ResolveJoin(jc, cteScope),
        DerivedTableReference dt => ResolveDerivedTable(dt, cteScope, outerSchema),
        WindowTableFunction wtf => ResolveWindowTableFunction(wtf, cteScope, outerSchema),
        PreResolvedRelation pr => pr.Plan,
        _ => throw new ResolveException($"unsupported FROM clause: {from.GetType().Name}"),
    };

    /// <summary>
    /// Resolve a streaming windowing TVF (<c>TABLE(TUMBLE|HOP(…))</c>) to a window
    /// assignment over its source, exposing <c>window_start</c> / <c>window_end</c>
    /// columns. It is pure lowering onto existing plan nodes — no new operator:
    /// <list type="bullet">
    /// <item>TUMBLE: a single <see cref="ProjectPlan"/> appending
    /// <c>window_start = tumble_start(t, size)</c> and <c>window_end = window_start
    /// + size</c> (each row joins exactly one non-overlapping window).</item>
    /// <item>HOP: a <see cref="UnionAllPlan"/> of <c>size / slide</c> shifted
    /// projections — branch <c>k</c> assigns <c>window_start = tumble_start(t,
    /// slide) − k·slide</c> — so each row fans out to every overlapping window it
    /// belongs to.</item>
    /// </list>
    /// </summary>
    /// <remarks>
    /// The window-start expression is monotone in the time column, but HOP's
    /// per-branch <c>− k·slide</c> shift is not currently followed by the
    /// monotonicity analyzer, so a HOP <c>GROUP BY window_start</c> is not GC'd
    /// (correct, but unbounded state under LATENESS). A sound HOP frontier transform
    /// (<c>v → v − size</c>, not the structural <c>bucket_floor − k·slide</c> which
    /// over-drops by up to a slide) is a deferred follow-on. TUMBLE's window start
    /// is plain <c>tumble_start</c>, which the analyzer GCs as usual.
    /// </remarks>
    private LogicalPlan ResolveWindowTableFunction(
        WindowTableFunction wtf,
        IReadOnlyDictionary<string, CteRef> cteScope,
        Schema? outerSchema)
    {
        var source = ResolveFrom(wtf.Source, cteScope, outerSchema);

        var timeIdx = source.Schema.Resolve(null, wtf.TimeColumn);
        var timeType = source.Schema[timeIdx].Type;
        if (timeType is not (SqlTimestampType or SqlDateType))
        {
            throw new ResolveException(
                $"{wtf.Kind.ToUpperInvariant()} time column '{wtf.TimeColumn}' must be TIMESTAMP or DATE, " +
                $"got {timeType.Display}");
        }

        var sizeLit = ResolveWindowIntervalArg(wtf.SizeArgs[^1], source.Schema, wtf.Kind);
        var slideLit = wtf.Kind == "hop"
            ? ResolveWindowIntervalArg(wtf.SizeArgs[0], source.Schema, wtf.Kind)
            : sizeLit;

        var sizeMicros = ((Interval)sizeLit.Value!).Micros;
        var slideMicros = ((Interval)slideLit.Value!).Micros;
        if (sizeMicros % slideMicros != 0)
        {
            throw new ResolveException(
                $"{wtf.Kind.ToUpperInvariant()} window size must be a whole multiple of the slide");
        }

        var windows = (int)(sizeMicros / slideMicros); // 1 for TUMBLE
        var branches = new List<LogicalPlan>(windows);
        for (var k = 0; k < windows; k++)
        {
            branches.Add(BuildWindowBranch(
                source, wtf.Alias, timeIdx, timeType, slideLit, sizeLit, slideMicros, k));
        }

        return windows == 1 ? branches[0] : new UnionAllPlan(branches, branches[0].Schema);
    }

    private static ResolvedLiteral ResolveWindowIntervalArg(Expression expr, Schema schema, string kind)
    {
        var resolved = ResolveScalarExpression(expr, schema);
        if (resolved is not ResolvedLiteral { Value: Interval iv } lit || resolved.Type is not SqlIntervalType)
        {
            throw new ResolveException(
                $"{kind.ToUpperInvariant()} window arguments must be constant INTERVAL literals");
        }

        if (iv.Months != 0)
        {
            throw new ResolveException(
                $"{kind.ToUpperInvariant()} window arguments must be day-time INTERVALs (no month/year)");
        }

        if (iv.Micros <= 0)
        {
            throw new ResolveException(
                $"{kind.ToUpperInvariant()} window arguments must be positive INTERVALs");
        }

        return lit;
    }

    /// <summary>Build one window-assignment branch: the source rows re-qualified by
    /// the TVF alias, widened with <c>window_start</c> / <c>window_end</c>. Branch
    /// <paramref name="k"/> shifts the start back by <c>k·slide</c> (0 for TUMBLE /
    /// HOP's leading window).</summary>
    private static LogicalPlan BuildWindowBranch(
        LogicalPlan source, string? alias, int timeIdx, SqlType timeType,
        ResolvedLiteral slideLit, ResolvedLiteral sizeLit, long slideMicros, int k)
    {
        var startBase = ScalarFunctionRegistry.Resolve(
            "tumble_start",
            new ResolvedExpression[] { new ResolvedColumn(timeIdx, timeType), slideLit });

        ResolvedExpression windowStart = startBase;
        if (k > 0)
        {
            var kSlide = slideLit with { Value = new Interval(0, (long)k * slideMicros) };
            windowStart = new ResolvedBinary(BinaryOperator.Subtract, startBase, kSlide, timeType);
        }

        var windowEnd = new ResolvedBinary(BinaryOperator.Add, windowStart, sizeLit, timeType);

        var cols = new List<SchemaColumn>(source.Schema.Count + 2);
        var projections = new List<ProjectionItem>(source.Schema.Count + 2);
        for (var i = 0; i < source.Schema.Count; i++)
        {
            var c = source.Schema[i];
            var qualifier = alias ?? c.Qualifier;
            cols.Add(new SchemaColumn(c.Name, c.Type, qualifier));
            projections.Add(new ProjectionItem(new ResolvedColumn(i, c.Type), c.Name, qualifier));
        }

        cols.Add(new SchemaColumn("window_start", timeType, alias));
        cols.Add(new SchemaColumn("window_end", timeType, alias));
        projections.Add(new ProjectionItem(windowStart, "window_start", alias));
        projections.Add(new ProjectionItem(windowEnd, "window_end", alias));

        return new ProjectPlan(source, projections, new Schema(cols));
    }

    /// <summary>
    /// Resolve a subquery in <c>FROM</c> position: inline the subquery's
    /// plan, then wrap it in an identity projection whose schema re-qualifies
    /// every output column with the derived table's alias. The identity
    /// projection is recognized and skipped by the plan→circuit compiler, so
    /// runtime cost is nil.
    /// </summary>
    /// <remarks>
    /// When a derived table sits inside a correlated subquery, the
    /// enclosing scope's columns are visible — pass <paramref name="outerSchema"/>
    /// through to the inner <see cref="ResolveQuery"/> so correlation refs
    /// resolve to <see cref="ResolvedCorrelationRef"/>. Top-level callers
    /// pass <c>null</c>, preserving non-LATERAL semantics.
    /// </remarks>
    private LogicalPlan ResolveDerivedTable(
        DerivedTableReference dt,
        IReadOnlyDictionary<string, CteRef> cteScope,
        Schema? outerSchema = null)
    {
        var inner = ResolveQuery(dt.Query, cteScope, outerSchema: outerSchema);

        var cols = new List<SchemaColumn>(inner.Schema.Count);
        var projections = new List<ProjectionItem>(inner.Schema.Count);
        for (var i = 0; i < inner.Schema.Count; i++)
        {
            var srcCol = inner.Schema[i];
            cols.Add(new SchemaColumn(srcCol.Name, srcCol.Type, dt.Alias));
            projections.Add(new ProjectionItem(
                new ResolvedColumn(i, srcCol.Type), srcCol.Name, dt.Alias));
        }

        return new ProjectPlan(inner, projections, new Schema(cols));
    }

    private LogicalPlan ResolveTableReference(TableReference tr, IReadOnlyDictionary<string, CteRef> cteScope)
    {
        if (cteScope.TryGetValue(tr.TableName, out var cteRef))
        {
            var qualifier = tr.Alias ?? tr.TableName;
            var cteCols = new List<SchemaColumn>(cteRef.Plan.Schema.Count);
            foreach (var c in cteRef.Plan.Schema.Columns)
            {
                cteCols.Add(new SchemaColumn(c.Name, c.Type, qualifier));
            }

            return new CteScanPlan(cteRef, new Schema(cteCols));
        }

        var baseSchema = _catalog.Get(tr.TableName);
        var tblQualifier = tr.Alias ?? tr.TableName;
        // Rewrite schema columns to carry the (alias-or-table) as qualifier, and
        // lift any declared LATENESS bounds into the scan's ColumnLateness map.
        var cols = new List<SchemaColumn>(baseSchema.Count);
        Dictionary<int, long>? lateness = null;
        for (var i = 0; i < baseSchema.Count; i++)
        {
            var c = baseSchema.Columns[i];
            cols.Add(new SchemaColumn(c.Name, c.Type, tblQualifier));
            if (c.Lateness is { } bound)
            {
                (lateness ??= new Dictionary<int, long>())[i] = bound;
            }
        }

        return new ScanPlan(tr.TableName, new Schema(cols), lateness);
    }

    private LogicalPlan ResolveJoin(JoinClause join, IReadOnlyDictionary<string, CteRef> cteScope)
    {
        var left = ResolveFrom(join.Left, cteScope);
        var right = ResolveFrom(join.Right, cteScope);
        CheckNoDuplicateQualifiers(left.Schema, right.Schema);

        var leftCount = left.Schema.Count;

        // JOIN ... USING (c1, …): equi-join on the shared columns plus a
        // projection that merges each shared column to a single unqualified
        // copy. Handled before the ON path since OnCondition is null here.
        if (join.UsingColumns is not null)
        {
            return ResolveUsingJoin(join, left, right, leftCount);
        }

        // Combined schema for ON-clause resolution uses each side's declared
        // nullability. The OUTPUT schema may widen right-side columns to
        // nullable (for LEFT OUTER) but the predicate sees non-null where
        // declared — a right-side row that exists and matches never presents
        // as NULL inside the ON clause.
        var combined = left.Schema.Concat(right.Schema);
        var onResolved = ResolveScalarExpression(join.OnCondition!, combined);
        EnsureBooleanCoercible(onResolved, "JOIN ON");
        var equi = new List<JoinEquality>();
        var residuals = new List<ResolvedExpression>();
        var computed = new List<ComputedEquiKey>();
        foreach (var conjunct in SplitAnd(onResolved))
        {
            if (TryExtractEquiKey(conjunct, leftCount, out var eq))
            {
                equi.Add(eq);
            }
            else if (TryExtractComputedEquiKey(conjunct, leftCount, out var ck))
            {
                computed.Add(ck);
            }
            else
            {
                residuals.Add(conjunct);
            }
        }

        // Computed equi-keys (`ON CAST(a.x AS VARCHAR) = CAST(b.y AS VARCHAR)`,
        // `ON UPPER(a.x) = b.y`) are lowered to bare-column keys: the key
        // expressions are hoisted into synthetic columns projected onto each
        // input, the join keys on those, and a projection above strips them.
        // Nothing downstream sees an expression key, so the trace/GC/pruning/
        // typed paths are untouched. Without this a computed key is a residual,
        // which for INNER means a keyless unit-key cross product — correct but
        // quadratic — and for an outer join means no equi-key at all.
        var leftSynth = new List<ProjectionItem>();
        var rightSynth = new List<ProjectionItem>();
        foreach (var ck in computed)
        {
            var li = HoistKeySide(ck.LeftExpr, left.Schema, leftSynth, "__jkl");
            var ri = HoistKeySide(ck.RightExpr, right.Schema, rightSynth, "__jkr");
            equi.Add(new JoinEquality(li, ri, ck.KeyType));
        }

        ResolvedExpression? residual = null;
        foreach (var r in residuals)
        {
            residual = residual is null
                ? r
                : new ResolvedBinary(BinaryOperator.And, residual, r,
                    new SqlBooleanType(residual.Type.Nullable || r.Type.Nullable));
        }

        // An outer join may carry a residual, and may have no equi-key at all.
        // Neither is expressible in IncrementalLeftJoin/FullJoin, whose
        // match-presence is a per-key emptiness test; PlanToCircuit lowers both
        // to the anti-join rewrite (CompileOuterJoinWithResidual). The plan node
        // keeps its natural JoinPlan(LeftOuter, equi, residual) shape so
        // BatchPlanEvaluator can implement the semantics directly and serve as
        // an independent oracle.

        // Widen the inputs with any hoisted key columns. Bare-column equi-keys
        // are unaffected: JoinEquality holds per-side indices and the synthetic
        // columns are appended, so existing indices keep pointing at the same
        // columns.
        var joinLeft = WidenWithSynthKeys(left, leftSynth);
        var joinRight = WidenWithSynthKeys(right, rightSynth);

        // The residual is indexed against the ORIGINAL combined schema. Widening
        // the left input shifts every right-side column right by leftSynth.Count.
        if (residual is not null && leftSynth.Count > 0)
        {
            var remap = new int[leftCount + right.Schema.Count];
            for (var i = 0; i < remap.Length; i++)
            {
                remap[i] = i < leftCount ? i : i + leftSynth.Count;
            }

            residual = ExpressionRewriter.RemapColumnIndices(residual, remap);
        }

        var outputSchema = join.Type switch
        {
            JoinType.Inner => joinLeft.Schema.Concat(joinRight.Schema),
            JoinType.LeftOuter => MakeSideNullable(joinLeft.Schema, joinRight.Schema, makeLeftNullable: false),
            JoinType.RightOuter => MakeSideNullable(joinLeft.Schema, joinRight.Schema, makeLeftNullable: true),
            JoinType.FullOuter => MakeBothNullable(joinLeft.Schema, joinRight.Schema),
            _ => throw new ResolveException($"unsupported join type {join.Type}"),
        };

        var joinPlan = new JoinPlan(joinLeft, joinRight, join.Type, equi, residual, outputSchema);
        if (leftSynth.Count == 0 && rightSynth.Count == 0)
        {
            return joinPlan;
        }

        // Strip the synthetic key columns, restoring the caller-visible
        // [left cols…, right cols…] shape (nullability taken from the join's
        // own output schema, not the inputs').
        var keep = new List<int>(leftCount + right.Schema.Count);
        for (var i = 0; i < leftCount; i++)
        {
            keep.Add(i);
        }

        var rightStart = leftCount + leftSynth.Count;
        for (var i = 0; i < right.Schema.Count; i++)
        {
            keep.Add(rightStart + i);
        }

        return ProjectColumns(joinPlan, keep);
    }

    /// <summary>Side of a join a resolved expression reads from.</summary>
    private enum JoinSide
    {
        /// <summary>No column references — a constant.</summary>
        None,
        Left,
        Right,

        /// <summary>Reads both inputs; can never be a join key.</summary>
        Mixed,
    }

    private sealed record ComputedEquiKey(
        ResolvedExpression LeftExpr,
        ResolvedExpression RightExpr,
        SqlType KeyType);

    private static JoinSide SideOf(ResolvedExpression e, int leftCount)
    {
        var indices = ExpressionRewriter.CollectColumnIndices(e);
        if (indices.Count == 0)
        {
            return JoinSide.None;
        }

        var anyLeft = false;
        var anyRight = false;
        foreach (var i in indices)
        {
            if (i < leftCount)
            {
                anyLeft = true;
            }
            else
            {
                anyRight = true;
            }
        }

        return anyLeft && anyRight ? JoinSide.Mixed
            : anyLeft ? JoinSide.Left
            : JoinSide.Right;
    }

    /// <summary>
    /// Match an equality whose operands are each side-pure but not both bare
    /// columns — <c>CAST(a.x AS VARCHAR) = CAST(b.y AS VARCHAR)</c>,
    /// <c>UPPER(a.x) = b.y</c>. The returned expressions are in per-side index
    /// space (the right operand shifted down by <paramref name="leftCount"/>),
    /// ready to project onto their input.
    ///
    /// A constant operand (<c>a.x = 5</c>) is <see cref="JoinSide.None"/> and
    /// stays a residual — it filters, it does not join.
    /// </summary>
    private static bool TryExtractComputedEquiKey(
        ResolvedExpression e, int leftCount, out ComputedEquiKey key)
    {
        key = default!;
        if (e is not ResolvedBinary { Operator: BinaryOperator.Equal } bin)
        {
            return false;
        }

        var ls = SideOf(bin.Left, leftCount);
        var rs = SideOf(bin.Right, leftCount);

        ResolvedExpression leftExpr, rightExpr;
        if (ls == JoinSide.Left && rs == JoinSide.Right)
        {
            leftExpr = bin.Left;
            rightExpr = ExpressionRewriter.ShiftColumnIndices(bin.Right, -leftCount);
        }
        else if (ls == JoinSide.Right && rs == JoinSide.Left)
        {
            leftExpr = bin.Right;
            rightExpr = ExpressionRewriter.ShiftColumnIndices(bin.Left, -leftCount);
        }
        else
        {
            return false;
        }

        key = new ComputedEquiKey(
            leftExpr, rightExpr, TypeInference.CommonComparableType(leftExpr.Type, rightExpr.Type));
        return true;
    }

    /// <summary>
    /// Resolve one side of a computed equi-key to a column index on its input,
    /// appending a synthetic projection when the key is not already a bare
    /// column. Returns the per-side index.
    /// </summary>
    private static int HoistKeySide(
        ResolvedExpression keyExpr, Schema inputSchema, List<ProjectionItem> synth, string prefix)
    {
        if (keyExpr is ResolvedColumn c)
        {
            return c.Index;
        }

        var index = inputSchema.Count + synth.Count;
        synth.Add(new ProjectionItem(keyExpr, prefix + synth.Count));
        return index;
    }

    /// <summary>Append synthetic key columns to an input, or pass it through.</summary>
    private static LogicalPlan WidenWithSynthKeys(LogicalPlan input, List<ProjectionItem> synth)
    {
        if (synth.Count == 0)
        {
            return input;
        }

        var items = new List<ProjectionItem>(input.Schema.Count + synth.Count);
        var cols = new List<SchemaColumn>(input.Schema.Count + synth.Count);
        for (var i = 0; i < input.Schema.Count; i++)
        {
            var sc = input.Schema[i];
            items.Add(new ProjectionItem(new ResolvedColumn(i, sc.Type), sc.Name, sc.Qualifier));
            cols.Add(sc);
        }

        foreach (var s in synth)
        {
            items.Add(s);
            cols.Add(new SchemaColumn(s.Name, s.Expression.Type));
        }

        return new ProjectPlan(input, items, new Schema(cols));
    }

    /// <summary>Positional projection of a plan down to a subset of its columns.</summary>
    private static LogicalPlan ProjectColumns(LogicalPlan input, IReadOnlyList<int> keep)
    {
        var items = new List<ProjectionItem>(keep.Count);
        var cols = new List<SchemaColumn>(keep.Count);
        foreach (var i in keep)
        {
            var sc = input.Schema[i];
            items.Add(new ProjectionItem(new ResolvedColumn(i, sc.Type), sc.Name, sc.Qualifier));
            cols.Add(sc);
        }

        return new ProjectPlan(input, items, new Schema(cols));
    }

    /// <summary>
    /// Resolve a <c>JOIN ... USING (c1, …)</c>. Each named column must exist
    /// on both sides; it becomes an equi-key and is merged in the output to a
    /// single unqualified copy (taken from the preserved side for outer
    /// joins). Output order follows the SQL standard: the merged USING columns
    /// first (in USING order), then the remaining left columns, then the
    /// remaining right columns.
    /// </summary>
    private LogicalPlan ResolveUsingJoin(JoinClause join, LogicalPlan left, LogicalPlan right, int leftCount)
    {
        var equi = new List<JoinEquality>();
        var usingLeftIdx = new List<int>();
        var usingRightCombinedIdx = new List<int>();
        var seen = new HashSet<string>(StringComparer.Ordinal);

        foreach (var col in join.UsingColumns!)
        {
            if (!seen.Add(col))
            {
                throw new ResolveException($"duplicate column '{col}' in JOIN USING");
            }

            var li = left.Schema.Resolve(null, col);
            var ri = right.Schema.Resolve(null, col);
            var keyType = TypeInference.CommonComparableType(left.Schema[li].Type, right.Schema[ri].Type);
            equi.Add(new JoinEquality(li, ri, keyType));
            usingLeftIdx.Add(li);
            usingRightCombinedIdx.Add(leftCount + ri);
        }

        var joinOutputSchema = join.Type switch
        {
            JoinType.Inner => left.Schema.Concat(right.Schema),
            JoinType.LeftOuter => MakeSideNullable(left.Schema, right.Schema, makeLeftNullable: false),
            JoinType.RightOuter => MakeSideNullable(left.Schema, right.Schema, makeLeftNullable: true),
            JoinType.FullOuter => MakeBothNullable(left.Schema, right.Schema),
            _ => throw new ResolveException($"unsupported join type {join.Type}"),
        };

        var joinPlan = new JoinPlan(left, right, join.Type, equi, Residual: null, joinOutputSchema);

        // INNER and LEFT keep the (non-null) left copy of each shared column;
        // RIGHT keeps the (non-null) right copy. FULL has no non-null side, so
        // each shared column merges via COALESCE(left, right) — at least one
        // side is non-null for any output row (the join key is present on the
        // preserved side).
        var takeLeftForMerged = join.Type != JoinType.RightOuter;
        var mergedLeft = new HashSet<int>(usingLeftIdx);
        var mergedRight = new HashSet<int>(usingRightCombinedIdx);

        var projections = new List<ProjectionItem>();
        for (var i = 0; i < join.UsingColumns!.Count; i++)
        {
            if (join.Type == JoinType.FullOuter)
            {
                var leftCol = joinOutputSchema[usingLeftIdx[i]];
                var rightCol = joinOutputSchema[usingRightCombinedIdx[i]];
                var mergedType = equi[i].KeyType.WithNullable(false);
                var coalesce = new ResolvedFunctionCall(
                    "coalesce",
                    new ResolvedExpression[]
                    {
                        new ResolvedColumn(usingLeftIdx[i], leftCol.Type),
                        new ResolvedColumn(usingRightCombinedIdx[i], rightCol.Type),
                    },
                    mergedType);
                projections.Add(new ProjectionItem(coalesce, join.UsingColumns[i], Qualifier: null));
                continue;
            }

            var idx = takeLeftForMerged ? usingLeftIdx[i] : usingRightCombinedIdx[i];
            var c = joinOutputSchema[idx];
            projections.Add(new ProjectionItem(new ResolvedColumn(idx, c.Type), join.UsingColumns[i], Qualifier: null));
        }

        for (var i = 0; i < leftCount; i++)
        {
            if (mergedLeft.Contains(i))
            {
                continue;
            }

            var c = joinOutputSchema[i];
            projections.Add(new ProjectionItem(new ResolvedColumn(i, c.Type), c.Name, c.Qualifier));
        }

        for (var i = leftCount; i < joinOutputSchema.Count; i++)
        {
            if (mergedRight.Contains(i))
            {
                continue;
            }

            var c = joinOutputSchema[i];
            projections.Add(new ProjectionItem(new ResolvedColumn(i, c.Type), c.Name, c.Qualifier));
        }

        return new ProjectPlan(joinPlan, projections, BuildProjectSchema(projections));
    }

    private static string JoinTypeName(JoinType t) => t switch
    {
        JoinType.Inner => "INNER JOIN",
        JoinType.LeftOuter => "LEFT JOIN",
        JoinType.RightOuter => "RIGHT JOIN",
        JoinType.FullOuter => "FULL JOIN",
        _ => t.ToString(),
    };

    private static Schema MakeSideNullable(Schema left, Schema right, bool makeLeftNullable)
    {
        var cols = new List<SchemaColumn>(left.Count + right.Count);
        foreach (var c in left.Columns)
        {
            cols.Add(makeLeftNullable
                ? new SchemaColumn(c.Name, c.Type.WithNullable(true), c.Qualifier)
                : c);
        }

        foreach (var c in right.Columns)
        {
            cols.Add(makeLeftNullable
                ? c
                : new SchemaColumn(c.Name, c.Type.WithNullable(true), c.Qualifier));
        }

        return new Schema(cols);
    }

    private static Schema MakeBothNullable(Schema left, Schema right)
    {
        var cols = new List<SchemaColumn>(left.Count + right.Count);
        foreach (var c in left.Columns)
        {
            cols.Add(new SchemaColumn(c.Name, c.Type.WithNullable(true), c.Qualifier));
        }

        foreach (var c in right.Columns)
        {
            cols.Add(new SchemaColumn(c.Name, c.Type.WithNullable(true), c.Qualifier));
        }

        return new Schema(cols);
    }

    private static void CheckNoDuplicateQualifiers(Schema left, Schema right)
    {
        var leftQuals = new HashSet<string>(StringComparer.Ordinal);
        foreach (var c in left.Columns)
        {
            if (c.Qualifier is { } q)
            {
                leftQuals.Add(q);
            }
        }

        foreach (var c in right.Columns)
        {
            if (c.Qualifier is { } q && leftQuals.Contains(q))
            {
                throw new ResolveException($"duplicate table alias '{q}' in FROM clause");
            }
        }
    }

    private static IEnumerable<ResolvedExpression> SplitAnd(ResolvedExpression e)
    {
        if (e is ResolvedBinary { Operator: BinaryOperator.And } b)
        {
            foreach (var l in SplitAnd(b.Left))
            {
                yield return l;
            }

            foreach (var r in SplitAnd(b.Right))
            {
                yield return r;
            }
        }
        else
        {
            yield return e;
        }
    }

    private static bool TryExtractEquiKey(ResolvedExpression e, int leftCount, out JoinEquality equality)
    {
        equality = default!;
        if (e is not ResolvedBinary { Operator: BinaryOperator.Equal } bin)
        {
            return false;
        }

        if (bin.Left is not ResolvedColumn lc || bin.Right is not ResolvedColumn rc)
        {
            return false;
        }

        int leftIdx, rightIdx;
        SqlType keyType;
        if (lc.Index < leftCount && rc.Index >= leftCount)
        {
            leftIdx = lc.Index;
            rightIdx = rc.Index - leftCount;
            keyType = TypeInference.CommonComparableType(lc.Type, rc.Type);
        }
        else if (rc.Index < leftCount && lc.Index >= leftCount)
        {
            leftIdx = rc.Index;
            rightIdx = lc.Index - leftCount;
            keyType = TypeInference.CommonComparableType(rc.Type, lc.Type);
        }
        else
        {
            return false;
        }

        equality = new JoinEquality(leftIdx, rightIdx, keyType);
        return true;
    }

    // ---------- Projections ----------

    private List<ProjectionItem> ResolveProjections(
        IReadOnlyList<SelectItem> items,
        Schema schema,
        IReadOnlyDictionary<SubqueryExpression, SubqueryBinding>? subqueryMap = null,
        IReadOnlyDictionary<Expression, ResolvedExpression>? preBound = null)
    {
        var result = new List<ProjectionItem>();
        foreach (var item in items)
        {
            switch (item)
            {
                case StarSelectItem s when s.TableQualifier is null:
                    for (var i = 0; i < schema.Count; i++)
                    {
                        var c = schema[i];
                        // Don't expand the hidden subquery-join columns.
                        if (c.Name.StartsWith("$sub", StringComparison.Ordinal) && c.Qualifier is null)
                        {
                            continue;
                        }

                        result.Add(new ProjectionItem(new ResolvedColumn(i, c.Type), c.Name, c.Qualifier));
                    }

                    break;
                case StarSelectItem s:
                    var any = false;
                    for (var i = 0; i < schema.Count; i++)
                    {
                        var c = schema[i];
                        if (string.Equals(c.Qualifier, s.TableQualifier, StringComparison.Ordinal))
                        {
                            result.Add(new ProjectionItem(new ResolvedColumn(i, c.Type), c.Name, c.Qualifier));
                            any = true;
                        }
                    }

                    if (!any)
                    {
                        throw new ResolveException($"unknown table '{s.TableQualifier}' in SELECT {s.TableQualifier}.*");
                    }

                    break;
                case ExpressionSelectItem e:
                    var resolved = ResolveScalarExpression(e.Expression, schema, subqueryMap, outerSchema: null, preBound);
                    var (name, qualifier) = DeriveProjectionName(e.Expression, e.Alias);
                    result.Add(new ProjectionItem(resolved, name, qualifier));
                    break;
                default:
                    throw new ResolveException($"unsupported SELECT item: {item.GetType().Name}");
            }
        }

        return result;
    }

    private static (string Name, string? Qualifier) DeriveProjectionName(Expression expr, string? alias)
    {
        if (alias is not null)
        {
            return (alias, null);
        }

        if (expr is ColumnReference cr)
        {
            return (cr.Name, cr.Qualifier);
        }

        return ("$col", null);
    }

    private static Schema BuildProjectSchema(IReadOnlyList<ProjectionItem> items)
    {
        var cols = new List<SchemaColumn>(items.Count);
        foreach (var p in items)
        {
            cols.Add(new SchemaColumn(p.Name, p.Expression.Type, p.Qualifier));
        }

        return new Schema(cols);
    }

    // ---------- Scalar subquery rewriting ----------

    private static List<SubqueryExpression> CollectSubqueries(Expression expr)
    {
        var result = new List<SubqueryExpression>();
        CollectSubqueriesInto(expr, result);
        return result;
    }

    private static void CollectSubqueriesInto(Expression expr, List<SubqueryExpression> acc)
    {
        switch (expr)
        {
            case SubqueryExpression sq:
                acc.Add(sq);
                // Don't descend — the subquery has its own scope and will be
                // resolved as its own plan. Any nested subqueries inside it
                // are handled during that recursive resolution.
                break;
            case BinaryExpression b:
                CollectSubqueriesInto(b.Left, acc);
                CollectSubqueriesInto(b.Right, acc);
                break;
            case UnaryExpression u:
                CollectSubqueriesInto(u.Operand, acc);
                break;
            case IsNullExpression isn:
                CollectSubqueriesInto(isn.Operand, acc);
                break;
            case CastExpression cast:
                CollectSubqueriesInto(cast.Operand, acc);
                break;
            case FunctionCallExpression fn:
                foreach (var a in fn.Arguments)
                {
                    CollectSubqueriesInto(a, acc);
                }

                break;
            case InListExpression il:
                CollectSubqueriesInto(il.Probe, acc);
                foreach (var v in il.Values)
                {
                    CollectSubqueriesInto(v, acc);
                }

                break;
            case InSubqueryExpression:
                // Don't recurse — InSubqueryExpression is either lifted to a
                // SemiJoinPlan by the WHERE pre-pass (its inner subquery is
                // handled there with its own scope) or rejected by
                // ResolveScalarExpression with a "deferred" error.
                break;
            case ExistsExpression existsE:
                // Register the cached CountSubquery: it's the scalar that
                // WrapWithScalarSubqueries needs to hide as a column when
                // the EXISTS appears in a non-WHERE position (SELECT/HAVING)
                // or as a nested-boolean subexpression. For WHERE-top-level
                // EXISTS the lift in LiftExistsOrDesugar handles it; for
                // correlated EXISTS the path skips this walker entirely.
                acc.Add(existsE.CountSubquery);
                break;
            case CaseExpression ce:
                foreach (var w in ce.Whens)
                {
                    CollectSubqueriesInto(w.Condition, acc);
                    CollectSubqueriesInto(w.Result, acc);
                }

                if (ce.ElseResult is not null)
                {
                    CollectSubqueriesInto(ce.ElseResult, acc);
                }

                break;
            // Literals, column refs: no subqueries possible.
        }
    }

    /// <summary>
    /// Like <see cref="CollectSubqueriesInto"/> but skips
    /// <see cref="ExistsExpression"/> / <see cref="InSubqueryExpression"/>
    /// nodes that have already been bound to hidden match-count columns by
    /// the non-WHERE boolean pre-pass. Avoids the existing CountSubquery
    /// from also being registered as a regular scalar subquery.
    /// </summary>
    private static void CollectSubqueriesIntoExcludingBound(
        Expression expr,
        List<SubqueryExpression> acc,
        IReadOnlyDictionary<ExistsExpression, BooleanSubqueryBinding> boundExists,
        IReadOnlyDictionary<InSubqueryExpression, BooleanSubqueryBinding> boundIn)
    {
        switch (expr)
        {
            case SubqueryExpression sq:
                acc.Add(sq);
                break;
            case BinaryExpression b:
                CollectSubqueriesIntoExcludingBound(b.Left, acc, boundExists, boundIn);
                CollectSubqueriesIntoExcludingBound(b.Right, acc, boundExists, boundIn);
                break;
            case UnaryExpression u:
                CollectSubqueriesIntoExcludingBound(u.Operand, acc, boundExists, boundIn);
                break;
            case IsNullExpression isn:
                CollectSubqueriesIntoExcludingBound(isn.Operand, acc, boundExists, boundIn);
                break;
            case CastExpression cast:
                CollectSubqueriesIntoExcludingBound(cast.Operand, acc, boundExists, boundIn);
                break;
            case FunctionCallExpression fn:
                foreach (var a in fn.Arguments)
                {
                    CollectSubqueriesIntoExcludingBound(a, acc, boundExists, boundIn);
                }

                break;
            case InListExpression il:
                CollectSubqueriesIntoExcludingBound(il.Probe, acc, boundExists, boundIn);
                foreach (var v in il.Values)
                {
                    CollectSubqueriesIntoExcludingBound(v, acc, boundExists, boundIn);
                }

                break;
            case InSubqueryExpression isq when boundIn.ContainsKey(isq):
                break;
            case InSubqueryExpression:
                // Fall through to today's reject behaviour at scalar resolve.
                break;
            case ExistsExpression existsE when boundExists.ContainsKey(existsE):
                break;
            case ExistsExpression existsE:
                acc.Add(existsE.CountSubquery);
                break;
            case CaseExpression ce:
                foreach (var w in ce.Whens)
                {
                    CollectSubqueriesIntoExcludingBound(w.Condition, acc, boundExists, boundIn);
                    CollectSubqueriesIntoExcludingBound(w.Result, acc, boundExists, boundIn);
                }

                if (ce.ElseResult is not null)
                {
                    CollectSubqueriesIntoExcludingBound(ce.ElseResult, acc, boundExists, boundIn);
                }

                break;
        }
    }

    /// <summary>
    /// Walk a boolean expression and return its top-level AND conjuncts.
    /// Non-AND expressions return as a single-element list. Used by the
    /// WHERE pre-pass to peel <c>IN (subquery)</c> terms off into a
    /// <see cref="SemiJoinPlan"/> while keeping scalar predicates in a
    /// <see cref="FilterPlan"/>.
    /// </summary>
    private static List<Expression> SplitAndConjuncts(Expression expr)
    {
        var result = new List<Expression>();
        Walk(expr, result);
        return result;

        static void Walk(Expression e, List<Expression> acc)
        {
            if (e is BinaryExpression { Operator: BinaryOperator.And } b)
            {
                Walk(b.Left, acc);
                Walk(b.Right, acc);
            }
            else
            {
                acc.Add(e);
            }
        }
    }

    /// <summary>Re-AND a non-empty conjunct list into a left-leaning chain.</summary>
    private static Expression JoinAnd(IReadOnlyList<Expression> conjuncts)
    {
        var acc = conjuncts[0];
        for (var i = 1; i < conjuncts.Count; i++)
        {
            acc = new BinaryExpression(BinaryOperator.And, acc, conjuncts[i]);
        }

        return acc;
    }

    // ---------- Temporal filters (NOW() / CURRENT_TIMESTAMP) ----------

    private enum TemporalBoundKind
    {
        /// <summary>A lower bound on NOW(): the row appears (becomes valid)
        /// when the clock reaches <c>TimeKey + OffsetMicros</c>.</summary>
        Appear,

        /// <summary>An upper bound on NOW(): the row disappears (is retracted)
        /// once the clock passes <c>TimeKey + OffsetMicros</c>.</summary>
        Disappear,
    }

    /// <summary>One side of a temporal-filter window, recognised from a single
    /// <c>key OP clock±d</c> conjunct. <see cref="Offset"/> and the
    /// <see cref="TimeKey"/> are in the unit fixed by <see cref="Clock"/>
    /// (microseconds for <see cref="TemporalClock.Timestamp"/>, whole days for
    /// <see cref="TemporalClock.Date"/>).</summary>
    private sealed record TemporalHalfBound(
        ResolvedExpression TimeKey,
        TemporalBoundKind Kind,
        long Offset,
        bool Inclusive,
        TemporalClock Clock);

    private static string NowDisplay(NowExpression now) => now.Function switch
    {
        NowFunction.CurrentTimestamp => "CURRENT_TIMESTAMP",
        NowFunction.CurrentDate => "CURRENT_DATE",
        NowFunction.CurrentTime => "CURRENT_TIME",
        _ => "NOW()",
    };

    /// <summary>The single advancing-clock niladic inside <paramref name="e"/>
    /// (there is exactly one on the now-side of a recognised conjunct).</summary>
    private static NowExpression FindNow(Expression e) => e switch
    {
        NowExpression n => n,
        BinaryExpression b => MentionsNow(b.Left) ? FindNow(b.Left) : FindNow(b.Right),
        UnaryExpression u => FindNow(u.Operand),
        IsNullExpression isn => FindNow(isn.Operand),
        CastExpression c => FindNow(c.Operand),
        _ => throw new ResolveException("expected an advancing-clock expression (NOW())."),
    };

    /// <summary>
    /// Does this expression reference <c>NOW()</c> / <c>CURRENT_TIMESTAMP</c>
    /// at this query level? A NOW() inside a subquery opens a new scope and is
    /// that subquery's own concern (rejected there), so subquery-bearing nodes
    /// report false.
    /// </summary>
    private static bool MentionsNow(Expression e) => e switch
    {
        NowExpression => true,
        BinaryExpression b => MentionsNow(b.Left) || MentionsNow(b.Right),
        UnaryExpression u => MentionsNow(u.Operand),
        IsNullExpression isn => MentionsNow(isn.Operand),
        CastExpression c => MentionsNow(c.Operand),
        FunctionCallExpression f => f.Arguments.Any(MentionsNow),
        InListExpression il => MentionsNow(il.Probe) || il.Values.Any(MentionsNow),
        CaseExpression ce =>
            ce.Whens.Any(w => MentionsNow(w.Condition) || MentionsNow(w.Result))
            || (ce.ElseResult is not null && MentionsNow(ce.ElseResult)),
        _ => false,
    };

    /// <summary>
    /// Recognise a single sanctioned temporal-filter conjunct
    /// (<c>key {&lt;|&lt;=|&gt;|&gt;=} NOW()±d</c>) and reduce it to a
    /// <see cref="TemporalHalfBound"/>. Throws a precise
    /// <see cref="ResolveException"/> for any other shape that mentions NOW().
    /// </summary>
    private static TemporalHalfBound MatchTemporalConjunct(Expression conjunct, Schema schema)
    {
        if (conjunct is not BinaryExpression bin
            || bin.Operator is not (BinaryOperator.Less or BinaryOperator.LessEqual
                or BinaryOperator.Greater or BinaryOperator.GreaterEqual))
        {
            throw new ResolveException(
                "the advancing clock (NOW() / CURRENT_TIMESTAMP / CURRENT_DATE) is only allowed " +
                "in a temporal-filter comparison (<, <=, >, >=) of a TIMESTAMP (or, for " +
                "CURRENT_DATE, a DATE) expression against it, e.g. WHERE ts > NOW() - INTERVAL '1' HOUR.");
        }

        var leftNow = MentionsNow(bin.Left);
        var rightNow = MentionsNow(bin.Right);
        if (leftNow && rightNow)
        {
            throw new ResolveException(
                "the advancing clock may appear on only one side of a temporal-filter comparison.");
        }

        var nowSide = leftNow ? bin.Left : bin.Right;
        var keySide = leftNow ? bin.Right : bin.Left;

        // The clock spelling fixes the value space (TIMESTAMP µs vs DATE days)
        // and is rejected outright if cyclic (CURRENT_TIME).
        var clock = ClockOf(FindNow(nowSide));

        // Offset (in clock units) added to the clock on the now-side: clock -> 0,
        // clock + INTERVAL d -> +d, clock - INTERVAL d -> -d.
        var nowPlusK = ParseNowOffset(nowSide, schema, clock);

        var timeKey = ResolveScalarExpression(keySide, schema);
        var keyOk = clock == TemporalClock.Date
            ? timeKey.Type is SqlDateType
            : timeKey.Type is SqlTimestampType;
        if (!keyOk)
        {
            var expected = clock == TemporalClock.Date ? "DATE" : "TIMESTAMP";
            throw new ResolveException(
                $"the non-clock side of a CURRENT_DATE/NOW() temporal filter must be a {expected} " +
                $"expression; got {timeKey.Type.Display}.");
        }

        // Normalise to `timeKey OP clock+k`; flip OP if the clock was on the left.
        // Solving for the clock puts the threshold at key - k, so the offset from
        // the key is -k.
        var op = leftNow ? FlipComparison(bin.Operator) : bin.Operator;
        var offset = checked(-nowPlusK);
        return op switch
        {
            BinaryOperator.Less =>
                new TemporalHalfBound(timeKey, TemporalBoundKind.Appear, offset, Inclusive: false, clock),
            BinaryOperator.LessEqual =>
                new TemporalHalfBound(timeKey, TemporalBoundKind.Appear, offset, Inclusive: true, clock),
            BinaryOperator.Greater =>
                new TemporalHalfBound(timeKey, TemporalBoundKind.Disappear, offset, Inclusive: false, clock),
            _ => // GreaterEqual
                new TemporalHalfBound(timeKey, TemporalBoundKind.Disappear, offset, Inclusive: true, clock),
        };
    }

    /// <summary>The temporal-filter value space of an advancing-clock niladic.
    /// <c>CURRENT_TIME</c> is cyclic (<c>now mod day</c>), so it is not monotone
    /// and has no sound advancing semantics — rejected here.</summary>
    private static TemporalClock ClockOf(NowExpression now) => now.Function switch
    {
        NowFunction.CurrentDate => TemporalClock.Date,
        NowFunction.CurrentTime => throw new ResolveException(
            "CURRENT_TIME is cyclic (it wraps every midnight) and so is not monotone; it has no " +
            "sound advancing-clock semantics and cannot be used in a temporal filter. Use " +
            "CURRENT_TIMESTAMP for an absolute-time window."),
        _ => TemporalClock.Timestamp,
    };

    private static long ParseNowOffset(Expression nowSide, Schema schema, TemporalClock clock)
    {
        switch (nowSide)
        {
            case NowExpression:
                return 0;
            case BinaryExpression { Operator: BinaryOperator.Add } add:
                if (add.Left is NowExpression && !MentionsNow(add.Right))
                {
                    return IntervalOffset(add.Right, schema, clock);
                }

                if (add.Right is NowExpression && !MentionsNow(add.Left))
                {
                    return IntervalOffset(add.Left, schema, clock);
                }

                break;
            case BinaryExpression { Operator: BinaryOperator.Subtract } sub:
                // clock - INTERVAL d only; `interval - clock` is type-invalid.
                if (sub.Left is NowExpression && !MentionsNow(sub.Right))
                {
                    return checked(-IntervalOffset(sub.Right, schema, clock));
                }

                break;
        }

        throw new ResolveException(
            "unsupported advancing-clock expression in a temporal filter; only the bare clock, " +
            "clock + INTERVAL <d>, and clock - INTERVAL <d> (a constant day-time interval) are allowed.");
    }

    // (FlipComparison is shared with the partitioned TOP-K recogniser above.)

    /// <summary>The constant INTERVAL offset, in the clock's unit (microseconds
    /// for a TIMESTAMP clock, whole days for a DATE clock). A DATE offset must be
    /// a whole number of days — a sub-day interval shifts the day-truncated clock
    /// off a day boundary, which the day-space comparison cannot represent.</summary>
    private static long IntervalOffset(Expression intervalExpr, Schema schema, TemporalClock clock)
    {
        var resolved = ResolveScalarExpression(intervalExpr, schema);
        if (resolved.Type is not SqlIntervalType || resolved is not ResolvedLiteral { Value: Interval interval })
        {
            throw new ResolveException(
                "an advancing-clock offset in a temporal filter must be a constant INTERVAL literal.");
        }

        if (interval.Months != 0)
        {
            throw new ResolveException(
                "temporal-filter clock offsets must be day-time intervals (DAY / HOUR / MINUTE / " +
                "SECOND); month/year intervals are not a constant number of microseconds.");
        }

        if (clock == TemporalClock.Date)
        {
            if (interval.Micros % Interval.MicrosPerDay != 0)
            {
                throw new ResolveException(
                    "a CURRENT_DATE temporal-filter offset must be a whole number of days " +
                    "(INTERVAL '<n>' DAY); a sub-day interval has no meaning against a DATE clock.");
            }

            return interval.Micros / Interval.MicrosPerDay;
        }

        return interval.Micros;
    }

    /// <summary>
    /// Fold the recognised temporal half-bounds into <see cref="TemporalFilterPlan"/>
    /// nodes stacked on <paramref name="plan"/>. Half-bounds sharing a time key
    /// merge into a single node (one appear + one disappear); any extra
    /// same-direction bound on the same key is stacked as its own node — still
    /// a sound conjunction, since stacked filters intersect.
    /// </summary>
    private static LogicalPlan ApplyTemporalFilters(LogicalPlan plan, IReadOnlyList<TemporalHalfBound> bounds)
    {
        if (bounds.Count == 0)
        {
            return plan;
        }

        var byKey = new Dictionary<ResolvedExpression, List<TemporalHalfBound>>();
        var order = new List<ResolvedExpression>();
        foreach (var b in bounds)
        {
            if (!byKey.TryGetValue(b.TimeKey, out var list))
            {
                list = new List<TemporalHalfBound>();
                byKey[b.TimeKey] = list;
                order.Add(b.TimeKey);
            }

            list.Add(b);
        }

        foreach (var key in order)
        {
            long? appearOffset = null;
            var appearIncl = false;
            long? disappearOffset = null;
            var disappearIncl = false;
            var extras = new List<TemporalHalfBound>();

            // Every bound on a given key shares the same clock unit (the key's
            // type fixes it), so the first bound's clock is the node's clock.
            var clock = byKey[key][0].Clock;
            foreach (var b in byKey[key])
            {
                if (b.Kind == TemporalBoundKind.Appear && appearOffset is null)
                {
                    appearOffset = b.Offset;
                    appearIncl = b.Inclusive;
                }
                else if (b.Kind == TemporalBoundKind.Disappear && disappearOffset is null)
                {
                    disappearOffset = b.Offset;
                    disappearIncl = b.Inclusive;
                }
                else
                {
                    extras.Add(b);
                }
            }

            plan = new TemporalFilterPlan(plan, key, appearOffset, appearIncl, disappearOffset, disappearIncl, clock);
            foreach (var ex in extras)
            {
                plan = ex.Kind == TemporalBoundKind.Appear
                    ? new TemporalFilterPlan(plan, key, ex.Offset, ex.Inclusive, null, false, clock)
                    : new TemporalFilterPlan(plan, key, null, false, ex.Offset, ex.Inclusive, clock);
            }
        }

        return plan;
    }

    /// <summary>
    /// Build the COALESCE-desugar for <c>EXISTS (sq)</c>:
    /// <c>COALESCE(countSubquery, 0) &gt; 0</c>. The
    /// <paramref name="countSubquery"/> is the parser-cached
    /// <c>(SELECT COUNT(*) FROM (sq) AS __exists_inner)</c> — reusing the
    /// same instance everywhere keeps reference-equality dedup in
    /// <see cref="WrapWithScalarSubqueries"/> working across the WHERE
    /// pre-pass's uncorrelated arm and the scalar-resolve path.
    /// </summary>
    private static Expression BuildExistsCoalesceDesugar(SubqueryExpression countSubquery)
    {
        var zero = new LiteralExpression(LiteralKind.Integer, 0L);
        var coalesced = new FunctionCallExpression(
            "coalesce", new Expression[] { countSubquery, zero }, IsStar: false);
        return new BinaryExpression(BinaryOperator.Greater, coalesced, zero);
    }

    /// <summary>
    /// Lift a top-level <c>WHERE EXISTS (subquery)</c> or
    /// <c>WHERE NOT EXISTS (subquery)</c> conjunct.
    /// Uncorrelated cases re-synthesise the COALESCE-desugar and append it
    /// to <paramref name="scalarConjunctsForLater"/> for normal scalar
    /// resolution. Correlated cases lift to a <see cref="SemiJoinPlan"/>
    /// with the correlation columns as equi-keys (no IN-probe key, unlike
    /// <see cref="LiftInSubqueryToSemiJoin"/>). Correlated NOT EXISTS lifts to
    /// the same <see cref="SemiJoinPlan"/> with <c>IsAnti: true</c>, which the
    /// compiler emits as <c>outer − SemiJoin(outer, sq)</c>.
    /// </summary>
    private LogicalPlan LiftExistsOrDesugar(
        LogicalPlan plan,
        ExistsExpression e,
        bool isNegated,
        IReadOnlyDictionary<string, CteRef> cteScope,
        List<Expression> scalarConjunctsForLater)
    {
        var subPlan = ResolveQuery(e.Subquery.Query, cteScope, outerSchema: plan.Schema);
        var correlations = FindAllCorrelations(subPlan);

        if (correlations.Count == 0)
        {
            // Uncorrelated EXISTS: route through the existing scalar-subquery
            // path by appending the COALESCE-desugar to the pending scalar
            // conjuncts. Wrap with the unary-NOT if this was NOT EXISTS.
            Expression desugared = BuildExistsCoalesceDesugar(e.CountSubquery);
            if (isNegated)
            {
                desugared = new UnaryExpression(UnaryOperator.Not, desugared);
            }

            scalarConjunctsForLater.Add(desugared);
            return plan;
        }

        var (decorrelated, correlationKeys) = DecorrelateSubqueryPlan(subPlan, plan.Schema);
        return new SemiJoinPlan(plan, decorrelated, correlationKeys, IsAnti: isNegated);
    }

    /// <summary>
    /// Lift a single <c>probe IN (subquery)</c> conjunct into a
    /// <see cref="SemiJoinPlan"/> over <paramref name="plan"/>. The subquery
    /// is resolved with the outer plan's schema available; any
    /// <see cref="ResolvedCorrelationRef"/> the inner produces is then
    /// decorrelated into additional equi-keys against the corresponding
    /// outer columns (see <see cref="DecorrelateSubqueryPlan"/>).
    /// </summary>
    private LogicalPlan LiftInSubqueryToSemiJoin(
        LogicalPlan plan,
        InSubqueryExpression isq,
        IReadOnlyDictionary<string, CteRef> cteScope)
    {
        var probe = ResolveScalarExpression(isq.Probe, plan.Schema);
        var subPlan = ResolveQuery(isq.Subquery.Query, cteScope, outerSchema: plan.Schema);
        if (subPlan.Schema.Count != 1)
        {
            throw new ResolveException(
                $"IN-subquery must return exactly 1 column; got {subPlan.Schema.Count}");
        }

        // Detect correlations introduced via ResolvedCorrelationRef and
        // decorrelate: returns the rewritten subquery plan with the
        // correlation columns appended to its schema, plus the per-outer-index
        // equi-key list. The original SELECT column moves to the LAST inner
        // column (after the projected correlation columns); the probe pairs
        // with that index.
        var (decorrelated, correlationKeys) = DecorrelateSubqueryPlan(subPlan, plan.Schema);

        var probeInnerIndex = decorrelated.Schema.Count - 1;
        var probeInnerType = decorrelated.Schema[probeInnerIndex].Type;
        var common = TypeInference.CommonComparableType(probe.Type, probeInnerType);
        probe = MaybeCast(probe, common);
        if (!SameTypeIgnoringNullable(probeInnerType, common))
        {
            // Cast the last column (the original SELECT) to the common type
            // via a narrowing ProjectPlan that preserves the correlation
            // columns ahead of it.
            var projItems = new List<ProjectionItem>(decorrelated.Schema.Count);
            var newCols = new List<SchemaColumn>(decorrelated.Schema.Count);
            for (var i = 0; i < decorrelated.Schema.Count - 1; i++)
            {
                projItems.Add(new ProjectionItem(
                    new ResolvedColumn(i, decorrelated.Schema[i].Type),
                    decorrelated.Schema[i].Name,
                    decorrelated.Schema[i].Qualifier));
                newCols.Add(decorrelated.Schema[i]);
            }

            var probeCol = decorrelated.Schema[probeInnerIndex];
            var castedProbe = new ResolvedCast(
                new ResolvedColumn(probeInnerIndex, probeInnerType), common);
            projItems.Add(new ProjectionItem(castedProbe, probeCol.Name, probeCol.Qualifier));
            newCols.Add(new SchemaColumn(probeCol.Name, common, probeCol.Qualifier));
            decorrelated = new ProjectPlan(decorrelated, projItems, new Schema(newCols));
        }

        // For NOT IN with nullable probe or nullable subquery column, SQL
        // 3VL says the row drops when probe is NULL OR (no match found AND
        // any value in the subquery is NULL). The anti-semi-join handles
        // "no match found"; the rest is layered via LayerNullCountAndFilter:
        // it appends a hidden per-correlation-group null-count column,
        // then filters on probe IS NOT NULL AND (null_count IS NULL OR = 0).
        if (isq.IsNegated
            && (probe.Type.Nullable || decorrelated.Schema[probeInnerIndex].Type.Nullable))
        {
            plan = LayerNullCountAndFilter(plan, decorrelated, correlationKeys, probe, probeInnerIndex);
        }

        var equiKeys = new List<SemiJoinEqui>(correlationKeys.Count + 1)
        {
            new SemiJoinEqui(probe, probeInnerIndex, common),
        };
        equiKeys.AddRange(correlationKeys);

        return new SemiJoinPlan(plan, decorrelated, equiKeys, IsAnti: isq.IsNegated);
    }

    /// <summary>
    /// For nullable-operand <c>NOT IN (subquery)</c>: layer a hidden
    /// "null_count" column onto <paramref name="plan"/> (per-correlation-
    /// group when correlated, single global value when uncorrelated),
    /// then add a <see cref="FilterPlan"/> that enforces SQL three-valued
    /// semantics — drop the row if the probe is NULL or the subquery's
    /// correlation group contains any NULL value.
    /// </summary>
    /// <remarks>
    /// The null-count plan is built by filtering the decorrelated subquery
    /// to value-is-NULL rows and aggregating <c>COUNT(*)</c> grouped by
    /// the correlation columns. For uncorrelated cases the aggregate has
    /// no group keys (single global count); for correlated cases it
    /// groups by the correlation columns. Wraps via the existing
    /// <see cref="ScalarSubqueryJoinPlan"/> / <see cref="CorrelatedScalarSubqueryJoinPlan"/>
    /// machinery so the appended column is nullable (LEFT JOIN null-pad
    /// covers correlation groups with no inner rows, and DbspNet's
    /// aggregate emits no row for empty inputs).
    /// </remarks>
    private static LogicalPlan LayerNullCountAndFilter(
        LogicalPlan plan,
        LogicalPlan decorrelated,
        IReadOnlyList<SemiJoinEqui> correlationKeys,
        ResolvedExpression probe,
        int probeInnerIndex)
    {
        // Step 1: Filter the decorrelated subquery to rows where the value
        // column is NULL.
        var valueType = decorrelated.Schema[probeInnerIndex].Type;
        var valueColRef = new ResolvedColumn(probeInnerIndex, valueType);
        var isNullPred = new ResolvedIsNull(valueColRef, Negated: false, new SqlBooleanType(false));
        LogicalPlan nullsOnly = new FilterPlan(decorrelated, isNullPred);

        // Step 2: Aggregate COUNT(*) over the NULL-filtered subquery,
        // grouped by correlation columns (or no group key if uncorrelated).
        var groupKeys = new List<ResolvedExpression>(correlationKeys.Count);
        var groupKeyCols = new List<SchemaColumn>(correlationKeys.Count);
        for (var i = 0; i < correlationKeys.Count; i++)
        {
            var k = correlationKeys[i];
            groupKeys.Add(new ResolvedColumn(k.InnerColumnIndex, k.Type));
            groupKeyCols.Add(new SchemaColumn("__null_corr_" + i, k.Type));
        }

        var countType = new SqlBigintType(false);
        var aggregates = new List<AggregateCall>
        {
            new AggregateCall(AggregateKind.CountStar, Argument: null, countType),
        };

        var aggSchemaCols = new List<SchemaColumn>(groupKeyCols.Count + 1);
        aggSchemaCols.AddRange(groupKeyCols);
        aggSchemaCols.Add(new SchemaColumn("__null_count", countType));
        LogicalPlan nullCountPlan = new AggregatePlan(
            nullsOnly, groupKeys, aggregates, new Schema(aggSchemaCols));

        // For the uncorrelated case the ScalarSubqueryJoinPlan expects a
        // single-column subquery, which matches here (no group keys).
        // For the correlated case the inner is [__null_corr_0..N, __null_count];
        // the CorrelatedScalarSubqueryJoinPlan reads the last column.

        // Step 3: Layer as hidden column on `plan`. The added column lands
        // at index plan.Schema.Count and is nullable (LEFT JOIN null-pad).
        var hiddenColType = countType.WithNullable(true);
        var hiddenColName = $"$nullcnt{plan.Schema.Count}";
        var augmentedSchema = plan.Schema.Concat(
            new Schema([new SchemaColumn(hiddenColName, hiddenColType)]));
        var nullCountColIndex = plan.Schema.Count;

        if (correlationKeys.Count > 0)
        {
            var scalarIdx = nullCountPlan.Schema.Count - 1;
            plan = new CorrelatedScalarSubqueryJoinPlan(
                plan, nullCountPlan, correlationKeys, scalarIdx, augmentedSchema);
        }
        else
        {
            plan = new ScalarSubqueryJoinPlan(
                plan, new List<LogicalPlan> { nullCountPlan }, augmentedSchema);
        }

        // Step 4: Build the 3VL filter predicate:
        //   probe IS NOT NULL AND (null_count IS NULL OR null_count = 0)
        var boolNonNull = new SqlBooleanType(false);
        var boolNullable = new SqlBooleanType(true);
        ResolvedExpression probeNotNull = new ResolvedIsNull(probe, Negated: true, boolNonNull);

        var nullCountCol = new ResolvedColumn(nullCountColIndex, hiddenColType);
        ResolvedExpression nullCountIsNull =
            new ResolvedIsNull(nullCountCol, Negated: false, boolNonNull);
        var zeroLit = new ResolvedLiteral(LiteralKind.Integer, 0L, countType);
        ResolvedExpression nullCountEqZero = new ResolvedBinary(
            BinaryOperator.Equal, nullCountCol, zeroLit, boolNullable);
        ResolvedExpression nullCountIsNullOrZero = new ResolvedBinary(
            BinaryOperator.Or, nullCountIsNull, nullCountEqZero, boolNullable);
        ResolvedExpression filter = new ResolvedBinary(
            BinaryOperator.And, probeNotNull, nullCountIsNullOrZero, boolNullable);

        return new FilterPlan(plan, filter);
    }

    /// <summary>
    /// Detect correlation references in <paramref name="subPlan"/> (produced by
    /// <see cref="ResolvedCorrelationRef"/> during inner resolution) and rewrite
    /// the plan so that each correlation is lifted into an equi-key against an
    /// outer column. For each unique outer index referenced, the subquery's
    /// WHERE must contain an equality <c>ResolvedCorrelationRef(i) =
    /// ResolvedColumn(j)</c> (in either order); that conjunct is removed from
    /// the WHERE, and column <c>j</c> is projected into the subquery's schema
    /// as a new prepended column. Anything else (correlation in JOIN ON / HAVING
    /// / GROUP BY / aggregates, or non-equi correlation comparison) rejects.
    /// </summary>
    private (LogicalPlan Plan, IReadOnlyList<SemiJoinEqui> CorrelationKeys) DecorrelateSubqueryPlan(
        LogicalPlan subPlan, Schema outerSchema)
    {
        // Walk the plan to surface any FilterPlans and detect correlation
        // refs. v1 supports correlation only in the OUTERMOST FilterPlan
        // (which corresponds to the inner SELECT's WHERE clause after
        // resolution); deeper / nested-join correlation rejects.
        var correlationsInSubPlan = FindAllCorrelations(subPlan);
        if (correlationsInSubPlan.Count == 0)
        {
            return (subPlan, Array.Empty<SemiJoinEqui>());
        }

        // Find a FilterPlan in the subquery — must be the outermost wrapper
        // (or one wrapped by a single ProjectPlan, which the resolver always
        // emits at the SELECT boundary).
        var (filter, rebuild) = LocateOuterFilter(subPlan);
        if (filter is null)
        {
            throw new ResolveException(
                "correlated IN-subquery must have a WHERE clause containing the correlation equality");
        }

        var conjuncts = ExpressionRewriter.SplitAnd(filter.Predicate);
        var matchedOuterIndices = new Dictionary<int, int>(); // outer-index → inner-column-index
        var remainingConjuncts = new List<ResolvedExpression>();

        foreach (var c in conjuncts)
        {
            if (TryMatchEquiCorrelation(c, out var outerIdx, out var innerIdx))
            {
                if (matchedOuterIndices.ContainsKey(outerIdx))
                {
                    throw new ResolveException(
                        "correlated IN-subquery references the same outer column from multiple equi-predicates");
                }

                matchedOuterIndices[outerIdx] = innerIdx;
            }
            else if (ExpressionRewriter.CollectCorrelationIndices(c).Count > 0)
            {
                throw new ResolveException(
                    "only equi-correlation predicates (outer.col = inner.col) are supported in v1");
            }
            else
            {
                remainingConjuncts.Add(c);
            }
        }

        // Sanity-check every detected correlation was covered.
        foreach (var idx in correlationsInSubPlan)
        {
            if (!matchedOuterIndices.ContainsKey(idx))
            {
                throw new ResolveException(
                    "every correlated outer reference must appear in an equi-predicate (outer.col = inner.col) in v1");
            }
        }

        // Rebuild the FilterPlan with the correlation conjuncts removed. If
        // nothing's left, drop the filter entirely.
        LogicalPlan filterInput = filter.Input;
        LogicalPlan filtered = remainingConjuncts.Count == 0
            ? filterInput
            : new FilterPlan(filterInput, ExpressionRewriter.AndAll(remainingConjuncts));

        // We replace the outer Project (if any) entirely: the new subquery
        // schema is [corr_col_1, ..., corr_col_N, original_select_outputs...].
        // The original outer-Project's projections referenced the filter's
        // schema (Scan-relative indices); they still do, since the filter's
        // schema is unchanged. We just compose them with the correlation
        // projections.
        var (outerProjections, _) = GetOuterProjections(rebuild);
        var orderedOuter = matchedOuterIndices.Keys.ToList();
        orderedOuter.Sort();
        var projCols = new List<ProjectionItem>(orderedOuter.Count + outerProjections.Count);
        var newSchemaCols = new List<SchemaColumn>(orderedOuter.Count + outerProjections.Count);
        var keys = new List<SemiJoinEqui>(orderedOuter.Count);
        for (var i = 0; i < orderedOuter.Count; i++)
        {
            var outerIdx = orderedOuter[i];
            var innerIdx = matchedOuterIndices[outerIdx];
            var innerType = filtered.Schema[innerIdx].Type;
            var common = TypeInference.CommonComparableType(outerSchema[outerIdx].Type, innerType);
            ResolvedExpression projected = new ResolvedColumn(innerIdx, innerType);
            if (!SameTypeIgnoringNullable(innerType, common))
            {
                projected = new ResolvedCast(projected, common);
            }

            projCols.Add(new ProjectionItem(projected, "__corr_" + i, null));
            newSchemaCols.Add(new SchemaColumn("__corr_" + i, common, null));
            keys.Add(new SemiJoinEqui(
                new ResolvedColumn(outerIdx, outerSchema[outerIdx].Type),
                InnerColumnIndex: i,
                Type: common));
        }

        // Append the original outer-Project's projections (which targeted the
        // filter's schema). If there was no outer Project (subPlan is just a
        // FilterPlan), default to passing every column of the filtered plan
        // through.
        if (outerProjections.Count == 0)
        {
            for (var i = 0; i < filtered.Schema.Count; i++)
            {
                projCols.Add(new ProjectionItem(
                    new ResolvedColumn(i, filtered.Schema[i].Type),
                    filtered.Schema[i].Name,
                    filtered.Schema[i].Qualifier));
                newSchemaCols.Add(filtered.Schema[i]);
            }
        }
        else
        {
            foreach (var pi in outerProjections)
            {
                projCols.Add(pi);
                newSchemaCols.Add(new SchemaColumn(pi.Name, pi.Expression.Type, pi.Qualifier));
            }
        }

        LogicalPlan result = new ProjectPlan(filtered, projCols, new Schema(newSchemaCols));
        return (result, keys);
    }

    /// <summary>
    /// Inspect the rebuild callback returned by <see cref="LocateOuterFilter"/>
    /// to extract the outer ProjectPlan's projections (if any). The callback
    /// either returns its argument unchanged (FilterPlan was the root — no
    /// outer Project) or wraps it in a captured ProjectPlan. We probe by
    /// invoking with a sentinel input and reading off any ProjectPlan
    /// the rebuild wraps around it.
    /// </summary>
    private static (IReadOnlyList<ProjectionItem> Projections, Schema? Schema) GetOuterProjections(
        Func<LogicalPlan, LogicalPlan> rebuild)
    {
        var sentinel = SentinelPlan.Instance;
        var probed = rebuild(sentinel);
        if (probed is ProjectPlan p && ReferenceEquals(p.Input, sentinel))
        {
            return (p.Projections, p.Schema);
        }

        return (Array.Empty<ProjectionItem>(), null);
    }

    /// <summary>Probe sentinel plan used only by <see cref="GetOuterProjections"/>.</summary>
    private sealed record SentinelPlan() : LogicalPlan(Schema.Empty)
    {
        public static readonly SentinelPlan Instance = new();
    }

    /// <summary>
    /// Try to match <c>expr</c> as a correlated equi-predicate of the form
    /// <c>ResolvedCorrelationRef(i) = ResolvedColumn(j)</c> (in either order).
    /// </summary>
    private static bool TryMatchEquiCorrelation(
        ResolvedExpression expr, out int outerIdx, out int innerIdx)
    {
        outerIdx = -1;
        innerIdx = -1;
        if (expr is not ResolvedBinary { Operator: BinaryOperator.Equal } bin)
        {
            return false;
        }

        if (TryUnwrapCast(bin.Left) is ResolvedCorrelationRef cl
            && TryUnwrapCast(bin.Right) is ResolvedColumn cr)
        {
            outerIdx = cl.OuterIndex;
            innerIdx = cr.Index;
            return true;
        }

        if (TryUnwrapCast(bin.Right) is ResolvedCorrelationRef cl2
            && TryUnwrapCast(bin.Left) is ResolvedColumn cr2)
        {
            outerIdx = cl2.OuterIndex;
            innerIdx = cr2.Index;
            return true;
        }

        return false;
    }

    private static ResolvedExpression TryUnwrapCast(ResolvedExpression e) =>
        e is ResolvedCast c ? c.Operand : e;

    /// <summary>
    /// Walk <paramref name="plan"/> to collect every outer-column index
    /// referenced by a <see cref="ResolvedCorrelationRef"/>. v1 only supports
    /// correlation in the inner SELECT's WHERE; correlation in JOIN ON /
    /// HAVING / GROUP BY / projections rejects elsewhere in
    /// <see cref="DecorrelateSubqueryPlan"/>.
    /// </summary>
    /// <summary>
    /// Decorrelate a correlated scalar subquery. Expected shape (post-resolve)
    /// is <c>Project(Aggregate(GroupKeys=[…], Aggregates=[scalar],
    /// Filter(p, Scan(...))))</c> — the user wrote
    /// <c>SELECT MAX(y) FROM t WHERE t.k = outer.k</c>. The decorrelator
    /// strips the equi-correlation predicate from the inner filter,
    /// prepends correlation columns to the aggregate's GROUP BY, and
    /// rebuilds the outer Project to expose [corr_0, …, corr_N, scalar].
    /// Returns the rewritten plan plus the equi-key list and the index of
    /// the scalar column in the new schema.
    /// </summary>
    private (LogicalPlan Plan, IReadOnlyList<SemiJoinEqui> CorrelationKeys, int ScalarColumnIndex)
        DecorrelateScalarSubqueryPlan(LogicalPlan subPlan, Schema outerSchema)
    {
        // v1 only handles Project(Aggregate(Filter(...))) shape. Unwrap the
        // outer Project and find an AggregatePlan above a FilterPlan.
        if (subPlan is not ProjectPlan outerProject)
        {
            throw new ResolveException(
                "correlated scalar subquery must end in a projection over an aggregate in v1");
        }

        if (outerProject.Input is not AggregatePlan aggregate)
        {
            throw new ResolveException(
                "correlated scalar subquery without an aggregate is not supported in v1 " +
                "(the inner must group its result per correlation key)");
        }

        if (aggregate.Input is not FilterPlan filter)
        {
            throw new ResolveException(
                "correlated scalar subquery's inner aggregate must filter the input directly in v1");
        }

        // Correlation must live ONLY inside the FilterPlan's predicate.
        // Anywhere else (in GroupKeys, in Aggregates, in scalar projections)
        // is out of scope for v1.
        var inGroupKeys = aggregate.GroupKeys.Any(k =>
            ExpressionRewriter.CollectCorrelationIndices(k).Count > 0);
        var inAggregates = aggregate.Aggregates.Any(a =>
            a.Argument is { } arg && ExpressionRewriter.CollectCorrelationIndices(arg).Count > 0);
        if (inGroupKeys || inAggregates)
        {
            throw new ResolveException(
                "correlated scalar subquery with correlation inside the aggregate or " +
                "GROUP BY is not supported in v1");
        }

        var inProjections = outerProject.Projections.Any(p =>
            ExpressionRewriter.CollectCorrelationIndices(p.Expression).Count > 0);
        if (inProjections)
        {
            throw new ResolveException(
                "correlated scalar subquery with correlation inside its SELECT projection is not supported in v1");
        }

        // Split the FilterPlan's predicate at top-level AND. Pull out the
        // equi-correlation conjuncts; the rest stays as the residual filter.
        var conjuncts = ExpressionRewriter.SplitAnd(filter.Predicate);
        var matched = new Dictionary<int, int>();    // outer-index → inner-column-index in Filter's schema
        var residual = new List<ResolvedExpression>();
        foreach (var c in conjuncts)
        {
            if (TryMatchEquiCorrelation(c, out var oIdx, out var iIdx))
            {
                if (matched.ContainsKey(oIdx))
                {
                    throw new ResolveException(
                        "correlated scalar subquery references the same outer column via multiple equi-predicates");
                }

                matched[oIdx] = iIdx;
            }
            else if (ExpressionRewriter.CollectCorrelationIndices(c).Count > 0)
            {
                throw new ResolveException(
                    "only equi-correlation predicates (outer.col = inner.col) are supported in v1");
            }
            else
            {
                residual.Add(c);
            }
        }

        if (matched.Count == 0)
        {
            throw new ResolveException(
                "correlated scalar subquery has correlation refs but no equi-correlation predicate was found");
        }

        // Rebuild the filter without correlation conjuncts.
        LogicalPlan filteredInput = residual.Count == 0
            ? filter.Input
            : new FilterPlan(filter.Input, ExpressionRewriter.AndAll(residual));

        // Prepend correlation columns to the aggregate's GroupKeys. The
        // aggregate's output schema becomes
        //   [user GroupKeys..., correlation GroupKeys..., aggregate results...].
        // Reorder so correlation columns come FIRST, matching the
        // CorrelatedScalarSubqueryJoinPlan's expected
        // [__corr_0, ..., __corr_N, ..., scalar] shape; the
        // outer Project re-references them by their new positions.
        var orderedOuter = matched.Keys.ToList();
        orderedOuter.Sort();

        var newGroupKeys = new List<ResolvedExpression>(orderedOuter.Count + aggregate.GroupKeys.Count);
        var newGroupKeyAstCount = orderedOuter.Count + aggregate.GroupKeys.Count;
        var correlationInnerSchemaCols = new List<SchemaColumn>(orderedOuter.Count);
        var equiKeys = new List<SemiJoinEqui>(orderedOuter.Count);
        for (var i = 0; i < orderedOuter.Count; i++)
        {
            var outerIdx = orderedOuter[i];
            var innerIdx = matched[outerIdx];
            var innerType = filter.Input.Schema[innerIdx].Type;
            var common = TypeInference.CommonComparableType(
                outerSchema[outerIdx].Type, innerType);
            ResolvedExpression projected = new ResolvedColumn(innerIdx, innerType);
            if (!SameTypeIgnoringNullable(innerType, common))
            {
                projected = new ResolvedCast(projected, common);
            }

            newGroupKeys.Add(projected);
            correlationInnerSchemaCols.Add(new SchemaColumn($"__corr_{i}", common));
            equiKeys.Add(new SemiJoinEqui(
                new ResolvedColumn(outerIdx, outerSchema[outerIdx].Type),
                InnerColumnIndex: i,
                Type: common));
        }

        // Append the user's existing GroupKeys (preserve their semantics).
        foreach (var k in aggregate.GroupKeys)
        {
            newGroupKeys.Add(k);
        }

        // Build the aggregate's new schema:
        // [__corr_0..N, user GroupKey cols..., aggregate-result cols...].
        var aggOutCols = new List<SchemaColumn>(aggregate.Schema.Count + orderedOuter.Count);
        aggOutCols.AddRange(correlationInnerSchemaCols);
        // The aggregate's existing schema layout = [user GroupKeys..., agg results...].
        for (var i = 0; i < aggregate.Schema.Count; i++)
        {
            aggOutCols.Add(aggregate.Schema[i]);
        }

        var newAggregate = new AggregatePlan(
            filteredInput, newGroupKeys, aggregate.Aggregates, new Schema(aggOutCols));

        // Rebuild the outer Project: prepend correlation column refs
        // (now at indices [0, N) of the aggregate's schema), then re-emit
        // the user's original projections shifted by N.
        var newProjs = new List<ProjectionItem>(orderedOuter.Count + outerProject.Projections.Count);
        var newProjCols = new List<SchemaColumn>(orderedOuter.Count + outerProject.Projections.Count);
        for (var i = 0; i < orderedOuter.Count; i++)
        {
            var col = correlationInnerSchemaCols[i];
            newProjs.Add(new ProjectionItem(
                new ResolvedColumn(i, col.Type), col.Name, col.Qualifier));
            newProjCols.Add(col);
        }

        // The user's projections referenced the OLD aggregate schema
        // (where aggregate-result columns start at GroupKeys.Count). The
        // new aggregate schema shifts user GroupKey + aggregate-result
        // columns by N (the correlation columns added in front).
        for (var i = 0; i < outerProject.Projections.Count; i++)
        {
            var p = outerProject.Projections[i];
            var shifted = ExpressionRewriter.ShiftColumnIndices(p.Expression, delta: orderedOuter.Count);
            newProjs.Add(new ProjectionItem(shifted, p.Name, p.Qualifier));
            newProjCols.Add(new SchemaColumn(p.Name, shifted.Type, p.Qualifier));
        }

        var newProject = new ProjectPlan(newAggregate, newProjs, new Schema(newProjCols));
        var scalarIdx = newProject.Schema.Count - 1;
        return (newProject, equiKeys, scalarIdx);
    }

    /// <summary>
    /// Collect every outer-column index referenced by a
    /// <see cref="ResolvedCorrelationRef"/> anywhere in the plan tree.
    /// </summary>
    private static HashSet<int> FindAllCorrelations(LogicalPlan plan)
    {
        var result = new HashSet<int>();
        WalkResolvedExpressions(plan, expr =>
        {
            foreach (var idx in ExpressionRewriter.CollectCorrelationIndices(expr))
            {
                result.Add(idx);
            }
        });
        return result;
    }

    /// <summary>
    /// Walk every <see cref="ResolvedExpression"/> contained in
    /// <paramref name="plan"/>'s tree (predicates, projections, equi-keys,
    /// aggregate args, etc.) and invoke <paramref name="action"/> on each.
    /// </summary>
    private static void WalkResolvedExpressions(LogicalPlan plan, Action<ResolvedExpression> action)
    {
        switch (plan)
        {
            case FilterPlan f:
                action(f.Predicate);
                WalkResolvedExpressions(f.Input, action);
                break;
            case ProjectPlan p:
                foreach (var it in p.Projections) action(it.Expression);
                WalkResolvedExpressions(p.Input, action);
                break;
            case JoinPlan j:
                if (j.Residual is { } res) action(res);
                WalkResolvedExpressions(j.Left, action);
                WalkResolvedExpressions(j.Right, action);
                break;
            case AggregatePlan a:
                foreach (var k in a.GroupKeys) action(k);
                foreach (var c in a.Aggregates)
                {
                    if (c.Argument is { } arg) action(arg);
                }

                WalkResolvedExpressions(a.Input, action);
                break;
            case DistinctPlan d:
                WalkResolvedExpressions(d.Input, action);
                break;
            case UnionAllPlan u:
                foreach (var b in u.Branches) WalkResolvedExpressions(b, action);
                break;
            case DifferencePlan diff:
                WalkResolvedExpressions(diff.Left, action);
                WalkResolvedExpressions(diff.Right, action);
                break;
            case ScalarSubqueryJoinPlan ss:
                WalkResolvedExpressions(ss.Input, action);
                foreach (var sq in ss.Subqueries) WalkResolvedExpressions(sq, action);
                break;
            case SemiJoinPlan sj:
                foreach (var k in sj.EquiKeys) action(k.OuterKey);
                WalkResolvedExpressions(sj.Input, action);
                WalkResolvedExpressions(sj.Subquery, action);
                break;
            // Scan / CteScan / RecursiveCte: no resolved expressions to walk
            // (they're handled by their own machinery).
        }
    }

    /// <summary>
    /// Locate the outermost <see cref="FilterPlan"/> in a sub-plan tree
    /// shaped like <c>Project(Filter(Input))</c> (the standard SELECT...
    /// WHERE shape after resolution). Returns the filter plus a callback that
    /// rebuilds the surrounding tree with a replacement child.
    /// </summary>
    private static (FilterPlan? Filter, Func<LogicalPlan, LogicalPlan> Rebuild) LocateOuterFilter(LogicalPlan plan)
    {
        switch (plan)
        {
            case FilterPlan f:
                return (f, replacement => replacement);
            case ProjectPlan p when p.Input is FilterPlan f2:
                return (f2, replacement => p with { Input = replacement });
            default:
                return (null, x => x);
        }
    }

    /// <summary>
    /// Walk an expression for <see cref="ExistsExpression"/> and
    /// <see cref="InSubqueryExpression"/> nodes — anywhere they appear in
    /// non-WHERE positions (SELECT / HAVING / nested boolean). Used by the
    /// non-WHERE subquery boolean pre-pass to lift each to a hidden
    /// match-count column on the running plan. (WHERE-conjunct uses are
    /// peeled off in the WHERE pre-pass before this walker runs and don't
    /// reach here.)
    /// </summary>
    private static void CollectNonWhereBooleanSubqueries(
        Expression expr,
        List<ExistsExpression> existsAcc,
        List<InSubqueryExpression> inAcc)
    {
        switch (expr)
        {
            case ExistsExpression e:
                existsAcc.Add(e);
                // Don't recurse into the subquery; it has its own scope and
                // is handled when the boolean is lifted.
                break;
            case InSubqueryExpression isq:
                inAcc.Add(isq);
                break;
            case BinaryExpression b:
                CollectNonWhereBooleanSubqueries(b.Left, existsAcc, inAcc);
                CollectNonWhereBooleanSubqueries(b.Right, existsAcc, inAcc);
                break;
            case UnaryExpression u:
                CollectNonWhereBooleanSubqueries(u.Operand, existsAcc, inAcc);
                break;
            case IsNullExpression isn:
                CollectNonWhereBooleanSubqueries(isn.Operand, existsAcc, inAcc);
                break;
            case CastExpression c:
                CollectNonWhereBooleanSubqueries(c.Operand, existsAcc, inAcc);
                break;
            case FunctionCallExpression fn:
                foreach (var a in fn.Arguments)
                {
                    CollectNonWhereBooleanSubqueries(a, existsAcc, inAcc);
                }

                break;
            case InListExpression il:
                CollectNonWhereBooleanSubqueries(il.Probe, existsAcc, inAcc);
                foreach (var v in il.Values)
                {
                    CollectNonWhereBooleanSubqueries(v, existsAcc, inAcc);
                }

                break;
            case CaseExpression ce:
                foreach (var w in ce.Whens)
                {
                    CollectNonWhereBooleanSubqueries(w.Condition, existsAcc, inAcc);
                    CollectNonWhereBooleanSubqueries(w.Result, existsAcc, inAcc);
                }

                if (ce.ElseResult is not null)
                {
                    CollectNonWhereBooleanSubqueries(ce.ElseResult, existsAcc, inAcc);
                }

                break;
        }
    }

    /// <summary>
    /// Lift each <see cref="ExistsExpression"/> and
    /// <see cref="InSubqueryExpression"/> in non-WHERE positions to a
    /// hidden match-count column on <paramref name="plan"/>. Returns the
    /// augmented plan plus a binding map keyed by reference identity.
    /// </summary>
    /// <remarks>
    /// For each subquery: resolve with <c>outerSchema</c>, decorrelate via
    /// <see cref="DecorrelateSubqueryPlan"/> (correlation cols projected at
    /// the front of the inner schema), apply an optional <c>probe = value</c>
    /// filter (for IN/NOT IN), and aggregate <c>COUNT(*)</c> grouped by the
    /// correlation columns. The resulting per-correlation-group count is
    /// layered via <see cref="CorrelatedScalarSubqueryJoinPlan"/> (when
    /// correlated) or <see cref="ScalarSubqueryJoinPlan"/> (uncorrelated).
    /// The bound expression is read as <c>COALESCE(count, 0) &gt; 0</c>;
    /// <c>NOT IN</c> /<c>NOT EXISTS</c> inverts via the binding's
    /// <c>IsNegated</c> flag at scalar-resolve time.
    /// </remarks>
    private (LogicalPlan Plan,
        IReadOnlyDictionary<ExistsExpression, BooleanSubqueryBinding> ExistsMap,
        IReadOnlyDictionary<InSubqueryExpression, BooleanSubqueryBinding> InMap)
        WrapWithNonWhereBooleanSubqueries(
            LogicalPlan plan,
            IReadOnlyDictionary<string, CteRef> cteScope,
            IReadOnlyList<ExistsExpression> existsExprs,
            IReadOnlyList<InSubqueryExpression> inExprs)
    {
        var existsMap = new Dictionary<ExistsExpression, BooleanSubqueryBinding>();
        var inMap = new Dictionary<InSubqueryExpression, BooleanSubqueryBinding>();
        if (existsExprs.Count == 0 && inExprs.Count == 0)
        {
            return (plan, existsMap, inMap);
        }

        // Process EXISTS first, then IN.
        foreach (var e in existsExprs)
        {
            if (existsMap.ContainsKey(e)) { continue; }
            plan = LayerBooleanSubqueryCount(plan, cteScope, e.Subquery, probe: null,
                isNegated: false, out var bindingIdx);
            existsMap[e] = new BooleanSubqueryBinding(bindingIdx, IsNegated: false);
        }

        foreach (var isq in inExprs)
        {
            if (inMap.ContainsKey(isq)) { continue; }
            var probe = ResolveScalarExpression(isq.Probe, plan.Schema);

            // Decide the path by operand nullability. When neither the probe
            // nor the subquery column can be NULL, IN/NOT IN are two-valued and
            // the plain match-count comparison suffices (fast path). Otherwise
            // SQL three-valued logic can produce NULL, which a count comparison
            // can't express — layer total / null counts and emit a CASE.
            var probeSub = ResolveQuery(isq.Subquery.Query, cteScope, outerSchema: plan.Schema);
            if (probeSub.Schema.Count != 1)
            {
                throw new ResolveException(
                    $"IN (subquery) requires exactly 1 subquery column; got {probeSub.Schema.Count}");
            }

            var nullable = probe.Type.Nullable || probeSub.Schema[0].Type.Nullable;
            if (!nullable)
            {
                plan = LayerBooleanSubqueryCount(plan, cteScope, isq.Subquery,
                    probe: probe, isNegated: isq.IsNegated, out var bindingIdx);
                inMap[isq] = new BooleanSubqueryBinding(bindingIdx, isq.IsNegated);
                continue;
            }

            plan = LayerBooleanSubqueryCount(plan, cteScope, isq.Subquery,
                probe: probe, isNegated: false, out var matchIdx);
            plan = LayerBooleanSubqueryCount(plan, cteScope, isq.Subquery,
                probe: null, isNegated: false, out var totalIdx);
            plan = LayerNullCountColumn(plan, cteScope, isq.Subquery, out var nullIdx);
            inMap[isq] = new BooleanSubqueryBinding(matchIdx, isq.IsNegated)
            {
                NullableProbe = probe,
                TotalCountColumnIndex = totalIdx,
                NullCountColumnIndex = nullIdx,
            };
        }

        return (plan, existsMap, inMap);
    }

    /// <summary>
    /// Layer a per-correlation-group <c>COUNT(*)</c> hidden column on
    /// <paramref name="plan"/>. If <paramref name="probe"/> is non-null,
    /// the count only includes rows matching <c>value_col = probe</c>
    /// (i.e. IN-membership). Otherwise it counts all rows in the correlation
    /// group (i.e. EXISTS).
    /// </summary>
    private LogicalPlan LayerBooleanSubqueryCount(
        LogicalPlan plan,
        IReadOnlyDictionary<string, CteRef> cteScope,
        SubqueryExpression subquery,
        ResolvedExpression? probe,
        bool isNegated,
        out int countColIndex)
    {
        var subPlan = ResolveQuery(subquery.Query, cteScope, outerSchema: plan.Schema);
        if (probe is not null && subPlan.Schema.Count != 1)
        {
            throw new ResolveException(
                $"IN (subquery) requires exactly 1 subquery column; got {subPlan.Schema.Count}");
        }

        // Nullable operands are handled by the caller layering extra total /
        // null count columns and emitting a full 3VL CASE; this method just
        // produces the per-(correlation,value)-group match count. NULL probe
        // keys and NULL subquery values never equi-join, so the match count
        // correctly excludes them (the CASE then resolves their 3VL outcome).

        // Decorrelate; correlation columns project at the front, value column
        // (if any) sits at the back.
        var (decorrelated, correlationKeys) = DecorrelateSubqueryPlan(subPlan, plan.Schema);

        // Build Aggregate(GroupBy=correlation_cols [+ value_col when IN],
        // CountStar). For IN, the probe joins to the value column on the
        // outer side via an additional equi-key — so the value column becomes
        // a synthetic "correlation" column from the join layer's perspective.
        var groupKeys = new List<ResolvedExpression>(correlationKeys.Count + 1);
        var groupKeyCols = new List<SchemaColumn>(correlationKeys.Count + 1);
        for (var i = 0; i < correlationKeys.Count; i++)
        {
            var k = correlationKeys[i];
            groupKeys.Add(new ResolvedColumn(k.InnerColumnIndex, k.Type));
            groupKeyCols.Add(new SchemaColumn("__b_corr_" + i, k.Type));
        }

        // For IN, append the value column as an extra group key.
        SqlType? probeCommonType = null;
        int valueGroupKeyIndex = -1;
        if (probe is not null)
        {
            var probeInnerIndex = decorrelated.Schema.Count - 1;
            var probeInnerType = decorrelated.Schema[probeInnerIndex].Type;
            probeCommonType = TypeInference.CommonComparableType(probe.Type, probeInnerType);
            ResolvedExpression valueGroupExpr = new ResolvedColumn(probeInnerIndex, probeInnerType);
            if (!SameTypeIgnoringNullable(probeInnerType, probeCommonType))
            {
                valueGroupExpr = new ResolvedCast(valueGroupExpr, probeCommonType);
            }

            valueGroupKeyIndex = groupKeys.Count;
            groupKeys.Add(valueGroupExpr);
            groupKeyCols.Add(new SchemaColumn("__b_val", probeCommonType));
        }

        var countType = new SqlBigintType(false);
        var aggregates = new List<AggregateCall>
        {
            new AggregateCall(AggregateKind.CountStar, Argument: null, countType),
        };

        var aggCols = new List<SchemaColumn>(groupKeyCols.Count + 1);
        aggCols.AddRange(groupKeyCols);
        aggCols.Add(new SchemaColumn("__b_count", countType));
        LogicalPlan countPlan = new AggregatePlan(decorrelated, groupKeys, aggregates, new Schema(aggCols));

        // Layer as a hidden column on plan.
        var hiddenColType = countType.WithNullable(true);
        var hiddenColName = $"$bcount{plan.Schema.Count}";
        var augmentedSchema = plan.Schema.Concat(
            new Schema([new SchemaColumn(hiddenColName, hiddenColType)]));
        countColIndex = plan.Schema.Count;

        // For IN: augment correlationKeys with the probe→value-column key.
        // For uncorrelated IN this means we still use CorrelatedScalarSubqueryJoinPlan
        // (with a single equi-key) — the outer probe drives the per-row lookup.
        if (probe is not null)
        {
            var augmentedKeys = new List<SemiJoinEqui>(correlationKeys.Count + 1);
            augmentedKeys.AddRange(correlationKeys);
            augmentedKeys.Add(new SemiJoinEqui(probe, valueGroupKeyIndex, probeCommonType!));

            var scalarIdx = countPlan.Schema.Count - 1;
            return new CorrelatedScalarSubqueryJoinPlan(
                plan, countPlan, augmentedKeys, scalarIdx, augmentedSchema);
        }

        // EXISTS path: only correlation keys (if any) drive the lookup.
        if (correlationKeys.Count > 0)
        {
            var scalarIdx = countPlan.Schema.Count - 1;
            return new CorrelatedScalarSubqueryJoinPlan(
                plan, countPlan, correlationKeys, scalarIdx, augmentedSchema);
        }

        return new ScalarSubqueryJoinPlan(
            plan, new List<LogicalPlan> { countPlan }, augmentedSchema);
    }

    /// <summary>
    /// Layer a hidden per-correlation-group count of <em>NULL subquery
    /// values</em> onto <paramref name="plan"/>, returning its column index.
    /// Used by the nullable-operand non-WHERE <c>IN</c> / <c>NOT IN</c> 3VL
    /// path: a non-matching probe whose subquery group contains a NULL value
    /// yields UNKNOWN rather than FALSE. Mirrors the aggregation half of
    /// <see cref="LayerNullCountAndFilter"/> (the WHERE path) but appends a
    /// column instead of filtering. The appended column is nullable: empty
    /// correlation groups null-pad through the LEFT-JOIN layering, and
    /// <see cref="BuildBooleanSubqueryRef"/> reads it as <c>COALESCE(_, 0)</c>.
    /// </summary>
    private LogicalPlan LayerNullCountColumn(
        LogicalPlan plan,
        IReadOnlyDictionary<string, CteRef> cteScope,
        SubqueryExpression subquery,
        out int nullCountColIndex)
    {
        var subPlan = ResolveQuery(subquery.Query, cteScope, outerSchema: plan.Schema);
        var (decorrelated, correlationKeys) = DecorrelateSubqueryPlan(subPlan, plan.Schema);

        // Filter the decorrelated subquery to rows whose value column is NULL.
        var valueIndex = decorrelated.Schema.Count - 1;
        var valueType = decorrelated.Schema[valueIndex].Type;
        var isNullPred = new ResolvedIsNull(
            new ResolvedColumn(valueIndex, valueType), Negated: false, new SqlBooleanType(false));
        LogicalPlan nullsOnly = new FilterPlan(decorrelated, isNullPred);

        // COUNT(*) grouped by the correlation columns (none → single global count).
        var groupKeys = new List<ResolvedExpression>(correlationKeys.Count);
        var groupKeyCols = new List<SchemaColumn>(correlationKeys.Count);
        for (var i = 0; i < correlationKeys.Count; i++)
        {
            var k = correlationKeys[i];
            groupKeys.Add(new ResolvedColumn(k.InnerColumnIndex, k.Type));
            groupKeyCols.Add(new SchemaColumn("__nin_corr_" + i, k.Type));
        }

        var countType = new SqlBigintType(false);
        var aggregates = new List<AggregateCall>
        {
            new AggregateCall(AggregateKind.CountStar, Argument: null, countType),
        };

        var aggCols = new List<SchemaColumn>(groupKeyCols.Count + 1);
        aggCols.AddRange(groupKeyCols);
        aggCols.Add(new SchemaColumn("__nin_null_count", countType));
        LogicalPlan nullCountPlan = new AggregatePlan(
            nullsOnly, groupKeys, aggregates, new Schema(aggCols));

        var hiddenColType = countType.WithNullable(true);
        var hiddenColName = $"$nincnt{plan.Schema.Count}";
        var augmentedSchema = plan.Schema.Concat(
            new Schema([new SchemaColumn(hiddenColName, hiddenColType)]));
        nullCountColIndex = plan.Schema.Count;

        if (correlationKeys.Count > 0)
        {
            var scalarIdx = nullCountPlan.Schema.Count - 1;
            return new CorrelatedScalarSubqueryJoinPlan(
                plan, nullCountPlan, correlationKeys, scalarIdx, augmentedSchema);
        }

        return new ScalarSubqueryJoinPlan(
            plan, new List<LogicalPlan> { nullCountPlan }, augmentedSchema);
    }

    /// <summary>
    /// Build a single AST-keyed substitution dictionary from the per-AST
    /// EXISTS / IN binding maps. <see cref="ResolveScalarExpression"/>
    /// consults this dictionary at the top of its dispatch to short-circuit
    /// bound nodes into their lifted column reference.
    /// </summary>
    private static IReadOnlyDictionary<Expression, ResolvedExpression>? BuildPreBoundFromBoolMaps(
        IReadOnlyDictionary<ExistsExpression, BooleanSubqueryBinding> existsMap,
        IReadOnlyDictionary<InSubqueryExpression, BooleanSubqueryBinding> inMap)
    {
        if (existsMap.Count == 0 && inMap.Count == 0)
        {
            return null;
        }

        var d = new Dictionary<Expression, ResolvedExpression>();
        foreach (var (e, bind) in existsMap)
        {
            d[e] = BuildBooleanSubqueryRef(bind);
        }

        foreach (var (isq, bind) in inMap)
        {
            d[isq] = BuildBooleanSubqueryRef(bind);
        }

        return d;
    }

    /// <summary>
    /// Build the resolved boolean that a bound non-WHERE EXISTS / IN node
    /// reads as. The two-valued fast path (EXISTS, NOT-NULL-operand IN) is
    /// <c>COALESCE(match, 0) &gt; 0</c> (or <c>= 0</c> when negated). The
    /// nullable-operand IN path emits the full SQL three-valued
    /// <c>CASE</c> — see <see cref="BuildNullableInSubqueryRef"/>.
    /// </summary>
    private static ResolvedExpression BuildBooleanSubqueryRef(BooleanSubqueryBinding bind)
    {
        if (bind.NullableProbe is not null)
        {
            return BuildNullableInSubqueryRef(bind);
        }

        var countNullable = new SqlBigintType(true);
        var countNotNull = new SqlBigintType(false);
        var boolNullable = new SqlBooleanType(true);
        var countCol = new ResolvedColumn(bind.CountColumnIndex, countNullable);
        var zero = new ResolvedLiteral(LiteralKind.Integer, 0L, countNotNull);
        var coalesced = new ResolvedFunctionCall(
            "coalesce",
            new List<ResolvedExpression> { countCol, zero },
            countNotNull);
        return new ResolvedBinary(
            bind.IsNegated ? BinaryOperator.Equal : BinaryOperator.Greater,
            coalesced, zero, boolNullable);
    }

    /// <summary>
    /// Build the full SQL three-valued <c>IN</c> result for a nullable-operand
    /// subquery in a non-WHERE position, as a <c>CASE</c> over the three
    /// hidden count columns (match / total / null), then negate for
    /// <c>NOT IN</c>:
    /// <code>
    ///   CASE WHEN COALESCE(match, 0) > 0 THEN TRUE   -- a value equals the probe
    ///        WHEN COALESCE(total, 0) = 0 THEN FALSE  -- empty subquery group
    ///        WHEN probe IS NULL          THEN NULL   -- NULL probe vs non-empty
    ///        WHEN COALESCE(null,  0) > 0 THEN NULL   -- no match, but a NULL value
    ///        ELSE FALSE END                          -- no match, no NULLs
    /// </code>
    /// <c>NOT IN</c> wraps this in <c>NOT(...)</c>; three-valued NOT maps
    /// TRUE→FALSE, FALSE→TRUE, NULL→NULL, which is exactly the SQL
    /// <c>NOT IN</c> truth table.
    /// </summary>
    private static ResolvedExpression BuildNullableInSubqueryRef(BooleanSubqueryBinding bind)
    {
        var boolNonNull = new SqlBooleanType(false);
        var boolNullable = new SqlBooleanType(true);

        ResolvedExpression trueLit = new ResolvedLiteral(LiteralKind.Boolean, true, boolNonNull);
        ResolvedExpression falseLit = new ResolvedLiteral(LiteralKind.Boolean, false, boolNonNull);
        ResolvedExpression nullLit = new ResolvedLiteral(LiteralKind.Null, null, boolNullable);

        var clauses = new List<ResolvedCaseClause>
        {
            new ResolvedCaseClause(CountCompare(bind.CountColumnIndex, BinaryOperator.Greater), trueLit),
            new ResolvedCaseClause(CountCompare(bind.TotalCountColumnIndex, BinaryOperator.Equal), falseLit),
            new ResolvedCaseClause(
                new ResolvedIsNull(bind.NullableProbe!, Negated: false, boolNonNull), nullLit),
            new ResolvedCaseClause(CountCompare(bind.NullCountColumnIndex, BinaryOperator.Greater), nullLit),
        };

        ResolvedExpression positive = new ResolvedCaseWhen(clauses, falseLit, boolNullable);
        return bind.IsNegated
            ? new ResolvedUnary(UnaryOperator.Not, positive, boolNullable)
            : positive;

        // COALESCE(countCol, 0) <op> 0 — a definite boolean over a nullable count.
        static ResolvedExpression CountCompare(int countColIndex, BinaryOperator op)
        {
            var countNullable = new SqlBigintType(true);
            var countNotNull = new SqlBigintType(false);
            var countCol = new ResolvedColumn(countColIndex, countNullable);
            var zero = new ResolvedLiteral(LiteralKind.Integer, 0L, countNotNull);
            var coalesced = new ResolvedFunctionCall(
                "coalesce", new List<ResolvedExpression> { countCol, zero }, countNotNull);
            return new ResolvedBinary(op, coalesced, zero, new SqlBooleanType(false));
        }
    }

    /// <summary>
    /// Resolve each (distinct-by-AST) scalar subquery in <paramref name="subqueries"/>
    /// and append a hidden column per subquery to <paramref name="plan"/>'s
    /// schema via a <see cref="ScalarSubqueryJoinPlan"/>. Returns the wrapped
    /// plan and populates <paramref name="subqueryMap"/> so expression
    /// resolution can rewrite each <see cref="SubqueryExpression"/> to a
    /// <see cref="ResolvedColumn"/> pointing at the new hidden column.
    /// </summary>
    private LogicalPlan WrapWithScalarSubqueries(
        LogicalPlan plan,
        IReadOnlyDictionary<string, CteRef> cteScope,
        IReadOnlyList<SubqueryExpression> subqueries,
        out IReadOnlyDictionary<SubqueryExpression, SubqueryBinding> subqueryMap)
    {
        var map = new Dictionary<SubqueryExpression, SubqueryBinding>();
        subqueryMap = map;
        if (subqueries.Count == 0)
        {
            return plan;
        }

        // Uncorrelated subqueries stay batched in a single
        // ScalarSubqueryJoinPlan (one hidden column per subquery, unit-key
        // LEFT JOIN — the existing path). Correlated subqueries each get
        // their own layered CorrelatedScalarSubqueryJoinPlan on top of the
        // running plan because every one has its own equi-key tuple.
        var uncorrelatedSubPlans = new List<LogicalPlan>();
        var uncorrelatedSubqueries = new List<SubqueryExpression>();

        foreach (var sq in subqueries)
        {
            // Dedup by structural AST equality. In practice two separate
            // parses of the same subquery text produce ASTs that compare
            // UNEQUAL because C# records embed their list-valued fields by
            // reference, so this only helps when the same SubqueryExpression
            // instance is referenced twice inside one expression tree.
            // Callers who need guaranteed sharing should use a CTE.
            if (map.ContainsKey(sq))
            {
                continue;
            }

            var subPlan = ResolveQuery(sq.Query, cteScope, outerSchema: plan.Schema);
            if (subPlan.Schema.Count != 1)
            {
                throw new ResolveException(
                    $"scalar subquery must return exactly 1 column; got {subPlan.Schema.Count}");
            }

            var correlations = FindAllCorrelations(subPlan);
            if (correlations.Count == 0)
            {
                uncorrelatedSubPlans.Add(subPlan);
                uncorrelatedSubqueries.Add(sq);
                continue;
            }

            // Correlated scalar subquery: decorrelate to a multi-column
            // LEFT JOIN. Layer one CorrelatedScalarSubqueryJoinPlan per
            // correlated subquery on top of the running plan; the binding
            // points at the appended scalar column on this new layer.
            var (decorrelated, correlationKeys, scalarIdx) =
                DecorrelateScalarSubqueryPlan(subPlan, plan.Schema);
            var scalarType = decorrelated.Schema[scalarIdx].Type.WithNullable(true);
            var hiddenColIdx = plan.Schema.Count;
            var hiddenColName = $"$sub{hiddenColIdx}";
            var newSchema = plan.Schema.Concat(
                new Schema([new SchemaColumn(hiddenColName, scalarType)]));
            plan = new CorrelatedScalarSubqueryJoinPlan(
                plan, decorrelated, correlationKeys, scalarIdx, newSchema);
            map[sq] = new SubqueryBinding(hiddenColIdx, scalarType);
        }

        if (uncorrelatedSubPlans.Count > 0)
        {
            var newCols = new List<SchemaColumn>(plan.Schema.Columns);
            for (var i = 0; i < uncorrelatedSubPlans.Count; i++)
            {
                var sq = uncorrelatedSubqueries[i];
                var subPlan = uncorrelatedSubPlans[i];
                var resultType = subPlan.Schema[0].Type.WithNullable(true);
                var colIdx = plan.Schema.Count + i;
                map[sq] = new SubqueryBinding(colIdx, resultType);
                newCols.Add(new SchemaColumn($"$sub{colIdx}", resultType));
            }

            plan = new ScalarSubqueryJoinPlan(plan, uncorrelatedSubPlans, new Schema(newCols));
        }

        return plan;
    }

    // ---------- Aggregate collection (pre-walk) ----------

    private static void CollectAggregatesInto(
        Expression expr,
        Schema preSchema,
        IReadOnlyList<(Expression Ast, int OutputIndex)> groupKeys,
        List<AggregateCall> aggregates,
        Dictionary<AggregateKey, int> aggIndex)
    {
        // Match against GROUP BY keys first so `foo(g)` where g is grouped
        // doesn't recurse looking for aggregates inside a group-key column.
        for (var i = 0; i < groupKeys.Count; i++)
        {
            if (AstEqual(groupKeys[i].Ast, expr))
            {
                return;
            }
        }

        switch (expr)
        {
            case FunctionCallExpression call when IsAggregateName(call.FunctionName, call.IsStar):
                {
                    var kind = ToAggregateKind(call);
                    var (arg, fraction, discrete) = ResolveAggregateArgs(call, kind, preSchema);
                    var resultType = ComputeAggregateResultType(kind, arg?.Type);
                    var key = new AggregateKey(kind, arg, fraction, discrete);
                    if (!aggIndex.ContainsKey(key))
                    {
                        aggIndex[key] = aggregates.Count;
                        aggregates.Add(new AggregateCall(kind, arg, resultType, fraction, discrete));
                    }
                }

                break;
            case FunctionCallExpression fn:
                foreach (var a in fn.Arguments)
                {
                    CollectAggregatesInto(a, preSchema, groupKeys, aggregates, aggIndex);
                }

                break;
            case BinaryExpression b:
                CollectAggregatesInto(b.Left, preSchema, groupKeys, aggregates, aggIndex);
                CollectAggregatesInto(b.Right, preSchema, groupKeys, aggregates, aggIndex);
                break;
            case UnaryExpression u:
                CollectAggregatesInto(u.Operand, preSchema, groupKeys, aggregates, aggIndex);
                break;
            case IsNullExpression isn:
                CollectAggregatesInto(isn.Operand, preSchema, groupKeys, aggregates, aggIndex);
                break;
            case CastExpression c:
                CollectAggregatesInto(c.Operand, preSchema, groupKeys, aggregates, aggIndex);
                break;
            case InListExpression il:
                CollectAggregatesInto(il.Probe, preSchema, groupKeys, aggregates, aggIndex);
                foreach (var v in il.Values)
                {
                    CollectAggregatesInto(v, preSchema, groupKeys, aggregates, aggIndex);
                }

                break;
            case CaseExpression ce:
                foreach (var w in ce.Whens)
                {
                    CollectAggregatesInto(w.Condition, preSchema, groupKeys, aggregates, aggIndex);
                    CollectAggregatesInto(w.Result, preSchema, groupKeys, aggregates, aggIndex);
                }

                if (ce.ElseResult is not null)
                {
                    CollectAggregatesInto(ce.ElseResult, preSchema, groupKeys, aggregates, aggIndex);
                }

                break;
            case ExistsExpression:
                // EXISTS's inner subquery lives in its own scope; its
                // aggregates don't roll up to the outer SELECT.
                break;
            // Literals, column refs, subqueries contribute no aggregates here.
        }
    }

    // ---------- Scalar expression resolution ----------

    private static ResolvedExpression ResolveScalarExpression(
        Expression expr,
        Schema schema,
        IReadOnlyDictionary<SubqueryExpression, SubqueryBinding>? subqueryMap = null,
        Schema? outerSchema = null,
        IReadOnlyDictionary<Expression, ResolvedExpression>? preBound = null)
    {
        if (preBound is not null && preBound.TryGetValue(expr, out var pre))
        {
            return pre;
        }

        return expr switch
        {
            LiteralExpression lit => ResolveLiteral(lit),
            ColumnReference cr => ResolveColumn(cr, schema, outerSchema),
            UnaryExpression un => ResolveUnary(un, schema, subqueryMap, outerSchema, preBound),
            BinaryExpression bin => ResolveBinary(bin, schema, subqueryMap, outerSchema, preBound),
            IsNullExpression isn => ResolveIsNull(isn, schema, subqueryMap, outerSchema, preBound),
            CastExpression cast => ResolveCast(cast, schema, subqueryMap, outerSchema, preBound),
            FunctionCallExpression fn when !IsAggregateName(fn.FunctionName, fn.IsStar) => ResolveScalarFunction(fn, schema, subqueryMap, outerSchema, preBound),
            FunctionCallExpression fn => throw new ResolveException(
                $"aggregate function '{fn.FunctionName}' is not allowed here"),
            SubqueryExpression sq => ResolveSubqueryReference(sq, subqueryMap),
            InListExpression il => ResolveInList(il, schema, subqueryMap, outerSchema, preBound),
            CaseExpression ce => ResolveCaseWhen(ce, schema, subqueryMap, outerSchema, preBound),
            InSubqueryExpression => throw new ResolveException(
                "IN (subquery) is only supported as a top-level conjunct of WHERE, " +
                "or in SELECT/HAVING with NOT NULL probe and subquery column"),
            ExistsExpression e => ResolveExistsAsScalar(e, schema, subqueryMap),
            NowExpression { Function: NowFunction.CurrentTime } => throw new ResolveException(
                "CURRENT_TIME is cyclic (it wraps every midnight) and so is not monotone; it has no " +
                "sound advancing-clock semantics and is not supported. Use CURRENT_TIMESTAMP for an " +
                "absolute-time value."),
            NowExpression now => throw new ResolveException(
                $"{NowDisplay(now)} is only allowed inside a temporal-filter predicate in WHERE " +
                "(a comparison of a TIMESTAMP — or, for CURRENT_DATE, a DATE — expression against it, " +
                "optionally shifted by a constant day-time INTERVAL, e.g. WHERE ts > NOW() - " +
                "INTERVAL '1' HOUR). It is not a general scalar function."),
            _ => throw new ResolveException($"unsupported expression: {expr.GetType().Name}"),
        };
    }

    /// <summary>
    /// Resolve <see cref="ExistsExpression"/> outside the WHERE-conjunct
    /// fast path: synthesise the <c>COALESCE((SELECT COUNT(*) FROM (sq)), 0) &gt; 0</c>
    /// desugar and recursively resolve. <see cref="outerSchema"/> is dropped
    /// on the recursive call — correlated EXISTS in SELECT / HAVING / nested
    /// boolean positions is deferred; any outer-column reference in the
    /// inner subquery will fail at <see cref="Schema.Resolve"/> with the
    /// usual "unknown column" error.
    /// </summary>
    private static ResolvedExpression ResolveExistsAsScalar(
        ExistsExpression e,
        Schema schema,
        IReadOnlyDictionary<SubqueryExpression, SubqueryBinding>? subqueryMap)
    {
        var desugared = BuildExistsCoalesceDesugar(e.CountSubquery);
        return ResolveScalarExpression(desugared, schema, subqueryMap, outerSchema: null);
    }

    private static ResolvedExpression ResolveInList(
        InListExpression il,
        Schema schema,
        IReadOnlyDictionary<SubqueryExpression, SubqueryBinding>? subqueryMap,
        Schema? outerSchema = null,
        IReadOnlyDictionary<Expression, ResolvedExpression>? preBound = null)
    {
        if (il.Values.Count == 0)
        {
            throw new ResolveException("IN (...) requires at least one value");
        }

        var probe = ResolveScalarExpression(il.Probe, schema, subqueryMap, outerSchema, preBound);
        var values = new List<ResolvedExpression>(il.Values.Count);
        foreach (var v in il.Values)
        {
            values.Add(ResolveScalarExpression(v, schema, subqueryMap, outerSchema, preBound));
        }

        // Fold a common comparable type across probe and every value, then
        // cast each side to it — same shape as a chain of binary equalities.
        // Mismatched types fall out of CommonComparableType with a clear error.
        var common = probe.Type;
        foreach (var v in values)
        {
            common = TypeInference.CommonComparableType(common, v.Type);
        }

        probe = MaybeCast(probe, common);
        for (var i = 0; i < values.Count; i++)
        {
            values[i] = MaybeCast(values[i], common);
        }

        // Result is BOOLEAN; nullable iff probe or any value is nullable
        // (NULL probe → NULL result; non-match with NULL among values → NULL).
        var nullable = probe.Type.Nullable;
        foreach (var v in values)
        {
            nullable |= v.Type.Nullable;
        }

        return new ResolvedInList(probe, values, il.IsNegated, new SqlBooleanType(nullable));
    }

    private static ResolvedExpression ResolveCaseWhen(
        CaseExpression ce,
        Schema schema,
        IReadOnlyDictionary<SubqueryExpression, SubqueryBinding>? subqueryMap,
        Schema? outerSchema = null,
        IReadOnlyDictionary<Expression, ResolvedExpression>? preBound = null)
    {
        if (ce.Whens.Count == 0)
        {
            throw new ResolveException("CASE requires at least one WHEN branch");
        }

        // Resolve every condition (must be BOOLEAN) and every result. Fold a
        // common result type across all THEN branches and the ELSE — same
        // unification used for UNION branches and COALESCE.
        var conditions = new List<ResolvedExpression>(ce.Whens.Count);
        var results = new List<ResolvedExpression>(ce.Whens.Count);
        foreach (var clause in ce.Whens)
        {
            var cond = ResolveScalarExpression(clause.Condition, schema, subqueryMap, outerSchema, preBound);
            if (cond.Type is not SqlBooleanType)
            {
                throw new ResolveException(
                    $"CASE WHEN condition must be BOOLEAN, got {cond.Type.Display}");
            }

            conditions.Add(cond);
            results.Add(ResolveScalarExpression(clause.Result, schema, subqueryMap, outerSchema, preBound));
        }

        ResolvedExpression? elseResult = ce.ElseResult is null
            ? null
            : ResolveScalarExpression(ce.ElseResult, schema, subqueryMap, outerSchema, preBound);

        var common = results[0].Type;
        for (var i = 1; i < results.Count; i++)
        {
            common = TypeInference.CommonComparableType(common, results[i].Type);
        }

        if (elseResult is not null)
        {
            common = TypeInference.CommonComparableType(common, elseResult.Type);
        }

        // Result is nullable if any branch is nullable (CommonComparableType
        // already ORs that in) OR if there's no ELSE (unmatched → NULL).
        common = common.WithNullable(common.Nullable || elseResult is null);

        var clauses = new List<ResolvedCaseClause>(ce.Whens.Count);
        for (var i = 0; i < conditions.Count; i++)
        {
            clauses.Add(new ResolvedCaseClause(conditions[i], MaybeCast(results[i], common)));
        }

        var resolvedElse = elseResult is null ? null : MaybeCast(elseResult, common);
        return new ResolvedCaseWhen(clauses, resolvedElse, common);
    }

    private static ResolvedColumn ResolveSubqueryReference(
        SubqueryExpression sq,
        IReadOnlyDictionary<SubqueryExpression, SubqueryBinding>? subqueryMap)
    {
        if (subqueryMap is null || !subqueryMap.TryGetValue(sq, out var binding))
        {
            throw new ResolveException(
                "scalar subquery is not allowed here (v1 supports them in WHERE, SELECT, and HAVING only)");
        }

        return new ResolvedColumn(binding.ColumnIndex, binding.Type);
    }

    private static ResolvedExpression ResolveLiteral(LiteralExpression lit) => lit.Kind switch
    {
        // Integer literals narrow to the smallest numeric SQL type that fits.
        // The lexer gives us long (or decimal for out-of-range); the compiler
        // unboxes by CLR type at runtime, so the stored value's CLR type must
        // match the resolved SQL type.
        LiteralKind.Integer => NarrowIntegerLiteral((long)lit.Value!),
        LiteralKind.Decimal => new ResolvedLiteral(lit.Kind, lit.Value,
            new SqlDecimalType(38, lit.DecimalScale, false)),
        LiteralKind.Float => new ResolvedLiteral(lit.Kind, lit.Value, new SqlDoubleType(false)),
        LiteralKind.String => new ResolvedLiteral(lit.Kind,
            Utf8String.Of((string)lit.Value!),
            new SqlVarcharType(null, false)),
        LiteralKind.Boolean => new ResolvedLiteral(lit.Kind, lit.Value, new SqlBooleanType(false)),
        LiteralKind.Null => new ResolvedLiteral(lit.Kind, null, new SqlIntegerType(true)),
        _ => throw new ResolveException($"unknown literal kind {lit.Kind}"),
    };

    private static ResolvedLiteral NarrowIntegerLiteral(long v)
    {
        if (v >= int.MinValue && v <= int.MaxValue)
        {
            return new ResolvedLiteral(LiteralKind.Integer, (int)v, new SqlIntegerType(false));
        }

        return new ResolvedLiteral(LiteralKind.Integer, v, new SqlBigintType(false));
    }

    private static ResolvedExpression ResolveColumn(
        ColumnReference cr, Schema schema, Schema? outerSchema = null)
    {
        var idx = schema.TryResolve(cr.Qualifier, cr.Name);
        if (idx >= 0)
        {
            return new ResolvedColumn(idx, schema[idx].Type);
        }

        if (outerSchema is not null)
        {
            var outerIdx = outerSchema.TryResolve(cr.Qualifier, cr.Name);
            if (outerIdx >= 0)
            {
                return new ResolvedCorrelationRef(outerIdx, outerSchema[outerIdx].Type);
            }
        }

        // Re-call the throwing form on the local schema so the error message
        // matches what callers see today (column-not-found pointing at the
        // local scope, not the outer scope).
        schema.Resolve(cr.Qualifier, cr.Name);
        throw new InvalidOperationException("unreachable");
    }

    private static ResolvedExpression ResolveUnary(
        UnaryExpression un,
        Schema schema,
        IReadOnlyDictionary<SubqueryExpression, SubqueryBinding>? subqueryMap,
        Schema? outerSchema = null,
        IReadOnlyDictionary<Expression, ResolvedExpression>? preBound = null)
    {
        var operand = ResolveScalarExpression(un.Operand, schema, subqueryMap, outerSchema, preBound);
        if (un.Operator == UnaryOperator.Not)
        {
            if (!TypeInference.IsBoolean(operand.Type))
            {
                throw new ResolveException("NOT requires a BOOLEAN operand");
            }

            return new ResolvedUnary(un.Operator, operand, operand.Type);
        }

        // Negate
        if (!TypeInference.IsNumeric(operand.Type))
        {
            throw new ResolveException("unary - requires a numeric operand");
        }

        return new ResolvedUnary(un.Operator, operand, operand.Type);
    }

    private static ResolvedExpression ResolveBinary(
        BinaryExpression bin,
        Schema schema,
        IReadOnlyDictionary<SubqueryExpression, SubqueryBinding>? subqueryMap,
        Schema? outerSchema = null,
        IReadOnlyDictionary<Expression, ResolvedExpression>? preBound = null)
    {
        var left = ResolveScalarExpression(bin.Left, schema, subqueryMap, outerSchema, preBound);
        var right = ResolveScalarExpression(bin.Right, schema, subqueryMap, outerSchema, preBound);
        switch (bin.Operator)
        {
            case BinaryOperator.Add:
            case BinaryOperator.Subtract:
            case BinaryOperator.Multiply:
            case BinaryOperator.Divide:
            case BinaryOperator.Modulo:
                return ResolveArithmetic(bin.Operator, left, right);
            case BinaryOperator.Equal:
            case BinaryOperator.NotEqual:
            case BinaryOperator.Less:
            case BinaryOperator.LessEqual:
            case BinaryOperator.Greater:
            case BinaryOperator.GreaterEqual:
                var commonType = TypeInference.CommonComparableType(left.Type, right.Type);
                left = MaybeCast(left, commonType);
                right = MaybeCast(right, commonType);
                return new ResolvedBinary(bin.Operator, left, right,
                    new SqlBooleanType(left.Type.Nullable || right.Type.Nullable));
            case BinaryOperator.And:
            case BinaryOperator.Or:
                if (!TypeInference.IsBoolean(left.Type) || !TypeInference.IsBoolean(right.Type))
                {
                    throw new ResolveException($"{bin.Operator} requires BOOLEAN operands");
                }

                return new ResolvedBinary(bin.Operator, left, right,
                    new SqlBooleanType(left.Type.Nullable || right.Type.Nullable));
            default:
                throw new ResolveException($"unknown binary operator {bin.Operator}");
        }
    }

    private static ResolvedExpression MaybeCast(ResolvedExpression e, SqlType target)
    {
        if (SameTypeIgnoringNullable(e.Type, target))
        {
            return e;
        }

        return new ResolvedCast(e, target);
    }

    /// <summary>
    /// Resolve a numeric arithmetic binary op (+, −, *, /, %). When at least
    /// one operand is DECIMAL — including via INT/BIGINT promotion — the
    /// result type follows operator-specific SQL Server / Substrait
    /// promotion rules via <see cref="TypeInference.DecimalArithmeticType"/>;
    /// operands stay at their natural types because the decimal kernels
    /// rescale internally. For non-decimal numeric mixes (INT + INT,
    /// DOUBLE + REAL, etc.) the result is the simple <c>CommonNumericType</c>
    /// promotion and operands are coerced to the common type.
    /// </summary>
    private static ResolvedExpression ResolveArithmetic(
        BinaryOperator op, ResolvedExpression left, ResolvedExpression right)
    {
        // Temporal / interval arithmetic (date ± interval, ts − ts,
        // interval ± interval, interval * n, …) is resolved separately —
        // it doesn't follow the numeric-promotion lattice.
        if (TypeInference.IsTemporal(left.Type) || TypeInference.IsTemporal(right.Type)
            || left.Type is SqlIntervalType || right.Type is SqlIntervalType)
        {
            return ResolveTemporalArithmetic(op, left, right);
        }

        // If neither side is decimal, and both are numeric, the existing
        // common-type promotion lattice produces an INT/BIGINT/REAL/DOUBLE
        // result that the compiler handles directly.
        if (left.Type is not SqlDecimalType && right.Type is not SqlDecimalType)
        {
            var numType = TypeInference.CommonNumericType(left.Type, right.Type);
            // CommonNumericType may still pick DECIMAL when both sides are
            // small integers — but that path is unreachable here (rank ≤ 2
            // can't combine to rank 3).
            if (numType is SqlDecimalType decFromInts)
            {
                // Defensive: rank promotion produced DECIMAL despite both
                // operands being non-decimal. Use per-op rules.
                _ = decFromInts;
                return BuildDecimalArithmetic(op, left, right);
            }

            return new ResolvedBinary(op,
                MaybeCast(left, numType),
                MaybeCast(right, numType),
                numType);
        }

        // At least one side is DECIMAL. If the other side is REAL/DOUBLE,
        // the common type is float — fall back to lattice promotion.
        if ((left.Type is SqlRealType or SqlDoubleType)
            || (right.Type is SqlRealType or SqlDoubleType))
        {
            var numType = TypeInference.CommonNumericType(left.Type, right.Type);
            return new ResolvedBinary(op,
                MaybeCast(left, numType),
                MaybeCast(right, numType),
                numType);
        }

        return BuildDecimalArithmetic(op, left, right);
    }

    /// <summary>
    /// Build a decimal-typed <see cref="ResolvedBinary"/> with the per-op
    /// SQL Server / Substrait result type and natural-typed operands. The
    /// expression compiler's <c>ToDecimalOperand</c> handles INT/BIGINT
    /// promotion and the kernels rescale to the result type.
    /// </summary>
    private static ResolvedExpression BuildDecimalArithmetic(
        BinaryOperator op, ResolvedExpression left, ResolvedExpression right)
    {
        var resultType = TypeInference.DecimalArithmeticType(op, left.Type, right.Type);
        return new ResolvedBinary(op, left, right, resultType);
    }

    /// <summary>
    /// Resolve arithmetic involving a temporal (DATE/TIME/TIMESTAMP) and/or
    /// INTERVAL operand. The supported forms (operands kept at their natural
    /// types; the expression compilers dispatch on the operand + result types):
    /// <list type="bullet">
    ///   <item>temporal ± interval → same temporal type</item>
    ///   <item>interval + temporal → that temporal type</item>
    ///   <item>temporal − temporal (same kind) → interval</item>
    ///   <item>interval ± interval (same class) → interval</item>
    ///   <item>interval × numeric / interval ÷ numeric → interval</item>
    /// </list>
    /// Anything else (e.g. DATE + DATE, temporal × numeric, modulo) is a
    /// resolve error. DATE arithmetic is day-granular: a day-time interval
    /// shifts a DATE by whole days, dropping any sub-day part.
    /// </summary>
    private static ResolvedExpression ResolveTemporalArithmetic(
        BinaryOperator op, ResolvedExpression left, ResolvedExpression right)
    {
        var nullable = left.Type.Nullable || right.Type.Nullable;
        var lt = left.Type;
        var rt = right.Type;
        var lTemporal = TypeInference.IsTemporal(lt);
        var rTemporal = TypeInference.IsTemporal(rt);
        var lInterval = lt is SqlIntervalType;
        var rInterval = rt is SqlIntervalType;

        ResolvedExpression Result(SqlType type) => new ResolvedBinary(op, left, right, type);

        switch (op)
        {
            case BinaryOperator.Add:
                // temporal + interval  (and the commuted interval + temporal)
                if (lTemporal && rInterval)
                {
                    RejectMonthsOnTime(lt, rt);
                    return Result(lt.WithNullable(nullable));
                }

                if (lInterval && rTemporal)
                {
                    RejectMonthsOnTime(rt, lt);
                    return Result(rt.WithNullable(nullable));
                }

                if (lInterval && rInterval)
                {
                    return Result(CombineIntervalTypes(op, (SqlIntervalType)lt, (SqlIntervalType)rt, nullable));
                }

                break;

            case BinaryOperator.Subtract:
                if (lTemporal && rInterval)
                {
                    RejectMonthsOnTime(lt, rt);
                    return Result(lt.WithNullable(nullable));
                }

                if (lTemporal && rTemporal)
                {
                    if (!SameTemporalKind(lt, rt))
                    {
                        throw new ResolveException(
                            $"cannot subtract {rt.Display} from {lt.Display}");
                    }

                    // DATE − DATE → INTERVAL DAY; TIME/TIMESTAMP − same →
                    // INTERVAL DAY TO SECOND (a microsecond span).
                    var q = lt is SqlDateType ? IntervalQualifier.Day : IntervalQualifier.DayToSecond;
                    return Result(new SqlIntervalType(q, nullable));
                }

                if (lInterval && rInterval)
                {
                    return Result(CombineIntervalTypes(op, (SqlIntervalType)lt, (SqlIntervalType)rt, nullable));
                }

                break;

            case BinaryOperator.Multiply:
                // interval × numeric (either order)
                if (lInterval && IsScalableNumeric(rt))
                {
                    return Result(((SqlIntervalType)lt) with { Nullable = nullable });
                }

                if (rInterval && IsScalableNumeric(lt))
                {
                    return Result(((SqlIntervalType)rt) with { Nullable = nullable });
                }

                break;

            case BinaryOperator.Divide:
                if (lInterval && IsScalableNumeric(rt))
                {
                    return Result(((SqlIntervalType)lt) with { Nullable = nullable });
                }

                break;
        }

        throw new ResolveException(
            $"operator {op} is not defined for operands {lt.Display} and {rt.Display}");
    }

    /// <summary>INT/BIGINT/REAL/DOUBLE may scale an interval (DECIMAL not yet).</summary>
    private static bool IsScalableNumeric(SqlType t) =>
        t is SqlIntegerType or SqlBigintType or SqlRealType or SqlDoubleType;

    private static bool SameTemporalKind(SqlType a, SqlType b) =>
        (a is SqlDateType && b is SqlDateType)
        || (a is SqlTimeType && b is SqlTimeType)
        || (a is SqlTimestampType && b is SqlTimestampType);

    private static void RejectMonthsOnTime(SqlType temporal, SqlType interval)
    {
        if (temporal is SqlTimeType && interval is SqlIntervalType { IsYearMonth: true })
        {
            throw new ResolveException("cannot add a YEAR/MONTH interval to a TIME value");
        }
    }

    private static SqlIntervalType CombineIntervalTypes(
        BinaryOperator op, SqlIntervalType a, SqlIntervalType b, bool nullable)
    {
        if (a.IsYearMonth != b.IsYearMonth)
        {
            throw new ResolveException(
                $"cannot {op} a year-month interval and a day-time interval");
        }

        return new SqlIntervalType(a.Qualifier, nullable);
    }

    private static bool SameTypeIgnoringNullable(SqlType a, SqlType b) =>
        a.WithNullable(false).Equals(b.WithNullable(false));

    private static ResolvedExpression ResolveIsNull(
        IsNullExpression isn,
        Schema schema,
        IReadOnlyDictionary<SubqueryExpression, SubqueryBinding>? subqueryMap,
        Schema? outerSchema = null,
        IReadOnlyDictionary<Expression, ResolvedExpression>? preBound = null)
    {
        var operand = ResolveScalarExpression(isn.Operand, schema, subqueryMap, outerSchema, preBound);
        return new ResolvedIsNull(operand, isn.Negated, new SqlBooleanType(false));
    }

    private static ResolvedExpression ResolveCast(
        CastExpression cast,
        Schema schema,
        IReadOnlyDictionary<SubqueryExpression, SubqueryBinding>? subqueryMap,
        Schema? outerSchema = null,
        IReadOnlyDictionary<Expression, ResolvedExpression>? preBound = null)
    {
        var operand = ResolveScalarExpression(cast.Operand, schema, subqueryMap, outerSchema, preBound);
        var target = TypeInference.FromSpec(cast.TargetType, nullable: operand.Type.Nullable);

        // Constant-fold CAST('<literal>' AS INTERVAL q) — the shape the parser
        // emits for an INTERVAL literal — into a typed interval value so it is
        // parsed once at compile time rather than per row.
        if (target is SqlIntervalType intervalType
            && operand is ResolvedLiteral { Kind: LiteralKind.String, Value: Utf8String text })
        {
            var value = Interval.Parse(text.ToStringDecoded(), intervalType.Qualifier);
            return new ResolvedLiteral(LiteralKind.String, value, intervalType.WithNullable(false));
        }

        return new ResolvedCast(operand, target);
    }

    private static ResolvedExpression ResolveScalarFunction(
        FunctionCallExpression fn,
        Schema schema,
        IReadOnlyDictionary<SubqueryExpression, SubqueryBinding>? subqueryMap,
        Schema? outerSchema = null,
        IReadOnlyDictionary<Expression, ResolvedExpression>? preBound = null)
    {
        if (!ScalarFunctionRegistry.IsKnown(fn.FunctionName))
        {
            throw new ResolveException($"unknown function '{fn.FunctionName}'");
        }

        var args = new List<ResolvedExpression>(fn.Arguments.Count);
        foreach (var a in fn.Arguments)
        {
            args.Add(ResolveScalarExpression(a, schema, subqueryMap, outerSchema, preBound));
        }

        return ScalarFunctionRegistry.Resolve(fn.FunctionName, args);
    }

    private static void EnsureBooleanCoercible(ResolvedExpression e, string context)
    {
        if (!TypeInference.IsBoolean(e.Type))
        {
            throw new ResolveException($"{context} predicate must be BOOLEAN, got {e.Type.Display}");
        }
    }

    // ---------- Aggregates ----------

    private static bool IsAggregateName(string name, bool isStar)
    {
        if (isStar)
        {
            return string.Equals(name, "count", StringComparison.Ordinal);
        }

        return name switch
        {
            "count" or "sum" or "min" or "max" or "avg" or "approx_count_distinct"
                or "approx_percentile" or "median" or "percentile_cont" or "percentile_disc"
                or "stddev" or "stddev_samp" or "stddev_pop"
                or "variance" or "var_samp" or "var_pop" => true,
            _ => false,
        };
    }

    private static bool HasAggregate(IReadOnlyList<SelectItem> items)
    {
        foreach (var item in items)
        {
            if (item is ExpressionSelectItem esi && HasAggregate(esi.Expression))
            {
                return true;
            }
        }

        return false;
    }

    private static bool HasAggregate(Expression expr) => expr switch
    {
        FunctionCallExpression fn when IsAggregateName(fn.FunctionName, fn.IsStar) => true,
        FunctionCallExpression fn => AnyHasAggregate(fn.Arguments),
        BinaryExpression bin => HasAggregate(bin.Left) || HasAggregate(bin.Right),
        UnaryExpression un => HasAggregate(un.Operand),
        IsNullExpression isn => HasAggregate(isn.Operand),
        CastExpression cast => HasAggregate(cast.Operand),
        InListExpression il => HasAggregate(il.Probe) || AnyHasAggregate(il.Values),
        // ExistsExpression / InSubqueryExpression / SubqueryExpression:
        // the inner subquery is a separate scope and its aggregates don't
        // count as outer aggregates.
        _ => false,
    };

    private static bool AnyHasAggregate(IReadOnlyList<Expression> args)
    {
        foreach (var a in args)
        {
            if (HasAggregate(a))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Dedup key for <see cref="AggregateCall"/>s. The argument is compared
    /// <b>structurally</b> (<see cref="ResolvedExprEqual"/>), not by record
    /// equality: a collection-bearing arg (e.g. <c>SUM(CASE …)</c>,
    /// <c>COUNT(DISTINCT CASE …)</c>) re-resolved at a second collection site
    /// would otherwise compare unequal (its <c>Whens</c>/<c>Arguments</c> list
    /// is reference-compared) and the aggregate would be collected twice — a
    /// duplicate the typed compiler can't lay out, forcing a structural/
    /// single-only fallback. The hash is coarse (kind + arg type + fraction +
    /// discrete) so structurally-equal args share a bucket; <see cref="Equals"/>
    /// settles collisions.
    /// </summary>
    private readonly struct AggregateKey : IEquatable<AggregateKey>
    {
        public AggregateKey(
            AggregateKind kind, ResolvedExpression? argument, double? fraction = null, bool discrete = false)
        {
            Kind = kind;
            Argument = argument;
            Fraction = fraction;
            Discrete = discrete;
        }

        public AggregateKind Kind { get; }

        public ResolvedExpression? Argument { get; }

        public double? Fraction { get; }

        public bool Discrete { get; }

        public bool Equals(AggregateKey other) =>
            Kind == other.Kind
            && Nullable.Equals(Fraction, other.Fraction)
            && Discrete == other.Discrete
            && ResolvedExprEqual(Argument, other.Argument);

        public override bool Equals(object? obj) => obj is AggregateKey k && Equals(k);

        public override int GetHashCode() => HashCode.Combine(Kind, Argument?.Type, Fraction, Discrete);
    }

    /// <summary>
    /// Resolve an aggregate call's value argument and — for the quantile family
    /// (<see cref="AggregateKind.ApproxPercentile"/>) — its constant fraction and
    /// whether it is the discrete (<c>PERCENTILE_DISC</c>) spelling.
    /// <c>COUNT(*)</c> has neither argument nor fraction; every other aggregate
    /// takes a single value argument, no fraction, and is not discrete.
    /// </summary>
    private static (ResolvedExpression? Argument, double? Fraction, bool Discrete) ResolveAggregateArgs(
        FunctionCallExpression call, AggregateKind kind, Schema schema)
    {
        if (kind == AggregateKind.CountStar)
        {
            return (null, null, false);
        }

        if (kind == AggregateKind.ApproxPercentile)
        {
            var (arg, fraction) = ResolvePercentileArgs(call, schema);
            return (arg, fraction, call.FunctionName == "percentile_disc");
        }

        if (call.Arguments.Count != 1)
        {
            throw new ResolveException(
                $"{call.FunctionName.ToUpperInvariant()} takes exactly 1 argument");
        }

        return (ResolveScalarExpression(call.Arguments[0], schema), null, false);
    }

    /// <summary>
    /// Resolve the value expression and constant fraction of an approximate
    /// quantile call. <c>MEDIAN(x)</c> fixes the fraction at 0.5;
    /// <c>APPROX_PERCENTILE(x, f)</c> / <c>PERCENTILE_CONT(f) WITHIN GROUP
    /// (ORDER BY x)</c> (already lowered to <c>(x, f)</c> by the parser) carry an
    /// explicit literal fraction in [0, 1].
    /// </summary>
    private static (ResolvedExpression? Argument, double? Fraction) ResolvePercentileArgs(
        FunctionCallExpression call, Schema schema)
    {
        Expression valueExpr;
        double fraction;
        if (call.FunctionName == "median")
        {
            if (call.Arguments.Count != 1)
            {
                throw new ResolveException("MEDIAN takes exactly 1 argument");
            }

            valueExpr = call.Arguments[0];
            fraction = 0.5;
        }
        else
        {
            if (call.Arguments.Count != 2)
            {
                throw new ResolveException(
                    $"{call.FunctionName.ToUpperInvariant()} takes a value and a fraction argument");
            }

            valueExpr = call.Arguments[0];
            fraction = ReadFractionLiteral(call.Arguments[1], call.FunctionName);
        }

        return (ResolveScalarExpression(valueExpr, schema), fraction);
    }

    /// <summary>
    /// Read a percentile's fraction: a numeric constant literal in [0, 1]. The
    /// fraction is fixed at plan time (it selects which quantile the DDSketch
    /// reports), so a non-constant fraction is rejected here.
    /// </summary>
    private static double ReadFractionLiteral(Expression expr, string functionName)
    {
        var name = functionName.ToUpperInvariant();
        var fraction = expr switch
        {
            LiteralExpression lit => ReadNumericLiteral(lit, name),
            // The parser lowers `WITHIN GROUP (ORDER BY x DESC)` to `1 - f`; fold
            // that (and any constant `literal - literal`) so the fraction is
            // measured from the bottom like the ascending case.
            BinaryExpression { Operator: BinaryOperator.Subtract, Left: LiteralExpression l, Right: LiteralExpression r }
                => ReadNumericLiteral(l, name) - ReadNumericLiteral(r, name),
            _ => throw new ResolveException($"{name} fraction must be a numeric constant in [0, 1]"),
        };

        if (fraction is < 0.0 or > 1.0)
        {
            throw new ResolveException($"{name} fraction must be in [0, 1]");
        }

        return fraction;
    }

    private static double ReadNumericLiteral(LiteralExpression lit, string functionName)
    {
        if (lit.Value is null)
        {
            throw new ResolveException($"{functionName} fraction must be a numeric constant in [0, 1]");
        }

        return lit.Kind switch
        {
            LiteralKind.Integer or LiteralKind.Float => Convert.ToDouble(lit.Value, System.Globalization.CultureInfo.InvariantCulture),
            LiteralKind.Decimal => (double)((Decimal128)lit.Value).Mantissa / Math.Pow(10, lit.DecimalScale),
            _ => throw new ResolveException($"{functionName} fraction must be a numeric constant in [0, 1]"),
        };
    }

    /// <summary>
    /// Resolve an expression in the post-aggregate context. Every sub-tree
    /// is checked against the GROUP BY keys (matched by <c>(qualifier, name)</c>);
    /// aggregate calls are collected into <paramref name="aggregates"/> and
    /// their argument is resolved against the pre-aggregate schema; a bare
    /// column reference that is not a GROUP BY key is an error. Scalar
    /// subqueries, if any were pre-registered into <paramref name="subqueryMap"/>,
    /// resolve to hidden columns on the post-wrap schema.
    /// </summary>
    private static ResolvedExpression ResolvePostAggregateExpression(
        Expression expr,
        Schema preSchema,
        IReadOnlyList<(Expression Ast, int OutputIndex)> groupKeys,
        List<AggregateCall> aggregates,
        Dictionary<AggregateKey, int> aggIndex,
        int aggStartColumn,
        IReadOnlyDictionary<SubqueryExpression, SubqueryBinding>? subqueryMap = null,
        Schema? postSchema = null,
        IReadOnlyDictionary<Expression, ResolvedExpression>? preBound = null)
    {
        // Non-WHERE boolean pre-pass substitution: bound EXISTS / IN nodes
        // resolve to the lifted hidden-column expression.
        if (preBound is not null && preBound.TryGetValue(expr, out var sub))
        {
            return sub;
        }

        // Scalar subquery reference: look up by AST identity.
        if (expr is SubqueryExpression sq)
        {
            return ResolveSubqueryReference(sq, subqueryMap);
        }

        // Match against GROUP BY keys first — a whole SELECT/HAVING sub-tree that
        // equals a group key (a bare column or an expression like CAST(ts AS DATE))
        // reads straight from that key's output column. Its type is the resolved
        // key's type; since `expr` is syntactically the key, resolving it against
        // the pre-aggregate schema yields exactly that.
        for (var i = 0; i < groupKeys.Count; i++)
        {
            if (AstEqual(expr, groupKeys[i].Ast))
            {
                var keyType = ResolveScalarExpression(expr, preSchema).Type;
                return new ResolvedColumn(groupKeys[i].OutputIndex, keyType);
            }
        }

        // Aggregate call?
        if (expr is FunctionCallExpression call && IsAggregateName(call.FunctionName, call.IsStar))
        {
            var kind = ToAggregateKind(call);
            var (arg, fraction, discrete) = ResolveAggregateArgs(call, kind, preSchema);
            var resultType = ComputeAggregateResultType(kind, arg?.Type);
            var key = new AggregateKey(kind, arg, fraction, discrete);
            if (!aggIndex.TryGetValue(key, out var idx))
            {
                idx = aggregates.Count;
                aggregates.Add(new AggregateCall(kind, arg, resultType, fraction, discrete));
                aggIndex[key] = idx;
            }

            return new ResolvedColumn(aggStartColumn + idx, resultType);
        }

        return expr switch
        {
            LiteralExpression lit => ResolveLiteral(lit),
            UnaryExpression un => BuildUnaryPost(un, preSchema, groupKeys, aggregates, aggIndex, aggStartColumn, subqueryMap, postSchema, preBound),
            BinaryExpression bin => BuildBinaryPost(bin, preSchema, groupKeys, aggregates, aggIndex, aggStartColumn, subqueryMap, postSchema, preBound),
            IsNullExpression isn => BuildIsNullPost(isn, preSchema, groupKeys, aggregates, aggIndex, aggStartColumn, subqueryMap, postSchema, preBound),
            CastExpression cast => BuildCastPost(cast, preSchema, groupKeys, aggregates, aggIndex, aggStartColumn, subqueryMap, postSchema, preBound),
            FunctionCallExpression fn when ScalarFunctionRegistry.IsKnown(fn.FunctionName) && !IsAggregateName(fn.FunctionName, fn.IsStar)
                => BuildBuiltinCallPost(fn, preSchema, groupKeys, aggregates, aggIndex, aggStartColumn, subqueryMap, postSchema, preBound),
            ColumnReference cr => throw new ResolveException(
                $"column '{(cr.Qualifier is null ? cr.Name : cr.Qualifier + "." + cr.Name)}' must appear in GROUP BY or in an aggregate"),
            _ => throw new ResolveException($"unsupported expression in aggregate query: {expr.GetType().Name}"),
        };
    }

    private static ResolvedExpression BuildUnaryPost(
        UnaryExpression un, Schema pre,
        IReadOnlyList<(Expression, int)> gk, List<AggregateCall> aggs,
        Dictionary<AggregateKey, int> idx, int start,
        IReadOnlyDictionary<SubqueryExpression, SubqueryBinding>? subMap,
        Schema? postSchema,
        IReadOnlyDictionary<Expression, ResolvedExpression>? preBound = null)
    {
        var op = ResolvePostAggregateExpression(un.Operand, pre, gk, aggs, idx, start, subMap, postSchema, preBound);
        if (un.Operator == UnaryOperator.Not)
        {
            if (!TypeInference.IsBoolean(op.Type))
            {
                throw new ResolveException("NOT requires a BOOLEAN operand");
            }

            return new ResolvedUnary(un.Operator, op, op.Type);
        }

        if (!TypeInference.IsNumeric(op.Type))
        {
            throw new ResolveException("unary - requires a numeric operand");
        }

        return new ResolvedUnary(un.Operator, op, op.Type);
    }

    private static ResolvedExpression BuildBinaryPost(
        BinaryExpression bin, Schema pre,
        IReadOnlyList<(Expression, int)> gk, List<AggregateCall> aggs,
        Dictionary<AggregateKey, int> idx, int start,
        IReadOnlyDictionary<SubqueryExpression, SubqueryBinding>? subMap,
        Schema? postSchema,
        IReadOnlyDictionary<Expression, ResolvedExpression>? preBound = null)
    {
        var l = ResolvePostAggregateExpression(bin.Left, pre, gk, aggs, idx, start, subMap, postSchema, preBound);
        var r = ResolvePostAggregateExpression(bin.Right, pre, gk, aggs, idx, start, subMap, postSchema, preBound);
        switch (bin.Operator)
        {
            case BinaryOperator.Add:
            case BinaryOperator.Subtract:
            case BinaryOperator.Multiply:
            case BinaryOperator.Divide:
            case BinaryOperator.Modulo:
                return ResolveArithmetic(bin.Operator, l, r);
            case BinaryOperator.Equal:
            case BinaryOperator.NotEqual:
            case BinaryOperator.Less:
            case BinaryOperator.LessEqual:
            case BinaryOperator.Greater:
            case BinaryOperator.GreaterEqual:
                var common = TypeInference.CommonComparableType(l.Type, r.Type);
                return new ResolvedBinary(bin.Operator, MaybeCast(l, common), MaybeCast(r, common),
                    new SqlBooleanType(l.Type.Nullable || r.Type.Nullable));
            case BinaryOperator.And:
            case BinaryOperator.Or:
                if (!TypeInference.IsBoolean(l.Type) || !TypeInference.IsBoolean(r.Type))
                {
                    throw new ResolveException($"{bin.Operator} requires BOOLEAN operands");
                }

                return new ResolvedBinary(bin.Operator, l, r,
                    new SqlBooleanType(l.Type.Nullable || r.Type.Nullable));
            default:
                throw new ResolveException($"unknown binary operator {bin.Operator}");
        }
    }

    private static ResolvedExpression BuildIsNullPost(
        IsNullExpression isn, Schema pre,
        IReadOnlyList<(Expression, int)> gk, List<AggregateCall> aggs,
        Dictionary<AggregateKey, int> idx, int start,
        IReadOnlyDictionary<SubqueryExpression, SubqueryBinding>? subMap,
        Schema? postSchema,
        IReadOnlyDictionary<Expression, ResolvedExpression>? preBound = null)
    {
        var op = ResolvePostAggregateExpression(isn.Operand, pre, gk, aggs, idx, start, subMap, postSchema, preBound);
        return new ResolvedIsNull(op, isn.Negated, new SqlBooleanType(false));
    }

    private static ResolvedExpression BuildCastPost(
        CastExpression cast, Schema pre,
        IReadOnlyList<(Expression, int)> gk, List<AggregateCall> aggs,
        Dictionary<AggregateKey, int> idx, int start,
        IReadOnlyDictionary<SubqueryExpression, SubqueryBinding>? subMap,
        Schema? postSchema,
        IReadOnlyDictionary<Expression, ResolvedExpression>? preBound = null)
    {
        var op = ResolvePostAggregateExpression(cast.Operand, pre, gk, aggs, idx, start, subMap, postSchema, preBound);
        var target = TypeInference.FromSpec(cast.TargetType, nullable: op.Type.Nullable);
        return new ResolvedCast(op, target);
    }

    private static ResolvedExpression BuildBuiltinCallPost(
        FunctionCallExpression fn, Schema pre,
        IReadOnlyList<(Expression, int)> gk, List<AggregateCall> aggs,
        Dictionary<AggregateKey, int> idx, int start,
        IReadOnlyDictionary<SubqueryExpression, SubqueryBinding>? subMap,
        Schema? postSchema,
        IReadOnlyDictionary<Expression, ResolvedExpression>? preBound = null)
    {
        var args = new List<ResolvedExpression>(fn.Arguments.Count);
        foreach (var a in fn.Arguments)
        {
            args.Add(ResolvePostAggregateExpression(a, pre, gk, aggs, idx, start, subMap, postSchema, preBound));
        }

        return ScalarFunctionRegistry.Resolve(fn.FunctionName, args);
    }

    private static AggregateKind ToAggregateKind(FunctionCallExpression call)
    {
        if (call.IsStar)
        {
            return AggregateKind.CountStar;
        }

        if (call.Distinct)
        {
            // DISTINCT is only wired for COUNT; every other aggregate would
            // silently ignore it, so reject rather than mislead.
            return call.FunctionName == "count"
                ? AggregateKind.CountDistinct
                : throw new ResolveException(
                    $"DISTINCT is not supported for {call.FunctionName.ToUpperInvariant()}");
        }

        return call.FunctionName switch
        {
            "count" => AggregateKind.Count,
            "sum" => AggregateKind.Sum,
            "min" => AggregateKind.Min,
            "max" => AggregateKind.Max,
            "avg" => AggregateKind.Avg,
            "approx_count_distinct" => AggregateKind.ApproxCountDistinct,
            "approx_percentile" or "median" or "percentile_cont" or "percentile_disc"
                => AggregateKind.ApproxPercentile,
            // Bare STDDEV / VARIANCE are the SAMPLE forms (n−1), matching
            // PostgreSQL, Spark, and DuckDB — the ivm-bench participants.
            "stddev" or "stddev_samp" => AggregateKind.StddevSamp,
            "stddev_pop" => AggregateKind.StddevPop,
            "variance" or "var_samp" => AggregateKind.VarSamp,
            "var_pop" => AggregateKind.VarPop,
            _ => throw new ResolveException($"unknown aggregate '{call.FunctionName}'"),
        };
    }

    private static SqlType ComputeAggregateResultType(AggregateKind kind, SqlType? argType)
    {
        switch (kind)
        {
            case AggregateKind.CountStar:
                return new SqlBigintType(false);
            case AggregateKind.Count:
                return new SqlBigintType(false);
            case AggregateKind.CountDistinct:
                if (argType is null)
                {
                    throw new ResolveException("COUNT(DISTINCT) requires an argument");
                }

                return new SqlBigintType(false);
            case AggregateKind.ApproxCountDistinct:
                if (argType is null)
                {
                    throw new ResolveException("APPROX_COUNT_DISTINCT requires an argument");
                }

                return new SqlBigintType(false);
            case AggregateKind.ApproxPercentile:
                if (argType is null)
                {
                    throw new ResolveException("APPROX_PERCENTILE requires an argument");
                }

                // Numeric → nullable DOUBLE (DDSketch). DATE/TIMESTAMP (exact) and
                // INTERVAL (DDSketch) return their own type. The quantile of an
                // empty / all-NULL group is NULL, like MIN/MAX, so always nullable.
                if (TypeInference.IsNumeric(argType))
                {
                    return new SqlDoubleType(true);
                }

                if (argType is SqlDateType or SqlTimestampType or SqlIntervalType)
                {
                    return argType.WithNullable(true);
                }

                throw new ResolveException(
                    "APPROX_PERCENTILE requires a numeric, DATE, TIMESTAMP, or INTERVAL argument");
            case AggregateKind.Sum:
                if (argType is null || !TypeInference.IsNumeric(argType))
                {
                    throw new ResolveException("SUM requires a numeric argument");
                }

                return PromoteSumType(argType);
            case AggregateKind.Min:
            case AggregateKind.Max:
                if (argType is null)
                {
                    throw new ResolveException($"{kind} requires an argument");
                }

                return argType.WithNullable(true);
            case AggregateKind.Avg:
                if (argType is null || !TypeInference.IsNumeric(argType))
                {
                    throw new ResolveException("AVG requires a numeric argument");
                }

                return argType is SqlDecimalType ? argType.WithNullable(true) : new SqlDoubleType(true);
            case AggregateKind.VarSamp:
            case AggregateKind.VarPop:
            case AggregateKind.StddevSamp:
            case AggregateKind.StddevPop:
                if (argType is null || !TypeInference.IsNumeric(argType))
                {
                    throw new ResolveException($"{kind} requires a numeric argument");
                }

                // Always DOUBLE (even for DECIMAL input) and always nullable —
                // an empty group is NULL, and the sample forms are NULL for n<2.
                return new SqlDoubleType(true);
            default:
                throw new ResolveException($"unsupported aggregate {kind}");
        }
    }

    private static SqlType PromoteSumType(SqlType t)
    {
        // SQL semantics: SUM promotes small integers to a wider result. Keep
        // it conservative — INT→BIGINT, BIGINT→BIGINT, DECIMAL→DECIMAL,
        // REAL→DOUBLE, DOUBLE→DOUBLE. SUM of empty set is NULL, so result is
        // always nullable.
        return t switch
        {
            SqlIntegerType => new SqlBigintType(true),
            SqlBigintType => new SqlBigintType(true),
            SqlDecimalType d => new SqlDecimalType(d.Precision, d.Scale, true),
            SqlRealType => new SqlDoubleType(true),
            SqlDoubleType => new SqlDoubleType(true),
            _ => throw new ResolveException($"SUM not supported on {t.Display}"),
        };
    }

    // Structural equality on *resolved* scalar expressions — used to dedup
    // AggregateCalls (see AggregateKey). Like AstEqual below, record auto-equality
    // is unusable for the collection-bearing resolved nodes
    // (ResolvedCaseWhen.Whens, ResolvedFunctionCall.Arguments,
    // ResolvedInList.Values), which compare their lists by reference. Hence an
    // explicit recursive walk; leaf nodes (Column/Literal/CorrelationRef) and any
    // unlisted node fall back to record equality, which is correct for them and a
    // safe — merely non-deduping — default for anything else.
    private static bool ResolvedExprEqual(ResolvedExpression? a, ResolvedExpression? b)
    {
        if (ReferenceEquals(a, b))
        {
            return true;
        }

        if (a is null || b is null || !a.Type.Equals(b.Type))
        {
            return false;
        }

        switch (a, b)
        {
            case (ResolvedBinary x, ResolvedBinary y):
                return x.Operator == y.Operator
                    && ResolvedExprEqual(x.Left, y.Left)
                    && ResolvedExprEqual(x.Right, y.Right);
            case (ResolvedUnary x, ResolvedUnary y):
                return x.Operator == y.Operator && ResolvedExprEqual(x.Operand, y.Operand);
            case (ResolvedIsNull x, ResolvedIsNull y):
                return x.Negated == y.Negated && ResolvedExprEqual(x.Operand, y.Operand);
            case (ResolvedCast x, ResolvedCast y):
                return ResolvedExprEqual(x.Operand, y.Operand);
            case (ResolvedFunctionCall x, ResolvedFunctionCall y):
                return string.Equals(x.FunctionName, y.FunctionName, StringComparison.Ordinal)
                    && ResolvedListEqual(x.Arguments, y.Arguments);
            case (ResolvedInList x, ResolvedInList y):
                return x.IsNegated == y.IsNegated
                    && ResolvedExprEqual(x.Probe, y.Probe)
                    && ResolvedListEqual(x.Values, y.Values);
            case (ResolvedCaseWhen x, ResolvedCaseWhen y):
                if (x.Whens.Count != y.Whens.Count)
                {
                    return false;
                }

                for (var i = 0; i < x.Whens.Count; i++)
                {
                    if (!ResolvedExprEqual(x.Whens[i].Condition, y.Whens[i].Condition)
                        || !ResolvedExprEqual(x.Whens[i].Result, y.Whens[i].Result))
                    {
                        return false;
                    }
                }

                return ResolvedExprEqual(x.ElseResult, y.ElseResult);
            default:
                return a.Equals(b);
        }
    }

    private static bool ResolvedListEqual(
        IReadOnlyList<ResolvedExpression> a, IReadOnlyList<ResolvedExpression> b)
    {
        if (a.Count != b.Count)
        {
            return false;
        }

        for (var i = 0; i < a.Count; i++)
        {
            if (!ResolvedExprEqual(a[i], b[i]))
            {
                return false;
            }
        }

        return true;
    }

    // Syntactic equality on AST expressions — used to match SELECT/HAVING/ORDER BY
    // sub-trees against GROUP BY items. Record auto-equality is unusable here:
    // nodes with collection members (FunctionCallExpression.Arguments,
    // InListExpression.Values, CaseExpression.Whens) compare those lists by
    // reference, so two separately-parsed `LENGTH(name)` would compare unequal.
    // Hence an explicit structural walk that compares list members element-wise.
    private static bool AstEqual(Expression a, Expression b)
    {
        if (ReferenceEquals(a, b))
        {
            return true;
        }

        return (a, b) switch
        {
            (ColumnReference x, ColumnReference y) =>
                x.Qualifier == y.Qualifier && string.Equals(x.Name, y.Name, StringComparison.Ordinal),
            (LiteralExpression x, LiteralExpression y) => x.Equals(y),
            (NowExpression x, NowExpression y) => x.Function == y.Function,
            (UnaryExpression x, UnaryExpression y) =>
                x.Operator == y.Operator && AstEqual(x.Operand, y.Operand),
            (BinaryExpression x, BinaryExpression y) =>
                x.Operator == y.Operator && AstEqual(x.Left, y.Left) && AstEqual(x.Right, y.Right),
            (IsNullExpression x, IsNullExpression y) =>
                x.Negated == y.Negated && AstEqual(x.Operand, y.Operand),
            (CastExpression x, CastExpression y) =>
                Equals(x.TargetType, y.TargetType) && AstEqual(x.Operand, y.Operand),
            (FunctionCallExpression x, FunctionCallExpression y) =>
                string.Equals(x.FunctionName, y.FunctionName, StringComparison.Ordinal)
                && x.IsStar == y.IsStar && AstListEqual(x.Arguments, y.Arguments),
            (InListExpression x, InListExpression y) =>
                x.IsNegated == y.IsNegated && AstEqual(x.Probe, y.Probe) && AstListEqual(x.Values, y.Values),
            (CaseExpression x, CaseExpression y) =>
                (x.ElseResult is null) == (y.ElseResult is null)
                && (x.ElseResult is null || AstEqual(x.ElseResult, y.ElseResult!))
                && x.Whens.Count == y.Whens.Count
                && WhenClausesEqual(x.Whens, y.Whens),
            // Subqueries / window functions can't be GROUP BY keys; fall back to
            // record identity (only reached for like-typed pairs).
            _ => a.Equals(b),
        };
    }

    private static bool AstListEqual(IReadOnlyList<Expression> a, IReadOnlyList<Expression> b)
    {
        if (a.Count != b.Count)
        {
            return false;
        }

        for (var i = 0; i < a.Count; i++)
        {
            if (!AstEqual(a[i], b[i]))
            {
                return false;
            }
        }

        return true;
    }

    private static bool WhenClausesEqual(IReadOnlyList<CaseWhenClause> a, IReadOnlyList<CaseWhenClause> b)
    {
        for (var i = 0; i < a.Count; i++)
        {
            if (!AstEqual(a[i].Condition, b[i].Condition) || !AstEqual(a[i].Result, b[i].Result))
            {
                return false;
            }
        }

        return true;
    }
}
