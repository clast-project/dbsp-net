// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections.Generic;
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
            var scalarConjuncts = new List<Expression>();
            foreach (var c in conjuncts)
            {
                if (c is InSubqueryExpression isq)
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

        // Aggregation path. Resolve GROUP BY keys against the pre-aggregate schema.
        var groupKeyExprs = new List<ResolvedExpression>();
        var groupKeyAstItems = new List<(Expression Ast, int OutputIndex)>();
        var groupCols = new List<SchemaColumn>();
        var seenKeys = new HashSet<(string? Qual, string Name)>();
        for (var i = 0; i < stmt.GroupBy.Count; i++)
        {
            var gb = stmt.GroupBy[i];
            if (gb is not ColumnReference cref)
            {
                throw new ResolveException("GROUP BY supports only bare column references in v1");
            }

            var idx = plan.Schema.Resolve(cref.Qualifier, cref.Name);
            var src = plan.Schema[idx];
            var key = (src.Qualifier, src.Name);
            if (!seenKeys.Add(key))
            {
                throw new ResolveException($"duplicate GROUP BY column '{src.Name}'");
            }

            groupKeyExprs.Add(new ResolvedColumn(idx, src.Type));
            groupKeyAstItems.Add((cref, i));
            groupCols.Add(new SchemaColumn(src.Name, src.Type, src.Qualifier));
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
        _ => throw new ResolveException($"unsupported FROM clause: {from.GetType().Name}"),
    };

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
        foreach (var conjunct in SplitAnd(onResolved))
        {
            if (TryExtractEquiKey(conjunct, leftCount, out var eq))
            {
                equi.Add(eq);
            }
            else
            {
                residuals.Add(conjunct);
            }
        }

        if (equi.Count == 0)
        {
            throw new ResolveException($"{JoinTypeName(join.Type)} requires at least one equi-key (v1)");
        }

        ResolvedExpression? residual = null;
        foreach (var r in residuals)
        {
            residual = residual is null
                ? r
                : new ResolvedBinary(BinaryOperator.And, residual, r,
                    new SqlBooleanType(residual.Type.Nullable || r.Type.Nullable));
        }

        // v1 restriction: outer joins with non-equi conjuncts in ON are
        // semantically subtle (failing a residual drops the match but keeps
        // the preserved row with NULLs, which requires residual-aware logic
        // in the operator). Defer.
        if (join.Type != JoinType.Inner && residual is not null)
        {
            throw new ResolveException(
                $"{JoinTypeName(join.Type)} with a non-equi ON conjunct is not supported in v1");
        }

        var outputSchema = join.Type switch
        {
            JoinType.Inner => combined,
            JoinType.LeftOuter => MakeSideNullable(left.Schema, right.Schema, makeLeftNullable: false),
            JoinType.RightOuter => MakeSideNullable(left.Schema, right.Schema, makeLeftNullable: true),
            _ => throw new ResolveException($"unsupported join type {join.Type}"),
        };

        return new JoinPlan(left, right, join.Type, equi, residual, outputSchema);
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
            _ => throw new ResolveException($"unsupported join type {join.Type}"),
        };

        var joinPlan = new JoinPlan(left, right, join.Type, equi, Residual: null, joinOutputSchema);

        // INNER and LEFT keep the (non-null) left copy of each shared column;
        // RIGHT keeps the (non-null) right copy.
        var takeLeftForMerged = join.Type != JoinType.RightOuter;
        var mergedLeft = new HashSet<int>(usingLeftIdx);
        var mergedRight = new HashSet<int>(usingRightCombinedIdx);

        var projections = new List<ProjectionItem>();
        for (var i = 0; i < join.UsingColumns!.Count; i++)
        {
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
    /// <see cref="LiftInSubqueryToSemiJoin"/>). Correlated NOT EXISTS
    /// rejects with a deferred message — the anti-semi-join primitive
    /// isn't in v1.
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
            if (groupKeys[i].Ast.Equals(expr))
            {
                return;
            }
        }

        switch (expr)
        {
            case FunctionCallExpression call when IsAggregateName(call.FunctionName, call.IsStar):
                {
                    var kind = ToAggregateKind(call);
                    ResolvedExpression? arg = null;
                    if (kind != AggregateKind.CountStar)
                    {
                        if (call.Arguments.Count != 1)
                        {
                            throw new ResolveException(
                                $"{call.FunctionName.ToUpperInvariant()} takes exactly 1 argument");
                        }

                        arg = ResolveScalarExpression(call.Arguments[0], preSchema);
                    }

                    var resultType = ComputeAggregateResultType(kind, arg?.Type);
                    var key = new AggregateKey(kind, arg);
                    if (!aggIndex.ContainsKey(key))
                    {
                        aggIndex[key] = aggregates.Count;
                        aggregates.Add(new AggregateCall(kind, arg, resultType));
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
        return new ResolvedCast(operand, target);
    }

    private static ResolvedExpression ResolveScalarFunction(
        FunctionCallExpression fn,
        Schema schema,
        IReadOnlyDictionary<SubqueryExpression, SubqueryBinding>? subqueryMap,
        Schema? outerSchema = null,
        IReadOnlyDictionary<Expression, ResolvedExpression>? preBound = null)
    {
        if (!BuiltinScalarFunctions.IsKnown(fn.FunctionName))
        {
            throw new ResolveException($"unknown function '{fn.FunctionName}'");
        }

        var args = new List<ResolvedExpression>(fn.Arguments.Count);
        foreach (var a in fn.Arguments)
        {
            args.Add(ResolveScalarExpression(a, schema, subqueryMap, outerSchema, preBound));
        }

        return BuiltinScalarFunctions.Resolve(fn.FunctionName, args);
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
            "count" or "sum" or "min" or "max" or "avg" => true,
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

    private readonly record struct AggregateKey(AggregateKind Kind, ResolvedExpression? Argument);

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

        // Match against GROUP BY keys first.
        for (var i = 0; i < groupKeys.Count; i++)
        {
            if (AstEqual(expr, groupKeys[i].Ast))
            {
                if (expr is ColumnReference cref)
                {
                    var idx = preSchema.Resolve(cref.Qualifier, cref.Name);
                    return new ResolvedColumn(groupKeys[i].OutputIndex, preSchema[idx].Type);
                }
            }
        }

        // Aggregate call?
        if (expr is FunctionCallExpression call && IsAggregateName(call.FunctionName, call.IsStar))
        {
            var kind = ToAggregateKind(call);
            ResolvedExpression? arg = null;
            if (kind != AggregateKind.CountStar)
            {
                if (call.Arguments.Count != 1)
                {
                    throw new ResolveException($"{call.FunctionName.ToUpperInvariant()} takes exactly 1 argument");
                }

                arg = ResolveScalarExpression(call.Arguments[0], preSchema);
            }

            var resultType = ComputeAggregateResultType(kind, arg?.Type);
            var key = new AggregateKey(kind, arg);
            if (!aggIndex.TryGetValue(key, out var idx))
            {
                idx = aggregates.Count;
                aggregates.Add(new AggregateCall(kind, arg, resultType));
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
            FunctionCallExpression fn when BuiltinScalarFunctions.IsKnown(fn.FunctionName) && !IsAggregateName(fn.FunctionName, fn.IsStar)
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

        return BuiltinScalarFunctions.Resolve(fn.FunctionName, args);
    }

    private static AggregateKind ToAggregateKind(FunctionCallExpression call)
    {
        if (call.IsStar)
        {
            return AggregateKind.CountStar;
        }

        return call.FunctionName switch
        {
            "count" => AggregateKind.Count,
            "sum" => AggregateKind.Sum,
            "min" => AggregateKind.Min,
            "max" => AggregateKind.Max,
            "avg" => AggregateKind.Avg,
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

    // Syntactic equality on AST expressions — used to match SELECT/HAVING
    // sub-trees against GROUP BY items. For v1 we only ever compare
    // ColumnReferences, but this stays general so later we can extend.
    private static bool AstEqual(Expression a, Expression b) => a.Equals(b);
}
