// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections.Generic;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;
using BinOp = DbspNet.Sql.Parser.Ast.BinaryOperator;

namespace DbspNet.Sql.Optimizer;

/// <summary>
/// Structural manipulation of <see cref="ResolvedExpression"/> trees —
/// helpers the optimizer uses for predicate pushdown and projection
/// composition. Every routine is pure and returns a new tree; nothing is
/// mutated.
/// </summary>
internal static class ExpressionRewriter
{
    /// <summary>
    /// Split a top-level <c>AND</c> conjunction into its conjuncts. Nested
    /// ANDs flatten, so <c>(a AND b) AND c</c> yields <c>[a, b, c]</c>.
    /// Non-AND expressions yield a single-element list.
    /// </summary>
    public static List<ResolvedExpression> SplitAnd(ResolvedExpression expr)
    {
        var result = new List<ResolvedExpression>();
        Walk(expr, result);
        return result;

        static void Walk(ResolvedExpression e, List<ResolvedExpression> acc)
        {
            if (e is ResolvedBinary { Operator: BinOp.And } b)
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

    /// <summary>
    /// Combine a list of conjuncts into a single <c>a AND b AND …</c>
    /// expression (left-associative). Throws on an empty list.
    /// </summary>
    public static ResolvedExpression AndAll(IReadOnlyList<ResolvedExpression> conjuncts)
    {
        if (conjuncts.Count == 0)
        {
            throw new ArgumentException("AndAll requires at least one conjunct", nameof(conjuncts));
        }

        var result = conjuncts[0];
        for (var i = 1; i < conjuncts.Count; i++)
        {
            var nullable = result.Type.Nullable || conjuncts[i].Type.Nullable;
            result = new ResolvedBinary(
                BinOp.And, result, conjuncts[i], new SqlBooleanType(nullable));
        }

        return result;
    }

    /// <summary>Collect every distinct column index referenced by the expression.</summary>
    public static HashSet<int> CollectColumnIndices(ResolvedExpression expr)
    {
        var set = new HashSet<int>();
        Walk(expr, set);
        return set;

        static void Walk(ResolvedExpression e, HashSet<int> acc)
        {
            switch (e)
            {
                case ResolvedColumn c:
                    acc.Add(c.Index);
                    break;
                case ResolvedBinary b:
                    Walk(b.Left, acc);
                    Walk(b.Right, acc);
                    break;
                case ResolvedUnary u:
                    Walk(u.Operand, acc);
                    break;
                case ResolvedIsNull isn:
                    Walk(isn.Operand, acc);
                    break;
                case ResolvedCast cast:
                    Walk(cast.Operand, acc);
                    break;
                case ResolvedFunctionCall fn:
                    foreach (var a in fn.Arguments)
                    {
                        Walk(a, acc);
                    }

                    break;
                case ResolvedInList il:
                    Walk(il.Probe, acc);
                    foreach (var v in il.Values)
                    {
                        Walk(v, acc);
                    }

                    break;
                case ResolvedCorrelationRef:
                    // Correlation refs aren't local-column refs — they index
                    // into the OUTER schema. The decorrelator walks them
                    // separately via CollectCorrelationIndices.
                    break;
                // ResolvedLiteral: no columns.
            }
        }
    }

    /// <summary>
    /// Collect the set of outer-column indices referenced by
    /// <see cref="ResolvedCorrelationRef"/> nodes in the expression. Used
    /// by the decorrelator to identify which outer columns a correlated
    /// subquery depends on.
    /// </summary>
    public static HashSet<int> CollectCorrelationIndices(ResolvedExpression expr)
    {
        var set = new HashSet<int>();
        Walk(expr, set);
        return set;

        static void Walk(ResolvedExpression e, HashSet<int> acc)
        {
            switch (e)
            {
                case ResolvedCorrelationRef r:
                    acc.Add(r.OuterIndex);
                    break;
                case ResolvedBinary b:
                    Walk(b.Left, acc);
                    Walk(b.Right, acc);
                    break;
                case ResolvedUnary u:
                    Walk(u.Operand, acc);
                    break;
                case ResolvedIsNull isn:
                    Walk(isn.Operand, acc);
                    break;
                case ResolvedCast cast:
                    Walk(cast.Operand, acc);
                    break;
                case ResolvedFunctionCall fn:
                    foreach (var a in fn.Arguments)
                    {
                        Walk(a, acc);
                    }

                    break;
                case ResolvedInList il:
                    Walk(il.Probe, acc);
                    foreach (var v in il.Values)
                    {
                        Walk(v, acc);
                    }

                    break;
            }
        }
    }

    /// <summary>
    /// Shift every column index in the expression by <paramref name="delta"/>.
    /// Used when pushing a predicate past a join into the right-side input:
    /// join-output columns [leftCount, leftCount + rightCount) need to
    /// become [0, rightCount) before being applied to the right input.
    /// </summary>
    public static ResolvedExpression ShiftColumnIndices(ResolvedExpression expr, int delta)
    {
        return expr switch
        {
            ResolvedColumn c => new ResolvedColumn(c.Index + delta, c.Type),
            ResolvedBinary b => new ResolvedBinary(
                b.Operator,
                ShiftColumnIndices(b.Left, delta),
                ShiftColumnIndices(b.Right, delta),
                b.Type),
            ResolvedUnary u => new ResolvedUnary(
                u.Operator, ShiftColumnIndices(u.Operand, delta), u.Type),
            ResolvedIsNull isn => new ResolvedIsNull(
                ShiftColumnIndices(isn.Operand, delta), isn.Negated, isn.Type),
            ResolvedCast cast => new ResolvedCast(
                ShiftColumnIndices(cast.Operand, delta), cast.Type),
            ResolvedFunctionCall fn => new ResolvedFunctionCall(
                fn.FunctionName,
                ShiftArgs(fn.Arguments, delta),
                fn.Type),
            ResolvedInList il => new ResolvedInList(
                ShiftColumnIndices(il.Probe, delta),
                ShiftArgs(il.Values, delta),
                il.IsNegated,
                il.Type),
            // Correlation refs index OUTER columns, not local ones — shifting
            // a local-column delta past them is a no-op.
            ResolvedCorrelationRef => expr,
            _ => expr,
        };

        static List<ResolvedExpression> ShiftArgs(IReadOnlyList<ResolvedExpression> args, int d)
        {
            var list = new List<ResolvedExpression>(args.Count);
            foreach (var a in args)
            {
                list.Add(ShiftColumnIndices(a, d));
            }

            return list;
        }
    }

    /// <summary>
    /// Rewrite every <see cref="ResolvedColumn"/>(i) reference to
    /// <see cref="ResolvedColumn"/>(<paramref name="remap"/>[i]).
    /// Used when inserting a narrowing projection that re-indexes
    /// its parent's input column space — every column index
    /// referenced by the expression must be present in the remap
    /// (no -1 sentinels).
    /// </summary>
    public static ResolvedExpression RemapColumnIndices(ResolvedExpression expr, int[] remap)
    {
        return expr switch
        {
            ResolvedColumn c => c.Index < 0 || c.Index >= remap.Length || remap[c.Index] < 0
                ? throw new ArgumentException(
                    $"RemapColumnIndices: column {c.Index} not covered by remap (length {remap.Length})",
                    nameof(remap))
                : new ResolvedColumn(remap[c.Index], c.Type),
            ResolvedBinary b => new ResolvedBinary(
                b.Operator,
                RemapColumnIndices(b.Left, remap),
                RemapColumnIndices(b.Right, remap),
                b.Type),
            ResolvedUnary u => new ResolvedUnary(
                u.Operator, RemapColumnIndices(u.Operand, remap), u.Type),
            ResolvedIsNull isn => new ResolvedIsNull(
                RemapColumnIndices(isn.Operand, remap), isn.Negated, isn.Type),
            ResolvedCast cast => new ResolvedCast(
                RemapColumnIndices(cast.Operand, remap), cast.Type),
            ResolvedFunctionCall fn => new ResolvedFunctionCall(
                fn.FunctionName,
                RemapArgs(fn.Arguments, remap),
                fn.Type),
            ResolvedInList il => new ResolvedInList(
                RemapColumnIndices(il.Probe, remap),
                RemapArgs(il.Values, remap),
                il.IsNegated,
                il.Type),
            ResolvedCorrelationRef => expr,
            _ => expr,
        };

        static List<ResolvedExpression> RemapArgs(IReadOnlyList<ResolvedExpression> args, int[] remap)
        {
            var list = new List<ResolvedExpression>(args.Count);
            foreach (var a in args)
            {
                list.Add(RemapColumnIndices(a, remap));
            }

            return list;
        }
    }

    /// <summary>
    /// Substitute each <see cref="ResolvedColumn"/>(i) reference in
    /// <paramref name="expr"/> with the expression
    /// <paramref name="projection"/>[i]. Used to push a predicate past a
    /// <see cref="ProjectPlan"/>: a predicate referring to projection
    /// output columns is rewritten to refer to the projection's input.
    /// </summary>
    public static ResolvedExpression SubstituteViaProjection(
        ResolvedExpression expr,
        IReadOnlyList<ProjectionItem> projection)
    {
        return expr switch
        {
            ResolvedColumn c => projection[c.Index].Expression,
            ResolvedBinary b => new ResolvedBinary(
                b.Operator,
                SubstituteViaProjection(b.Left, projection),
                SubstituteViaProjection(b.Right, projection),
                b.Type),
            ResolvedUnary u => new ResolvedUnary(
                u.Operator, SubstituteViaProjection(u.Operand, projection), u.Type),
            ResolvedIsNull isn => new ResolvedIsNull(
                SubstituteViaProjection(isn.Operand, projection), isn.Negated, isn.Type),
            ResolvedCast cast => new ResolvedCast(
                SubstituteViaProjection(cast.Operand, projection), cast.Type),
            ResolvedFunctionCall fn => new ResolvedFunctionCall(
                fn.FunctionName,
                SubstituteArgs(fn.Arguments, projection),
                fn.Type),
            ResolvedInList il => new ResolvedInList(
                SubstituteViaProjection(il.Probe, projection),
                SubstituteArgs(il.Values, projection),
                il.IsNegated,
                il.Type),
            ResolvedCorrelationRef => expr,
            _ => expr,
        };

        static List<ResolvedExpression> SubstituteArgs(
            IReadOnlyList<ResolvedExpression> args,
            IReadOnlyList<ProjectionItem> p)
        {
            var list = new List<ResolvedExpression>(args.Count);
            foreach (var a in args)
            {
                list.Add(SubstituteViaProjection(a, p));
            }

            return list;
        }
    }
}
