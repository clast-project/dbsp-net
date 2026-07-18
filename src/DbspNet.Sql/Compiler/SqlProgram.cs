// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System;
using System.Collections.Generic;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;
using SqlParser = DbspNet.Sql.Parser.Parser;

namespace DbspNet.Sql.Compiler;

/// <summary>
/// Front-end for compiling a multi-statement SQL <b>program</b> (a DAG of
/// <c>CREATE TABLE</c> sources and <c>CREATE VIEW</c> definitions, in dependency order)
/// into a single <see cref="CompiledProgram"/>. Resolves each statement against a shared
/// catalog, registering every view's output schema so later views can reference it, then
/// hands the resolved tables + views to <see cref="PlanToCircuit.CompileProgram"/>.
/// </summary>
public static class SqlProgram
{
    /// <summary>
    /// Compile <paramref name="statements"/> (each a <c>CREATE TABLE</c> or
    /// <c>CREATE VIEW</c>, in dependency order) into a program. <paramref name="outputViews"/>
    /// names the views to materialise as outputs (the rest are internal, shared streams).
    /// </summary>
    public static CompiledProgram Compile(
        IReadOnlyList<string> statements,
        ISet<string> outputViews,
        ISqlSnapshotCodecs? snapshotCodecs = null,
        CompileOptions? options = null,
        bool numericStringCoercion = false,
        NullCollation nullCollation = NullCollation.High)
    {
        var resolved = Resolve(statements, outputViews, numericStringCoercion, nullCollation);
        return PlanToCircuit.CompileProgram(resolved.Tables, resolved.Views, snapshotCodecs, options);
    }

    /// <summary>
    /// Resolve (without compiling) — parse each statement against a shared catalog,
    /// registering each view's schema so later views resolve references to it. Exposed so
    /// callers (and tests) can inspect the resolved plans before / instead of compiling.
    /// <paramref name="numericStringCoercion"/> enables implicit numeric↔string comparison
    /// coercion for the whole program (the ivm-bench / Spark / DuckDB / Feldera behaviour;
    /// off = PostgreSQL-faithful). <paramref name="nullCollation"/> selects the default NULL
    /// placement for a bare <c>ORDER BY</c> key (<see cref="NullCollation.High"/> =
    /// PostgreSQL/DuckDB default; <see cref="NullCollation.Low"/> = Calcite/Feldera).
    /// </summary>
    public static ResolvedProgram Resolve(
        IReadOnlyList<string> statements,
        ISet<string> outputViews,
        bool numericStringCoercion = false,
        NullCollation nullCollation = NullCollation.High)
    {
        ArgumentNullException.ThrowIfNull(statements);
        ArgumentNullException.ThrowIfNull(outputViews);

        var previousCoercion = NumericStringCoercionMode.Enabled;
        var previousCollation = NullCollationMode.Collation;
        NumericStringCoercionMode.Enabled = numericStringCoercion;
        NullCollationMode.Collation = nullCollation;
        try
        {
            return ResolveCore(statements, outputViews);
        }
        finally
        {
            NumericStringCoercionMode.Enabled = previousCoercion;
            NullCollationMode.Collation = previousCollation;
        }
    }

    private static ResolvedProgram ResolveCore(IReadOnlyList<string> statements, ISet<string> outputViews)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        var tables = new List<CreateTablePlan>();
        var views = new List<ProgramView>();

        foreach (var sql in statements)
        {
            var plan = resolver.Resolve(SqlParser.ParseStatement(sql));
            switch (plan)
            {
                case CreateTablePlan table:
                    // ResolveCreateTable already registered the table in the catalog.
                    tables.Add(table);
                    break;

                case CreateViewPlan viewPlan:
                    // Register the view's output schema so downstream views resolve
                    // `FROM <view>` (the resolver treats it as a table).
                    catalog.Register(viewPlan.ViewName, viewPlan.Query.Schema);
                    views.Add(new ProgramView(
                        viewPlan.ViewName, viewPlan.Query, outputViews.Contains(viewPlan.ViewName)));
                    break;

                default:
                    throw new ArgumentException(
                        $"a program statement must be CREATE TABLE or CREATE VIEW, got {plan.GetType().Name}");
            }
        }

        return new ResolvedProgram(tables, views);
    }
}

/// <summary>A resolved program: source tables and views (in dependency order).</summary>
public sealed record ResolvedProgram(
    IReadOnlyList<CreateTablePlan> Tables, IReadOnlyList<ProgramView> Views);
