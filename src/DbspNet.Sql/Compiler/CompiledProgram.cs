// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections.Generic;
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Sql.Plan;

namespace DbspNet.Sql.Compiler;

/// <summary>
/// A compiled multi-view <b>program</b>: a whole DAG of <c>CREATE TABLE</c> source
/// tables and <c>CREATE VIEW</c> definitions lowered into a <b>single</b>
/// <see cref="RootCircuit"/> where views are shared circuit streams (a view referenced
/// by several downstream views is computed once), with one input handle per source table
/// and one integrated (materialised) output per designated output view. This is the
/// engine shape an IVM benchmark expects — sources → bronze → silver → gold maintained
/// incrementally as one circuit — as opposed to <see cref="CompiledQuery"/>, which is a
/// single query with one output.
/// </summary>
public sealed class CompiledProgram
{
    internal CompiledProgram(
        RootCircuit circuit,
        IReadOnlyDictionary<string, TableInput> inputs,
        IReadOnlyDictionary<string, ProgramOutput> outputs)
    {
        Circuit = circuit;
        Inputs = inputs;
        Outputs = outputs;
    }

    public RootCircuit Circuit { get; }

    /// <summary>Input handle per source (<c>CREATE TABLE</c>) — push INSERT/DELETE deltas.</summary>
    public IReadOnlyDictionary<string, TableInput> Inputs { get; }

    /// <summary>Materialised view per designated output (<c>+stored</c> gold) view.</summary>
    public IReadOnlyDictionary<string, ProgramOutput> Outputs { get; }

    /// <summary>Commit queued input deltas and fire the whole circuit one tick.</summary>
    public void Step() => Circuit.Step();

    public TableInput Table(string name) => Inputs[name];
}

/// <summary>
/// One resolved view in a program to compile: its name, its resolved query plan (whose
/// scans of prior tables/views resolve by name), and whether it is a materialised output.
/// Views must be listed in dependency order (a view may reference earlier ones).
/// </summary>
public sealed record ProgramView(string ViewName, LogicalPlan Query, bool IsOutput);

/// <summary>
/// One output view of a <see cref="CompiledProgram"/>: its schema and the integrated
/// (full-state) view contents, for a truncate-mode sink.
/// </summary>
public sealed record ProgramOutput(Schema Schema, IntegratedViewHandle<StructuralRow> View)
{
    /// <summary>The full current view contents (valid until the next
    /// <see cref="CompiledProgram.Step"/>).</summary>
    public ZSet<StructuralRow, Z64> CurrentView => View.Current;
}
