// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections.Generic;
using System.Linq;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;
using DbspNet.Tests.Sql;

namespace DbspNet.Tests.EndToEnd;

/// <summary>
/// One delta event pushed at some tick: a row-with-weight change on a named
/// table. <see cref="Weight"/> is +1 for an <c>INSERT</c> and -1 for a
/// <c>DELETE</c>, matching the SQL surface (the property-based generators
/// stick to unit-weight events; the operator-laws tests in
/// <c>Operators/</c> cover multi-weight behaviour).
/// </summary>
internal readonly record struct InputEvent(string Table, object?[] Row, long Weight)
{
    public override string ToString() =>
        $"{(Weight < 0 ? "-" : "+")}{Math.Abs(Weight)} {Table}({string.Join(", ", Row.Select(x => x is null ? "NULL" : x.ToString()))})";
}

/// <summary>
/// Reusable glue for the four correctness laws from the plan:
/// <list type="number">
/// <item>accumulated output deltas equal the batch re-computation</item>
/// <item>per-tick output equals the derivative of the batch</item>
/// <item>splitting input ticks finer preserves accumulated output</item>
/// <item>an empty input tick produces an empty output delta</item>
/// </list>
/// </summary>
internal static class IncrementalOracle
{
    internal static CompiledQuery CompileQuery(string[] ddl, string sql)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in ddl)
        {
            resolver.Resolve(Parser.ParseStatement(s));
        }

        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(sql))).Query;
        return PlanToCircuit.Compile(plan);
    }

    /// <summary>
    /// Push a sequence of ticks through the circuit. Each inner list is one
    /// tick's worth of events (possibly empty); all events in that tick are
    /// pushed before <see cref="CompiledQuery.Step"/> fires.
    /// </summary>
    internal static ZSet<StructuralRow, Z64> RunAndAccumulate(
        CompiledQuery query,
        IReadOnlyList<IReadOnlyList<InputEvent>> ticks)
    {
        var accum = ZSet<StructuralRow, Z64>.Empty;
        foreach (var tick in ticks)
        {
            foreach (var ev in tick)
            {
                if (ev.Weight == 1)
                {
                    query.Table(ev.Table).Insert(ev.Row);
                }
                else if (ev.Weight == -1)
                {
                    query.Table(ev.Table).Delete(ev.Row);
                }
                else
                {
                    query.Table(ev.Table).Push(new[] { (ev.Row, ev.Weight) });
                }
            }

            query.Step();
            accum += query.Current;
        }

        return accum;
    }

    /// <summary>
    /// Compute the net accumulated Z-set for a single table from a stream of
    /// events; used to feed the batch oracle.
    /// </summary>
    internal static ZSet<StructuralRow, Z64> NetTable(IEnumerable<InputEvent> events, string table)
    {
        var b = new ZSetBuilder<StructuralRow, Z64>();
        foreach (var e in events)
        {
            if (e.Table == table)
            {
                // The actual circuit goes through TableInput, which encodes
                // raw strings to Utf8String for VARCHAR columns. Mirror that
                // here so the oracle row representation matches.
                b.Add(new StructuralRow(SqlTestHelpers.EncodeStrings(e.Row)), new Z64(e.Weight));
            }
        }

        return b.Build();
    }
}
