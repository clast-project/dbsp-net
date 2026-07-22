// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections.Generic;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Sql.Plan;

/// <summary>
/// A single positional column in a <see cref="Schema"/>. The optional
/// <see cref="Qualifier"/> is the table alias (or table name if no alias)
/// that produced the column; it lets <c>t.c</c>-style references disambiguate
/// joined schemas.
/// </summary>
/// <param name="Lateness">
/// Declared <c>LATENESS</c> bound in the column's native units, for base-table
/// columns only (null elsewhere). Carries the bound from <c>CREATE TABLE</c>
/// through the catalog to <c>ScanPlan.ColumnLateness</c>; not part of the
/// schema fingerprint.
/// </param>
public sealed record SchemaColumn(string Name, SqlType Type, string? Qualifier = null, long? Lateness = null);

/// <summary>
/// An ordered positional tuple of <see cref="SchemaColumn"/>s. Two schemas
/// produced by different plan nodes may share column names — lookup takes
/// both a qualifier and a name, and is ambiguous only when multiple columns
/// would match both.
/// </summary>
public sealed class Schema
{
    private readonly IReadOnlyList<SchemaColumn> _columns;

    public Schema(IReadOnlyList<SchemaColumn> columns)
    {
        ArgumentNullException.ThrowIfNull(columns);
        _columns = columns;
    }

    public static Schema Empty { get; } = new(Array.Empty<SchemaColumn>());

    public IReadOnlyList<SchemaColumn> Columns => _columns;

    public int Count => _columns.Count;

    public SchemaColumn this[int index] => _columns[index];

    public Schema Concat(Schema other)
    {
        ArgumentNullException.ThrowIfNull(other);
        var combined = new List<SchemaColumn>(_columns.Count + other._columns.Count);
        combined.AddRange(_columns);
        combined.AddRange(other._columns);
        return new Schema(combined);
    }

    /// <summary>
    /// Project this schema to a subset of its columns, in the order given by
    /// <paramref name="indices"/>. Used to derive join-key / group-by-key
    /// schemas from the parent row schema.
    /// </summary>
    public Schema SubsetByIndex(IReadOnlyList<int> indices)
    {
        ArgumentNullException.ThrowIfNull(indices);
        var subset = new SchemaColumn[indices.Count];
        for (var i = 0; i < indices.Count; i++)
        {
            subset[i] = _columns[indices[i]];
        }

        return new Schema(subset);
    }

    /// <summary>
    /// Resolve a column reference to its positional index, or throw
    /// <see cref="ResolveException"/> on unknown / ambiguous.
    /// </summary>
    public int Resolve(string? qualifier, string name)
    {
        var idx = TryResolve(qualifier, name);
        if (idx == -1)
        {
            throw new ResolveException($"unknown column '{Format(qualifier, name)}'");
        }

        return idx;
    }

    /// <summary>
    /// Like <see cref="Resolve"/> but returns -1 on a miss instead of throwing.
    /// Ambiguous matches still throw (the call site can't recover from that).
    /// Used by the resolver's correlated-subquery path to look up an identifier
    /// in an inner scope and then fall through to the outer scope on a miss
    /// without paying the exception cost.
    /// </summary>
    public int TryResolve(string? qualifier, string name)
    {
        var found = -1;
        for (var i = 0; i < _columns.Count; i++)
        {
            var c = _columns[i];
            if (!string.Equals(c.Name, name, StringComparison.Ordinal))
            {
                continue;
            }

            if (qualifier is not null && !string.Equals(c.Qualifier, qualifier, StringComparison.Ordinal))
            {
                continue;
            }

            if (found != -1)
            {
                throw new ResolveException($"column '{Format(qualifier, name)}' is ambiguous");
            }

            found = i;
        }

        return found;
    }

    private static string Format(string? qualifier, string name) =>
        qualifier is null ? name : $"{qualifier}.{name}";
}

/// <summary>
/// A mutable registry of <c>CREATE TABLE</c>-declared schemas keyed by table
/// name. Populated by the resolver as it processes DDL; queried on every
/// <c>SELECT … FROM t</c>.
/// </summary>
public sealed class Catalog
{
    private readonly Dictionary<string, Schema> _tables = new(StringComparer.Ordinal);
    private readonly HashSet<string> _appendOnly = new(StringComparer.Ordinal);

    public void Register(string name, Schema schema) => Register(name, schema, appendOnly: false);

    /// <param name="appendOnly">
    /// The table's declared <c>WITH ('append_only' = 'true')</c> property: rows are
    /// only ever inserted, never deleted or updated, so the table's Z-set weights
    /// are non-negative at every tick. Optimizer rules that are sound only over a
    /// non-negative Z-set consult this through
    /// <c>PlanWeightPositivity</c> (see <c>docs/design-row-representation.md</c> §18.6).
    /// It is a <i>contract</i>, like <c>NOT NULL</c>: the engine does not police it.
    /// </param>
    public void Register(string name, Schema schema, bool appendOnly)
    {
        ArgumentNullException.ThrowIfNull(name);
        ArgumentNullException.ThrowIfNull(schema);
        _tables[name] = schema;
        if (appendOnly)
        {
            _appendOnly.Add(name);
        }
        else
        {
            _appendOnly.Remove(name);
        }
    }

    /// <summary>Whether <paramref name="name"/> was declared <c>append_only</c>.</summary>
    public bool IsAppendOnly(string name) => _appendOnly.Contains(name);

    public bool TryGet(string name, out Schema? schema) => _tables.TryGetValue(name, out schema);

    public Schema Get(string name)
    {
        if (!_tables.TryGetValue(name, out var s))
        {
            throw new ResolveException($"unknown table '{name}'");
        }

        return s;
    }

    public IReadOnlyDictionary<string, Schema> Tables => _tables;
}

public sealed class ResolveException : Exception
{
    public ResolveException(string message) : base(message)
    {
    }

    public ResolveException(string message, Exception inner) : base(message, inner)
    {
    }
}
