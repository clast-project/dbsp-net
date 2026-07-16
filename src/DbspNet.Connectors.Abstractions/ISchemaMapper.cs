// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Arrow;
using DbspNet.Sql.Plan;
using ArrowSchema = Apache.Arrow.Schema;

namespace DbspNet.Connectors.Abstractions;

/// <summary>
/// Maps between a source's Arrow schema and a DbspNet <see cref="Schema"/>, and binds
/// a source onto a declared schema (the "infer-unless-declared" contract). The only
/// place external type ambiguity is resolved.
/// </summary>
public interface ISchemaMapper
{
    /// <summary>Infer a DbspNet schema from a source's Arrow schema (used when the
    /// table was not declared). Rejects nested/unsupported Arrow types.</summary>
    Schema Infer(ArrowSchema source);

    /// <summary>Validate a source against a <paramref name="declared"/> schema and
    /// produce the binding used to project every input batch onto it: which source
    /// column feeds each declared column, ignoring unused source columns. Throws when
    /// a declared column is missing from the source or the types are incompatible.</summary>
    SchemaBinding Bind(Schema declared, ArrowSchema source);
}

/// <summary>
/// The result of binding a declared schema to a source: for each declared column
/// (in order), the index of the source column that feeds it. A batch is projected by
/// selecting <see cref="SourceIndexByDeclaredColumn"/> columns in order. Unused source
/// columns are dropped; column order/naming differences are absorbed here.
/// </summary>
public sealed record SchemaBinding(
    Schema Declared,
    IReadOnlyList<int> SourceIndexByDeclaredColumn);

/// <summary>
/// Default <see cref="ISchemaMapper"/> backed by <see cref="ArrowSchemaBridge"/>. Infer
/// = <c>FromArrow</c>; bind matches declared columns to source columns by name
/// (case-insensitive), checks type compatibility via a round-trip through
/// <c>FromArrowType</c>, and records the projection. v1 requires exact type
/// compatibility (an inferred source type equal to the declared type); coercion beyond
/// nullability is a follow-on.
/// </summary>
public sealed class ArrowSchemaMapper : ISchemaMapper
{
    public static ArrowSchemaMapper Instance { get; } = new();

    public Schema Infer(ArrowSchema source)
    {
        ArgumentNullException.ThrowIfNull(source);
        return ArrowSchemaBridge.FromArrow(source);
    }

    public SchemaBinding Bind(Schema declared, ArrowSchema source)
    {
        ArgumentNullException.ThrowIfNull(declared);
        ArgumentNullException.ThrowIfNull(source);

        // Index source fields by name (case-insensitive), matching how SQL identifiers
        // are compared for a table's columns.
        var sourceByName = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < source.FieldsList.Count; i++)
        {
            // First occurrence wins; a duplicate name in the source is ambiguous but
            // only matters if a declared column asks for it.
            sourceByName.TryAdd(source.FieldsList[i].Name, i);
        }

        var indices = new int[declared.Count];
        for (var d = 0; d < declared.Count; d++)
        {
            var col = declared[d];
            if (!sourceByName.TryGetValue(col.Name, out var srcIdx))
            {
                throw new InvalidOperationException(
                    $"declared column '{col.Name}' has no matching column in the source schema");
            }

            var srcField = source.FieldsList[srcIdx];
            var inferred = ArrowSchemaBridge.FromArrowType(srcField.DataType, srcField.IsNullable);

            // Compare ignoring nullability (a NOT NULL declaration over a nullable
            // source column is allowed — the source is trusted to honour it).
            if (!inferred.WithNullable(false).Equals(col.Type.WithNullable(false)))
            {
                throw new InvalidOperationException(
                    $"declared column '{col.Name}' is {col.Type.Display} but the source " +
                    $"column is {inferred.Display} — incompatible (coercion is a v1 follow-on)");
            }

            indices[d] = srcIdx;
        }

        return new SchemaBinding(declared, indices);
    }
}
