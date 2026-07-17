// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using Apache.Arrow;
using Apache.Arrow.Types;
using ArrowSchema = Apache.Arrow.Schema;

namespace DbspNet.Connectors.Abstractions;

/// <summary>
/// Resolves a declared column name against a (possibly nested) Arrow source and extracts
/// the corresponding leaf column, flattening nested structs on demand.
///
/// <para>DbspNet has a flat row model: the SQL compiler lowers a <c>ROW(...)</c> column
/// into dotted leaf columns (e.g. <c>Customer.Account.CA_B_ID</c>). Sources written by
/// engines that keep structure (spark-xml, Parquet with nested groups) present those as a
/// nested Arrow <see cref="StructType"/>. This bridges the two: a declared dotted name is
/// resolved as a path into the source's nested structs, and the leaf is projected out.</para>
///
/// <para>The resolution is <b>schema-driven</b>, not an unconditional flatten: an exact
/// top-level match is tried first (so all-flat sources — and any source field that happens
/// to contain a literal dot — are unchanged), and a nested walk is only attempted when the
/// declared name is dotted and has no top-level field. When DbspNet later retains structs
/// end-to-end, a declared struct column will match the source struct directly through the
/// same top-level path and nothing here has to change.</para>
/// </summary>
public static class NestedArrowResolver
{
    /// <summary>
    /// Resolve <paramref name="declaredName"/> against <paramref name="source"/>. On success
    /// returns the field-index path (top-level index first, then child indices for each
    /// nested level) and the resolved leaf <see cref="Field"/>. A top-level match yields a
    /// single-element path.
    /// </summary>
    public static bool TryResolve(ArrowSchema source, string declaredName, out int[] path, out Field? leaf)
    {
        // Exact top-level match wins — preserves flat-source behaviour and lets a source
        // field with a literal '.' in its name bind directly rather than being split.
        for (var i = 0; i < source.FieldsList.Count; i++)
        {
            if (string.Equals(source.FieldsList[i].Name, declaredName, StringComparison.OrdinalIgnoreCase))
            {
                path = new[] { i };
                leaf = source.FieldsList[i];
                return true;
            }
        }

        if (!declaredName.Contains('.', StringComparison.Ordinal))
        {
            path = System.Array.Empty<int>();
            leaf = null;
            return false;
        }

        // Nested walk: match each dotted segment against the struct field names in turn.
        var segments = declaredName.Split('.');
        var topIdx = IndexOf(source.FieldsList, segments[0]);
        if (topIdx < 0)
        {
            path = System.Array.Empty<int>();
            leaf = null;
            return false;
        }

        var indices = new int[segments.Length];
        indices[0] = topIdx;
        var field = source.FieldsList[topIdx];
        for (var s = 1; s < segments.Length; s++)
        {
            if (field.DataType is not StructType st)
            {
                path = System.Array.Empty<int>();
                leaf = null;
                return false;
            }

            var childIdx = IndexOf(st.Fields, segments[s]);
            if (childIdx < 0)
            {
                path = System.Array.Empty<int>();
                leaf = null;
                return false;
            }

            indices[s] = childIdx;
            field = st.Fields[childIdx];
        }

        path = indices;
        leaf = field;
        return true;
    }

    /// <summary>
    /// Extract the leaf array named by <paramref name="path"/> from <paramref name="batch"/>.
    /// A single-element path returns the top-level column unchanged. A nested path descends
    /// through the struct columns and returns the leaf, <b>propagating parent-struct nulls</b>:
    /// a row under any null ancestor struct is null in the result even if the leaf's own slot
    /// holds a stale value.
    /// </summary>
    public static IArrowArray Extract(RecordBatch batch, int[] path)
    {
        ArgumentNullException.ThrowIfNull(batch);
        ArgumentNullException.ThrowIfNull(path);
        if (path.Length == 0)
        {
            throw new ArgumentException("path must have at least one segment", nameof(path));
        }

        var top = batch.Column(path[0]);
        if (path.Length == 1)
        {
            return top;
        }

        var length = batch.Length;
        var structs = new StructArray[path.Length - 1];
        IArrowArray current = top;
        for (var k = 0; k < path.Length - 1; k++)
        {
            if (current is not StructArray sa)
            {
                throw new InvalidOperationException(
                    $"path segment {k} is not a struct; cannot descend to extract the leaf");
            }

            // Offset handling for nested slices is not needed for Delta CDF / Parquet
            // batches (they arrive at offset 0). Guard rather than risk misaligning a
            // rebuilt validity bitmap against offset value buffers.
            if (sa.Offset != 0)
            {
                throw new NotSupportedException(
                    "nested struct extraction over a non-zero array offset is not supported");
            }

            structs[k] = sa;
            current = sa.Fields[path[k + 1]];
        }

        var leaf = current;
        if (leaf.Data.Offset != 0)
        {
            throw new NotSupportedException(
                "nested leaf extraction over a non-zero array offset is not supported");
        }

        // If no ancestor struct has nulls, the leaf's own validity already tells the whole
        // story — return it as-is (zero-copy).
        var anyAncestorNull = false;
        foreach (var s in structs)
        {
            if (s.NullCount != 0)
            {
                anyAncestorNull = true;
                break;
            }
        }

        if (!anyAncestorNull)
        {
            return leaf;
        }

        // Rebuild the leaf with validity = AND(all ancestor structs, leaf). Values are
        // shared with the original leaf (only buffer 0, the validity bitmap, is replaced).
        var validity = new byte[BitUtility.ByteCount(length)];
        var nullCount = 0;
        for (var r = 0; r < length; r++)
        {
            var valid = leaf.IsValid(r);
            if (valid)
            {
                foreach (var s in structs)
                {
                    if (!s.IsValid(r))
                    {
                        valid = false;
                        break;
                    }
                }
            }

            if (valid)
            {
                BitUtility.SetBit(validity, r);
            }
            else
            {
                nullCount++;
            }
        }

        var buffers = leaf.Data.Buffers.ToArray();
        buffers[0] = new ArrowBuffer(validity);
        var data = new ArrayData(leaf.Data.DataType, length, nullCount, 0, buffers, leaf.Data.Children);
        return ArrowArrayFactory.BuildArray(data);
    }

    private static int IndexOf(IReadOnlyList<Field> fields, string name)
    {
        for (var i = 0; i < fields.Count; i++)
        {
            if (string.Equals(fields[i].Name, name, StringComparison.OrdinalIgnoreCase))
            {
                return i;
            }
        }

        return -1;
    }
}
