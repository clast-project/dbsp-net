// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
namespace DbspNet.Core.Collections;

/// <summary>
/// Per-schema descriptor shared by all <see cref="TypedStructuralRow{TRow}"/>
/// instances of one row type: the column count, a typed hash function that
/// reproduces <see cref="StructuralRow.ComputeHash"/> directly from the struct's
/// fields (no boxing), and a per-column boxing accessor used lazily by the
/// indexer. Built once at compile time and captured by every wrapped row, so a
/// row instance carries only the struct plus one reference.
/// </summary>
public sealed class StructuralRowShape<TRow>
{
    public StructuralRowShape(int arity, Func<TRow, int> hash, Func<TRow, int, object?> box)
    {
        ArgumentNullException.ThrowIfNull(hash);
        ArgumentNullException.ThrowIfNull(box);
        Arity = arity;
        Hash = hash;
        Box = box;
    }

    public int Arity { get; }

    /// <summary>Computes the canonical <see cref="StructuralRow"/> hash from the
    /// typed fields without allocating an <c>object?[]</c> or boxing.</summary>
    public Func<TRow, int> Hash { get; }

    /// <summary>Boxes the value of column <c>i</c> on demand.</summary>
    public Func<TRow, int, object?> Box { get; }
}

/// <summary>
/// A <see cref="StructuralRow"/> that wraps an emitted typed row struct inline
/// and boxes columns <em>lazily</em>, only when the indexer is read. This is the
/// output-boundary representation for the typed compile path
/// (docs/design-row-representation.md §16, lever 2): instead of eagerly building
/// an <c>object?[]</c> and boxing every field at the typed→structural boundary
/// (the dominant per-output-row allocation on output-heavy queries like q18/q19),
/// the boundary constructs one of these per output row — a single heap object
/// holding the value-type struct, with the canonical hash computed directly from
/// the typed fields.
/// </summary>
/// <remarks>
/// Indistinguishable from a backing-array <see cref="StructuralRow"/> with the
/// same logical values: same <see cref="Count"/>, same indexer values, and — by
/// construction — the same hash (<see cref="StructuralRowShape{TRow}.Hash"/>
/// reproduces <see cref="StructuralRow.ComputeHash"/> field-by-field, valid
/// because <c>HashCode.Add(typedField)</c> and <c>HashCode.Add((object)boxed)</c>
/// feed the identical per-element hash). So output Z-set dedup, cross-type lookups,
/// and <see cref="StructuralRow.Equals(StructuralRow)"/> all behave exactly as
/// before (the inherited equality walks the virtual indexer).
/// </remarks>
public sealed class TypedStructuralRow<TRow> : StructuralRow
{
    private readonly TRow _row;
    private readonly StructuralRowShape<TRow> _shape;

    public TypedStructuralRow(TRow row, StructuralRowShape<TRow> shape)
        : base((shape ?? throw new ArgumentNullException(nameof(shape))).Hash(row))
    {
        _row = row;
        _shape = shape;
    }

    public override int Count => _shape.Arity;

    public override object? this[int index] => _shape.Box(_row, index);
}
