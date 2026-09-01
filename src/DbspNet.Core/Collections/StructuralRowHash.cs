// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Numerics;
using Clast.DatabaseDecimal.Values;

namespace DbspNet.Core.Collections;

/// <summary>
/// The engine's row hash: a deterministic 64-bit mix over per-cell value seeds, folded to 32 bits
/// for the <see cref="object.GetHashCode"/> contract.
/// </summary>
/// <remarks>
/// <para><b>Why not <see cref="HashCode"/>.</b> <see cref="HashCode"/> is seeded per process, so
/// every row hash used to differ between runs — even a row of nothing but <see cref="long"/>s
/// (docs/design-incremental-persistence.md §11.1 measured it). That made a persisted hash, a
/// persisted Bloom block, and any cross-process digest impossible, and it is why the engine already
/// carried two hand-written deterministic hashes elsewhere (<c>StablePartitionHash</c> for shard
/// placement, <c>HllHashing</c> for sketches). This is the third caller, and the one on the hot
/// path.</para>
/// <para><b>No dispatch hook.</b> An earlier revision routed the types Core cannot name through a
/// delegate the SQL layer installed. That put an indirect call and a second type-test chain on
/// every cell — measured at roughly double the cost of the virtual <c>GetHashCode</c> it replaced.
/// Only one cell type actually needed fixing, so Core names that one and lets every other type
/// answer for itself.</para>
/// <para><b>Contract.</b> <see cref="Cell(object?)"/> and the typed overloads must agree for the
/// same logical value — <c>Cell(42L)</c> and <c>Cell((object)42L)</c> return the same seed — because
/// the typed compile path hashes struct fields directly while the structural path hashes boxed
/// cells, and the two representations meet in one dictionary
/// (<see cref="TypedStructuralRow{TRow}"/>).</para>
/// <para><b>Shape.</b> One rotate+multiply per cell, one SplitMix64 finalizer per row: the same
/// per-value budget <see cref="HashCode"/> spends, with the avalanche moved to the fold.</para>
/// </remarks>
public static class StructuralRowHash
{
    private const ulong Prime = 0x9E3779B97F4A7C15UL;

    /// <summary>Seed for a SQL NULL. Distinct from any numeric zero so a NULL column and a 0
    /// column do not produce the same row hash. Public because the SQL layer's nullable overloads
    /// must use the identical value.</summary>
    public const ulong NullSeed = 0xD1B54A32D192ED03UL;

    /// <summary>
    /// Seed for one of four independent accumulator lanes, as <see cref="HashCode"/> and xxHash32
    /// use: cell <c>k</c> folds into lane <c>k &amp; 3</c>, so a wide row's multiplies overlap in the
    /// pipeline instead of forming one dependent chain. The lanes are plain <see cref="ulong"/>
    /// locals rather than a struct — every caller (this class, the typed hash delegate, the emitted
    /// row's IL) knows each cell's lane at compile time, so nothing carries an index at runtime and
    /// nothing copies an accumulator. Order still matters: a cell's lane comes from its position and
    /// the lanes combine in a fixed order.
    /// </summary>
    public static ulong LaneSeed(int lane, int arity) => lane switch
    {
        0 => Prime ^ (uint)arity,
        1 => 0xBF58476D1CE4E5B9UL,
        2 => 0x94D049BB133111EBUL,
        _ => 0xD1B54A32D192ED03UL,
    };

    /// <summary>Folds one cell seed into one lane.</summary>
    public static ulong StepLane(ulong lane, ulong cell) =>
        BitOperations.RotateLeft(lane ^ cell, 31) * Prime;

    /// <summary>The full 64-bit row hash.</summary>
    public static ulong Wide(ulong l0, ulong l1, ulong l2, ulong l3) => Mix(
        BitOperations.RotateLeft(l0, 1)
        + BitOperations.RotateLeft(l1, 7)
        + BitOperations.RotateLeft(l2, 12)
        + BitOperations.RotateLeft(l3, 18));

    /// <summary>Narrows a row hash to the <see cref="object.GetHashCode"/> contract.</summary>
    public static int Fold(ulong l0, ulong l1, ulong l2, ulong l3)
    {
        var h = Wide(l0, l1, l2, l3);
        return (int)(h ^ (h >> 32));
    }

    /// <summary>The 64-bit hash of an ordered cell sequence.</summary>
    public static ulong Of(IReadOnlyList<object?> values)
    {
        ArgumentNullException.ThrowIfNull(values);
        var n = values.Count;
        ulong l0 = LaneSeed(0, n), l1 = LaneSeed(1, n), l2 = LaneSeed(2, n), l3 = LaneSeed(3, n);
        var i = 0;
        for (; i + 4 <= n; i += 4)
        {
            l0 = StepLane(l0, Cell(values[i]));
            l1 = StepLane(l1, Cell(values[i + 1]));
            l2 = StepLane(l2, Cell(values[i + 2]));
            l3 = StepLane(l3, Cell(values[i + 3]));
        }

        for (; i < n; i++)
        {
            switch (i & 3)
            {
                case 0: l0 = StepLane(l0, Cell(values[i])); break;
                case 1: l1 = StepLane(l1, Cell(values[i])); break;
                case 2: l2 = StepLane(l2, Cell(values[i])); break;
                default: l3 = StepLane(l3, Cell(values[i])); break;
            }
        }

        return Wide(l0, l1, l2, l3);
    }

    /// <summary>
    /// Value seed for a boxed cell. Every branch depends only on the value's content; unknown types
    /// go to <see cref="ExternalCellHash"/> and, failing that, to the type's own
    /// <see cref="object.GetHashCode"/> (which is content-based and stable for every type the SQL
    /// runtime produces except <c>Decimal128</c>, the case the hook exists for).
    /// </summary>
    /// <summary>
    /// Value seed for a boxed cell. The hot primitives are named here; everything else falls
    /// through to the type's own <see cref="object.GetHashCode"/>, which is content-based — and so
    /// process-stable — for every type the SQL runtime puts in a row
    /// (<c>Utf8String</c> hashes with XxHash3, the temporal types are record structs over their
    /// tick counts). <see cref="Decimal128"/> is the single exception: its
    /// <see cref="object.GetHashCode"/> is seeded per process, and it exposes a content-based
    /// <c>StableHash64</c>, so it is named explicitly.
    /// </summary>
    /// <remarks>
    /// A cell type whose equality is by reference (a raw array, say) would hash by identity and
    /// break cross-process determinism. No such type reaches a row today, and one that did would
    /// already be broken for row equality, which compares cells with <see cref="object.Equals(object, object)"/>.
    /// </remarks>
    public static ulong Cell(object? value) => value switch
    {
        null => NullSeed,
        long l => Cell(l),
        double d => Cell(d),
        string s => Cell(s),
        Decimal128 m => Cell(m),
        _ => Opaque(value),
    };

    /// <summary>
    /// Seed for a type this class does not name — its own content-based hash, widened. The typed
    /// compile path must use exactly this for the same column type, which is what
    /// <c>SqlCellHash</c>'s overloads do.
    /// </summary>
    public static ulong Opaque(object value) => (ulong)(uint)value.GetHashCode();

    /// <summary>Content-based, unlike <see cref="Decimal128.GetHashCode"/>, which is seeded.</summary>
    public static ulong Cell(Decimal128 value) => value.StableHash64();

    public static ulong Cell(long value) => (ulong)value;

    /// <summary>
    /// Deliberately the same as <see cref="Opaque"/> of the boxed form, so <c>int</c> need not sit
    /// in <see cref="Cell(object?)"/>'s type-test chain. Every cell type in that chain costs ~3 ns
    /// to the types below it, which on a SQL row is most of them.
    /// </summary>
    public static ulong Cell(int value) => (ulong)(uint)value.GetHashCode();

    /// <inheritdoc cref="Cell(int)"/>
    public static ulong Cell(bool value) => (ulong)(uint)value.GetHashCode();

    /// <summary>Collapses -0.0 to +0.0 so the two compare-equal zeros seed alike.</summary>
    public static ulong Cell(double value) =>
        (ulong)BitConverter.DoubleToInt64Bits(value == 0.0 ? 0.0 : value);

    /// <inheritdoc cref="Cell(int)"/>
    public static ulong Cell(float value) => (ulong)(uint)value.GetHashCode();

    /// <summary>FNV-1a/64 over the UTF-16 code units. Content-based, unlike
    /// <see cref="string.GetHashCode()"/>, which is randomized per process.</summary>
    public static ulong Cell(string? value)
    {
        if (value is null)
        {
            return NullSeed;
        }

        var h = 14695981039346656037UL;
        foreach (var c in value)
        {
            h = (h ^ c) * 1099511628211UL;
        }

        return h;
    }

    public static ulong Cell(long? value) => value.HasValue ? Cell(value.Value) : NullSeed;

    public static ulong Cell(int? value) => value.HasValue ? Cell(value.Value) : NullSeed;

    public static ulong Cell(bool? value) => value.HasValue ? Cell(value.Value) : NullSeed;

    public static ulong Cell(double? value) => value.HasValue ? Cell(value.Value) : NullSeed;

    public static ulong Cell(float? value) => value.HasValue ? Cell(value.Value) : NullSeed;

    /// <summary>SplitMix64's finalizer — the avalanche step.</summary>
    private static ulong Mix(ulong z)
    {
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9UL;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EBUL;
        return z ^ (z >> 31);
    }
}
