// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using Clast.DatabaseDecimal.Values;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Sql.Compiler;

/// <summary>
/// Process-independent hashing for SQL partition keys, one overload per
/// supported typed-row column type. Reduces each value to a primitive the
/// Core <see cref="StableHash"/> can hash deterministically — unlike
/// <see cref="object.GetHashCode"/>, which for strings (and any
/// <see cref="HashCode"/>-based emitted row) is randomized per process.
/// </summary>
/// <remarks>
/// <para>
/// Stability is the whole point: data-parallel sharding places each key on
/// <c>hash(key) % W</c>, and a circuit recovered from a snapshot must place
/// every key on the same worker it held before — otherwise group-by /
/// join / distinct state would re-shard onto the wrong replica. The
/// per-column projection here is what makes the SQL exchange placement
/// (Phase 4) and per-worker recovery (Phase 5) agree across runs.
/// </para>
/// <para>
/// Each overload must be consistent with the emitted row's equality: two
/// values the typed <c>Equals</c> treats as equal must hash identically, or
/// equal keys could split across workers. NULL columns collapse to a single
/// sentinel (<see cref="NullHash"/>) so two null keys co-locate, matching
/// the typed Nullable equality (both <c>HasValue == false</c> ⇒ equal).
/// </para>
/// </remarks>
internal static class StablePartitionHash
{
    /// <summary>
    /// The hash assigned to a NULL column value. Arbitrary but fixed; only
    /// distribution (not correctness) depends on the exact constant.
    /// </summary>
    internal const int NullHash = unchecked((int)0x9E3779B1);

    public static int Of(int value) => StableHash.Of(value);

    public static int Of(long value) => StableHash.Of(value);

    public static int Of(bool value) => StableHash.Of(value ? 1 : 0);

    /// <summary>
    /// Hash a double by its IEEE-754 bits. <c>-0.0</c> is normalised to
    /// <c>+0.0</c> so the two (which compare equal) co-locate; every NaN
    /// payload still hashes by its bits, harmless because NaN never equals
    /// another key.
    /// </summary>
    public static int Of(double value) =>
        StableHash.Of(BitConverter.DoubleToInt64Bits(value == 0.0 ? 0.0 : value));

    public static int Of(string? value) => value is null ? NullHash : StableHash.Of(value);

    /// <summary>Hash a UTF-8 string by its bytes (Utf8String equality is byte-wise).</summary>
    public static int Of(Utf8String value) => StableHash.Of(value.Span);

    /// <summary>Hash a fixed-scale decimal by its library-provided stable digest.</summary>
    public static int Of(Decimal128 value) => StableHash.Of(unchecked((long)value.StableHash64()));

    public static int Of(Date32 value) => StableHash.Of(value.Days);

    public static int Of(Time64 value) => StableHash.Of(value.Microseconds);

    public static int Of(Timestamp value) => StableHash.Of(value.Microseconds);

    /// <summary>
    /// Hash a boxed structural-row slot value against its declared
    /// <see cref="SqlType"/>. The structural pipeline carries every column as a
    /// boxed <see cref="object"/> (<see cref="StructuralRow.this"/>), so it has no
    /// static CLR field type to overload on — this is the structural counterpart
    /// of the typed <see cref="Of(int)"/>… overloads and MUST reduce to the very
    /// same <see cref="StableHash"/> primitive for each type, so a value hashes to
    /// the same worker whether it arrives typed or structural (typed↔structural
    /// A/B parity and snapshot/restore co-location, see docs §4).
    /// </summary>
    /// <remarks>
    /// NULL (a <see langword="null"/> slot) collapses to <see cref="NullHash"/>,
    /// matching the typed Nullable path. A column type outside the stable-hash
    /// surface (REAL, INTERVAL, the untyped NULL literal) throws
    /// <see cref="NotSupportedException"/> — mirroring the typed compiler, which
    /// refuses the parallel compile rather than shard on an unstable key.
    /// </remarks>
    public static int OfBoxed(object? value, SqlType type)
    {
        if (value is null)
        {
            return NullHash;
        }

        return type switch
        {
            SqlIntegerType => Of((int)value),
            SqlBigintType => Of((long)value),
            SqlBooleanType => Of((bool)value),
            SqlDoubleType => Of((double)value),
            SqlDecimalType => Of((Decimal128)value),
            SqlVarcharType => Of((Utf8String)value),
            SqlDateType => Of((Date32)value),
            SqlTimeType => Of((Time64)value),
            SqlTimestampType => Of((Timestamp)value),
            _ => throw new NotSupportedException(
                $"no stable partition hash for SQL column type {type.GetType().Name}"),
        };
    }

    /// <summary>
    /// Stable partition hash of a structural row's key columns:
    /// <paramref name="keyIndices"/> selects the slots and the parallel
    /// <paramref name="keySchema"/> supplies each slot's <see cref="SqlType"/>.
    /// Single-column keys hash the one slot directly; composite keys fold the
    /// per-column hashes through <see cref="StableHash.Combine(int[])"/> — the
    /// exact reduction the typed <c>BuildStableRowHash</c> uses, so a typed and a
    /// structural build place the same key on the same worker.
    /// </summary>
    public static int OfRow(StructuralRow row, int[] keyIndices, Schema keySchema)
    {
        if (keyIndices.Length == 1)
        {
            return OfBoxed(row[keyIndices[0]], keySchema[0].Type);
        }

        var hashes = new int[keyIndices.Length];
        for (var k = 0; k < keyIndices.Length; k++)
        {
            hashes[k] = OfBoxed(row[keyIndices[k]], keySchema[k].Type);
        }

        return StableHash.Combine(hashes);
    }

    /// <summary>
    /// Stable partition hash over every column of a structural row (the whole-row
    /// key used by DISTINCT and by the table-input shard split). Equal rows hash
    /// identically, so they co-locate on one worker.
    /// </summary>
    public static int OfWholeRow(StructuralRow row, Schema schema)
    {
        if (schema.Count == 1)
        {
            return OfBoxed(row[0], schema[0].Type);
        }

        var hashes = new int[schema.Count];
        for (var i = 0; i < schema.Count; i++)
        {
            hashes[i] = OfBoxed(row[i], schema[i].Type);
        }

        return StableHash.Combine(hashes);
    }
}
