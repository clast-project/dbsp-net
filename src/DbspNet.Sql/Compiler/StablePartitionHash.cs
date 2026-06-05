// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using Clast.DatabaseDecimal.Values;
using DbspNet.Core.Circuit;
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
}
