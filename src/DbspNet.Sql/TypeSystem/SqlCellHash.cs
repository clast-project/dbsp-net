// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.IO.Hashing;
using System.Reflection;
using System.Runtime.CompilerServices;
using Clast.DatabaseDecimal.Values;
using DbspNet.Core.Collections;

namespace DbspNet.Sql.TypeSystem;

/// <summary>
/// Value seeds for the SQL scalar types <c>DbspNet.Core</c> cannot name, completing
/// <see cref="StructuralRowHash"/>. Registered into Core's hook by <see cref="Register"/>, a module
/// initializer, so it is installed before any SQL value can exist.
/// </summary>
/// <remarks>
/// <para>Two of these types hash non-deterministically on their own: <see cref="string"/> (handled
/// in Core) and <see cref="Decimal128"/>, whose <see cref="object.GetHashCode"/> is seeded per
/// process — measured in docs/design-incremental-persistence.md §11.1, where it made three ivm-bench
/// views appear to restore wrong. The package exposes <c>StableHash64()</c>, which is content-based;
/// that is what a row hash must use.</para>
/// <para><b>One algorithm, three call sites.</b> The boxed path
/// (<see cref="StructuralRowHash.Cell(object?)"/> → <see cref="OfBoxed"/>), the typed hash delegate
/// (<c>TypedPlanCompiler.BuildTypedHashDelegate</c>) and the emitted row struct's own
/// <c>GetHashCode</c> (<c>TypedRowEmitter</c>) all resolve their per-column seed through
/// <see cref="MethodFor"/>, so a typed row and a boxed row of the same values cannot drift apart.
/// </para>
/// </remarks>
public static class SqlCellHash
{
    // CA2255: a module initializer is exactly the right tool here and the guidance's concern
    // (surprising work at load time) does not apply — this is one field assignment. It has to run
    // before any SQL value is hashed, because installing the hook later would mean two different
    // seeds for one value inside a process; StructuralRowHash freezes the hook after first use and
    // throws rather than let that happen silently.
#pragma warning disable CA2255
    [ModuleInitializer]
#pragma warning restore CA2255
    internal static void Register() => StructuralRowHash.ExternalCellHash = OfBoxed;

    public static ulong Of(Utf8String value) => XxHash3.HashToUInt64(value.Span);

    /// <summary>Content-based, unlike <see cref="Decimal128.GetHashCode"/>, which is seeded.</summary>
    public static ulong Of(Decimal128 value) => value.StableHash64();

    public static ulong Of(Date32 value) => StructuralRowHash.Cell((long)value.Days);

    public static ulong Of(Time64 value) => StructuralRowHash.Cell(value.Microseconds);

    public static ulong Of(Timestamp value) => StructuralRowHash.Cell(value.Microseconds);

    public static ulong Of(Interval value) =>
        StructuralRowHash.Cell(value.Micros) ^ System.Numerics.BitOperations.RotateLeft(
            StructuralRowHash.Cell((long)value.Months), 32);

    public static ulong Of(Utf8String? value) => value.HasValue ? Of(value.Value) : StructuralRowHash.NullSeed;

    public static ulong Of(Decimal128? value) => value.HasValue ? Of(value.Value) : StructuralRowHash.NullSeed;

    public static ulong Of(Date32? value) => value.HasValue ? Of(value.Value) : StructuralRowHash.NullSeed;

    public static ulong Of(Time64? value) => value.HasValue ? Of(value.Value) : StructuralRowHash.NullSeed;

    public static ulong Of(Timestamp? value) => value.HasValue ? Of(value.Value) : StructuralRowHash.NullSeed;

    public static ulong Of(Interval? value) => value.HasValue ? Of(value.Value) : StructuralRowHash.NullSeed;

    /// <summary>Boxed dispatch — Core's <see cref="StructuralRowHash.ExternalCellHash"/> hook.</summary>
    /// <remarks>Ordered by how often each type is actually a cell, because this is the whole
    /// per-cell dispatch once the SQL layer is loaded: VARCHAR and the integer keys first, the
    /// exotic temporal types last, then back to Core for the plain primitives.</remarks>
    public static ulong OfBoxed(object value) => value switch
    {
        Utf8String u => Of(u),
        long l => StructuralRowHash.Cell(l),
        double d => StructuralRowHash.Cell(d),
        Date32 dt => Of(dt),
        Timestamp ts => Of(ts),
        Decimal128 d => Of(d),
        int i => StructuralRowHash.Cell(i),
        bool b => StructuralRowHash.Cell(b),
        Time64 t => Of(t),
        Interval iv => Of(iv),
        _ => StructuralRowHash.CoreCell(value),
    };

    /// <summary>
    /// The seed method the compilers should call for a column of static type
    /// <paramref name="clrType"/>: a non-boxing typed overload where one exists, otherwise Core's
    /// boxed <see cref="StructuralRowHash.Cell(object?)"/>. Returns <see langword="null"/> when the
    /// caller must box.
    /// </summary>
    public static MethodInfo? MethodFor(Type clrType)
    {
        ArgumentNullException.ThrowIfNull(clrType);
        var typed = typeof(SqlCellHash).GetMethod(
            nameof(Of), BindingFlags.Public | BindingFlags.Static, null, new[] { clrType }, null);
        if (typed is not null && typed.GetParameters()[0].ParameterType == clrType)
        {
            return typed;
        }

        var core = typeof(StructuralRowHash).GetMethod(
            nameof(StructuralRowHash.Cell), BindingFlags.Public | BindingFlags.Static, null,
            new[] { clrType }, null);
        return core is not null && core.GetParameters()[0].ParameterType == clrType ? core : null;
    }

    /// <summary>Core's boxed seed, for column types with no typed overload.</summary>
    public static MethodInfo BoxedMethod { get; } = typeof(StructuralRowHash).GetMethod(
        nameof(StructuralRowHash.Cell), BindingFlags.Public | BindingFlags.Static, null,
        new[] { typeof(object) }, null)!;
}
