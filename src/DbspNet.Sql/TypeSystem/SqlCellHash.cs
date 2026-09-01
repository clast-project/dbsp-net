// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Reflection;
using Clast.DatabaseDecimal.Values;
using DbspNet.Core.Collections;

namespace DbspNet.Sql.TypeSystem;

/// <summary>
/// The per-column seed the typed compile path uses, and the reason it agrees with the boxed path.
/// </summary>
/// <remarks>
/// <para>Each overload returns exactly what <see cref="StructuralRowHash.Cell(object?)"/> would
/// return for the same value boxed — for most SQL types that is the type's own content-based
/// <see cref="object.GetHashCode"/> (widened), and for <see cref="Decimal128"/>, whose
/// <see cref="object.GetHashCode"/> is seeded per process, the content-based <c>StableHash64</c>
/// that <c>DbspNet.Core</c> also calls. Each calls <c>GetHashCode</c> on the statically-typed
/// value, so the struct's own override is invoked directly and nothing boxes — forwarding to
/// <see cref="StructuralRowHash.Opaque"/> instead would box on the one path that exists to avoid
/// boxing.</para>
/// <para><b>One algorithm, three call sites.</b> The boxed walk in
/// <see cref="StructuralRow.ComputeHash"/>, the typed hash delegate
/// (<c>TypedPlanCompiler.BuildTypedHashDelegate</c>) and the emitted row struct's own
/// <c>GetHashCode</c> (<c>TypedRowEmitter</c>) all resolve their per-column seed through
/// <see cref="MethodFor"/>, so a typed row and a boxed row of the same values cannot drift apart.
/// <c>TypedRowHashAgreementTests</c> pins it.</para>
/// </remarks>
public static class SqlCellHash
{
    public static ulong Of(Utf8String value) => (ulong)(uint)value.GetHashCode();

    public static ulong Of(Decimal128 value) => StructuralRowHash.Cell(value);

    public static ulong Of(Date32 value) => (ulong)(uint)value.GetHashCode();

    public static ulong Of(Time64 value) => (ulong)(uint)value.GetHashCode();

    public static ulong Of(Timestamp value) => (ulong)(uint)value.GetHashCode();

    public static ulong Of(Interval value) => (ulong)(uint)value.GetHashCode();

    public static ulong Of(Utf8String? value) =>
        value.HasValue ? Of(value.Value) : StructuralRowHash.NullSeed;

    public static ulong Of(Decimal128? value) =>
        value.HasValue ? Of(value.Value) : StructuralRowHash.NullSeed;

    public static ulong Of(Date32? value) =>
        value.HasValue ? Of(value.Value) : StructuralRowHash.NullSeed;

    public static ulong Of(Time64? value) =>
        value.HasValue ? Of(value.Value) : StructuralRowHash.NullSeed;

    public static ulong Of(Timestamp? value) =>
        value.HasValue ? Of(value.Value) : StructuralRowHash.NullSeed;

    public static ulong Of(Interval? value) =>
        value.HasValue ? Of(value.Value) : StructuralRowHash.NullSeed;

    /// <summary>
    /// The seed method the compilers should call for a column of static type
    /// <paramref name="clrType"/>: a non-boxing typed overload where one exists, otherwise
    /// <see langword="null"/>, meaning the caller must box and use <see cref="BoxedMethod"/>.
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
