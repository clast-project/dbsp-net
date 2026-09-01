// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Reflection;
using Clast.DatabaseDecimal.Values;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;
using Xunit;

namespace DbspNet.Tests.Sql;

/// <summary>
/// The row hash has three implementations — <see cref="StructuralRow.ComputeHash"/> over boxed
/// cells, the Expression-tree delegate behind <c>StructuralRowShape</c>, and the IL the emitted row
/// struct carries — and they must produce the same number for the same values. When they drift, a
/// typed key and a backing-array key stop finding each other in one dictionary and lookups silently
/// miss; nothing throws. Nothing covered this before (docs/design-incremental-persistence.md §11.1).
/// </summary>
public class TypedRowHashAgreementTests
{
    private static Schema MixedSchema => new(new[]
    {
        new SchemaColumn("i", new SqlIntegerType(false)),
        new SchemaColumn("l", new SqlBigintType(false)),
        new SchemaColumn("d", new SqlDoubleType(false)),
        new SchemaColumn("b", new SqlBooleanType(false)),
        new SchemaColumn("s", new SqlVarcharType(null, false)),
    });

    private static readonly object?[] MixedValues = { 1, 2L, 3.5, true, Utf8String.Of("hello") };

    [Fact]
    public void EmittedRowStructHashesLikeAStructuralRow()
    {
        var type = TypedRowEmitter.EmitRowType(MixedSchema)!;
        var emitted = TypedRowEmitter.BuildBoxedFactory(MixedSchema)!(MixedValues);

        Assert.Equal(StructuralRow.ComputeHash(MixedValues), emitted.GetHashCode());
        Assert.Equal(new StructuralRow(MixedValues).GetHashCode(), emitted.GetHashCode());
        Assert.Equal(type, emitted.GetType());
    }

    [Fact]
    public void TypedHashDelegateHashesLikeAStructuralRow()
    {
        // Reached by reflection on purpose: the delegate is an implementation detail of the typed
        // compile path, but the contract it honours is not, and it is the site most likely to be
        // edited without the other two.
        var rowType = TypedRowEmitter.EmitRowType(MixedSchema)!;
        var fields = new FieldInfo[MixedSchema.Count];
        for (var i = 0; i < MixedSchema.Count; i++)
        {
            fields[i] = rowType.GetField("F" + i)!;
        }

        var build = typeof(TypedPlanCompiler).GetMethod(
            "BuildTypedHashDelegate", BindingFlags.NonPublic | BindingFlags.Static);
        Assert.True(build is not null, "TypedPlanCompiler.BuildTypedHashDelegate was renamed — this test guards it");

        var hash = (Delegate)build!.Invoke(null, new object[] { rowType, fields })!;
        var row = TypedRowEmitter.BuildBoxedFactory(MixedSchema)!(MixedValues);

        Assert.Equal(
            StructuralRow.ComputeHash(MixedValues),
            (int)hash.DynamicInvoke(row)!);
    }

    [Fact]
    public void AgreementHoldsForTheSqlValueTypes()
    {
        // Utf8String and Decimal128 are the two that reach Core through the hook, and Decimal128 is
        // the one whose own GetHashCode is process-seeded.
        var schema = new Schema(new[]
        {
            new SchemaColumn("s", new SqlVarcharType(null, false)),
            new SchemaColumn("m", new SqlDecimalType(38, 4, false)),
            new SchemaColumn("dt", new SqlDateType(false)),
            new SchemaColumn("ts", new SqlTimestampType(false)),
        });
        object?[] values =
        {
            Utf8String.Of("ACME"),
            new Decimal128((System.Int128)12345),
            new Date32(19000),
            new Timestamp(1234567),
        };

        var emitted = TypedRowEmitter.BuildBoxedFactory(schema)!(values);
        Assert.Equal(StructuralRow.ComputeHash(values), emitted.GetHashCode());
    }
}
