// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;


namespace DbspNet.Tests.Sql;

/// <summary>
/// Phase 1.0 smoke tests for <see cref="TypedRowEmitter"/>. Verifies
/// the emitted struct has the right shape, equality / hash work, and
/// instances behave correctly as keys in dictionaries / Z-sets — the
/// uses the later phases of the typed-row pipeline will lean on.
/// </summary>
public class TypedRowEmitterTests
{
    private static Schema TwoIntSchema => new(new[]
    {
        new SchemaColumn("a", new SqlIntegerType(false)),
        new SchemaColumn("b", new SqlIntegerType(false)),
    });

    private static Schema IntStringSchema => new(new[]
    {
        new SchemaColumn("id", new SqlIntegerType(false)),
        new SchemaColumn("name", new SqlVarcharType(null, false)),
    });

    private static Schema MixedSchema => new(new[]
    {
        new SchemaColumn("i", new SqlIntegerType(false)),
        new SchemaColumn("l", new SqlBigintType(false)),
        new SchemaColumn("d", new SqlDoubleType(false)),
        new SchemaColumn("b", new SqlBooleanType(false)),
        new SchemaColumn("s", new SqlVarcharType(null, false)),
    });

    [Fact]
    public void EmitsValueType()
    {
        var type = TypedRowEmitter.EmitRowType(TwoIntSchema);
        Assert.NotNull(type);
        Assert.True(type.IsValueType, "emitted typed row must be a struct");
        Assert.True(type.IsSealed, "emitted typed row must be sealed");
    }

    [Fact]
    public void IdenticalSchemasShareEmittedType()
    {
        var t1 = TypedRowEmitter.EmitRowType(TwoIntSchema);
        var t2 = TypedRowEmitter.EmitRowType(TwoIntSchema);
        Assert.Same(t1, t2);
    }

    [Fact]
    public void NullableColumnsSupported()
    {
        // Phase N1.1: nullable value-type columns get a Nullable<T>
        // field; nullable schemas no longer reject.
        var nullable = new Schema(new[]
        {
            new SchemaColumn("x", new SqlIntegerType(true)),
        });

        var type = TypedRowEmitter.EmitRowType(nullable);
        Assert.NotNull(type);
        Assert.True(TypedRowEmitter.IsSupported(nullable));

        // The single field is typed as int? (Nullable<int>).
        var field = type!.GetField("F0");
        Assert.NotNull(field);
        Assert.Equal(typeof(int?), field!.FieldType);
    }

    [Fact]
    public void NullableAndNonNullableSchemasGetDistinctEmittedTypes()
    {
        // The fingerprint must distinguish nullable from non-null —
        // they have different field layouts.
        var nn = new Schema(new[] { new SchemaColumn("x", new SqlIntegerType(false)) });
        var nul = new Schema(new[] { new SchemaColumn("x", new SqlIntegerType(true)) });

        var t1 = TypedRowEmitter.EmitRowType(nn);
        var t2 = TypedRowEmitter.EmitRowType(nul);
        Assert.NotNull(t1);
        Assert.NotNull(t2);
        Assert.NotSame(t1, t2);
        Assert.Equal(typeof(int), t1!.GetField("F0")!.FieldType);
        Assert.Equal(typeof(int?), t2!.GetField("F0")!.FieldType);
    }

    [Fact]
    public void NullableFactoryRoundTripsNullAndValue()
    {
        var schema = new Schema(new[]
        {
            new SchemaColumn("id", new SqlIntegerType(false)),
            new SchemaColumn("v", new SqlIntegerType(true)),
            new SchemaColumn("name", new SqlVarcharType(null, true)),
        });
        var factory = TypedRowEmitter.BuildBoxedFactory(schema)!;
        var getters = TypedRowEmitter.BuildFieldGetters(schema)!;

        var withValues = factory(new object?[] { 1, 42, Utf8String.Of("alice") });
        Assert.Equal(1, getters[0](withValues));
        Assert.Equal(42, getters[1](withValues));            // boxed int via Nullable<int>
        Assert.Equal(Utf8String.Of("alice"), getters[2](withValues));

        var withNulls = factory(new object?[] { 2, null, null });
        Assert.Equal(2, getters[0](withNulls));
        Assert.Null(getters[1](withNulls));                  // Nullable<int> with HasValue=false → null
        Assert.Null(getters[2](withNulls));                  // Utf8String? null
    }

    [Fact]
    public void NullableEqualityAndHashByValue()
    {
        var schema = new Schema(new[]
        {
            new SchemaColumn("a", new SqlIntegerType(false)),
            new SchemaColumn("b", new SqlIntegerType(true)),
        });
        var factory = TypedRowEmitter.BuildBoxedFactory(schema)!;

        var aNull1 = factory(new object?[] { 1, null });
        var aNull2 = factory(new object?[] { 1, null });   // equal
        var bVal = factory(new object?[] { 1, 5 });        // differs (null vs 5)
        var aDiff = factory(new object?[] { 2, null });    // differs (a differs)
        var bSame = factory(new object?[] { 1, 5 });       // equal to bVal

        Assert.True(aNull1.Equals(aNull2));
        Assert.Equal(aNull1.GetHashCode(), aNull2.GetHashCode());
        Assert.False(aNull1.Equals(bVal));
        Assert.False(aNull1.Equals(aDiff));
        Assert.True(bVal.Equals(bSame));
        Assert.Equal(bVal.GetHashCode(), bSame.GetHashCode());
    }

    [Fact]
    public void NullableWorksAsHashSetKey()
    {
        var schema = new Schema(new[]
        {
            new SchemaColumn("k", new SqlIntegerType(false)),
            new SchemaColumn("v", new SqlIntegerType(true)),
        });
        var factory = TypedRowEmitter.BuildBoxedFactory(schema)!;

        var set = new HashSet<object>
        {
            factory(new object?[] { 1, null }),
            factory(new object?[] { 1, null }),   // duplicate (null v)
            factory(new object?[] { 1, 5 }),
            factory(new object?[] { 2, null }),
        };

        Assert.Equal(3, set.Count);
        Assert.Contains(factory(new object?[] { 1, null }), set);
        Assert.DoesNotContain(factory(new object?[] { 99, null }), set);
    }

    [Fact]
    public void FactoryRoundTripsValues_Ints()
    {
        var factory = TypedRowEmitter.BuildBoxedFactory(TwoIntSchema)!;
        var getters = TypedRowEmitter.BuildFieldGetters(TwoIntSchema)!;

        var row = factory(new object?[] { 7, 42 });
        Assert.Equal(7, getters[0](row));
        Assert.Equal(42, getters[1](row));
    }

    [Fact]
    public void FactoryRoundTripsValues_MixedPrimitives()
    {
        var factory = TypedRowEmitter.BuildBoxedFactory(MixedSchema)!;
        var getters = TypedRowEmitter.BuildFieldGetters(MixedSchema)!;

        var row = factory(new object?[] { 1, 2L, 3.5, true, Utf8String.Of("hello") });
        Assert.Equal(1, getters[0](row));
        Assert.Equal(2L, getters[1](row));
        Assert.Equal(3.5, getters[2](row));
        Assert.Equal(true, getters[3](row));
        Assert.Equal(Utf8String.Of("hello"), getters[4](row));
    }

    [Fact]
    public void EqualsAndHashByValue()
    {
        var factory = TypedRowEmitter.BuildBoxedFactory(IntStringSchema)!;

        var a = factory(new object?[] { 7, Utf8String.Of("alpha") });
        var b = factory(new object?[] { 7, Utf8String.Of("alpha") });
        var c = factory(new object?[] { 7, Utf8String.Of("beta") });
        var d = factory(new object?[] { 8, Utf8String.Of("alpha") });

        Assert.True(a.Equals(b));
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
        Assert.False(a.Equals(c));
        Assert.False(a.Equals(d));
    }

    [Fact]
    public void WorksAsHashSetKey()
    {
        var factory = TypedRowEmitter.BuildBoxedFactory(IntStringSchema)!;
        var set = new HashSet<object>
        {
            factory(new object?[] { 1, Utf8String.Of("x") }),
            factory(new object?[] { 1, Utf8String.Of("x") }),       // duplicate
            factory(new object?[] { 1, Utf8String.Of("y") }),
            factory(new object?[] { 2, Utf8String.Of("x") }),
        };

        Assert.Equal(3, set.Count);
        Assert.Contains(factory(new object?[] { 1, Utf8String.Of("x") }), set);
        Assert.DoesNotContain(factory(new object?[] { 99, Utf8String.Of("z") }), set);
    }

    [Fact]
    public void TemporalColumnsSupported()
    {
        // Date32 / Time64 / Timestamp are record structs, so they
        // auto-generate op_Equality — the value-type fallback in
        // EmitFieldEqualityCheck handles them with no IL changes.
        var schema = new Schema(new[]
        {
            new SchemaColumn("d", new SqlDateType(false)),
            new SchemaColumn("t", new SqlTimeType(false)),
            new SchemaColumn("ts", new SqlTimestampType(false)),
        });
        var type = TypedRowEmitter.EmitRowType(schema);
        Assert.NotNull(type);
        var factory = TypedRowEmitter.BuildBoxedFactory(schema)!;

        var a = factory(new object?[] { new Date32(100), new Time64(2_000_000), new Timestamp(3_000_000) });
        var b = factory(new object?[] { new Date32(100), new Time64(2_000_000), new Timestamp(3_000_000) });
        var c = factory(new object?[] { new Date32(101), new Time64(2_000_000), new Timestamp(3_000_000) });

        Assert.True(a.Equals(b));
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
        Assert.False(a.Equals(c));
    }

    [Fact]
    public void Decimal128ColumnSupported()
    {
        // Decimal128 is a value type with op_Equality, so it picks up
        // the EmitFieldEqualityCheck value-type fallback automatically.
        var schema = new Schema(new[]
        {
            new SchemaColumn("amt", new SqlDecimalType(10, 2, false)),
        });
        var type = TypedRowEmitter.EmitRowType(schema);
        Assert.NotNull(type);
        var factory = TypedRowEmitter.BuildBoxedFactory(schema)!;
        var getters = TypedRowEmitter.BuildFieldGetters(schema)!;

        var v = new Clast.DatabaseDecimal.Values.Decimal128(12345);
        var row = factory(new object?[] { v });
        Assert.Equal(v, getters[0](row));

        // Equality by value, used as a HashSet key.
        var a = factory(new object?[] { new Clast.DatabaseDecimal.Values.Decimal128(100) });
        var b = factory(new object?[] { new Clast.DatabaseDecimal.Values.Decimal128(100) });
        var c = factory(new object?[] { new Clast.DatabaseDecimal.Values.Decimal128(101) });
        Assert.True(a.Equals(b));
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
        Assert.False(a.Equals(c));
    }

    [Fact]
    public void TypedFieldsCtorBuildsEquivalentInstance()
    {
        // The typed-fields ctor (Ti, Tj, ...) feeds the Phase 1.2+ typed
        // Map/Project: build an output row from individual typed values
        // with no boxing through object?[]. Round-trip via reflection
        // because the closed type isn't visible at compile time.
        var type = TypedRowEmitter.EmitRowType(MixedSchema)!;
        var typedCtor = type.GetConstructor(new[]
        {
            typeof(int), typeof(long), typeof(double), typeof(bool), typeof(Utf8String),
        })!;

        var fromTyped = typedCtor.Invoke(new object?[] { 1, 2L, 3.5, true, Utf8String.Of("hello") });
        var fromBoxed = TypedRowEmitter.BuildBoxedFactory(MixedSchema)!(
            new object?[] { 1, 2L, 3.5, true, Utf8String.Of("hello") });

        Assert.Equal(fromBoxed, fromTyped);
        Assert.Equal(fromBoxed.GetHashCode(), fromTyped.GetHashCode());
    }

    [Fact]
    public void WorksAsZSetKeyViaReflection()
    {
        // Confirms that the emitted struct plays nicely with the Core's
        // ZSet<TKey, TWeight> — the most important downstream use. We
        // construct ZSet<TEmitted, Z64> via reflection because TEmitted
        // isn't known at compile time; the typed-row pipeline phases
        // will close this generic argument at SQL-compile time.
        var type = TypedRowEmitter.EmitRowType(TwoIntSchema)!;
        var factory = TypedRowEmitter.BuildBoxedFactory(TwoIntSchema)!;

        // ZSet<TEmitted, Z64>.Empty
        var zsetType = typeof(ZSet<,>).MakeGenericType(type, typeof(Z64));
        var emptyProp = zsetType.GetProperty(nameof(ZSet<int, Z64>.Empty),
            System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static)!;
        var empty = emptyProp.GetValue(null)!;

        // ZSet.Singleton(row, Z64.One) — invoke generic static method
        var singletonMethod = typeof(ZSet)
            .GetMethods(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static)
            .Single(m => m.Name == nameof(ZSet.Singleton) && m.IsGenericMethodDefinition);
        var closedSingleton = singletonMethod.MakeGenericMethod(type, typeof(Z64));

        var r1 = factory(new object?[] { 1, 2 });
        var r2 = factory(new object?[] { 1, 2 });    // equal to r1
        var r3 = factory(new object?[] { 1, 3 });

        var s1 = closedSingleton.Invoke(null, new object?[] { r1, Z64.One })!;
        var s2 = closedSingleton.Invoke(null, new object?[] { r2, Z64.One })!;
        var s3 = closedSingleton.Invoke(null, new object?[] { r3, Z64.One })!;

        // Adding two ZSets with the same key should sum weights (one row, weight 2).
        var plusOp = zsetType.GetMethod("op_Addition")!;
        var sum12 = plusOp.Invoke(null, new[] { s1, s2 })!;

        var countProp = zsetType.GetProperty(nameof(ZSet<int, Z64>.Count))!;
        Assert.Equal(1, (int)countProp.GetValue(sum12)!);

        // s1 + s3 has two distinct keys.
        var sum13 = plusOp.Invoke(null, new[] { s1, s3 })!;
        Assert.Equal(2, (int)countProp.GetValue(sum13)!);

        _ = empty;  // touch to keep the variable used
    }
}
