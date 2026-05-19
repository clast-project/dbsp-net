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
    public void NullableColumnsAreUnsupported()
    {
        var nullable = new Schema(new[]
        {
            new SchemaColumn("x", new SqlIntegerType(true)),
        });

        Assert.Null(TypedRowEmitter.EmitRowType(nullable));
        Assert.False(TypedRowEmitter.IsSupported(nullable));
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
