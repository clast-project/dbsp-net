// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Linq.Expressions;
using System.Reflection;
using System.Reflection.Emit;

namespace DbspNet.Tests.Spike;

/// <summary>
/// Phase 1 spike B — proves that <see cref="AssemblyBuilder"/> +
/// <see cref="TypeBuilder"/> can emit a value-typed row struct at runtime
/// with a working ctor and sound equality semantics. For the spike we rely on
/// the runtime's default <see cref="ValueType.Equals(object?)"/> /
/// <see cref="ValueType.GetHashCode"/> (reflection-driven field comparison)
/// — this is correct but slow; a production implementation would hand-emit
/// <see cref="IEquatable{T}.Equals(T)"/> and a fast <c>GetHashCode</c>.
/// Spike A already validated that typed structs plug into the Core Z-set
/// pipeline, so this spike doesn't re-run the circuit; it focuses only on
/// the codegen question.
/// </summary>
public class EmittedRowSpikeTests
{
    private sealed record FieldDef(string Name, Type ClrType);

    /// <summary>
    /// Emits <c>public readonly struct {typeName} { public readonly T1 Field1; ... }</c>
    /// with a public constructor taking one argument per field. Returns the
    /// finished <see cref="Type"/>. Equality and hashing are left to the
    /// default <see cref="ValueType"/> implementation (reflection-based).
    /// </summary>
    private static Type EmitRowStruct(string typeName, IReadOnlyList<FieldDef> fields)
    {
        var asmName = new AssemblyName("DbspNet.SpikeEmitted");
        var asm = AssemblyBuilder.DefineDynamicAssembly(asmName, AssemblyBuilderAccess.Run);
        var module = asm.DefineDynamicModule("Main");

        var tb = module.DefineType(
            typeName,
            TypeAttributes.Public | TypeAttributes.Sealed | TypeAttributes.SequentialLayout,
            typeof(ValueType));

        var fieldBuilders = new FieldBuilder[fields.Count];
        for (var i = 0; i < fields.Count; i++)
        {
            fieldBuilders[i] = tb.DefineField(
                fields[i].Name,
                fields[i].ClrType,
                FieldAttributes.Public | FieldAttributes.InitOnly);
        }

        // Constructor: assign each argument to the corresponding field.
        var ctorArgTypes = fields.Select(f => f.ClrType).ToArray();
        var ctor = tb.DefineConstructor(
            MethodAttributes.Public | MethodAttributes.HideBySig |
            MethodAttributes.SpecialName | MethodAttributes.RTSpecialName,
            CallingConventions.Standard,
            ctorArgTypes);
        var il = ctor.GetILGenerator();
        for (var i = 0; i < fields.Count; i++)
        {
            il.Emit(OpCodes.Ldarg_0);                 // this
            il.Emit(OpCodes.Ldarg, (short)(i + 1));   // arg_{i+1}
            il.Emit(OpCodes.Stfld, fieldBuilders[i]); // this.Field_i = arg
        }

        il.Emit(OpCodes.Ret);

        return tb.CreateType();
    }

    /// <summary>
    /// Compiles <c>(object?[] vs) => new T((T1)vs[0], (T2)vs[1], ...)</c> via
    /// LINQ expression trees, returning a fast (non-reflection) constructor
    /// delegate. The TableInput's <c>Insert(params object?[])</c> path would
    /// use exactly this shape in the real pipeline.
    /// </summary>
    private static Func<object?[], object> BuildConstructorDelegate(Type rowType, IReadOnlyList<FieldDef> fields)
    {
        var ctor = rowType.GetConstructor(fields.Select(f => f.ClrType).ToArray())
            ?? throw new InvalidOperationException("emitted type has no matching ctor");

        var valuesParam = Expression.Parameter(typeof(object?[]), "vs");
        var args = new Expression[fields.Count];
        for (var i = 0; i < fields.Count; i++)
        {
            args[i] = Expression.Convert(
                Expression.ArrayIndex(valuesParam, Expression.Constant(i)),
                fields[i].ClrType);
        }

        var body = Expression.Convert(Expression.New(ctor, args), typeof(object));
        return Expression.Lambda<Func<object?[], object>>(body, valuesParam).Compile();
    }

    [Fact]
    public void EmittedStruct_IsValueTypeWithWorkingCtor()
    {
        var fields = new[]
        {
            new FieldDef("A", typeof(int)),
            new FieldDef("B", typeof(int)),
        };
        var rowType = EmitRowStruct("EmittedRow_TwoInt", fields);

        Assert.True(rowType.IsValueType);
        Assert.Equal(typeof(ValueType), rowType.BaseType);
        Assert.Equal(2, rowType.GetFields().Length);

        var build = BuildConstructorDelegate(rowType, fields);
        var instance = build(new object?[] { 7, 42 });
        Assert.NotNull(instance);
        Assert.Equal(rowType, instance!.GetType());

        // Field values round-trip via reflection.
        Assert.Equal(7, rowType.GetField("A")!.GetValue(instance));
        Assert.Equal(42, rowType.GetField("B")!.GetValue(instance));
    }

    [Fact]
    public void EmittedStruct_EqualityViaValueTypeReflection()
    {
        // Default ValueType.Equals walks fields via reflection. Slow but
        // correct — sufficient for the spike.
        var fields = new[]
        {
            new FieldDef("A", typeof(int)),
            new FieldDef("B", typeof(int)),
        };
        var rowType = EmitRowStruct("EmittedRow_EqCheck", fields);
        var build = BuildConstructorDelegate(rowType, fields);

        var r1 = build(new object?[] { 7, 42 });
        var r2 = build(new object?[] { 7, 42 });
        var r3 = build(new object?[] { 7, 43 });
        var r4 = build(new object?[] { 8, 42 });

        Assert.True(r1.Equals(r2));
        Assert.Equal(r1.GetHashCode(), r2.GetHashCode());
        Assert.False(r1.Equals(r3));
        Assert.False(r1.Equals(r4));
    }

    [Fact]
    public void EmittedStruct_WorksAsDictionaryKey()
    {
        // The real question: can instances be stored as keys in a
        // Dictionary<TKey, TValue>? ZSet is built on Dictionary; if this
        // works, the emitted type is plug-compatible with the Z-set pipeline.
        var fields = new[]
        {
            new FieldDef("A", typeof(int)),
            new FieldDef("B", typeof(int)),
        };
        var rowType = EmitRowStruct("EmittedRow_DictKey", fields);
        var build = BuildConstructorDelegate(rowType, fields);

        // Use a non-generic IDictionary so we don't need runtime-type gymnastics
        // to get a Dictionary<rowType, int> here.
        var dictType = typeof(Dictionary<,>).MakeGenericType(rowType, typeof(int));
        var dict = (System.Collections.IDictionary)Activator.CreateInstance(dictType)!;

        for (var i = 0; i < 1000; i++)
        {
            var key = build(new object?[] { i % 10, i % 7 });
            dict[key] = dict.Contains(key) ? ((int)dict[key]!) + 1 : 1;
        }

        // Same distinctness invariant as spike A's corresponding test.
        Assert.Equal(70, dict.Count);
    }
}
