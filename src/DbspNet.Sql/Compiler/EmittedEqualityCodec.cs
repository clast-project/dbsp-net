// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
using System.Reflection.Emit;
using DbspNet.Core.Collections;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Sql.Compiler;

/// <summary>
/// Per-schema emitted-subclass codec. For each distinct schema fingerprint
/// the codec encounters, it uses <see cref="System.Reflection.Emit"/> to
/// produce a sealed subclass of <see cref="StructuralRow"/> with typed
/// fields and overridden <c>Count</c> / <c>this[int]</c>. The base class's
/// virtual <see cref="StructuralRow.Equals(StructuralRow?)"/> walks via
/// these overrides, so the comparator path avoids the boxed-object[] walk.
/// Falls back to the base <see cref="StructuralRow"/> when the schema is
/// <c>null</c> (helper-built derived rows) or contains unsupported types.
/// </summary>
public sealed class EmittedEqualityCodec : IRowCodec<StructuralRow>
{
    public static EmittedEqualityCodec Instance { get; } = new();

    private readonly ConcurrentDictionary<Fingerprint, Func<object?[], StructuralRow>?> _cache =
        new();

    private static readonly ModuleBuilder _module = CreateModule();

    private static ModuleBuilder CreateModule()
    {
        var asm = AssemblyBuilder.DefineDynamicAssembly(
            new AssemblyName("DbspNet.EmittedRows"),
            AssemblyBuilderAccess.Run);
        return asm.DefineDynamicModule("Main");
    }

    public StructuralRow BuildRow(Schema? schema, ReadOnlySpan<object?> values)
    {
        if (schema is null || !TryFingerprint(schema, out var fp))
        {
            return StructuralRow.Of(values);
        }

        var ctor = _cache.GetOrAdd(fp, static f => BuildCtorFor(f));
        if (ctor is null)
        {
            // Schema's CLR types include something we don't emit fields for.
            return StructuralRow.Of(values);
        }

        return ctor(values.ToArray());
    }

    private static bool TryFingerprint(Schema schema, out Fingerprint fingerprint)
    {
        var arr = new (Type ClrType, bool Nullable)[schema.Count];
        for (var i = 0; i < schema.Count; i++)
        {
            var col = schema[i];
            var clr = col.Type.ClrType;
            // NOT NULL only. Nullable columns fall back to base StructuralRow
            // pending a future expansion of EmitFieldEqualityCheck.
            if (col.Type.Nullable || !IsSupportedClrType(clr))
            {
                fingerprint = default!;
                return false;
            }

            arr[i] = (clr, col.Type.Nullable);
        }

        fingerprint = new Fingerprint(arr);
        return true;
    }

    private static bool IsSupportedClrType(Type clr) =>
        clr == typeof(int) || clr == typeof(long) || clr == typeof(double)
        || clr == typeof(bool) || clr == typeof(string)
        || clr == typeof(Date32) || clr == typeof(Time64) || clr == typeof(Timestamp)
        || clr == typeof(Utf8String)
        || clr == typeof(Clast.DatabaseDecimal.Values.Decimal128);

    private static Func<object?[], StructuralRow>? BuildCtorFor(Fingerprint fp)
    {
        // 1. Define type.
        var typeName = "EmittedRow_" + Guid.NewGuid().ToString("N");
        var tb = _module.DefineType(
            typeName,
            TypeAttributes.Public | TypeAttributes.Sealed,
            typeof(StructuralRow));

        var fieldTypes = new Type[fp.Columns.Length];
        var fields = new FieldBuilder[fp.Columns.Length];
        for (var i = 0; i < fp.Columns.Length; i++)
        {
            var (clrType, nullable) = fp.Columns[i];
            // Value types get Nullable<T>; reference types are nullable as-is.
            var fieldType = (nullable && clrType.IsValueType)
                ? typeof(Nullable<>).MakeGenericType(clrType)
                : clrType;
            fieldTypes[i] = fieldType;
            fields[i] = tb.DefineField(
                "F" + i, fieldType, FieldAttributes.Public | FieldAttributes.InitOnly);
        }

        EmitCtor(tb, fp, fields, fieldTypes);
        EmitCountOverride(tb, fp.Columns.Length);
        EmitIndexerOverride(tb, fp, fields, fieldTypes);
        EmitTypedEquals(tb, fp, fields, fieldTypes);

        var emittedType = tb.CreateType();

        // 2. Build a fast Func<object?[], StructuralRow> via expression trees.
        var arrParam = Expression.Parameter(typeof(object?[]), "vs");
        var ctorInfo = emittedType.GetConstructor([typeof(object?[])])
            ?? throw new InvalidOperationException("emitted type missing ctor");
        var newExpr = Expression.New(ctorInfo, arrParam);
        var lambda = Expression.Lambda<Func<object?[], StructuralRow>>(newExpr, arrParam);
        return lambda.Compile();
    }

    private static void EmitCtor(
        TypeBuilder tb,
        Fingerprint fp,
        FieldBuilder[] fields,
        Type[] fieldTypes)
    {
        // ctor(object?[] values) :
        //   base(StructuralRow.ComputeHash(values))
        //   for each i: this.F_i = (FieldType_i)values[i];
        var ctor = tb.DefineConstructor(
            MethodAttributes.Public | MethodAttributes.HideBySig |
            MethodAttributes.SpecialName | MethodAttributes.RTSpecialName,
            CallingConventions.Standard,
            [typeof(object?[])]);
        var il = ctor.GetILGenerator();

        var computeHashMethod = typeof(StructuralRow).GetMethod(
            nameof(StructuralRow.ComputeHash), [typeof(IReadOnlyList<object?>)])!;
        var baseCtor = typeof(StructuralRow).GetConstructor(
            BindingFlags.Instance | BindingFlags.NonPublic, [typeof(int)])!;

        // base(StructuralRow.ComputeHash(values))
        il.Emit(OpCodes.Ldarg_0);                       // this
        il.Emit(OpCodes.Ldarg_1);                       // values
        il.Emit(OpCodes.Call, computeHashMethod);       // hash on stack
        il.Emit(OpCodes.Call, baseCtor);                // call base(hash)

        // Field assignments: this.F_i = (FieldType_i)values[i];
        for (var i = 0; i < fp.Columns.Length; i++)
        {
            il.Emit(OpCodes.Ldarg_0);                   // this
            il.Emit(OpCodes.Ldarg_1);                   // values
            il.Emit(OpCodes.Ldc_I4, i);                 // i
            il.Emit(OpCodes.Ldelem_Ref);                // values[i] (object)
            EmitCastTo(il, fieldTypes[i]);
            il.Emit(OpCodes.Stfld, fields[i]);
        }

        il.Emit(OpCodes.Ret);
    }

    /// <summary>
    /// Cast / unbox the boxed object on the stack to <paramref name="targetType"/>
    /// and leave the result on the stack. Handles value types via unbox.any,
    /// nullable value types via unbox.any (Nullable&lt;T&gt; box semantics
    /// already collapse null and Some), and reference types via castclass.
    /// </summary>
    private static void EmitCastTo(ILGenerator il, Type targetType)
    {
        if (targetType.IsValueType)
        {
            il.Emit(OpCodes.Unbox_Any, targetType);
        }
        else
        {
            il.Emit(OpCodes.Castclass, targetType);
        }
    }

    private static void EmitCountOverride(TypeBuilder tb, int n)
    {
        // public override int Count => N;
        var get = tb.DefineMethod(
            "get_Count",
            MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.HideBySig
                | MethodAttributes.SpecialName,
            typeof(int), Type.EmptyTypes);
        var il = get.GetILGenerator();
        il.Emit(OpCodes.Ldc_I4, n);
        il.Emit(OpCodes.Ret);

        var prop = tb.DefineProperty("Count", PropertyAttributes.None, typeof(int), null);
        prop.SetGetMethod(get);

        tb.DefineMethodOverride(
            get,
            typeof(StructuralRow).GetProperty(nameof(StructuralRow.Count))!.GetGetMethod()!);
    }

    /// <summary>
    /// Emit <c>public override bool Equals(StructuralRow? other)</c> that
    /// short-circuits same-type compares to a typed field walk (no boxing,
    /// no virtual indexer dispatch). Cross-type compares fall through to
    /// the base implementation, preserving correctness when an emitted row
    /// is compared against a base <see cref="StructuralRow"/> in the same
    /// dictionary.
    /// </summary>
    private static void EmitTypedEquals(
        TypeBuilder tb,
        Fingerprint fp,
        FieldBuilder[] fields,
        Type[] fieldTypes)
    {
        var equalsMethod = tb.DefineMethod(
            nameof(StructuralRow.Equals),
            MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.HideBySig,
            typeof(bool),
            [typeof(StructuralRow)]);
        var il = equalsMethod.GetILGenerator();

        var thisType = tb;
        var localT = il.DeclareLocal(thisType);
        var callBase = il.DefineLabel();
        var returnFalse = il.DefineLabel();

        // var t = other as ThisType; if (t == null) goto call_base;
        il.Emit(OpCodes.Ldarg_1);
        il.Emit(OpCodes.Isinst, thisType);
        il.Emit(OpCodes.Stloc, localT);
        il.Emit(OpCodes.Ldloc, localT);
        il.Emit(OpCodes.Brfalse, callBase);

        // Compare each field (this.Fi vs t.Fi).
        for (var i = 0; i < fp.Columns.Length; i++)
        {
            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Ldfld, fields[i]);
            il.Emit(OpCodes.Ldloc, localT);
            il.Emit(OpCodes.Ldfld, fields[i]);
            EmitFieldEqualityCheck(il, fieldTypes[i], returnFalse);
        }

        // All fields matched.
        il.Emit(OpCodes.Ldc_I4_1);
        il.Emit(OpCodes.Ret);

        il.MarkLabel(returnFalse);
        il.Emit(OpCodes.Ldc_I4_0);
        il.Emit(OpCodes.Ret);

        il.MarkLabel(callBase);
        // Different concrete type: fall through to base.Equals(other).
        var baseEquals = typeof(StructuralRow).GetMethod(
            nameof(StructuralRow.Equals), [typeof(StructuralRow)])!;
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Ldarg_1);
        il.Emit(OpCodes.Call, baseEquals);
        il.Emit(OpCodes.Ret);

        tb.DefineMethodOverride(equalsMethod, baseEquals);
    }

    /// <summary>
    /// Stack: [this.field, other.field]. Emits a comparison that branches
    /// to <paramref name="notEqualLabel"/> if the values differ.
    /// </summary>
    private static void EmitFieldEqualityCheck(ILGenerator il, Type type, Label notEqualLabel)
    {
        if (type == typeof(int) || type == typeof(long) || type == typeof(bool)
            || type == typeof(double))
        {
            // Primitive: bne.un — works correctly for int/long/bool;
            // for double, NaN != NaN matches IEEE-754 semantics, which
            // is what the base path's Equals(double, double) also does.
            il.Emit(OpCodes.Bne_Un, notEqualLabel);
            return;
        }

        if (type == typeof(decimal))
        {
            // decimal::op_Equality(decimal, decimal) -> bool
            var op = typeof(decimal).GetMethod("op_Equality", [typeof(decimal), typeof(decimal)])!;
            il.Emit(OpCodes.Call, op);
            il.Emit(OpCodes.Brfalse, notEqualLabel);
            return;
        }

        if (type == typeof(string))
        {
            // string.Equals(string, string) handles null on either side.
            var eq = typeof(string).GetMethod(
                nameof(string.Equals), [typeof(string), typeof(string)])!;
            il.Emit(OpCodes.Call, eq);
            il.Emit(OpCodes.Brfalse, notEqualLabel);
            return;
        }

        // Generic value-type fallback: any struct that exposes a static
        // op_Equality(T, T) -> bool. Record structs (Date32, Time64,
        // Timestamp) auto-generate this; same for any user struct that
        // overloads ==.
        if (type.IsValueType)
        {
            var op = type.GetMethod("op_Equality", [type, type]);
            if (op is not null)
            {
                il.Emit(OpCodes.Call, op);
                il.Emit(OpCodes.Brfalse, notEqualLabel);
                return;
            }
        }

        // Nullable<T> / unsupported types fall through. The fingerprint
        // filter in TryFingerprint should already exclude these.
        throw new NotSupportedException($"emitted Equals not supported for {type}");
    }

    private static void EmitIndexerOverride(
        TypeBuilder tb,
        Fingerprint fp,
        FieldBuilder[] fields,
        Type[] fieldTypes)
    {
        // public override object? this[int index] {
        //   get {
        //     switch (index) {
        //       case 0: return (object?)F0;
        //       case 1: return (object?)F1;
        //       ...
        //       default: throw new IndexOutOfRangeException();
        //     }
        //   }
        // }
        var get = tb.DefineMethod(
            "get_Item",
            MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.HideBySig
                | MethodAttributes.SpecialName,
            typeof(object), [typeof(int)]);
        var il = get.GetILGenerator();

        var labels = new Label[fp.Columns.Length];
        var defaultLabel = il.DefineLabel();
        for (var i = 0; i < labels.Length; i++)
        {
            labels[i] = il.DefineLabel();
        }

        il.Emit(OpCodes.Ldarg_1);
        il.Emit(OpCodes.Switch, labels);
        il.Emit(OpCodes.Br, defaultLabel);

        for (var i = 0; i < fp.Columns.Length; i++)
        {
            il.MarkLabel(labels[i]);
            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Ldfld, fields[i]);
            // Box value types so the indexer return type (object?) is uniform.
            if (fieldTypes[i].IsValueType)
            {
                il.Emit(OpCodes.Box, fieldTypes[i]);
            }

            il.Emit(OpCodes.Ret);
        }

        il.MarkLabel(defaultLabel);
        var ctor = typeof(IndexOutOfRangeException).GetConstructor(Type.EmptyTypes)!;
        il.Emit(OpCodes.Newobj, ctor);
        il.Emit(OpCodes.Throw);

        // PropertyBuilder for the indexer ("Item").
        var prop = tb.DefineProperty(
            "Item", PropertyAttributes.None, typeof(object), [typeof(int)]);
        prop.SetGetMethod(get);

        tb.DefineMethodOverride(
            get,
            typeof(StructuralRow).GetProperty("Item")!.GetGetMethod()!);
    }

    /// <summary>
    /// Schema fingerprint by structural per-column (CLR type, nullable).
    /// Two schemas with identical fingerprints share the same emitted type.
    /// </summary>
    private readonly struct Fingerprint : IEquatable<Fingerprint>
    {
        public readonly (Type ClrType, bool Nullable)[] Columns;
        private readonly int _hash;

        public Fingerprint((Type, bool)[] columns)
        {
            Columns = columns;
            var hc = default(HashCode);
            hc.Add(columns.Length);
            foreach (var (t, n) in columns)
            {
                hc.Add(t);
                hc.Add(n);
            }

            _hash = hc.ToHashCode();
        }

        public bool Equals(Fingerprint other)
        {
            if (_hash != other._hash || Columns.Length != other.Columns.Length)
            {
                return false;
            }

            for (var i = 0; i < Columns.Length; i++)
            {
                if (Columns[i] != other.Columns[i])
                {
                    return false;
                }
            }

            return true;
        }

        public override bool Equals(object? obj) => obj is Fingerprint f && Equals(f);

        public override int GetHashCode() => _hash;
    }
}
