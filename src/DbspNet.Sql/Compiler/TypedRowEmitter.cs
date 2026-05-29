// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
using System.Reflection.Emit;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Sql.Compiler;

/// <summary>
/// Emits — and caches — a sealed value type with typed fields for every
/// distinct schema fingerprint the SQL compiler encounters. The emitted
/// type is the per-schema row representation the typed-row pipeline
/// (Phase 1+) will pass through Core operators in place of
/// <see cref="DbspNet.Core.Collections.StructuralRow"/>.
/// </summary>
/// <remarks>
/// <para><b>Layout.</b> Each emitted type is a sealed struct with
/// sequential layout. Public readonly fields <c>F0..Fn</c> mirror the
/// schema columns. Nullable value-type columns become
/// <c>Nullable&lt;T&gt;</c> fields; nullable reference-type columns
/// stay as the reference type (naturally nullable).
/// <see cref="IEquatable{T}"/> is implemented to avoid the reflective
/// fallback in <see cref="ValueType.Equals(object)"/>;
/// <see cref="ValueType.GetHashCode"/> is overridden with a
/// <see cref="HashCode"/> combine over field values.
/// <see cref="IComparable{T}"/> + non-generic <see cref="IComparable"/>
/// are implemented lexicographically (left-to-right field compare via
/// <c>Comparer&lt;T&gt;.Default</c>) so the spine trace family
/// (<c>SpineZSetTrace</c> / <c>SpineIndexedZSetTrace</c>), which keeps
/// keys in sorted batches, can use <c>Comparer&lt;TRow&gt;.Default</c>
/// without an externally-supplied comparer. Order is consistent with
/// <c>Equals</c>: <c>a.CompareTo(b) == 0</c> iff <c>a.Equals(b)</c>.</para>
/// <para><b>Scope gate.</b> Columns of int, long, double, bool,
/// string, and the project's date/time/decimal/Utf8 value types.
/// Both NOT NULL and nullable variants of those types are supported;
/// other types return a <c>null</c> emitted type and callers fall
/// back to <see cref="StructuralRow"/>.</para>
/// </remarks>
public static class TypedRowEmitter
{
    private static readonly ConcurrentDictionary<Fingerprint, Type?> _cache = new();
    private static readonly ModuleBuilder _module = CreateModule();

    private static ModuleBuilder CreateModule()
    {
        var asm = AssemblyBuilder.DefineDynamicAssembly(
            new AssemblyName("DbspNet.TypedRows"),
            AssemblyBuilderAccess.Run);
        return asm.DefineDynamicModule("Main");
    }

    /// <summary>
    /// Returns the emitted typed-row <see cref="Type"/> for
    /// <paramref name="schema"/>, or <c>null</c> if the schema falls
    /// outside the supported subset (see remarks on the scope gate).
    /// Cached; identical schemas share a type.
    /// </summary>
    public static Type? EmitRowType(Schema schema)
    {
        ArgumentNullException.ThrowIfNull(schema);
        if (!TryFingerprint(schema, out var fp))
        {
            return null;
        }

        return _cache.GetOrAdd(fp, static f => BuildTypeFor(f));
    }

    /// <summary>True iff <paramref name="schema"/> can be lowered to an emitted typed row.</summary>
    public static bool IsSupported(Schema schema) => TryFingerprint(schema, out _);

    /// <summary>
    /// Returns a factory that boxes a new instance of the emitted type
    /// from a column-value array, or <c>null</c> if
    /// <paramref name="schema"/> isn't supported. The factory boxes
    /// because most call sites today are non-generic — typed-pipeline
    /// hot paths in later phases will use the closed type directly.
    /// </summary>
    public static Func<object?[], object>? BuildBoxedFactory(Schema schema)
    {
        var type = EmitRowType(schema);
        if (type is null)
        {
            return null;
        }

        var ctor = type.GetConstructor([typeof(object?[])])
            ?? throw new InvalidOperationException("emitted typed row missing object?[] ctor");
        var arrParam = Expression.Parameter(typeof(object?[]), "vs");
        var newExpr = Expression.New(ctor, arrParam);
        var boxed = Expression.Convert(newExpr, typeof(object));
        return Expression.Lambda<Func<object?[], object>>(boxed, arrParam).Compile();
    }

    /// <summary>
    /// Returns per-column field accessors that read a column out of a
    /// boxed emitted-row instance, or <c>null</c> if
    /// <paramref name="schema"/> isn't supported. Convenience for tests
    /// and any non-generic introspection; the typed pipeline will read
    /// fields directly off the closed struct.
    /// </summary>
    public static Func<object, object?>[]? BuildFieldGetters(Schema schema)
    {
        var type = EmitRowType(schema);
        if (type is null)
        {
            return null;
        }

        var result = new Func<object, object?>[schema.Count];
        for (var i = 0; i < schema.Count; i++)
        {
            var field = type.GetField("F" + i)
                ?? throw new InvalidOperationException("emitted typed row missing F" + i);
            var objParam = Expression.Parameter(typeof(object), "o");
            var unbox = Expression.Convert(objParam, type);
            var fieldExpr = Expression.Field(unbox, field);
            var boxed = Expression.Convert(fieldExpr, typeof(object));
            result[i] = Expression.Lambda<Func<object, object?>>(boxed, objParam).Compile();
        }

        return result;
    }

    private static bool TryFingerprint(Schema schema, out Fingerprint fingerprint)
    {
        var arr = new Type[schema.Count];
        for (var i = 0; i < schema.Count; i++)
        {
            var col = schema[i];
            if (!IsSupportedClrType(col.Type.ClrType))
            {
                fingerprint = default;
                return false;
            }

            arr[i] = FieldTypeFor(col);
        }

        fingerprint = new Fingerprint(arr);
        return true;
    }

    /// <summary>
    /// Derives the field's CLR type from the schema column. Nullable
    /// value-type columns become <see cref="Nullable{T}"/> fields;
    /// reference-type columns stay as the reference type (naturally
    /// nullable). Non-nullable value types keep their raw type.
    /// </summary>
    private static Type FieldTypeFor(SchemaColumn col)
    {
        var t = col.Type.ClrType;
        if (!col.Type.Nullable) return t;
        if (!t.IsValueType) return t;
        return typeof(Nullable<>).MakeGenericType(t);
    }

    private static bool IsSupportedClrType(Type t) =>
        t == typeof(int) || t == typeof(long) || t == typeof(double)
        || t == typeof(bool) || t == typeof(string)
        || t == typeof(Utf8String)
        || t == typeof(Clast.DatabaseDecimal.Values.Decimal128)
        || t == typeof(Date32) || t == typeof(Time64) || t == typeof(Timestamp);

    /// <summary>Returns the underlying <c>T</c> for a <c>Nullable&lt;T&gt;</c>, or null if the type isn't nullable.</summary>
    private static Type? UnderlyingNullable(Type t) =>
        t.IsGenericType && t.GetGenericTypeDefinition() == typeof(Nullable<>) ? t.GenericTypeArguments[0] : null;

    private static Type BuildTypeFor(Fingerprint fp)
    {
        var typeName = "TypedRow_" + Guid.NewGuid().ToString("N");
        var tb = _module.DefineType(
            typeName,
            TypeAttributes.Public | TypeAttributes.Sealed
                | TypeAttributes.SequentialLayout | TypeAttributes.BeforeFieldInit,
            typeof(ValueType));

        var fields = new FieldBuilder[fp.Columns.Length];
        for (var i = 0; i < fp.Columns.Length; i++)
        {
            fields[i] = tb.DefineField(
                "F" + i, fp.Columns[i], FieldAttributes.Public | FieldAttributes.InitOnly);
        }

        // Implement IEquatable<TSelf>. Pass the TypeBuilder as the type
        // argument — it's a Type and can close the generic interface
        // before the type is finalised.
        var iequatable = typeof(IEquatable<>).MakeGenericType(tb);
        tb.AddInterfaceImplementation(iequatable);

        // IComparable<TSelf> + non-generic IComparable. Comparer<T>.Default
        // for the emitted struct devirtualises to the typed CompareTo, so
        // the spine path can use it directly with no externally-supplied
        // IComparer<TKey>.
        var icomparable = typeof(IComparable<>).MakeGenericType(tb);
        tb.AddInterfaceImplementation(icomparable);
        tb.AddInterfaceImplementation(typeof(IComparable));

        EmitCtor(tb, fp, fields);
        EmitTypedFieldsCtor(tb, fp, fields);
        var typedEquals = EmitTypedEquals(tb, fp, fields, iequatable);
        EmitObjectEquals(tb, typedEquals);
        EmitGetHashCode(tb, fp, fields);
        var typedCompareTo = EmitTypedCompareTo(tb, fp, fields, icomparable);
        EmitObjectCompareTo(tb, typedCompareTo);

        return tb.CreateType();
    }

    private static void EmitCtor(TypeBuilder tb, Fingerprint fp, FieldBuilder[] fields)
    {
        // ctor(object?[] values):
        //   for each i: this.Fi = (FieldType_i)values[i];
        // For Nullable<T> fields, a null array entry stores
        // default(Nullable<T>) (HasValue=false); a non-null entry is
        // unboxed to T and wrapped via the Nullable<T>(T) ctor.
        var ctor = tb.DefineConstructor(
            MethodAttributes.Public | MethodAttributes.HideBySig |
            MethodAttributes.SpecialName | MethodAttributes.RTSpecialName,
            CallingConventions.Standard,
            [typeof(object?[])]);
        var il = ctor.GetILGenerator();

        for (var i = 0; i < fp.Columns.Length; i++)
        {
            var fieldType = fp.Columns[i];
            var underlying = UnderlyingNullable(fieldType);

            if (underlying is not null)
            {
                // Nullable<T> field — branch on values[i] == null.
                var storeNull = il.DefineLabel();
                var afterStore = il.DefineLabel();
                var nullableCtor = fieldType.GetConstructor([underlying])!;
                var local = il.DeclareLocal(fieldType);

                il.Emit(OpCodes.Ldarg_1);              // values
                il.Emit(OpCodes.Ldc_I4, i);
                il.Emit(OpCodes.Ldelem_Ref);
                il.Emit(OpCodes.Dup);                  // [boxed, boxed]
                il.Emit(OpCodes.Brfalse_S, storeNull); // if (boxed == null) goto storeNull

                // Non-null path: unbox to T, wrap via Nullable<T> ctor.
                il.Emit(OpCodes.Unbox_Any, underlying);
                il.Emit(OpCodes.Newobj, nullableCtor);
                il.Emit(OpCodes.Stloc, local);
                il.Emit(OpCodes.Br_S, afterStore);

                il.MarkLabel(storeNull);
                il.Emit(OpCodes.Pop);                  // pop the boxed null
                il.Emit(OpCodes.Ldloca_S, (byte)local.LocalIndex);
                il.Emit(OpCodes.Initobj, fieldType);   // local = default(Nullable<T>)

                il.MarkLabel(afterStore);
                il.Emit(OpCodes.Ldarg_0);              // this
                il.Emit(OpCodes.Ldloc, local);
                il.Emit(OpCodes.Stfld, fields[i]);
            }
            else
            {
                // Non-nullable field — direct cast/unbox path.
                il.Emit(OpCodes.Ldarg_0);              // this (managed pointer to struct)
                il.Emit(OpCodes.Ldarg_1);              // values
                il.Emit(OpCodes.Ldc_I4, i);
                il.Emit(OpCodes.Ldelem_Ref);           // values[i] (boxed object)
                EmitCastTo(il, fieldType);
                il.Emit(OpCodes.Stfld, fields[i]);
            }
        }

        il.Emit(OpCodes.Ret);
    }

    /// <summary>
    /// Emits <c>ctor(T0, T1, ..., Tn)</c> taking typed args in field-
    /// declaration order. Used by the typed pipeline (Project, Map) to
    /// build an output row from individual typed values without going
    /// through the <c>object?[]</c> boxing path.
    /// </summary>
    private static void EmitTypedFieldsCtor(TypeBuilder tb, Fingerprint fp, FieldBuilder[] fields)
    {
        var ctor = tb.DefineConstructor(
            MethodAttributes.Public | MethodAttributes.HideBySig |
            MethodAttributes.SpecialName | MethodAttributes.RTSpecialName,
            CallingConventions.Standard,
            fp.Columns);
        var il = ctor.GetILGenerator();

        for (var i = 0; i < fp.Columns.Length; i++)
        {
            il.Emit(OpCodes.Ldarg_0);                    // this (managed ptr)
            il.Emit(OpCodes.Ldarg, (short)(i + 1));      // arg i (Ti)
            il.Emit(OpCodes.Stfld, fields[i]);
        }

        il.Emit(OpCodes.Ret);
    }

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

    private static MethodBuilder EmitTypedEquals(
        TypeBuilder tb, Fingerprint fp, FieldBuilder[] fields, Type iequatable)
    {
        // bool Equals(TSelf other) — typed field-by-field comparison.
        // 'other' arrives as a struct value (not a managed pointer) so
        // field reads use Ldarga_S 1 + Ldfld. Nullable<T> fields go
        // through EmitNullableFieldEqualityCheck (HasValue + Value).
        var method = tb.DefineMethod(
            "Equals",
            MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.HideBySig
                | MethodAttributes.Final | MethodAttributes.NewSlot,
            typeof(bool),
            [tb]);
        var il = method.GetILGenerator();

        var returnFalse = il.DefineLabel();
        for (var i = 0; i < fp.Columns.Length; i++)
        {
            var fieldType = fp.Columns[i];
            if (UnderlyingNullable(fieldType) is { } underlying)
            {
                EmitNullableFieldEqualityCheck(il, fields[i], fieldType, underlying, returnFalse);
                continue;
            }

            il.Emit(OpCodes.Ldarg_0);               // this (managed ptr to struct)
            il.Emit(OpCodes.Ldfld, fields[i]);
            il.Emit(OpCodes.Ldarga_S, (byte)1);     // &other (managed ptr to struct arg)
            il.Emit(OpCodes.Ldfld, fields[i]);
            EmitFieldEqualityCheck(il, fieldType, returnFalse);
        }

        il.Emit(OpCodes.Ldc_I4_1);
        il.Emit(OpCodes.Ret);

        il.MarkLabel(returnFalse);
        il.Emit(OpCodes.Ldc_I4_0);
        il.Emit(OpCodes.Ret);

        // Wire as IEquatable<TSelf>.Equals. The closed generic
        // interface is over the TypeBuilder so reflection on it must go
        // through TypeBuilder.GetMethod to resolve the slot.
        var openIeqEquals = typeof(IEquatable<>).GetMethod(nameof(IEquatable<object>.Equals))!;
        var closedIeqEquals = TypeBuilder.GetMethod(iequatable, openIeqEquals);
        tb.DefineMethodOverride(method, closedIeqEquals);

        return method;
    }

    /// <summary>
    /// Emits a HasValue + Value equality check for a Nullable&lt;T&gt;
    /// field. Branches to <paramref name="notEqual"/> if this.F differs
    /// from other.F under Nullable equality semantics:
    ///   HasValue must match, AND if both have values they must compare equal.
    /// </summary>
    private static void EmitNullableFieldEqualityCheck(
        ILGenerator il, FieldInfo field, Type fieldType, Type underlying, Label notEqual)
    {
        var hasValueGet = fieldType.GetProperty(nameof(Nullable<int>.HasValue))!.GetGetMethod()!;
        var valueGet = fieldType.GetProperty(nameof(Nullable<int>.Value))!.GetGetMethod()!;

        // Stash other.F into a local so we can take its address.
        var otherLocal = il.DeclareLocal(fieldType);
        il.Emit(OpCodes.Ldarga_S, (byte)1);
        il.Emit(OpCodes.Ldfld, field);
        il.Emit(OpCodes.Stloc, otherLocal);

        // First gate: HasValue must agree.
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Ldflda, field);
        il.Emit(OpCodes.Call, hasValueGet);
        il.Emit(OpCodes.Ldloca_S, (byte)otherLocal.LocalIndex);
        il.Emit(OpCodes.Call, hasValueGet);
        il.Emit(OpCodes.Ceq);
        il.Emit(OpCodes.Brfalse, notEqual);

        // HasValue is equal. If both false, skip value compare.
        var afterValueCompare = il.DefineLabel();
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Ldflda, field);
        il.Emit(OpCodes.Call, hasValueGet);
        il.Emit(OpCodes.Brfalse, afterValueCompare);

        // Both have values — compare the unwrapped underlying type.
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Ldflda, field);
        il.Emit(OpCodes.Call, valueGet);
        il.Emit(OpCodes.Ldloca_S, (byte)otherLocal.LocalIndex);
        il.Emit(OpCodes.Call, valueGet);
        EmitFieldEqualityCheck(il, underlying, notEqual);

        il.MarkLabel(afterValueCompare);
    }

    /// <summary>Stack: [a, b]. Branches to <paramref name="notEqual"/> if a != b.</summary>
    private static void EmitFieldEqualityCheck(ILGenerator il, Type type, Label notEqual)
    {
        if (type == typeof(int) || type == typeof(long) || type == typeof(bool)
            || type == typeof(double))
        {
            il.Emit(OpCodes.Bne_Un, notEqual);
            return;
        }

        if (type == typeof(string))
        {
            var eq = typeof(string).GetMethod(
                nameof(string.Equals), [typeof(string), typeof(string)])!;
            il.Emit(OpCodes.Call, eq);
            il.Emit(OpCodes.Brfalse, notEqual);
            return;
        }

        // Value-type fallback: types that expose a static
        // op_Equality(T, T) -> bool (e.g. Utf8String, Date32, Time64,
        // Timestamp, Decimal128 — any record struct generates one).
        if (type.IsValueType)
        {
            var op = type.GetMethod("op_Equality", [type, type]);
            if (op is not null)
            {
                il.Emit(OpCodes.Call, op);
                il.Emit(OpCodes.Brfalse, notEqual);
                return;
            }
        }

        throw new NotSupportedException($"typed-row field equality not implemented for {type}");
    }

    private static void EmitObjectEquals(TypeBuilder tb, MethodBuilder typedEquals)
    {
        // public override bool Equals(object? obj) =>
        //   obj is TSelf other && this.Equals(other);
        var method = tb.DefineMethod(
            nameof(object.Equals),
            MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.HideBySig,
            typeof(bool),
            [typeof(object)]);
        var il = method.GetILGenerator();

        var returnFalse = il.DefineLabel();

        // if (!(obj is TSelf)) return false;
        il.Emit(OpCodes.Ldarg_1);
        il.Emit(OpCodes.Isinst, tb);
        il.Emit(OpCodes.Brfalse, returnFalse);

        // (TSelf)obj — unbox to managed pointer, then load value
        il.Emit(OpCodes.Ldarg_0);                   // this (ptr)
        il.Emit(OpCodes.Ldarg_1);                   // obj
        il.Emit(OpCodes.Unbox_Any, tb);             // TSelf on stack
        il.Emit(OpCodes.Call, typedEquals);         // this.Equals(other) — non-virtual on struct
        il.Emit(OpCodes.Ret);

        il.MarkLabel(returnFalse);
        il.Emit(OpCodes.Ldc_I4_0);
        il.Emit(OpCodes.Ret);
    }

    private static void EmitGetHashCode(TypeBuilder tb, Fingerprint fp, FieldBuilder[] fields)
    {
        // public override int GetHashCode():
        //   var hc = new HashCode();
        //   hc.Add(F0); hc.Add(F1); ...
        //   return hc.ToHashCode();
        var method = tb.DefineMethod(
            nameof(object.GetHashCode),
            MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.HideBySig,
            typeof(int),
            Type.EmptyTypes);
        var il = method.GetILGenerator();

        var hcLocal = il.DeclareLocal(typeof(HashCode));
        il.Emit(OpCodes.Ldloca_S, (byte)hcLocal.LocalIndex);
        il.Emit(OpCodes.Initobj, typeof(HashCode));

        for (var i = 0; i < fp.Columns.Length; i++)
        {
            il.Emit(OpCodes.Ldloca_S, (byte)hcLocal.LocalIndex);
            il.Emit(OpCodes.Ldarg_0);                  // this (ptr)
            il.Emit(OpCodes.Ldfld, fields[i]);
            var addGeneric = typeof(HashCode).GetMethods()
                .Single(m => m.Name == nameof(HashCode.Add) && m.IsGenericMethodDefinition && m.GetParameters().Length == 1);
            var addClosed = addGeneric.MakeGenericMethod(fp.Columns[i]);
            il.Emit(OpCodes.Call, addClosed);
        }

        il.Emit(OpCodes.Ldloca_S, (byte)hcLocal.LocalIndex);
        var toHashCode = typeof(HashCode).GetMethod(nameof(HashCode.ToHashCode))!;
        il.Emit(OpCodes.Call, toHashCode);
        il.Emit(OpCodes.Ret);
    }

    /// <summary>
    /// Emits <c>int CompareTo(TSelf other)</c> wired as the
    /// <see cref="IComparable{T}"/> slot for <c>TSelf</c>. Body is a
    /// lexicographic left-to-right field compare via
    /// <see cref="Comparer{T}.Default"/> — the first non-zero result
    /// returns, all-zero falls through to <c>0</c>.
    ///
    /// <para><c>Comparer&lt;T&gt;.Default</c> handles all our field
    /// types: primitives compare directly; <see cref="Nullable{T}"/>
    /// puts <c>null</c> before any non-null value and falls through to
    /// <c>T</c>'s default compare otherwise; <see cref="Utf8String"/>,
    /// <see cref="Clast.DatabaseDecimal.Values.Decimal128"/>, and the
    /// temporal types (<see cref="Date32"/>, <see cref="Time64"/>,
    /// <see cref="Timestamp"/>) all implement
    /// <c>IComparable&lt;T&gt;</c>. The resulting order is consistent
    /// with the emitted <see cref="IEquatable{T}.Equals(T)"/> — both
    /// reduce to per-field <c>Equals</c>/<c>CompareTo == 0</c>.</para>
    /// </summary>
    private static MethodBuilder EmitTypedCompareTo(
        TypeBuilder tb, Fingerprint fp, FieldBuilder[] fields, Type icomparable)
    {
        var method = tb.DefineMethod(
            nameof(IComparable<object>.CompareTo),
            MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.HideBySig
                | MethodAttributes.Final | MethodAttributes.NewSlot,
            typeof(int),
            [tb]);
        var il = method.GetILGenerator();

        var cLocal = il.DeclareLocal(typeof(int));
        var returnC = il.DefineLabel();

        for (var i = 0; i < fp.Columns.Length; i++)
        {
            var fieldType = fp.Columns[i];
            var comparerType = typeof(Comparer<>).MakeGenericType(fieldType);
            var defaultGet = comparerType
                .GetProperty(nameof(Comparer<int>.Default))!.GetGetMethod()!;
            var compareMethod = comparerType
                .GetMethod(nameof(Comparer<int>.Compare), [fieldType, fieldType])!;

            // c = Comparer<T>.Default.Compare(this.Fi, other.Fi);
            il.Emit(OpCodes.Call, defaultGet);
            il.Emit(OpCodes.Ldarg_0);                  // this (managed ptr)
            il.Emit(OpCodes.Ldfld, fields[i]);
            il.Emit(OpCodes.Ldarga_S, (byte)1);        // &other
            il.Emit(OpCodes.Ldfld, fields[i]);
            il.Emit(OpCodes.Callvirt, compareMethod);
            il.Emit(OpCodes.Stloc, cLocal);
            il.Emit(OpCodes.Ldloc, cLocal);
            il.Emit(OpCodes.Brtrue, returnC);          // if (c != 0) goto returnC
        }

        il.Emit(OpCodes.Ldc_I4_0);
        il.Emit(OpCodes.Ret);

        il.MarkLabel(returnC);
        il.Emit(OpCodes.Ldloc, cLocal);
        il.Emit(OpCodes.Ret);

        // Bind as IComparable<TSelf>.CompareTo — TypeBuilder.GetMethod
        // resolves the slot against the closed generic interface.
        var openIcmpCompareTo = typeof(IComparable<>)
            .GetMethod(nameof(IComparable<object>.CompareTo))!;
        var closedIcmpCompareTo = TypeBuilder.GetMethod(icomparable, openIcmpCompareTo);
        tb.DefineMethodOverride(method, closedIcmpCompareTo);

        return method;
    }

    /// <summary>
    /// Emits the explicit non-generic
    /// <see cref="IComparable.CompareTo"/> implementation. Throws
    /// <see cref="ArgumentException"/> on a non-<c>TSelf</c> non-null
    /// argument (matching <see cref="ValueType.CompareTo(object)"/>'s
    /// contract); returns <c>1</c> on <c>null</c> (so a value sorts
    /// after a null reference, which never occurs in practice but
    /// satisfies the interface).
    /// </summary>
    private static void EmitObjectCompareTo(TypeBuilder tb, MethodBuilder typedCompareTo)
    {
        var method = tb.DefineMethod(
            nameof(IComparable.CompareTo),
            MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.HideBySig
                | MethodAttributes.Final | MethodAttributes.NewSlot,
            typeof(int),
            [typeof(object)]);
        var il = method.GetILGenerator();

        var notNull = il.DefineLabel();
        var throwLabel = il.DefineLabel();

        // if (obj is null) return 1;
        il.Emit(OpCodes.Ldarg_1);
        il.Emit(OpCodes.Brtrue, notNull);
        il.Emit(OpCodes.Ldc_I4_1);
        il.Emit(OpCodes.Ret);

        il.MarkLabel(notNull);
        // if (!(obj is TSelf)) throw new ArgumentException(...);
        il.Emit(OpCodes.Ldarg_1);
        il.Emit(OpCodes.Isinst, tb);
        il.Emit(OpCodes.Brfalse, throwLabel);

        // return this.CompareTo((TSelf)obj);
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Ldarg_1);
        il.Emit(OpCodes.Unbox_Any, tb);
        il.Emit(OpCodes.Call, typedCompareTo);
        il.Emit(OpCodes.Ret);

        il.MarkLabel(throwLabel);
        var argExCtor = typeof(ArgumentException).GetConstructor([typeof(string)])!;
        il.Emit(OpCodes.Ldstr, "Argument is not a " + tb.Name);
        il.Emit(OpCodes.Newobj, argExCtor);
        il.Emit(OpCodes.Throw);

        tb.DefineMethodOverride(method,
            typeof(IComparable).GetMethod(nameof(IComparable.CompareTo))!);
    }

    /// <summary>
    /// Schema fingerprint by per-column CLR type. Two schemas with the
    /// same fingerprint share the same emitted type — column names don't
    /// matter to the layout.
    /// </summary>
    private readonly struct Fingerprint : IEquatable<Fingerprint>
    {
        public readonly Type[] Columns;
        private readonly int _hash;

        public Fingerprint(Type[] columns)
        {
            Columns = columns;
            var hc = default(HashCode);
            hc.Add(columns.Length);
            foreach (var t in columns)
            {
                hc.Add(t);
            }

            _hash = hc.ToHashCode();
        }

        public bool Equals(Fingerprint other)
        {
            if (Columns.Length != other.Columns.Length)
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
