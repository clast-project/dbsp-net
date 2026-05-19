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
/// <para><b>Phase 1.0 infrastructure</b>. Standalone — nothing in the SQL
/// compiler uses it yet; it's exercised by tests that build instances
/// directly and confirm they work as
/// <c>ZSet</c> keys, in <c>HashSet</c>, etc. Subsequent phases plumb
/// typed rows through TableInput, the operator stages, and the
/// expression compiler.</para>
/// <para><b>Layout.</b> Each emitted type is a sealed struct with
/// sequential layout. Public readonly fields <c>F0..Fn</c> mirror the
/// schema columns. <see cref="IEquatable{T}"/> is implemented to
/// avoid the reflective fallback in <see cref="ValueType.Equals(object)"/>;
/// <see cref="ValueType.GetHashCode"/> is overridden with a
/// <see cref="HashCode"/> combine over field values.</para>
/// <para><b>Scope gate.</b> Same as <see cref="EmittedEqualityCodec"/>:
/// NOT NULL columns of int, long, double, bool, string, and the project's
/// date/time/decimal value types. Schemas containing anything else
/// return a <c>null</c> emitted type — callers must keep the
/// <see cref="StructuralRow"/> fallback for those.</para>
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
            // NOT NULL primitives only in this phase. Nullable columns
            // would need each field to be Nullable<T> for value types
            // and the equality / hash IL would need to handle null
            // sentinels — straightforward but deferred.
            if (col.Type.Nullable || !IsSupportedClrType(col.Type.ClrType))
            {
                fingerprint = default;
                return false;
            }

            arr[i] = col.Type.ClrType;
        }

        fingerprint = new Fingerprint(arr);
        return true;
    }

    private static bool IsSupportedClrType(Type t) =>
        t == typeof(int) || t == typeof(long) || t == typeof(double)
        || t == typeof(bool) || t == typeof(string)
        || t == typeof(Utf8String)
        || t == typeof(Clast.DatabaseDecimal.Values.Decimal128);

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

        EmitCtor(tb, fp, fields);
        EmitTypedFieldsCtor(tb, fp, fields);
        var typedEquals = EmitTypedEquals(tb, fp, fields, iequatable);
        EmitObjectEquals(tb, typedEquals);
        EmitGetHashCode(tb, fp, fields);

        return tb.CreateType();
    }

    private static void EmitCtor(TypeBuilder tb, Fingerprint fp, FieldBuilder[] fields)
    {
        // ctor(object?[] values):
        //   for each i: this.Fi = (FieldType_i)values[i];
        var ctor = tb.DefineConstructor(
            MethodAttributes.Public | MethodAttributes.HideBySig |
            MethodAttributes.SpecialName | MethodAttributes.RTSpecialName,
            CallingConventions.Standard,
            [typeof(object?[])]);
        var il = ctor.GetILGenerator();

        for (var i = 0; i < fp.Columns.Length; i++)
        {
            il.Emit(OpCodes.Ldarg_0);              // this (managed pointer to struct)
            il.Emit(OpCodes.Ldarg_1);              // values
            il.Emit(OpCodes.Ldc_I4, i);
            il.Emit(OpCodes.Ldelem_Ref);           // values[i] (boxed object)
            EmitCastTo(il, fp.Columns[i]);
            il.Emit(OpCodes.Stfld, fields[i]);
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
        // field reads use Ldarga_S 1 + Ldfld.
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
            il.Emit(OpCodes.Ldarg_0);               // this (managed ptr to struct)
            il.Emit(OpCodes.Ldfld, fields[i]);
            il.Emit(OpCodes.Ldarga_S, (byte)1);     // &other (managed ptr to struct arg)
            il.Emit(OpCodes.Ldfld, fields[i]);
            EmitFieldEqualityCheck(il, fp.Columns[i], returnFalse);
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
