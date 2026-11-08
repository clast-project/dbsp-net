// Retained state by operator kind and by state structure (docs/design-flat-trace-state.md, phase 1).
// Like HeapCensus, a reflection walk of the live graph (gcdump on .NET 10 drops objects past ~10M),
// but every object is charged to the operator — and the operator field, two levels deep — that
// first reaches it, walking replicas and operators in registration order. Per bucket:
//   objects, bytes        as the GC sizes them
//   refs                  non-null reference slots in those objects: the edges a full mark walks
//   ref_free_bytes        bytes in objects whose type holds no references (byte[], long[], string…)
//   new_*                 objects absent from a baseline walk (e.g. taken right after restore): what
//                         the batch since then created and kept — the bytes young GCs promote — with
//                         their generation at the time of this walk
// Run from IvmMultiBatch with IVM_STATE_CENSUS=1. Slow and memory-hungry; census only.
using System.Globalization;
using System.Reflection;
using System.Runtime.CompilerServices;
using DbspNet.Core.Circuit;

namespace DbspNet.Tests.Scratch;

internal static class StateCensus
{
    private static readonly Func<object, nuint> RawSize =
        typeof(RuntimeHelpers).GetMethod("GetRawObjectDataSize", BindingFlags.NonPublic | BindingFlags.Static)!
            .CreateDelegate<Func<object, nuint>>();

    private static readonly Dictionary<Type, FieldInfo[]> RefFields = new();
    private static readonly Dictionary<Type, bool> ContainsRefs = new();

    private static long SizeOf(object o) => Math.Max(24, ((long)RawSize(o) + 16 + 7) & ~7L);

    public sealed class Bucket
    {
        public required int Op;
        public required string Kind;
        public required string Path;
        public long Objects, Bytes, Refs, RefFreeBytes, LohBytes, NewObjects, NewBytes;
        public readonly long[] NewBytesByGen = new long[3];
        public readonly Dictionary<Type, (long Objects, long Bytes, long Refs)> Types = new();
    }

    public sealed class Result
    {
        public required List<Bucket> Buckets;
        public required HashSet<object> Seen;
        public TimeSpan Took;
    }

    /// <summary>Walk the replicas' operators, then whatever else <paramref name="runner"/> holds.</summary>
    public static Result Walk(IReadOnlyList<RootCircuit> replicas, object runner, HashSet<object>? baseline, int depth)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var w = new Walker(baseline);
        foreach (var r in replicas)
        {
            w.Stop.Add(r);
            foreach (var op in r.Operators)
            {
                w.Stop.Add(op);
            }
        }

        // Pass 1: state. Streams (the current tick's deltas) are deferred to pass 2.
        var streams = new List<(int Op, string Kind, object Stream)>();
        foreach (var r in replicas)
        {
            for (var i = 0; i < r.Operators.Count; i++)
            {
                var op = r.Operators[i];
                var kind = op is IIntrospectable m ? m.MetricName : Short(op.GetType());
                w.Charge(op, w.BucketFor(i, kind, "(op)"));
                foreach (var f in FieldsOf(op.GetType()))
                {
                    var v = f.GetValue(op);
                    if (v is null)
                    {
                        continue;
                    }

                    w.Edge(w.BucketFor(i, kind, "(op)"));
                    if (IsStream(v.GetType()))
                    {
                        streams.Add((i, kind, v));
                        continue;
                    }

                    w.Labelled(v, i, kind, f.Name + ":" + Short(v.GetType()), depth - 1);
                }
            }
        }

        // Pass 2: the stream objects only. Entering their deltas would put every in-flight row in Seen
        // (hundreds of millions of objects mid-tick: the walk ran out of memory); see Streams instead.
        foreach (var (i, kind, s) in streams)
        {
            w.Labelled(s, i, kind, "(streams)", 0);
        }

        // Pass 3: the rest of the runner (connectors, output views, exchange), by runner field.
        w.Labelled(runner, -1, "(runner)", Short(runner.GetType()), depth);

        // Pass 4: closures. Delegates met above were counted but not entered; what only their targets
        // reach is charged to the op (or path's op) that held the delegate.
        var closures = 0;
        for (; closures < w.Delegates.Count; closures++)
        {
            var (d, b) = w.Delegates[closures];
            if (d.Target is { } target)
            {
                w.Contents(target, w.BucketFor(b.Op, b.Kind, "(closures)"));
            }
        }

        // Pass 5: static fields of the engine's assemblies (non-generic types), by type and field.
        foreach (var asm in AppDomain.CurrentDomain.GetAssemblies().Where(a => a.GetName().Name?.StartsWith("DbspNet", StringComparison.Ordinal) == true))
        {
            Type[] types;
            try
            {
                types = asm.GetTypes();
            }
            catch (ReflectionTypeLoadException e)
            {
                types = e.Types.Where(t => t is not null).ToArray()!;
            }

            foreach (var t in types.Where(t => !t.ContainsGenericParameters))
            {
                foreach (var f in t.GetFields(BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.DeclaredOnly))
                {
                    if (f.IsLiteral || !HasRefs(f.FieldType))
                    {
                        continue;
                    }

                    object? v;
                    try
                    {
                        v = f.GetValue(null);
                    }
                    catch
                    {
                        continue;
                    }

                    if (v is not null)
                    {
                        w.Contents(v, w.BucketFor(-1, "(statics)", Short(t) + "." + f.Name));
                    }
                }
            }
        }

        // Closures only statics reached.
        for (; closures < w.Delegates.Count; closures++)
        {
            var (d, b) = w.Delegates[closures];
            if (d.Target is { } target)
            {
                w.Contents(target, w.BucketFor(b.Op, b.Kind, "(closures)"));
            }
        }
        return new Result { Buckets = w.Buckets.Values.ToList(), Seen = w.Seen, Took = sw.Elapsed };
    }

    /// <summary>
    /// In-flight deltas, estimated: each distinct stream's current value (attributed to the first op holding
    /// it), its entry count, and the deep size of up to <paramref name="sample"/> entries extrapolated to the
    /// count (rows shared by two streams count in both). Block-backed values are counted, not sampled.
    /// Summed over replicas per op and field.
    /// </summary>
    public static IEnumerable<string> Streams(string tag, IReadOnlyList<RootCircuit> replicas, int sample = 256, int top = 30)
    {
        var seen = new HashSet<object>(ReferenceEqualityComparer.Instance);
        var rows = new Dictionary<(int Op, string Kind, string Field), (long Count, double Bytes, int External, int Streams)>();
        foreach (var r in replicas)
        {
            for (var i = 0; i < r.Operators.Count; i++)
            {
                var op = r.Operators[i];
                var kind = op is IIntrospectable m ? m.MetricName : Short(op.GetType());
                foreach (var f in FieldsOf(op.GetType()))
                {
                    if (f.GetValue(op) is not { } stream || !IsStream(stream.GetType()) || !seen.Add(stream))
                    {
                        continue;
                    }

                    var current = stream.GetType().GetField("Current", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(stream);
                    if (current is null)
                    {
                        continue;
                    }

                    var t = current.GetType();
                    var external = t.GetProperty("External", BindingFlags.Instance | BindingFlags.NonPublic)?.GetValue(current) is not null;
                    var count = t.GetProperty("Count")?.GetValue(current) is int c ? c
                        : t.GetProperty("GroupCount")?.GetValue(current) is int g ? g : 0;
                    double bytes = 0;
                    if (!external && count > 0 && current is System.Collections.IEnumerable items)
                    {
                        var w = new Walker(null);
                        var b = w.BucketFor(i, kind, f.Name);
                        var n = 0;
                        foreach (var item in items)
                        {
                            if (item is not null)
                            {
                                w.Contents(item, b);
                            }

                            if (++n >= sample)
                            {
                                break;
                            }
                        }

                        bytes = n == 0 ? 0 : (double)b.Bytes / n * count;
                    }

                    var key = (i, kind, f.Name);
                    rows.TryGetValue(key, out var e);
                    rows[key] = (e.Count + count, e.Bytes + bytes, e.External + (external ? 1 : 0), e.Streams + 1);
                }
            }
        }

        yield return string.Create(CultureInfo.InvariantCulture,
            $"STREAMS {tag} total streams={seen.Count} entries={rows.Values.Sum(v => v.Count)} est_mib={rows.Values.Sum(v => v.Bytes) / 1048576:F0} external={rows.Values.Sum(v => v.External)}");
        foreach (var ((op, kind, field), v) in rows.OrderByDescending(kv => kv.Value.Bytes).Take(top))
        {
            yield return string.Create(CultureInfo.InvariantCulture,
                $"STREAMS {tag} op={op} {kind} {field} entries={v.Count} est_mib={v.Bytes / 1048576:F0} external={v.External}/{v.Streams}");
        }
    }

    public static IEnumerable<string> Report(string tag, Result res, int top = 40)
    {
        var b = res.Buckets;
        long Sum(Func<Bucket, long> f) => b.Sum(f);
        yield return string.Create(
            CultureInfo.InvariantCulture,
            $"STATE {tag} total objects={Sum(x => x.Objects)} bytes={Sum(x => x.Bytes)} refs={Sum(x => x.Refs)} ref_free_bytes={Sum(x => x.RefFreeBytes)} loh_bytes={Sum(x => x.LohBytes)} new_objects={Sum(x => x.NewObjects)} new_bytes={Sum(x => x.NewBytes)} new_by_gen={Sum(x => x.NewBytesByGen[0])}/{Sum(x => x.NewBytesByGen[1])}/{Sum(x => x.NewBytesByGen[2])} took_s={res.Took.TotalSeconds:F0}");

        foreach (var line in Table(tag, "kind", b, x => x.Kind, top))
        {
            yield return line;
        }

        // Structure = operator kind + its field and that field's runtime type (first level).
        foreach (var line in Table(tag, "struct", b, x => x.Kind + " " + First(x.Path), top))
        {
            yield return line;
        }

        foreach (var line in Table(tag, "path", b, x => x.Kind + " " + x.Path, top))
        {
            yield return line;
        }

        foreach (var line in Table(tag, "op", b, x => "#" + x.Op.ToString(CultureInfo.InvariantCulture) + " " + x.Kind, 25))
        {
            yield return line;
        }

        // The object types inside the biggest structures.
        foreach (var g in b.GroupBy(x => x.Kind + " " + First(x.Path)).OrderByDescending(g => g.Sum(x => x.Refs)).Take(12))
        {
            var types = new Dictionary<Type, (long Objects, long Bytes, long Refs)>();
            foreach (var x in g)
            {
                foreach (var (t, v) in x.Types)
                {
                    types.TryGetValue(t, out var a);
                    types[t] = (a.Objects + v.Objects, a.Bytes + v.Bytes, a.Refs + v.Refs);
                }
            }

            foreach (var (t, v) in types.OrderByDescending(kv => kv.Value.Bytes).Take(6))
            {
                yield return string.Create(
                    CultureInfo.InvariantCulture,
                    $"STATE {tag} types [{g.Key}] {Name(t),-50} objects={v.Objects,11} bytes={v.Bytes,13} refs={v.Refs,11}");
            }
        }
    }

    private static string First(string path)
    {
        var slash = path.IndexOf('/', StringComparison.Ordinal);
        return slash < 0 ? path : path[..slash];
    }

    private static IEnumerable<string> Table(string tag, string by, List<Bucket> b, Func<Bucket, string> key, int top)
    {
        var totalRefs = Math.Max(1, b.Sum(x => x.Refs));
        var totalNew = Math.Max(1, b.Sum(x => x.NewBytes));
        foreach (var g in b.GroupBy(key).OrderByDescending(g => g.Sum(x => x.Refs)).Take(top))
        {
            yield return string.Create(
                CultureInfo.InvariantCulture,
                $"STATE {tag} {by} {g.Key,-90} objects={g.Sum(x => x.Objects),11} bytes={g.Sum(x => x.Bytes),13} refs={g.Sum(x => x.Refs),11} ({100.0 * g.Sum(x => x.Refs) / totalRefs:F1}%) ref_free={g.Sum(x => x.RefFreeBytes),12} new_bytes={g.Sum(x => x.NewBytes),12} ({100.0 * g.Sum(x => x.NewBytes) / totalNew:F1}%) new_gen012={g.Sum(x => x.NewBytesByGen[0])}/{g.Sum(x => x.NewBytesByGen[1])}/{g.Sum(x => x.NewBytesByGen[2])}");
        }
    }

    private static bool IsStream(Type t) => t.IsGenericType && t.GetGenericTypeDefinition() == typeof(DbspNet.Core.Circuit.Stream<>);

    private static string Short(Type t)
    {
        var n = t.Name;
        var tick = n.IndexOf('`', StringComparison.Ordinal);
        if (tick >= 0)
        {
            n = n[..tick];
        }

        return t.IsArray ? Short(t.GetElementType()!) + "[]" : n;
    }

    private static string Name(Type? t)
    {
        if (t is null)
        {
            return "?";
        }

        if (t.IsArray)
        {
            return Name(t.GetElementType()) + "[]";
        }

        if (!t.IsGenericType)
        {
            return t.Name;
        }

        var tick = t.Name.IndexOf('`', StringComparison.Ordinal);
        var n = tick < 0 ? t.Name : t.Name[..tick];
        return n + "<" + string.Join(",", t.GetGenericArguments().Select(Name)) + ">";
    }

    private static bool Skip(Type t) =>
        typeof(MemberInfo).IsAssignableFrom(t)
        || typeof(Assembly).IsAssignableFrom(t)
        || typeof(Module).IsAssignableFrom(t)
        || typeof(ParameterInfo).IsAssignableFrom(t)
        || typeof(Thread).IsAssignableFrom(t)
        || typeof(Delegate).IsAssignableFrom(t)
        || t.IsPointer;

    private static bool HasRefs(Type t)
    {
        lock (ContainsRefs)
        {
            if (!ContainsRefs.TryGetValue(t, out var has))
            {
                has = (bool)typeof(RuntimeHelpers).GetMethod(nameof(RuntimeHelpers.IsReferenceOrContainsReferences))!
                    .MakeGenericMethod(t).Invoke(null, null)!;
                ContainsRefs[t] = has;
            }

            return has;
        }
    }

    private static FieldInfo[] FieldsOf(Type t)
    {
        lock (RefFields)
        {
            if (RefFields.TryGetValue(t, out var fs))
            {
                return fs;
            }

            var list = new List<FieldInfo>();
            for (var c = t; c is not null; c = c.BaseType)
            {
                foreach (var f in c.GetFields(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.DeclaredOnly))
                {
                    if (!f.FieldType.IsPointer && f.FieldType != typeof(IntPtr) && HasRefs(f.FieldType))
                    {
                        list.Add(f);
                    }
                }
            }

            fs = list.ToArray();
            RefFields[t] = fs;
            return fs;
        }
    }

    private sealed class Walker(HashSet<object>? baseline)
    {
        public readonly HashSet<object> Seen = new(ReferenceEqualityComparer.Instance);
        public readonly HashSet<object> Stop = new(ReferenceEqualityComparer.Instance);
        public readonly Dictionary<(int, string, string), Bucket> Buckets = new();
        private readonly Stack<object> _stack = new();
        private Bucket _cur = null!;

        public Bucket BucketFor(int op, string kind, string path)
        {
            if (!Buckets.TryGetValue((op, kind, path), out var b))
            {
                b = new Bucket { Op = op, Kind = kind, Path = path };
                Buckets[(op, kind, path)] = b;
            }

            return b;
        }

        public void Edge(Bucket b) => b.Refs++;

        // Charge one object (already known unseen-or-seen) to a bucket, without walking it.
        public void Charge(object o, Bucket b)
        {
            if (Seen.Add(o))
            {
                Count(o, b);
            }
        }

        // The object at a labelled path: with depth left, it and its fields get labels of their own;
        // otherwise everything newly reached below it is charged to this path.
        public void Labelled(object v, int op, string kind, string path, int depth)
        {
            var t = v.GetType();
            var b = BucketFor(op, kind, path);
            if (depth > 0 && !t.IsArray && !t.IsValueType && !Skip(t) && !Stop.Contains(v) && t != typeof(string))
            {
                if (!Seen.Add(v))
                {
                    return;
                }

                Count(v, b);
                foreach (var f in FieldsOf(t))
                {
                    var c = f.GetValue(v);
                    if (c is null)
                    {
                        continue;
                    }

                    if (f.FieldType.IsValueType)
                    {
                        _cur = b;
                        Struct(c);
                        Drain();
                        continue;
                    }

                    b.Refs++;
                    if (IsStream(c.GetType()))
                    {
                        continue; // a nested stream reference: the current delta, not state
                    }

                    Labelled(c, op, kind, path + "/" + f.Name + ":" + Short(c.GetType()), depth - 1);
                }

                return;
            }

            _cur = b;
            Push(v);
            Drain();
        }

        private void Drain()
        {
            while (_stack.TryPop(out var o))
            {
                var t = o.GetType();
                if (t.IsArray)
                {
                    var arr = (Array)o;
                    var et = t.GetElementType()!;
                    if (!et.IsValueType && t.IsSZArray)
                    {
                        foreach (var x in Unsafe.As<object?[]>(arr))
                        {
                            Ref(x);
                        }
                    }
                    else if (et.IsValueType && HasRefs(et))
                    {
                        foreach (var x in arr)
                        {
                            Struct(x!);
                        }
                    }
                    else if (!et.IsValueType)
                    {
                        foreach (var x in arr)
                        {
                            Ref(x);
                        }
                    }
                }
                else
                {
                    Fields(o, t);
                }
            }
        }

        private void Fields(object o, Type t)
        {
            foreach (var f in FieldsOf(t))
            {
                var v = f.GetValue(o);
                if (v is null)
                {
                    continue;
                }

                if (f.FieldType.IsValueType)
                {
                    Struct(v);
                }
                else
                {
                    Ref(v);
                }
            }
        }

        private void Struct(object boxed) => Fields(boxed, boxed.GetType());

        private void Ref(object? x)
        {
            if (x is null)
            {
                return;
            }

            _cur.Refs++;
            var t = x.GetType();
            _cur.Types.TryGetValue(t, out var e);
            _cur.Types[t] = (e.Objects, e.Bytes, e.Refs + 1);
            Push(x);
        }

        /// <summary>Delegates reached but not entered, with the bucket that reached them (pass 4).</summary>
        public readonly List<(Delegate D, Bucket B)> Delegates = [];

        /// <summary>Charge <paramref name="o"/> (if new) and everything new below it to <paramref name="b"/>,
        /// entering it even when it is a stream (whose contents a plain push does not enter).</summary>
        public void Contents(object o, Bucket b)
        {
            _cur = b;
            if (Stop.Contains(o))
            {
                return;
            }

            var t = o.GetType();
            if (t.IsArray || Skip(t) || t == typeof(string))
            {
                Push(o); // counted and entered once, like any reference
            }
            else
            {
                if (Seen.Add(o))
                {
                    Count(o, b);
                }

                Fields(o, t); // entered even if seen: a stream was counted but never entered
            }

            Drain();
        }

        private void Push(object x)
        {
            if (Stop.Contains(x) || !Seen.Add(x))
            {
                return;
            }

            Count(x, _cur);
            var t = x.GetType();
            if (x is Delegate d)
            {
                Delegates.Add((d, _cur));
            }
            else if (!Skip(t) && !IsStream(t))
            {
                _stack.Push(x);
            }
        }

        private void Count(object x, Bucket b)
        {
            var t = x.GetType();
            var size = SizeOf(x);
            b.Objects++;
            b.Bytes += size;
            if (t == typeof(string) || (t.IsArray ? !HasRefs(t.GetElementType()!) : FieldsOf(t).Length == 0))
            {
                b.RefFreeBytes += size;
            }

            if (size >= 85_000)
            {
                b.LohBytes += size;
            }

            b.Types.TryGetValue(t, out var e);
            b.Types[t] = (e.Objects + 1, e.Bytes + size, e.Refs);
            if (baseline is not null && !baseline.Contains(x))
            {
                b.NewObjects++;
                b.NewBytes += size;
                b.NewBytesByGen[Math.Min(2, GC.GetGeneration(x))] += size;
            }
        }
    }
}
