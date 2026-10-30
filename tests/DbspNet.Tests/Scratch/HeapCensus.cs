// What survives ivm-bench batch 1, by kind (docs/design-row-arena.md, phase 0). gcdump on
// .NET 10 drops objects past ~10M, so this walks the live graph itself: every object reachable
// from a root through instance fields, deduplicated by identity and sized as the GC sizes it.
//   Reachable(root)       bytes / objects by exact type, everything reachable from the runner
//   Traces(counters)      the rows the traces hold: how many traces reference each top-level
//                         entry (the sharing factor), the bytes reachable from rows by type, and
//                         which traces own them (bytes attributed to the first trace to reach them)
// Run from IvmMultiBatch with IVM_HEAP_CENSUS=1 (it needs DBSPNET_TRACE_ACCESS_PROFILE=1 for the
// trace pass). Slow (minutes) and memory-hungry (a few GB of identity sets); census only.
using System.Globalization;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using DbspNet.Core.Collections;

namespace DbspNet.Tests.Scratch;

internal static class HeapCensus
{
    // MethodTable base size minus header and method-table pointer, plus the components: the same
    // number the GC uses, so SizeOf = raw + 16 rounded to 8 (24-byte minimum).
    private static readonly Func<object, nuint> RawSize =
        typeof(RuntimeHelpers).GetMethod("GetRawObjectDataSize", BindingFlags.NonPublic | BindingFlags.Static)!
            .CreateDelegate<Func<object, nuint>>();

    private static readonly Dictionary<Type, FieldInfo[]> RefFields = new();
    private static readonly Dictionary<Type, bool> ContainsRefs = new();

    public static long SizeOf(object o) => Math.Max(24, ((long)RawSize(o) + 16 + 7) & ~7L);

    /// <summary>Everything reachable from <paramref name="root"/>, by type.</summary>
    public static IEnumerable<string> Reachable(object root, int top = 40)
    {
        var w = new Walker();
        w.Visit(root, -1);
        yield return string.Create(CultureInfo.InvariantCulture, $"CENSUS reachable bytes={w.TotalBytes} objects={w.TotalObjects}");
        foreach (var line in w.Table("CENSUS reachable", top))
        {
            yield return line;
        }

        // Large-object-heap objects (>= 85,000 bytes): a young GC scans their dirty cards in both
        // mark and relocate, so what matters is which of them hold references.
        yield return string.Create(
            CultureInfo.InvariantCulture,
            $"CENSUS loh objects={w.Loh.Values.Sum(v => v.Objects)} bytes={w.Loh.Values.Sum(v => v.Bytes)} with_refs={w.Loh.Where(kv => HasRefs(kv.Key.GetElementType() ?? kv.Key)).Sum(kv => kv.Value.Bytes)}");
        foreach (var (t, v) in w.Loh.OrderByDescending(kv => kv.Value.Bytes).Take(top))
        {
            yield return string.Create(
                CultureInfo.InvariantCulture,
                $"CENSUS loh type {Name(t),-60} objects={v.Objects,8} bytes={v.Bytes,14} refs={HasRefs(t.GetElementType() ?? t)}");
        }
    }

    /// <summary>The rows the traces hold: sharing factor, bytes by type, bytes by trace.</summary>
    public static IEnumerable<string> Traces(IReadOnlyList<TraceAccessCounter> counters, int top = 40)
    {
        // Pass 1: for each top-level entry, how many distinct traces hold it. A value-type entry
        // (a typed key / value) is stored inline in the container: not shared, not a row object.
        var refs = new Dictionary<object, (int Last, int Traces)>(ReferenceEqualityComparer.Instance);
        long inlineEntries = 0, inlineBytes = 0, entryRefs = 0;
        for (var t = 0; t < counters.Count; t++)
        {
            foreach (var e in counters[t].Entries())
            {
                if (e.GetType().IsValueType)
                {
                    inlineEntries++;
                    inlineBytes += (long)RawSize(e);
                    continue;
                }

                entryRefs++;
                ref var slot = ref CollectionsMarshal.GetValueRefOrAddDefault(refs, e, out var exists);
                if (!exists)
                {
                    slot = (t, 1);
                }
                else if (slot.Last != t)
                {
                    slot = (t, slot.Traces + 1);
                }
            }
        }

        yield return string.Create(
            CultureInfo.InvariantCulture,
            $"CENSUS traces={counters.Count} entry_refs={entryRefs} distinct_entries={refs.Count} inline_struct_entries={inlineEntries} inline_struct_bytes={inlineBytes}");

        // Pass 2: walk from each entry, attributing first-reached bytes to the trace and to the
        // entry's sharing bucket. Sharing buckets: 1, 2, 3, 4, 5-8, >8 traces.
        static int Bucket(int n) => n switch { <= 4 => n - 1, <= 8 => 4, _ => 5 };
        var w = new Walker();
        var entriesBy = new long[6];
        var bytesBy = new long[6];
        var perTrace = new (long Bytes, long Entries, long Groups)[counters.Count];
        for (var t = 0; t < counters.Count; t++)
        {
            foreach (var e in counters[t].Entries())
            {
                if (e.GetType().IsValueType || !refs.TryGetValue(e, out var r) || r.Last < 0)
                {
                    continue;
                }

                refs[e] = (-1, r.Traces); // walked
                var before = w.TotalBytes;
                w.Visit(e, t);
                var b = Bucket(r.Traces);
                entriesBy[b]++;
                bytesBy[b] += w.TotalBytes - before;
                perTrace[t].Bytes += w.TotalBytes - before;
                perTrace[t].Entries++;
            }
        }

        yield return string.Create(CultureInfo.InvariantCulture, $"CENSUS rows bytes={w.TotalBytes} objects={w.TotalObjects}");
        yield return "CENSUS sharing [1,2,3,4,5-8,>8 traces] entries= " + string.Join(" ", entriesBy);
        yield return "CENSUS sharing [1,2,3,4,5-8,>8 traces] bytes=   " + string.Join(" ", bytesBy);
        foreach (var line in w.Table("CENSUS rows", top))
        {
            yield return line;
        }

        foreach (var g in Enumerable.Range(0, counters.Count).GroupBy(i => counters[i].Owner + " " + counters[i].Kind)
            .OrderByDescending(g => g.Sum(i => perTrace[i].Bytes)))
        {
            yield return string.Create(
                CultureInfo.InvariantCulture,
                $"CENSUS owner {g.Key} traces={g.Count()} keys={g.Sum(i => (long)counters[i].StateKeys)} entries_first={g.Sum(i => perTrace[i].Entries)} bytes={g.Sum(i => perTrace[i].Bytes)}");
        }

        foreach (var t in Enumerable.Range(0, counters.Count).OrderByDescending(i => perTrace[i].Bytes).Take(25))
        {
            var c = counters[t];
            yield return string.Create(
                CultureInfo.InvariantCulture,
                $"CENSUS trace #{t} {c.Owner} {c.Kind} keys={c.StateKeys} entries_first={perTrace[t].Entries} bytes={perTrace[t].Bytes} type={Name(c.CollectionType)}");
        }
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

    // Instance fields (whole hierarchy) that hold a reference or a struct containing one.
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

    private sealed class Walker
    {
        private readonly HashSet<object> _seen = new(ReferenceEqualityComparer.Instance);
        private readonly Dictionary<Type, (long Objects, long Bytes, long InRefs)> _byType = new();
        private readonly Stack<object> _stack = new();

        public long TotalBytes { get; private set; }

        public long TotalObjects { get; private set; }

        public Dictionary<Type, (long Objects, long Bytes)> Loh { get; } = new();

        public void Visit(object root, int owner)
        {
            Push(root);
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

        public IEnumerable<string> Table(string prefix, int top)
        {
            foreach (var (t, v) in _byType.OrderByDescending(kv => kv.Value.Bytes).Take(top))
            {
                yield return string.Create(
                    CultureInfo.InvariantCulture,
                    $"{prefix} type {Name(t),-60} objects={v.Objects,12} bytes={v.Bytes,14} ({100.0 * v.Bytes / Math.Max(1, TotalBytes):F1}%) inrefs={v.InRefs}");
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

        // A struct value (boxed by reflection, not a heap object of its own): walk its fields.
        private void Struct(object boxed) => Fields(boxed, boxed.GetType());

        private void Ref(object? x)
        {
            if (x is null)
            {
                return;
            }

            var t = x.GetType();
            if (_byType.TryGetValue(t, out var e))
            {
                _byType[t] = (e.Objects, e.Bytes, e.InRefs + 1);
            }
            else
            {
                _byType[t] = (0, 0, 1);
            }

            Push(x);
        }

        private void Push(object x)
        {
            if (!_seen.Add(x))
            {
                return;
            }

            var t = x.GetType();
            var size = SizeOf(x);
            TotalBytes += size;
            TotalObjects++;
            _byType.TryGetValue(t, out var e);
            _byType[t] = (e.Objects + 1, e.Bytes + size, e.InRefs);
            if (size >= 85_000)
            {
                Loh.TryGetValue(t, out var l);
                Loh[t] = (l.Objects + 1, l.Bytes + size);
            }

            if (!Skip(t))
            {
                _stack.Push(x);
            }
        }
    }
}
