// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections;
using System.Reflection;

namespace DbspNet.Core.Circuit;

/// <summary>A stream as the release planner sees it (docs/design-bounded-memory.md §3b).</summary>
internal interface IReleasableStream
{
    /// <summary>The operator index that wrote the stream on the first tick; −1 written outside a
    /// step (an input handle), −2 not yet seen.</summary>
    int Producer { get; set; }

    /// <summary>Read after <see cref="RootCircuit.Step"/> (an output or input handle): never released.</summary>
    bool Pinned { get; set; }

    /// <summary>Drop the current value (to null: a missed reader fails loudly).</summary>
    void Release();
}

/// <summary>
/// Releasing a tick's stream values once their last reader has run, as Feldera's scheduler does
/// with consumer tokens (docs/design-bounded-memory.md §3b): a value held until the next tick
/// overwrote it kept every delta of a tick alive to its end — 6.3 GB of ivm-bench's 14.1 GB batch-1
/// peak. The circuit declares no inputs, so readers are found by a conservative scan of each
/// operator's fields (a superset only releases later) and producers by watching the first tick's
/// writes. Only forward edges are released (producer before every reader); feedback through a
/// delay, input-handle streams and handle-read streams are kept.
/// On by default; <c>DBSPNET_STREAM_RELEASE=0</c> turns it off.
/// </summary>
internal static class StreamRelease
{
    public static bool Enabled { get; set; } = Environment.GetEnvironmentVariable("DBSPNET_STREAM_RELEASE") != "0";

    [ThreadStatic]
    private static int _currentOp;

    /// <summary>The operator now stepping on this thread (−1: none).</summary>
    public static int CurrentOp
    {
        get => _currentOp - 1;
        set => _currentOp = value + 1;
    }

    /// <summary>For each operator index, the streams to release right after it steps.</summary>
    public static List<IReleasableStream>[] Plan(IReadOnlyList<IOperator> operators)
    {
        var readers = new Dictionary<IReleasableStream, List<int>>(ReferenceEqualityComparer.Instance);
        var ops = new HashSet<object>(operators, ReferenceEqualityComparer.Instance);
        var opaque = new List<int>();
        for (var i = 0; i < operators.Count; i++)
        {
            var found = new HashSet<IReleasableStream>(ReferenceEqualityComparer.Instance);
            var scan = new ScanState(ops, found);
            Scan(operators[i], 0, scan);
            if (scan.Opaque)
            {
                opaque.Add(i);
            }

            foreach (var s in found)
            {
                (readers.TryGetValue(s, out var list) ? list : readers[s] = []).Add(i);
            }
        }

        OpaqueOperators = opaque.Count;

        var plan = new List<IReleasableStream>[operators.Count];
        for (var i = 0; i < plan.Length; i++)
        {
            plan[i] = [];
        }

        foreach (var (stream, refs) in readers)
        {
            var p = stream.Producer;
            if (p < 0 || stream.Pinned)
            {
                continue;
            }

            // An opaque operator may read any stream: after the producer it extends the stream's
            // life to it; before the producer it may read it as feedback, so it is kept.
            var others = refs.Where(r => r != p).ToList();
            if (others.Count == 0 || others.Any(r => r < p) || opaque.Any(o => o < p))
            {
                continue; // read by nobody we can see, or feedback (read before written)
            }

            plan[Math.Max(others.Max(), opaque.Count == 0 ? -1 : opaque.Max())].Add(stream);
        }

        return plan;
    }

    /// <summary>Operators in the last plan whose fields lead somewhere the scan does not walk (a
    /// dictionary or large collection that could hold a stream, or wiring deeper than the scan
    /// goes): they are taken to read every stream.</summary>
    public static int OpaqueOperators { get; private set; }

    private sealed class ScanState(HashSet<object> ops, HashSet<IReleasableStream> found)
    {
        public readonly HashSet<object> Ops = ops;
        public readonly HashSet<IReleasableStream> Found = found;
        public readonly HashSet<object> Seen = new(ReferenceEqualityComparer.Instance);
        public bool Opaque;
    }

    // Streams reachable from an operator's fields: through wrappers, closures, tuples and small
    // lists, never into another operator or a Z-set's contents. What it cannot walk and might hold
    // a stream marks the operator opaque.
    private static void Scan(object o, int depth, ScanState state)
    {
        if (o is IReleasableStream s)
        {
            state.Found.Add(s);
            return;
        }

        if (depth > 0 && state.Ops.Contains(o))
        {
            return;
        }

        if (depth > 8)
        {
            state.Opaque |= MayHoldStream(o.GetType());
            return;
        }

        if (!state.Seen.Add(o))
        {
            return;
        }

        var t = o.GetType();
        if (o is Delegate d)
        {
            foreach (var inner in d.GetInvocationList())
            {
                if (inner.Target is { } target)
                {
                    Scan(target, depth + 1, state);
                }
            }

            return;
        }

        if (o is Array arr)
        {
            if (!t.GetElementType()!.IsPrimitive && arr.Length <= 256)
            {
                foreach (var e in arr)
                {
                    if (e is not null)
                    {
                        Scan(e, depth + 1, state);
                    }
                }
            }

            return;
        }

        if (o is string || t.IsPrimitive || t.IsEnum)
        {
            return;
        }

        var ns = t.Namespace ?? string.Empty;
        if (o is IEnumerable and not IOperator)
        {
            // A small list of wiring (inputs of a multi-way operator); never a Z-set's rows.
            if (ns.StartsWith("DbspNet.Core.Collections", StringComparison.Ordinal))
            {
                return;
            }

            if (o is not IList list || list.Count > 256)
            {
                state.Opaque |= MayHoldStream(t);
                return;
            }

            foreach (var e in list)
            {
                if (e is not null)
                {
                    Scan(e, depth + 1, state);
                }
            }

            return;
        }

        // System types are walked only when generic (a tuple, Lazy, a box): their arguments may be
        // streams. Anything else from System holds no wiring.
        if (ns.StartsWith("DbspNet.Core.Collections", StringComparison.Ordinal) || (ns.StartsWith("System", StringComparison.Ordinal) && !t.IsGenericType))
        {
            return;
        }

        for (var ty = t; ty is not null && ty != typeof(object); ty = ty.BaseType)
        {
            foreach (var f in ty.GetFields(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.DeclaredOnly))
            {
                if (f.FieldType.IsPrimitive || f.FieldType.IsEnum || f.FieldType.IsPointer)
                {
                    continue;
                }

                if (f.GetValue(o) is { } v)
                {
                    Scan(v, depth + 1, state);
                }
            }
        }
    }

    // Whether a value of this type could reach a stream: a stream type, or a generic type with such
    // an argument, or an argument (object, an interface) that could be anything.
    private static bool MayHoldStream(Type t, int depth = 0)
    {
        if (typeof(IReleasableStream).IsAssignableFrom(t) || (t.IsAssignableFrom(typeof(Stream<int>)) && !t.IsValueType))
        {
            return true;
        }

        if (depth > 4)
        {
            return true;
        }

        if (t.IsArray)
        {
            return MayHoldStream(t.GetElementType()!, depth + 1);
        }

        foreach (var arg in t.IsGenericType ? t.GetGenericArguments() : [])
        {
            if (MayHoldStream(arg, depth + 1))
            {
                return true;
            }
        }

        // Non-generic: a stream behind its fields is still possible for a DbspNet wrapper type.
        return !t.IsGenericType && !t.IsPrimitive && t != typeof(string) && (t.Namespace ?? "").StartsWith("DbspNet", StringComparison.Ordinal)
            && !(t.Namespace ?? "").StartsWith("DbspNet.Core.Collections", StringComparison.Ordinal);
    }
}
