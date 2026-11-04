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
        var started = System.Diagnostics.Stopwatch.GetTimestamp();
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
        PlanTime = System.Diagnostics.Stopwatch.GetElapsedTime(started);

        var released = 0;
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
            released++;
        }

        Released = released;
        return plan;
    }

    /// <summary>Operators in the last plan whose fields lead somewhere the scan does not walk (a
    /// dictionary or large collection that could hold a stream, or wiring deeper than the scan
    /// goes): they are taken to read every stream.</summary>
    public static int OpaqueOperators { get; private set; }

    /// <summary>Streams the last plan releases (diagnostics).</summary>
    public static int Released { get; private set; }

    /// <summary>How long the last plan took to build (diagnostics).</summary>
    public static TimeSpan PlanTime { get; private set; }

    private sealed class ScanState(HashSet<object> ops, HashSet<IReleasableStream> found)
    {
        public readonly HashSet<object> Ops = ops;
        public readonly HashSet<IReleasableStream> Found = found;
        public readonly HashSet<object> Seen = new(ReferenceEqualityComparer.Instance);
        public bool Opaque;
    }

    // Streams reachable from an operator's fields: through wrappers, closures, tuples, plan objects
    // and collections, never into another root operator, the circuit, or a Z-set's rows. The walk
    // follows real objects (a visited set bounds it); a type that cannot hold a stream is not
    // entered. What it will not walk but might hold a stream (a collection of more than
    // MaxElements, wiring nested past MaxDepth) marks the operator opaque.
    private const int MaxDepth = 200;
    private const int MaxElements = 1 << 16;

    private static void Scan(object o, int depth, ScanState state)
    {
        if (o is IReleasableStream s)
        {
            state.Found.Add(s);
            return;
        }

        if (depth > 0 && (state.Ops.Contains(o) || o is RootCircuit))
        {
            return;
        }

        var t = o.GetType();
        if (!MayHoldStream(t) || !state.Seen.Add(o))
        {
            return;
        }

        if (depth > MaxDepth)
        {
            Opaque(state, "depth " + t);
            return;
        }

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

        // A materialised collection's elements are walked (those that cannot hold a stream are
        // skipped by type). A lazy sequence is not enumerated: what it yields comes from its fields,
        // which are walked below.
        if (o is ICollection collection)
        {
            if (collection.Count > MaxElements)
            {
                Opaque(state, "collection " + t);
                return;
            }

            foreach (var e in collection)
            {
                if (e is not null)
                {
                    Scan(e, depth + 1, state);
                }
            }

            if (o is Array)
            {
                return;
            }
        }

        for (var ty = t; ty is not null && ty != typeof(object); ty = ty.BaseType)
        {
            foreach (var f in Fields(ty))
            {
                if (f.GetValue(o) is { } v)
                {
                    Scan(v, depth + 1, state);
                }
            }
        }
    }

    /// <summary>Why operators were opaque, by reason (diagnostics, <c>DBSPNET_STREAM_RELEASE_WHY=1</c>).</summary>
    internal static readonly System.Collections.Concurrent.ConcurrentDictionary<string, int> OpaqueReasons = new();

    private static readonly bool Why = Environment.GetEnvironmentVariable("DBSPNET_STREAM_RELEASE_WHY") == "1";

    private static void Opaque(ScanState state, string why)
    {
        state.Opaque = true;
        if (Why)
        {
            OpaqueReasons.AddOrUpdate(why, 1, (_, n) => n + 1);
        }
    }

    private static readonly System.Collections.Concurrent.ConcurrentDictionary<Type, FieldInfo[]> FieldCache = new();

    // A type's own instance fields that might hold a stream.
    private static FieldInfo[] Fields(Type t) => FieldCache.GetOrAdd(t, static ty =>
        [.. ty.GetFields(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.DeclaredOnly)
            .Where(f => !f.FieldType.IsPointer && MayHoldStream(f.FieldType))]);

    private static readonly System.Collections.Concurrent.ConcurrentDictionary<Type, bool> MayHold = new();

    private static readonly System.Collections.Concurrent.ConcurrentDictionary<Assembly, bool> AssemblySees = new();

    // Whether an assembly is DbspNet.Core or references it.
    private static bool SeesStreams(Assembly a) => AssemblySees.GetOrAdd(a, static a =>
    {
        var core = typeof(StreamRelease).Assembly;
        return a == core || a.GetReferencedAssemblies().Any(r => r.Name == core.GetName().Name);
    });

    // Whether a value whose static type is t could reach a stream. Conservative: object, an
    // interface, an unsealed class or a delegate could be anything. A Z-set's rows (Core.Collections),
    // strings, primitives and non-generic System types hold no wiring.
    private static bool MayHoldStream(Type t) => MayHold.TryGetValue(t, out var known) ? known : Compute(t, []);

    private static bool Compute(Type t, HashSet<Type> visiting)
    {
        if (MayHold.TryGetValue(t, out var known))
        {
            return known;
        }

        bool result;
        var ns = t.Namespace ?? string.Empty;
        if (typeof(IReleasableStream).IsAssignableFrom(t))
        {
            result = true;
        }
        else if (t.IsPrimitive || t.IsEnum || t.IsPointer || t == typeof(string) || t == typeof(decimal)
            || ns.StartsWith("DbspNet.Core.Collections", StringComparison.Ordinal)
            || (ns.StartsWith("System", StringComparison.Ordinal) && !t.IsGenericType && !t.IsArray && t != typeof(object)
                && !t.IsInterface && !typeof(Delegate).IsAssignableFrom(t) && !typeof(IEnumerable).IsAssignableFrom(t)))
        {
            result = false;
        }
        else if (t.IsArray)
        {
            result = Compute(t.GetElementType()!, visiting);
        }
        else if (!ns.StartsWith("System", StringComparison.Ordinal) && !SeesStreams(t.Assembly))
        {
            // A library that cannot name a stream holds one only through its type arguments.
            result = t.IsGenericType && t.GetGenericArguments().Any(a => Compute(a, visiting));
        }
        else if (ns.StartsWith("System", StringComparison.Ordinal) && t.IsGenericType && !typeof(Delegate).IsAssignableFrom(t))
        {
            // A System collection, tuple or box holds only values of its type arguments.
            result = t.GetGenericArguments().Any(a => Compute(a, visiting));
        }
        else if (t.IsGenericParameter || t == typeof(object) || t.IsInterface || (t.IsClass && !t.IsSealed) || typeof(Delegate).IsAssignableFrom(t))
        {
            result = true;
        }
        else if (!visiting.Add(t))
        {
            return false; // a cycle: decided by the type's other fields
        }
        else
        {
            result = false;
            for (var ty = t; ty is not null && ty != typeof(object) && !result; ty = ty.BaseType)
            {
                foreach (var f in ty.GetFields(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.DeclaredOnly))
                {
                    if (Compute(f.FieldType, visiting))
                    {
                        result = true;
                        break;
                    }
                }
            }

            visiting.Remove(t);
        }

        // A result reached inside a cycle may rest on a provisional "no": only a top-level one is kept.
        if (visiting.Count == 0)
        {
            MayHold[t] = result;
        }

        return result;
    }
}
