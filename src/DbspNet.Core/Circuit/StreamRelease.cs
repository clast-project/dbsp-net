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
/// <c>DBSPNET_STREAM_RELEASE=1</c> turns it on.
/// </summary>
internal static class StreamRelease
{
    public static bool Enabled { get; set; } = Environment.GetEnvironmentVariable("DBSPNET_STREAM_RELEASE") == "1";

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
        for (var i = 0; i < operators.Count; i++)
        {
            var found = new HashSet<IReleasableStream>(ReferenceEqualityComparer.Instance);
            Scan(operators[i], 0, ops, new HashSet<object>(ReferenceEqualityComparer.Instance), found);
            foreach (var s in found)
            {
                (readers.TryGetValue(s, out var list) ? list : readers[s] = []).Add(i);
            }
        }

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

            var others = refs.Where(r => r != p).ToList();
            if (others.Count == 0 || others.Any(r => r < p))
            {
                continue; // read by nobody we can see, or feedback (read before written)
            }

            plan[others.Max()].Add(stream);
        }

        return plan;
    }

    // Streams reachable from an operator's fields: through wrappers, closures and small arrays,
    // never into another operator or a collection's contents.
    private static void Scan(object o, int depth, HashSet<object> ops, HashSet<object> seen, HashSet<IReleasableStream> found)
    {
        if (o is IReleasableStream s)
        {
            found.Add(s);
            return;
        }

        if (depth > 8 || !seen.Add(o) || (depth > 0 && ops.Contains(o)))
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
                    Scan(target, depth + 1, ops, seen, found);
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
                        Scan(e, depth + 1, ops, seen, found);
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
            if (ns.StartsWith("DbspNet.Core.Collections", StringComparison.Ordinal) || o is not IList list || list.Count > 256)
            {
                return;
            }

            foreach (var e in list)
            {
                if (e is not null)
                {
                    Scan(e, depth + 1, ops, seen, found);
                }
            }

            return;
        }

        if (ns.StartsWith("DbspNet.Core.Collections", StringComparison.Ordinal) || ns.StartsWith("System", StringComparison.Ordinal))
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
                    Scan(v, depth + 1, ops, seen, found);
                }
            }
        }
    }
}
