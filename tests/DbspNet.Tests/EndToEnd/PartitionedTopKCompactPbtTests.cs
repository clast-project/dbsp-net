// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using CsCheck;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Optimizer;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

namespace DbspNet.Tests.EndToEnd;

/// <summary>
/// Correctness gate for the compact partitioned-TOP-K state
/// (<see cref="PartitionedTopKCompactOp{TRow,TKey}"/>, docs/design-state-representation.md
/// §4): inline / sorted-array / tree partition storage, no materialised window (the old
/// window is recomputed from pre-delta state and merge-diffed), and on the typed path an
/// unboxed emitted-struct partition key.
/// <list type="number">
/// <item><b>Batch oracle</b> on the §22 small-tie domain (ROW_NUMBER / RANK /
/// DENSE_RANK, k ∈ 1..3, ASC/DESC, ±1 ticks).</item>
/// <item><b>Per-tick equivalence with the legacy operator</b> on a domain built to
/// cross every representation boundary (partitions grow past
/// <see cref="PartitionedTopKCompactOp{TRow,TKey}.SmallMax"/> into the tree and shrink
/// back to inline), with NULLable and VARCHAR partition columns and NULLable order
/// keys — through the structural compact op, the typed compact op (unboxed key, W=1
/// and W=2) and the typed compact op with the boxed key.</item>
/// </list>
/// </summary>
public class PartitionedTopKCompactPbtTests
{
    private const string SmallDdl = "CREATE TABLE t (g INT NOT NULL, v INT NOT NULL, id INT NOT NULL)";

    private static readonly string[] Functions = { "ROW_NUMBER", "RANK", "DENSE_RANK" };

    private static readonly Gen<PartitionedTopKNarrowingPbtTests.QuerySpec> GenSmallSpec =
        Gen.Select(Gen.Int[0, 2], Gen.Int[1, 3], Gen.Bool)
            .Select(p => new PartitionedTopKNarrowingPbtTests.QuerySpec(Functions[p.Item1], p.Item2, p.Item3));

    private static readonly Gen<IReadOnlyList<IReadOnlyList<InputEvent>>> GenSmallTicks =
        Gen.Select(Gen.Int[0, 2], Gen.Int[0, 3], Gen.Int[0, 2], Gen.OneOfConst(1L, -1L))
            .Select(p => new InputEvent("t", new object?[] { p.Item1, p.Item2, p.Item3 }, p.Item4))
            .Array[0, 6]
            .Select(arr => (IReadOnlyList<InputEvent>)arr)
            .Array[1, 8]
            .Select(arr => (IReadOnlyList<IReadOnlyList<InputEvent>>)arr);

    [Fact]
    public void RandomQuery_CompactEqualsBatch()
    {
        Gen.Select(GenSmallSpec, GenSmallTicks).Sample(
            (spec, ticks) =>
            {
                var compact = WithState(PartitionedTopKState.Compact, () =>
                    IncrementalOracle.RunAndAccumulate(IncrementalOracle.CompileQuery(new[] { SmallDdl }, spec.Sql), ticks));
                var batch = PartitionedTopKNarrowingPbtTests.BatchTopK(spec, ticks);
                if (!compact.Equals(batch))
                {
                    Console.Error.WriteLine($"SQL: {spec.Sql}\ncompact: {compact}\nbatch:   {batch}");
                    return false;
                }

                return true;
            },
            iter: 3000);
    }

    // ---- wide domain: representation boundaries, NULLs, VARCHAR / multi-column keys ----

    private const string WideDdl =
        "CREATE TABLE w (g INT, h VARCHAR, v INT, id INT NOT NULL)";

    private sealed record WideSpec(string Function, int K, string Partition, string Order)
    {
        public string Sql =>
            $"SELECT g, h, v, id FROM (SELECT g, h, v, id, {Function}() OVER " +
            $"(PARTITION BY {Partition} ORDER BY {Order}) AS rn FROM w) s WHERE rn <= {K}";
    }

    private static readonly string[] Partitions = { "g", "g, h", "h" };

    private static readonly string[] Orders =
    {
        "v DESC", "v ASC", "v ASC NULLS FIRST", "v DESC, id ASC", "id DESC",
    };

    private static readonly Gen<WideSpec> GenWideSpec =
        Gen.Select(Gen.Int[0, 2], Gen.OneOfConst(1, 2, 5, 40), Gen.Int[0, Partitions.Length - 1], Gen.Int[0, Orders.Length - 1])
            .Select(p => new WideSpec(Functions[p.Item1], p.Item2, Partitions[p.Item3], Orders[p.Item4]));

    // ~3:1 inserts:retractions so partitions grow past SmallMax (32) and then shrink.
    private static readonly Gen<InputEvent> GenWideEvent =
        Gen.Select(
                Gen.Int[0, 2].Select(i => i == 2 ? null : (object?)i),
                Gen.Int[0, 1].Select(i => (object?)(i == 0 ? "a" : "b")),
                Gen.Int[0, 10].Select(i => i == 10 ? null : (object?)i),
                Gen.Int[0, 7],
                Gen.OneOfConst(1L, 1L, 1L, -1L))
            .Select(p => new InputEvent("w", new object?[] { p.Item1, p.Item2, p.Item3, p.Item4 }, p.Item5));

    private static readonly Gen<IReadOnlyList<IReadOnlyList<InputEvent>>> GenWideTicks =
        GenWideEvent.Array[0, 60]
            .Select(arr => (IReadOnlyList<InputEvent>)arr)
            .Array[1, 10]
            .Select(arr => (IReadOnlyList<IReadOnlyList<InputEvent>>)arr);

    [Fact]
    public void RandomQuery_CompactEqualsLegacy_EveryTick_AllPaths()
    {
        Gen.Select(GenWideSpec, GenWideTicks).Sample(CheckWide, iter: 250);
    }

    [Fact]
    public void TypedPath_UsesUnboxedPartitionKey()
    {
        var plan = Plan(WideDdl, new WideSpec("ROW_NUMBER", 1, "g, h", "v DESC").Sql);
        var op = WithState(PartitionedTopKState.Compact, () =>
        {
            Assert.True(TypedPlanCompiler.TryCompile(plan, out var typed));
            return typed!.Circuit.Operators.Single(o => o.GetType().Name.StartsWith("PartitionedTopK", StringComparison.Ordinal));
        });
        var type = op.GetType();
        Assert.StartsWith("PartitionedTopKCompactOp", type.Name, StringComparison.Ordinal);
        Assert.NotEqual(typeof(StructuralRow), type.GetGenericArguments()[1]);
        Assert.True(type.GetGenericArguments()[1].IsValueType);
    }

    private static bool CheckWide(WideSpec spec, IReadOnlyList<IReadOnlyList<InputEvent>> ticks)
    {
        var plan = Plan(WideDdl, spec.Sql);
        var legacy = WithState(PartitionedTopKState.Legacy, () => PlanToCircuit.Compile(plan));
        var compact = WithState(PartitionedTopKState.Compact, () => PlanToCircuit.Compile(plan));
        TypedCompiledQuery? typed1 = null, typedBoxed = null;
        ParallelTypedCompiledQuery? typed2 = null;
        WithState(PartitionedTopKState.Compact, () =>
        {
            Assert.True(TypedPlanCompiler.TryCompile(plan, out typed1), "typed compile failed: " + spec.Sql);
            Assert.True(TypedPlanCompiler.TryCompileParallel(plan, 2, out typed2), "typed W=2 compile failed: " + spec.Sql);
            return 0;
        });
        WithState(PartitionedTopKState.CompactBoxedKey, () =>
        {
            Assert.True(TypedPlanCompiler.TryCompile(plan, out typedBoxed), "typed boxed compile failed: " + spec.Sql);
            return 0;
        });

        using (typed2)
        {
            var tickNo = 0;
            foreach (var tick in ticks)
            {
                tickNo++;
                foreach (var ev in tick)
                {
                    Push(legacy.Table(ev.Table).Insert, legacy.Table(ev.Table).Delete, ev);
                    Push(compact.Table(ev.Table).Insert, compact.Table(ev.Table).Delete, ev);
                    Push(typed1!.Table(ev.Table).Insert, typed1.Table(ev.Table).Delete, ev);
                    Push(typed2!.Table(ev.Table).Insert, typed2.Table(ev.Table).Delete, ev);
                    Push(typedBoxed!.Table(ev.Table).Insert, typedBoxed.Table(ev.Table).Delete, ev);
                }

                legacy.Step();
                compact.Step();
                typed1!.Step();
                typed2!.Step();
                typedBoxed!.Step();

                var expected = Canon(legacy.Current.Select(kv => (kv.Key.ToArray(), kv.Value.Value)));
                var arms = new (string Name, Dictionary<string, long> Got)[]
                {
                    ("structural compact", Canon(compact.Current.Select(kv => (kv.Key.ToArray(), kv.Value.Value)))),
                    ("typed compact W=1", Canon(typed1.Current)),
                    ("typed compact W=2", Canon(typed2.Current)),
                    ("typed compact boxed key", Canon(typedBoxed.Current)),
                };
                foreach (var (name, got) in arms)
                {
                    if (!DictEquals(expected, got))
                    {
                        Console.Error.WriteLine($"SQL: {spec.Sql}  tick {tickNo}  arm {name}");
                        Console.Error.WriteLine("legacy: " + string.Join(", ", expected.OrderBy(e => e.Key).Select(e => $"{e.Key}:{e.Value}")));
                        Console.Error.WriteLine("got:    " + string.Join(", ", got.OrderBy(e => e.Key).Select(e => $"{e.Key}:{e.Value}")));
                        return false;
                    }
                }
            }
        }

        return true;
    }

    private static void Push(Action<object?[]> insert, Action<object?[]> delete, InputEvent ev)
    {
        if (ev.Weight > 0)
        {
            insert(ev.Row);
        }
        else
        {
            delete(ev.Row);
        }
    }

    private static bool DictEquals(Dictionary<string, long> a, Dictionary<string, long> b)
        => a.Count == b.Count && a.All(kv => b.TryGetValue(kv.Key, out var v) && v == kv.Value);

    private static Dictionary<string, long> Canon(IEnumerable<(object?[] Values, long Weight)> current)
    {
        var map = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach (var (values, weight) in current)
        {
            var key = string.Join("|", values.Select(v => v?.ToString() ?? "<null>"));
            map[key] = map.GetValueOrDefault(key) + weight;
            if (map[key] == 0)
            {
                map.Remove(key);
            }
        }

        return map;
    }

    private static LogicalPlan Plan(string ddl, string sql)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        resolver.Resolve(Parser.ParseStatement(ddl));
        return PlanOptimizer.Optimize(((SelectPlan)resolver.Resolve(Parser.ParseStatement(sql))).Query);
    }

    private static T WithState<T>(PartitionedTopKState state, Func<T> body)
    {
        var prev = PartitionedTopKStateMode.Override;
        PartitionedTopKStateMode.Override = state;
        try
        {
            return body();
        }
        finally
        {
            PartitionedTopKStateMode.Override = prev;
        }
    }
}
