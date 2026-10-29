// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using CsCheck;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Optimizer;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

namespace DbspNet.Tests.EndToEnd;

/// <summary>
/// docs/design-state-representation.md §4A.1: the resolver no longer appends a hidden
/// copy of a PARTITION BY / ORDER BY key that is a bare column already in the windowed
/// derived table's select list; the key refers to that column instead. Computed keys
/// and keys over non-selected columns still ride as hidden columns.
/// <list type="number">
/// <item>Plan shape: which keys stay hidden.</item>
/// <item>Differential PBT: a query whose keys are bare columns (deduplicated) must
/// emit, tick for tick, exactly what the same query emits with every key spelled as a
/// computed identity (<c>c + 0</c>, <c>LOWER(h)</c>), which keeps the pre-§4A.1
/// hidden-column shape. Structural and typed paths.</item>
/// </list>
/// </summary>
public class PartitionedTopKHiddenColumnTests
{
    private const string Ddl = "CREATE TABLE w (g INT, h VARCHAR, v INT, id INT NOT NULL)";

    private static LogicalPlan Plan(string sql)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        resolver.Resolve(Parser.ParseStatement(Ddl));
        return PlanOptimizer.Optimize(((SelectPlan)resolver.Resolve(Parser.ParseStatement(sql))).Query);
    }

    private static PartitionedTopKPlan TopK(LogicalPlan plan) => plan switch
    {
        PartitionedTopKPlan p => p,
        ProjectPlan p => TopK(p.Input),
        FilterPlan f => TopK(f.Input),
        _ => throw new InvalidOperationException("no PartitionedTopKPlan under " + plan.GetType().Name),
    };

    [Fact]
    public void BareSelectedKeys_AreNotDuplicated()
    {
        // q18's shape: every key is a selected bare column (one of them aliased).
        var topk = TopK(Plan(
            "SELECT g, h, t, id FROM (SELECT g, h, v AS t, id, ROW_NUMBER() OVER " +
            "(PARTITION BY h, g ORDER BY v DESC, id) AS rn FROM w) s WHERE rn <= 1"));
        Assert.Equal(4, topk.Input.Schema.Count);
        Assert.Equal(
            [new ResolvedColumn(1, topk.Input.Schema[1].Type), new ResolvedColumn(0, topk.Input.Schema[0].Type)],
            topk.PartitionKeys);
        Assert.Equal(new ResolvedColumn(2, topk.Input.Schema[2].Type), topk.SortKeys[0].Expression);
        Assert.Equal(new ResolvedColumn(3, topk.Input.Schema[3].Type), topk.SortKeys[1].Expression);
    }

    [Fact]
    public void ComputedAndUnselectedKeys_StayHidden_RepeatedKeysShareOneColumn()
    {
        // g is selected (reused); v + 1 is computed (hidden); id is not selected
        // (hidden once, and its repeat in ORDER BY reuses that hidden column).
        var topk = TopK(Plan(
            "SELECT g, h FROM (SELECT g, h, RANK() OVER " +
            "(PARTITION BY g, id ORDER BY v + 1 DESC, id) AS rn FROM w) s WHERE rn <= 2"));
        Assert.Equal(4, topk.Input.Schema.Count);
        Assert.Equal(new ResolvedColumn(0, topk.Input.Schema[0].Type), topk.PartitionKeys[0]);
        Assert.Equal(new ResolvedColumn(2, topk.Input.Schema[2].Type), topk.PartitionKeys[1]);
        Assert.Equal(new ResolvedColumn(3, topk.Input.Schema[3].Type), topk.SortKeys[0].Expression);
        Assert.Equal(new ResolvedColumn(2, topk.Input.Schema[2].Type), topk.SortKeys[1].Expression);
    }

    // ---- differential PBT ----

    private static readonly string[] Functions = { "ROW_NUMBER", "RANK", "DENSE_RANK" };

    // Select lists: everything; aliased + reordered + some keys unselected; few columns.
    private static readonly string[] Selects = { "g, h, v, id", "id, v AS vv, g", "h, id" };

    private static readonly string[][] Partitions = { ["g"], ["g", "h"], ["h"], ["h", "g", "id"] };

    private static readonly (string Col, string Dir)[][] Orders =
    {
        [("v", "DESC")], [("v", "ASC NULLS FIRST")], [("v", "DESC"), ("id", "ASC")], [("id", "DESC")],
        [("g", "ASC"), ("v", "DESC")],
    };

    private static string Computed(string col) => col == "h" ? "LOWER(h)" : col + " + 0";

    internal sealed record Spec(string Function, int K, int Select, int Partition, int Order)
    {
        public string Sql(bool computedKeys)
        {
            string Key(string c) => computedKeys ? Computed(c) : c;
            var outer = string.Join(", ", Selects[Select].Split(", ").Select(s => s.Split(" AS ")[^1]));
            return $"SELECT {outer} FROM (SELECT {Selects[Select]}, {Function}() OVER " +
                   $"(PARTITION BY {string.Join(", ", Partitions[Partition].Select(Key))} " +
                   $"ORDER BY {string.Join(", ", Orders[Order].Select(o => Key(o.Col) + " " + o.Dir))}) AS rn " +
                   $"FROM w) s WHERE rn <= {K}";
        }
    }

    private static readonly Gen<Spec> GenSpec =
        Gen.Select(Gen.Int[0, 2], Gen.OneOfConst(1, 2, 5), Gen.Int[0, Selects.Length - 1],
                Gen.Int[0, Partitions.Length - 1], Gen.Int[0, Orders.Length - 1])
            .Select(p => new Spec(Functions[p.Item1], p.Item2, p.Item3, p.Item4, p.Item5));

    private static readonly Gen<IReadOnlyList<IReadOnlyList<InputEvent>>> GenTicks =
        Gen.Select(
                Gen.Int[0, 2].Select(i => i == 2 ? null : (object?)i),
                Gen.Int[0, 1].Select(i => (object?)(i == 0 ? "a" : "b")),
                Gen.Int[0, 6].Select(i => i == 6 ? null : (object?)i),
                Gen.Int[0, 5],
                Gen.OneOfConst(1L, 1L, -1L))
            .Select(p => new InputEvent("w", new object?[] { p.Item1, p.Item2, p.Item3, p.Item4 }, p.Item5))
            .Array[0, 12]
            .Select(arr => (IReadOnlyList<InputEvent>)arr)
            .Array[1, 8]
            .Select(arr => (IReadOnlyList<IReadOnlyList<InputEvent>>)arr);

    [Fact]
    public void RandomQuery_DedupedKeysEqualHiddenComputedKeys_EveryTick_BothPaths()
    {
        Gen.Select(GenSpec, GenTicks).Sample(Check, iter: 600);
    }

    private static bool Check(Spec spec, IReadOnlyList<IReadOnlyList<InputEvent>> ticks)
    {
        var bare = Plan(spec.Sql(computedKeys: false));
        var computed = Plan(spec.Sql(computedKeys: true));
        var visible = Selects[spec.Select].Split(", ").Length;
        var hiddenOracle = Partitions[spec.Partition].Length + Orders[spec.Order].Length;
        Assert.Equal(visible + hiddenOracle, TopK(computed).Input.Schema.Count);
        Assert.True(TypedPlanCompiler.TryCompile(bare, out _), "typed compile failed: " + spec.Sql(false));

        var structural = new CompileOptions { ShareArrangements = true };
        var arms = new[]
        {
            ("structural", PlanToCircuit.Compile(computed, null, structural), PlanToCircuit.Compile(bare, null, structural)),
            ("typed", PlanToCircuit.Compile(computed), PlanToCircuit.Compile(bare)),
        };

        var tickNo = 0;
        foreach (var tick in ticks)
        {
            tickNo++;
            foreach (var (_, oracle, dedup) in arms)
            {
                foreach (var ev in tick)
                {
                    foreach (var q in new[] { oracle, dedup })
                    {
                        if (ev.Weight > 0) q.Table("w").Insert(ev.Row);
                        else q.Table("w").Delete(ev.Row);
                    }
                }

                oracle.Step();
                dedup.Step();
            }

            foreach (var (name, oracle, dedup) in arms)
            {
                if (!oracle.Current.Equals(dedup.Current))
                {
                    Console.Error.WriteLine($"{name} tick {tickNo}: {spec.Sql(false)}");
                    Console.Error.WriteLine("oracle: " + oracle.Current);
                    Console.Error.WriteLine("dedup:  " + dedup.Current);
                    return false;
                }
            }
        }

        return true;
    }
}
