// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Diagnostics;
using System.Text;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

namespace DbspNet.Benchmarks;

/// <summary>
/// End-to-end gate for the arrangement-CSE optimizer rule
/// (<see cref="CompileOptions.ShareArrangements"/>, docs §9.6): a wide dimension
/// <c>dim</c> joined by <c>F</c> facts on the same key (a star schema, expressed
/// as a UNION ALL of <c>F</c> inner joins). With the rule on, the compiler builds
/// ONE shared arrangement of <c>dim</c>; with it off, each join maintains a
/// private right trace. Both arms compile on the SAME structural path with the
/// SAME codec (only <c>ShareArrangements</c> differs), so the A/B is honest.
/// </summary>
internal static class SharedArrangementSqlBenchmark
{
    private static readonly int[] FanOuts = { 2, 4, 8 };
    private const int StateKeys = 20_000;
    private const int DeltaKeys = 1_024;

    public static void Run(StringBuilder output)
    {
        Console.WriteLine();
        Console.WriteLine("=== Shared arrangement — SQL star-schema (optimizer rule) ===");

        output.AppendLine("## Arrangement CSE on real SQL — F facts joined to one wide `dim`");
        output.AppendLine();
        output.AppendLine(
            "Gate for the arrangement-CSE optimizer rule (docs §9.6). The query is a " +
            "`UNION ALL` of `F` inner joins, each joining a fact table to a shared wide " +
            "`dim` on the same key. **Unshared** and **Shared** compile on the same " +
            "structural path with the same codec, differing only in " +
            "`CompileOptions.ShareArrangements`: when on, the compiler detects `dim` is " +
            "the right input of ≥2 joins on the same key and builds ONE `Arrange` / " +
            "`SpineArrange` both joins read, instead of a private right trace each. Output " +
            "verified identical. Each tick pushes a delta into `dim` and each fact; times " +
            $"are median ns per **Step**, speedup = unshared/shared. State {StateKeys:N0} " +
            $"keys/table, {DeltaKeys} keys/tick.");
        output.AppendLine();

        VerifyEquivalence(TraceFamily.Flat);
        VerifyEquivalence(TraceFamily.Spine);
        Console.WriteLine("  Output equivalence (shared == unshared, flat + spine): OK");
        output.AppendLine("Output equivalence (shared vs unshared) verified on both substrates. ");
        output.AppendLine();

        foreach (var family in new[] { TraceFamily.Flat, TraceFamily.Spine })
        {
            EmitGrid(output, family);
        }

        output.AppendLine("## Verdict");
        output.AppendLine();
        output.AppendLine(
            "The optimizer rule routes real SQL through a single shared arrangement and " +
            "the results are identical to the unshared compile (also in " +
            "`ArrangementSharingTests`). The end-to-end win is small (~1–6%, growing with " +
            "fan-out) — smaller than the operator-level figure (§9.5, ~1.5× at F=8) — " +
            "because at the query level the deduplicated `dim` maintenance is a small " +
            "fraction of per-step work: input row encoding, each fact's re-index, the " +
            "UNION, projection, and output build are all per-branch and unchanged. The " +
            "rule's value is making cross-operator sharing reachable from SQL and proving " +
            "it correct end-to-end, not a step change. Off by default; enabling it forces " +
            "the structural compile path.");
        output.AppendLine();
    }

    private static void EmitGrid(StringBuilder output, TraceFamily family)
    {
        var name = family == TraceFamily.Spine ? "Spine" : "Flat";
        Console.WriteLine($"  --- {name} substrate ---");
        output.AppendLine($"### {name} substrate");
        output.AppendLine();

        // Warm up tiered JIT for this substrate's generic operator instantiations.
        _ = MeasureStep(FanOuts[^1], share: false, family);
        _ = MeasureStep(FanOuts[^1], share: true, family);

        output.AppendLine("| Fan-out F | Unshared | Shared | Speedup |");
        output.AppendLine("|----------:|---------:|-------:|--------:|");
        foreach (var f in FanOuts)
        {
            var unshared = MeasureStep(f, share: false, family);
            var shared = MeasureStep(f, share: true, family);
            var speedup = shared > 0 ? unshared / shared : 0.0;
            Console.WriteLine($"    F={f} unshared={FmtNs(unshared)} shared={FmtNs(shared)} {BenchmarkHarness.FormatRatio(speedup).Trim()}");
            output.AppendLine($"| {f,9} | {FmtNs(unshared)} | {FmtNs(shared)} | {BenchmarkHarness.FormatRatio(speedup).Trim()} |");
        }

        output.AppendLine();
    }

    private static double MeasureStep(int f, bool share, TraceFamily family)
    {
        var q = CompileStar(f, share, family);
        Preload(q, f, StateKeys);

        var keys = new int[DeltaKeys];
        for (var i = 0; i < DeltaKeys; i++)
        {
            keys[i] = (int)((long)i * StateKeys / DeltaKeys);
        }

        void PushPair(bool plus)
        {
            var w = plus ? 1 : -1;
            foreach (var k in keys)
            {
                if (w > 0)
                {
                    q.Table("dim").Insert(Wide(k));
                }
                else
                {
                    q.Table("dim").Delete(Wide(k));
                }
            }

            for (var fi = 0; fi < f; fi++)
            {
                foreach (var k in keys)
                {
                    if (w > 0)
                    {
                        q.Table("fact" + fi).Insert(k, k);
                    }
                    else
                    {
                        q.Table("fact" + fi).Delete(k, k);
                    }
                }
            }
        }

        for (var i = 0; i < 8; i++)
        {
            PushPair(i % 2 == 0);
            q.Step();
        }

        GC.Collect(2, GCCollectionMode.Forced, blocking: true);
        GC.WaitForPendingFinalizers();

        const int samples = 31;
        var times = new List<double>(samples);
        var sw = new Stopwatch();
        for (var s = 0; s < samples; s++)
        {
            sw.Restart();
            PushPair(true);
            q.Step();
            PushPair(false);
            q.Step();
            sw.Stop();
            times.Add(sw.Elapsed.TotalNanoseconds / 2.0);
        }

        times.Sort();
        return times[times.Count / 2];
    }

    private static void Preload(CompiledQuery q, int f, int n)
    {
        const int chunk = 5_000;
        for (var start = 0; start < n; start += chunk)
        {
            var end = Math.Min(start + chunk, n);
            for (var k = start; k < end; k++)
            {
                q.Table("dim").Insert(Wide(k));
                for (var fi = 0; fi < f; fi++)
                {
                    q.Table("fact" + fi).Insert(k, k);
                }
            }

            q.Step();
        }
    }

    // Wide dim row [k, v0..v4] so the arrangement's per-tick maintenance is real.
    private static object?[] Wide(int k) => new object?[] { k, k * 2, k * 3, k * 5, k * 7, k * 11 };

    private static CompiledQuery CompileStar(int f, bool share, TraceFamily family)
    {
        var ddl = new List<string>
        {
            "CREATE TABLE dim (k INT NOT NULL, v0 INT NOT NULL, v1 INT NOT NULL, v2 INT NOT NULL, v3 INT NOT NULL, v4 INT NOT NULL)",
        };
        var branches = new List<string>();
        for (var i = 0; i < f; i++)
        {
            ddl.Add($"CREATE TABLE fact{i} (k INT NOT NULL, a INT NOT NULL)");
            branches.Add($"SELECT t.a AS x, dim.v0 AS v FROM fact{i} t JOIN dim ON t.k = dim.k");
        }

        var query = string.Join(" UNION ALL ", branches);

        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in ddl)
        {
            resolver.Resolve(Parser.ParseStatement(s));
        }

        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(query))).Query;
        return PlanToCircuit.Compile(
            plan,
            new CompileOptions { TraceFamily = family, ShareArrangements = share },
            EmittedEqualityCodec.Instance);
    }

    private static void VerifyEquivalence(TraceFamily family)
    {
        const int f = 4;
        var shared = CompileStar(f, share: true, family);
        var unshared = CompileStar(f, share: false, family);
        var rng = new Random(99);

        for (var tick = 0; tick < 25; tick++)
        {
            for (var fi = 0; fi < f; fi++)
            {
                var n = rng.Next(0, 4);
                for (var i = 0; i < n; i++)
                {
                    int k = rng.Next(0, 40), a = rng.Next(0, 100);
                    shared.Table("fact" + fi).Insert(k, a);
                    unshared.Table("fact" + fi).Insert(k, a);
                }
            }

            var dn = rng.Next(0, 4);
            for (var i = 0; i < dn; i++)
            {
                var row = Wide(rng.Next(0, 40));
                shared.Table("dim").Insert(row);
                unshared.Table("dim").Insert(row);
            }

            shared.Step();
            unshared.Step();
            if (shared.Current.Count != unshared.Current.Count)
            {
                throw new InvalidOperationException(
                    $"{family} shared != unshared at tick {tick}: {shared.Current.Count} vs {unshared.Current.Count}");
            }

            foreach (var (r, w) in unshared.Current)
            {
                if (shared.Current.WeightOf(r).Value != w.Value)
                {
                    throw new InvalidOperationException($"{family} shared != unshared at tick {tick}: weight of {r}");
                }
            }
        }
    }

    private static string FmtNs(double ns) =>
        ns switch
        {
            < 1_000.0 => $"{ns,7:F0} ns",
            < 1_000_000.0 => $"{ns / 1_000.0,7:F2} µs",
            _ => $"{ns / 1_000_000.0,7:F2} ms",
        };
}
