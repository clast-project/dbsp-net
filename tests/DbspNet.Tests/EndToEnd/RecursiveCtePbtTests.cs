// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using CsCheck;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

namespace DbspNet.Tests.EndToEnd;

/// <summary>
/// Property-based "incremental ≡ batch" oracle for recursive CTEs, the test of
/// record for the nested fixpoint circuit. Drives random sequences of edge
/// inserts <b>and deletes</b> through the canonical transitive-closure query and
/// asserts, after every tick, that the accumulated output equals the batch
/// transitive closure of the current edge set. Exercises the retraction path
/// that the hand-written cases only spot-check — and is the safety net for the
/// incremental (semi-naive / DRED) iteration work.
/// </summary>
public class RecursiveCtePbtTests
{
    private const string ReachQuery =
        "WITH RECURSIVE reach AS ( " +
        "    SELECT src, dst FROM edges " +
        "    UNION ALL " +
        "    SELECT r.src, e.dst FROM reach r JOIN edges e ON r.dst = e.src) " +
        "SELECT src, dst FROM reach";

    private static CompiledQuery CompileReach()
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        resolver.Resolve(Parser.ParseStatement("CREATE TABLE edges (src INT NOT NULL, dst INT NOT NULL)"));
        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(ReachQuery))).Query;
        return PlanToCircuit.Compile(plan);
    }

    [Fact]
    public void TransitiveClosure_RandomInsertDeleteTicks_EqualsBatchEachTick()
    {
        // Small node space so closures are dense and cyclic; each op is an
        // edge plus an insert(true)/delete(false) flag, applied with set-guards
        // at run time so the circuit only ever sees clean +1 / -1 transitions.
        var genNode = Gen.Int[0, 3];
        var genOp = Gen.Select(genNode, genNode, Gen.Bool);
        var genPlan = genOp.Array[0, 4].Array[1, 6];

        genPlan.Sample(plan =>
        {
            var q = CompileReach();
            var edges = new HashSet<(int Src, int Dst)>();
            var accumulated = new Dictionary<(int, int), long>();

            foreach (var tick in plan)
            {
                foreach (var (src, dst, insert) in tick)
                {
                    if (insert)
                    {
                        if (edges.Add((src, dst)))
                        {
                            q.Table("edges").Insert(src, dst);
                        }
                    }
                    else if (edges.Remove((src, dst)))
                    {
                        q.Table("edges").Delete(src, dst);
                    }
                }

                q.Step();
                foreach (var (row, w) in q.Current)
                {
                    var key = ((int)row[0]!, (int)row[1]!);
                    accumulated[key] = accumulated.GetValueOrDefault(key) + w.Value;
                }

                // Set semantics: every accumulated row settles at weight 0 or 1.
                if (!accumulated.Values.All(v => v is 0 or 1))
                {
                    return false;
                }

                var live = accumulated.Where(kv => kv.Value == 1).Select(kv => kv.Key).ToHashSet();
                if (!live.SetEquals(BatchTransitiveClosure(edges)))
                {
                    return false;
                }
            }

            return true;
        }, iter: 1000);
    }

    private static HashSet<(int, int)> BatchTransitiveClosure(IEnumerable<(int Src, int Dst)> edges)
    {
        var e = edges.ToHashSet();
        var tc = new HashSet<(int, int)>(e);
        var changed = true;
        while (changed)
        {
            changed = false;
            foreach (var (a, b) in tc.ToList())
            {
                foreach (var (c, d) in e)
                {
                    if (b == c && tc.Add((a, d)))
                    {
                        changed = true;
                    }
                }
            }
        }

        return tc;
    }
}
