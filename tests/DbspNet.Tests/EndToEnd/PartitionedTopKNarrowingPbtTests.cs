// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using CsCheck;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful;

namespace DbspNet.Tests.EndToEnd;

/// <summary>
/// Correctness gate for the §22 narrow-key partitioned TOP-K
/// (<see cref="PartitionedTopKNarrowingMode"/>): the operator keys its per-partition
/// state by a narrow <c>{order value, wide row}</c> key instead of the whole row.
/// Unlike join column pruning (§21, unconditionally sound), this is an incremental
/// operator rewrite under retraction and ties, so it is default-off behind a seam and
/// must be proven equivalent before shipping.
///
/// <para>Two random-query checks over the windowed TOP-K surface — <c>ROW_NUMBER</c> /
/// <c>RANK</c> / <c>DENSE_RANK</c>, k ∈ {1,2,3}, ORDER BY ASC/DESC, single column —
/// driven by ±1 tick streams (inserts <b>and</b> retractions, over small domains that
/// force order-value ties and overlapping deletes):</para>
/// <list type="number">
/// <item><b>Batch oracle.</b> The seam-ON accumulated output equals an independent
/// batch TOP-K computed over the net input bag (rank cut reimplemented on a freshly
/// sorted list — separate code from the operator's incremental sorted-trace + per-tick
/// diff, so it catches incremental-maintenance bugs).</item>
/// <item><b>Whole-row equivalence.</b> The seam-ON accumulated output equals the
/// seam-OFF (whole-row <see cref="PartitionedTopKOp{TRow,TKey}"/>) accumulated output —
/// the trusted reference the rest of the suite already verifies — catching any
/// narrow-key-specific divergence (hashing, tie bucketing, recovery).</item>
/// </list>
/// The complementary guard is the full suite staying green with the seam OFF
/// (default), proving byte-identical-when-disabled.
/// </summary>
public class PartitionedTopKNarrowingPbtTests
{
    private const string Ddl = "CREATE TABLE t (g INT NOT NULL, v INT NOT NULL, id INT NOT NULL)";

    private static readonly string[] Functions = { "ROW_NUMBER", "RANK", "DENSE_RANK" };

    private sealed record QuerySpec(string Function, int K, bool Descending)
    {
        public string Sql =>
            $"SELECT g, v, id FROM (SELECT g, v, id, {Function}() OVER " +
            $"(PARTITION BY g ORDER BY v {(Descending ? "DESC" : "ASC")}) AS rn FROM t) s WHERE rn <= {K}";
    }

    private static readonly Gen<QuerySpec> GenSpec =
        Gen.Select(Gen.Int[0, 2], Gen.Int[1, 3], Gen.Bool)
            .Select(p => new QuerySpec(Functions[p.Item1], p.Item2, p.Item3));

    // Small domains so order-value ties (equal v) and retraction overlap are common.
    private static readonly Gen<InputEvent> GenEvent =
        Gen.Select(Gen.Int[0, 2], Gen.Int[0, 3], Gen.Int[0, 2], Gen.OneOfConst(1L, -1L))
            .Select(p => new InputEvent("t", new object?[] { p.Item1, p.Item2, p.Item3 }, p.Item4));

    private static readonly Gen<IReadOnlyList<IReadOnlyList<InputEvent>>> GenTicks =
        GenEvent.Array[0, 6]
            .Select(arr => (IReadOnlyList<InputEvent>)arr)
            .Array[1, 8]
            .Select(arr => (IReadOnlyList<IReadOnlyList<InputEvent>>)arr);

    [Fact]
    public void RandomQuery_NarrowEqualsBatchAndWholeRow()
    {
        Gen.Select(GenSpec, GenTicks)
            .Sample((spec, ticks) => CheckOne(spec, ticks), iter: 5000);
    }

    private static bool CheckOne(QuerySpec spec, IReadOnlyList<IReadOnlyList<InputEvent>> ticks)
    {
        var narrow = RunAccumulated(spec.Sql, ticks, narrowing: true);
        var wholeRow = RunAccumulated(spec.Sql, ticks, narrowing: false);
        var batch = BatchTopK(spec, ticks);

        if (!narrow.Equals(wholeRow))
        {
            Console.Error.WriteLine($"SQL: {spec.Sql}");
            Console.Error.WriteLine("narrow (seam on):  " + narrow);
            Console.Error.WriteLine("wholeRow (seam off): " + wholeRow);
            return false;
        }

        if (!narrow.Equals(batch))
        {
            Console.Error.WriteLine($"SQL: {spec.Sql}");
            Console.Error.WriteLine("narrow (seam on): " + narrow);
            Console.Error.WriteLine("batch oracle:     " + batch);
            return false;
        }

        return true;
    }

    private static ZSet<StructuralRow, Z64> RunAccumulated(
        string sql, IReadOnlyList<IReadOnlyList<InputEvent>> ticks, bool narrowing)
    {
        // The seam is read at operator construction, so enable it across Compile and
        // keep it for the circuit's lifetime, then restore. Thread-static: this test
        // body runs synchronously on one thread.
        var prev = PartitionedTopKNarrowingMode.Override;
        PartitionedTopKNarrowingMode.Override = narrowing
            ? PartitionedTopKNarrowing.ForceNarrow
            : PartitionedTopKNarrowing.ForceWholeRow;
        try
        {
            var query = IncrementalOracle.CompileQuery(new[] { Ddl }, sql);
            return IncrementalOracle.RunAndAccumulate(query, ticks);
        }
        finally
        {
            PartitionedTopKNarrowingMode.Override = prev;
        }
    }

    /// <summary>
    /// Independent batch TOP-K over the net input bag: accumulate ±1 events to a
    /// net multiset, drop non-positive rows, group by partition, sort by the same
    /// (order value, whole-row tiebreak) total order the operator uses, and apply the
    /// rank cut. The accumulated incremental output telescopes to the final window, so
    /// this is the expected accumulated output.
    /// </summary>
    private static ZSet<StructuralRow, Z64> BatchTopK(
        QuerySpec spec, IReadOnlyList<IReadOnlyList<InputEvent>> ticks)
    {
        // Net multiset of input rows.
        var net = new Dictionary<StructuralRow, long>();
        foreach (var tick in ticks)
        {
            foreach (var ev in tick)
            {
                var row = new StructuralRow(ev.Row);
                net[row] = net.GetValueOrDefault(row) + ev.Weight;
            }
        }

        // Partition by g (column 0); keep only positive-weight rows.
        var partitions = new Dictionary<object, List<(StructuralRow Row, long Weight)>>();
        foreach (var (row, weight) in net)
        {
            if (weight <= 0)
            {
                continue;
            }

            var key = row[0]!;
            if (!partitions.TryGetValue(key, out var list))
            {
                list = new List<(StructuralRow, long)>();
                partitions[key] = list;
            }

            list.Add((row, weight));
        }

        // Same total order the operator ranks within: ORDER BY v (column 1) with the
        // spec's direction, then a whole-row tiebreak.
        var order = new SortKeyComparer<StructuralRow>(
            new Func<StructuralRow, object?>[] { r => r[1] },
            new[] { spec.Descending },
            new[] { false },
            StructuralRowComparer.Instance);

        var builder = new ZSetBuilder<StructuralRow, Z64>();
        foreach (var list in partitions.Values)
        {
            list.Sort((a, b) => order.Compare(a.Row, b.Row));
            foreach (var (row, weight) in RankCut(list, spec))
            {
                builder.Add(row, new Z64(weight));
            }
        }

        return builder.Build();
    }

    /// <summary>The rows of one sorted partition kept by the rank cut, with their
    /// in-window weight — reimplemented independently of the operator.</summary>
    private static IEnumerable<(StructuralRow Row, long Weight)> RankCut(
        List<(StructuralRow Row, long Weight)> sorted, QuerySpec spec)
    {
        if (spec.K <= 0)
        {
            yield break;
        }

        if (spec.Function == "ROW_NUMBER")
        {
            long pos = 0;
            foreach (var (row, weight) in sorted)
            {
                var take = Math.Min(weight, spec.K - pos);
                if (take > 0)
                {
                    yield return (row, take);
                }

                pos += weight;
                if (pos >= spec.K)
                {
                    yield break;
                }
            }

            yield break;
        }

        var dense = spec.Function == "DENSE_RANK";
        long rowsBefore = 0;
        long denseRank = 0;
        object? groupV = null;
        var inGroup = false;
        foreach (var (row, weight) in sorted)
        {
            var v = row[1];
            if (!inGroup || !Equals(groupV, v))
            {
                denseRank++;
                var kept = dense ? denseRank <= spec.K : rowsBefore < spec.K;
                if (!kept)
                {
                    yield break;
                }

                groupV = v;
                inGroup = true;
            }

            yield return (row, weight);
            rowsBefore += weight;
        }
    }
}
