using CsCheck;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;

namespace DbspNet.Tests.EndToEnd;

/// <summary>
/// Property-based tests exercising the four DBSP correctness laws across
/// the canonical v1 queries. Each test:
/// <list type="number">
/// <item>Generates a (small) random sequence of input events.</item>
/// <item>Runs the circuit tick-by-tick and accumulates the output.</item>
/// <item>Computes the batch oracle in plain LINQ over the net input.</item>
/// <item>Asserts incremental == batch, plus the splitting / empty-tick laws.</item>
/// </list>
/// Generators are intentionally small (few keys, small weights) to keep
/// iterations fast. CsCheck shrinks counterexamples automatically.
/// </summary>
public class FourLawsTests
{
    // ---- Generators ----

    // Use a small key-space so joins and group-bys actually exercise collisions.
    private static readonly Gen<int> GenKey = Gen.Int[0, 3];

    private static readonly Gen<int> GenValue = Gen.Int[0, 9];

    // Weight is ±1 (unit INSERT / DELETE).
    private static readonly Gen<long> GenWeight = Gen.OneOfConst(1L, -1L);

    // A "tick plan" is a list of ticks, each a list of events. Few ticks and
    // few events per tick keeps the shrinker fast.
    private static Gen<IReadOnlyList<IReadOnlyList<InputEvent>>> GenTickPlan(
        Gen<InputEvent> genEvent) =>
            genEvent.Array[0, 4]
                .Select(arr => (IReadOnlyList<InputEvent>)arr)
                .Array[1, 5]
                .Select(arr => (IReadOnlyList<IReadOnlyList<InputEvent>>)arr);

    // ---- Helpers ----

    private static ZSet<StructuralRow, Z64> Filter(
        ZSet<StructuralRow, Z64> z, Func<StructuralRow, bool> p) => z.Filter(p);

    private static ZSet<StructuralRow, Z64> Project(
        ZSet<StructuralRow, Z64> z, Func<StructuralRow, StructuralRow> f) => z.MapKeys(f);

    // Cartesian product on the left Z-set ⋈ right Z-set, with a combiner.
    private static ZSet<StructuralRow, Z64> JoinOracle(
        ZSet<StructuralRow, Z64> left, Func<StructuralRow, object?> leftKey, int leftKeyIndex,
        ZSet<StructuralRow, Z64> right, Func<StructuralRow, object?> rightKey, int rightKeyIndex,
        Func<StructuralRow, StructuralRow, StructuralRow> combine)
    {
        var b = new ZSetBuilder<StructuralRow, Z64>();
        foreach (var (lrow, lw) in left)
        {
            var lk = leftKey(lrow);
            if (lk is null)
            {
                continue;
            }

            foreach (var (rrow, rw) in right)
            {
                var rk = rightKey(rrow);
                if (rk is null || !lk.Equals(rk))
                {
                    continue;
                }

                b.Add(combine(lrow, rrow), lw * rw);
            }
        }

        _ = leftKeyIndex;
        _ = rightKeyIndex;
        return b.Build();
    }

    // Group-by oracle for SUM over a single numeric column.
    //
    // DBSP aggregate semantics: the per-group multiset is keyed by FULL ROW
    // (matching how the compiler wires GroupProject(row => row) into the
    // IncrementalAggregate operator). A group is emitted whenever its
    // sub-Z-set is non-empty — i.e. has at least one non-zero-weight row —
    // NOT whenever the net row-count weight is positive. For example,
    // joined rows {(1,6,1,"r"):-1, (2,6,2,"r"):+1} form a non-empty group
    // whose SUM(amount) happens to be zero, and the circuit emits
    // ("r", 0) as the correct Z-set-algebraic answer.
    private static ZSet<StructuralRow, Z64> GroupBySumOracle(
        ZSet<StructuralRow, Z64> input, int groupIndex, int valueIndex)
    {
        var groups = new Dictionary<object, ZSetBuilder<StructuralRow, Z64>>();
        foreach (var (row, w) in input)
        {
            var g = row[groupIndex]!;
            if (!groups.TryGetValue(g, out var builder))
            {
                builder = new ZSetBuilder<StructuralRow, Z64>();
                groups[g] = builder;
            }

            builder.Add(row, w);
        }

        var b = new ZSetBuilder<StructuralRow, Z64>();
        foreach (var (g, builder) in groups)
        {
            var sub = builder.Build();
            if (sub.IsEmpty)
            {
                continue;
            }

            var sum = 0L;
            foreach (var (row, w) in sub)
            {
                sum += Convert.ToInt64(row[valueIndex]!, System.Globalization.CultureInfo.InvariantCulture) * w.Value;
            }

            b.Add(new StructuralRow(g, sum), new Z64(1));
        }

        return b.Build();
    }

    // ---- Law 1+2: incremental ≡ batch (filter) ----

    [Fact]
    public void Filter_IncrementalEqualsBatch()
    {
        var genEvent = Gen.Select(GenKey, GenValue, GenWeight)
            .Select(t => new InputEvent("t", [t.Item1, t.Item2], t.Item3));

        GenTickPlan(genEvent).Sample(ticks =>
        {
            var q = IncrementalOracle.CompileQuery(
                ["CREATE TABLE t (id INT NOT NULL, v INT NOT NULL)"],
                "SELECT id, v FROM t WHERE v > 5");

            var accumulated = IncrementalOracle.RunAndAccumulate(q, ticks);

            // Oracle: filter net input by v > 5.
            var allEvents = ticks.SelectMany(t => t);
            var netInput = IncrementalOracle.NetTable(allEvents, "t");
            var oracle = Filter(netInput, row => (int)row[1]! > 5);

            return accumulated.Equals(oracle);
        }, iter: 1200);
    }

    // ---- Law 3: split-tick invariance ----

    [Fact]
    public void Filter_SplittingTicksPreservesOutput()
    {
        var genEvent = Gen.Select(GenKey, GenValue, GenWeight)
            .Select(t => new InputEvent("t", [t.Item1, t.Item2], t.Item3));

        genEvent.Array[0, 8].Sample(events =>
        {
            // Run 1: all events in a single tick.
            var q1 = IncrementalOracle.CompileQuery(
                ["CREATE TABLE t (id INT NOT NULL, v INT NOT NULL)"],
                "SELECT id, v FROM t WHERE v > 5");
            var ticks1 = new IReadOnlyList<InputEvent>[] { events };
            var acc1 = IncrementalOracle.RunAndAccumulate(q1, ticks1);

            // Run 2: one event per tick.
            var q2 = IncrementalOracle.CompileQuery(
                ["CREATE TABLE t (id INT NOT NULL, v INT NOT NULL)"],
                "SELECT id, v FROM t WHERE v > 5");
            var ticks2 = events.Select(e => (IReadOnlyList<InputEvent>)new[] { e }).ToArray();
            var acc2 = IncrementalOracle.RunAndAccumulate(q2, ticks2);

            return acc1.Equals(acc2);
        }, iter: 1000);
    }

    // ---- Law 4: empty tick ⇒ empty output ----

    [Fact]
    public void Filter_EmptyTickProducesEmptyOutput()
    {
        var q = IncrementalOracle.CompileQuery(
            ["CREATE TABLE t (id INT NOT NULL, v INT NOT NULL)"],
            "SELECT id, v FROM t WHERE v > 5");
        q.Step();
        Assert.Empty(q.Current);
    }

    // ---- Inner join ----

    [Fact]
    public void InnerJoin_IncrementalEqualsBatch()
    {
        var genA = Gen.Select(GenKey, GenValue, GenWeight)
            .Select(t => new InputEvent("a", [t.Item1, t.Item2], t.Item3));
        var genB = Gen.Select(GenKey, GenValue, GenWeight)
            .Select(t => new InputEvent("b", [t.Item1, t.Item2], t.Item3));
        var genEvent = Gen.OneOf(genA, genB);

        GenTickPlan(genEvent).Sample(ticks =>
        {
            var q = IncrementalOracle.CompileQuery(
                [
                    "CREATE TABLE a (k INT NOT NULL, v INT NOT NULL)",
                    "CREATE TABLE b (k INT NOT NULL, w INT NOT NULL)",
                ],
                "SELECT a.v, b.w FROM a JOIN b ON a.k = b.k");

            var accumulated = IncrementalOracle.RunAndAccumulate(q, ticks);

            var allEvents = ticks.SelectMany(t => t).ToArray();
            var netA = IncrementalOracle.NetTable(allEvents, "a");
            var netB = IncrementalOracle.NetTable(allEvents, "b");
            var oracle = JoinOracle(
                netA, r => r[0], 0,
                netB, r => r[0], 0,
                (l, r) => new StructuralRow(l[1], r[1]));

            return accumulated.Equals(oracle);
        }, iter: 1000);
    }

    [Fact]
    public void InnerJoin_EmptyTickProducesEmptyOutput()
    {
        var q = IncrementalOracle.CompileQuery(
            [
                "CREATE TABLE a (k INT NOT NULL, v INT NOT NULL)",
                "CREATE TABLE b (k INT NOT NULL, w INT NOT NULL)",
            ],
            "SELECT a.v, b.w FROM a JOIN b ON a.k = b.k");
        q.Step();
        Assert.Empty(q.Current);
    }

    // ---- LEFT JOIN ----

    [Fact]
    public void LeftJoin_IncrementalEqualsBatch()
    {
        var genA = Gen.Select(GenKey, GenValue, GenWeight)
            .Select(t => new InputEvent("a", [t.Item1, t.Item2], t.Item3));
        var genB = Gen.Select(GenKey, GenValue, GenWeight)
            .Select(t => new InputEvent("b", [t.Item1, t.Item2], t.Item3));
        var genEvent = Gen.OneOf(genA, genB);

        GenTickPlan(genEvent).Sample(ticks =>
        {
            var q = IncrementalOracle.CompileQuery(
                [
                    "CREATE TABLE a (k INT NOT NULL, v INT NOT NULL)",
                    "CREATE TABLE b (k INT NOT NULL, w INT NOT NULL)",
                ],
                "SELECT a.v, b.w FROM a LEFT JOIN b ON a.k = b.k");

            var accumulated = IncrementalOracle.RunAndAccumulate(q, ticks);

            var allEvents = ticks.SelectMany(t => t).ToArray();
            var netA = IncrementalOracle.NetTable(allEvents, "a");
            var netB = IncrementalOracle.NetTable(allEvents, "b");
            var oracle = LeftJoinOracle(
                netA, r => r[0], 0,
                netB, r => r[0], 0,
                (l, r) => new StructuralRow(l[1], r[1]),
                l => new StructuralRow(l[1], null));

            return accumulated.Equals(oracle);
        }, iter: 1000);
    }

    // Batch LEFT JOIN oracle with Z-set semantics: for every left key that
    // has at least one non-zero right-side entry, emit the inner product;
    // for every left key without a right-side match, emit left weight × 1
    // NULL-padded row. NULL-keyed left rows always go to the NULL-padded
    // branch (never match).
    private static ZSet<StructuralRow, Z64> LeftJoinOracle(
        ZSet<StructuralRow, Z64> left, Func<StructuralRow, object?> leftKey, int leftKeyIndex,
        ZSet<StructuralRow, Z64> right, Func<StructuralRow, object?> rightKey, int rightKeyIndex,
        Func<StructuralRow, StructuralRow, StructuralRow> joinCombine,
        Func<StructuralRow, StructuralRow> nullPadCombine)
    {
        // Index right by its key (dropping NULL-keyed rows).
        var rightByKey = new Dictionary<object, ZSetBuilder<StructuralRow, Z64>>();
        foreach (var (rrow, rw) in right)
        {
            var rk = rightKey(rrow);
            if (rk is null)
            {
                continue;
            }

            if (!rightByKey.TryGetValue(rk, out var bldr))
            {
                bldr = new ZSetBuilder<StructuralRow, Z64>();
                rightByKey[rk] = bldr;
            }

            bldr.Add(rrow, rw);
        }

        var rightGroups = new Dictionary<object, ZSet<StructuralRow, Z64>>();
        foreach (var (k, bldr) in rightByKey)
        {
            rightGroups[k] = bldr.Build();
        }

        var b = new ZSetBuilder<StructuralRow, Z64>();
        foreach (var (lrow, lw) in left)
        {
            var lk = leftKey(lrow);
            if (lk is not null
                && rightGroups.TryGetValue(lk, out var matches)
                && !matches.IsEmpty)
            {
                foreach (var (rrow, rw) in matches)
                {
                    b.Add(joinCombine(lrow, rrow), Z64.Multiply(lw, rw));
                }
            }
            else
            {
                b.Add(nullPadCombine(lrow), lw);
            }
        }

        _ = leftKeyIndex;
        _ = rightKeyIndex;
        return b.Build();
    }

    [Fact]
    public void LeftJoin_EmptyTickProducesEmptyOutput()
    {
        var q = IncrementalOracle.CompileQuery(
            [
                "CREATE TABLE a (k INT NOT NULL, v INT NOT NULL)",
                "CREATE TABLE b (k INT NOT NULL, w INT NOT NULL)",
            ],
            "SELECT a.v, b.w FROM a LEFT JOIN b ON a.k = b.k");
        q.Step();
        Assert.Empty(q.Current);
    }

    // ---- RIGHT JOIN ----

    [Fact]
    public void RightJoin_IncrementalEqualsBatch()
    {
        var genA = Gen.Select(GenKey, GenValue, GenWeight)
            .Select(t => new InputEvent("a", [t.Item1, t.Item2], t.Item3));
        var genB = Gen.Select(GenKey, GenValue, GenWeight)
            .Select(t => new InputEvent("b", [t.Item1, t.Item2], t.Item3));
        var genEvent = Gen.OneOf(genA, genB);

        GenTickPlan(genEvent).Sample(ticks =>
        {
            var q = IncrementalOracle.CompileQuery(
                [
                    "CREATE TABLE a (k INT NOT NULL, v INT NOT NULL)",
                    "CREATE TABLE b (k INT NOT NULL, w INT NOT NULL)",
                ],
                "SELECT a.v, b.w FROM a RIGHT JOIN b ON a.k = b.k");

            var accumulated = IncrementalOracle.RunAndAccumulate(q, ticks);

            var allEvents = ticks.SelectMany(t => t).ToArray();
            var netA = IncrementalOracle.NetTable(allEvents, "a");
            var netB = IncrementalOracle.NetTable(allEvents, "b");

            // RIGHT JOIN(a, b) ≡ LEFT JOIN(b, a) with output cols re-assembled
            // as (a.v, b.w). The oracle treats b as the preserved side.
            var oracle = LeftJoinOracle(
                netB, r => r[0], 0,
                netA, r => r[0], 0,
                (brow, arow) => new StructuralRow(arow[1], brow[1]),
                brow => new StructuralRow(null, brow[1]));

            return accumulated.Equals(oracle);
        }, iter: 1000);
    }

    // ---- UNION ALL ----

    [Fact]
    public void UnionAll_IncrementalEqualsBatch()
    {
        var genA = Gen.Select(GenKey, GenWeight)
            .Select(tp => new InputEvent("a", [tp.Item1], tp.Item2));
        var genB = Gen.Select(GenKey, GenWeight)
            .Select(tp => new InputEvent("b", [tp.Item1], tp.Item2));
        var genEvent = Gen.OneOf(genA, genB);

        GenTickPlan(genEvent).Sample(ticks =>
        {
            var q = IncrementalOracle.CompileQuery(
                [
                    "CREATE TABLE a (x INT NOT NULL)",
                    "CREATE TABLE b (x INT NOT NULL)",
                ],
                "SELECT x FROM a UNION ALL SELECT x FROM b");

            var accumulated = IncrementalOracle.RunAndAccumulate(q, ticks);

            // Oracle: bag union = Z-set addition over both sides' net states.
            var allEvents = ticks.SelectMany(t => t).ToArray();
            var netA = IncrementalOracle.NetTable(allEvents, "a");
            var netB = IncrementalOracle.NetTable(allEvents, "b");
            var oracle = netA + netB;

            return accumulated.Equals(oracle);
        }, iter: 1000);
    }

    // ---- Scalar subquery ----

    [Fact]
    public void ScalarSubquery_IncrementalEqualsBatch()
    {
        // SELECT x FROM t WHERE x > (SELECT MAX(v) FROM thresh)
        // Two tables: t (values) and thresh (source of dynamic threshold).
        var genT = Gen.Select(GenKey, GenWeight)
            .Select(tp => new InputEvent("t", [tp.Item1], tp.Item2));
        var genThresh = Gen.Select(GenKey, GenWeight)
            .Select(tp => new InputEvent("thresh", [tp.Item1], tp.Item2));
        var genEvent = Gen.OneOf(genT, genThresh);

        GenTickPlan(genEvent).Sample(ticks =>
        {
            var q = IncrementalOracle.CompileQuery(
                [
                    "CREATE TABLE t (x INT NOT NULL)",
                    "CREATE TABLE thresh (v INT NOT NULL)",
                ],
                "SELECT x FROM t WHERE x > (SELECT MAX(v) FROM thresh)");

            var accumulated = IncrementalOracle.RunAndAccumulate(q, ticks);

            var allEvents = ticks.SelectMany(t => t).ToArray();
            var netT = IncrementalOracle.NetTable(allEvents, "t");
            var netThresh = IncrementalOracle.NetTable(allEvents, "thresh");

            // Oracle: compute MAX(v) over thresh (respecting Z-set weights for
            // "row presence"), filter t by x > MAX. Empty thresh → MAX is NULL
            // → comparison is NULL → row filtered out.
            int? batchMax = null;
            foreach (var (row, w) in netThresh)
            {
                if (!Z64.IsPositive(w))
                {
                    continue;
                }

                var v = (int)row[0]!;
                if (batchMax is null || v > batchMax.Value)
                {
                    batchMax = v;
                }
            }

            var b = new ZSetBuilder<StructuralRow, Z64>();
            if (batchMax is not null)
            {
                foreach (var (row, w) in netT)
                {
                    var x = (int)row[0]!;
                    if (x > batchMax.Value)
                    {
                        b.Add(row, w);
                    }
                }
            }

            var oracle = b.Build();
            return accumulated.Equals(oracle);
        }, iter: 1000);
    }

    // ---- Group-by SUM ----

    [Fact]
    public void GroupBySum_IncrementalEqualsBatch()
    {
        var genEvent = Gen.Select(GenKey, GenValue, GenWeight)
            .Select(t => new InputEvent("t", [t.Item1, t.Item2], t.Item3));

        GenTickPlan(genEvent).Sample(ticks =>
        {
            var q = IncrementalOracle.CompileQuery(
                ["CREATE TABLE t (g INT NOT NULL, v INT NOT NULL)"],
                "SELECT g, SUM(v) AS s FROM t GROUP BY g");

            var accumulated = IncrementalOracle.RunAndAccumulate(q, ticks);

            var allEvents = ticks.SelectMany(t => t);
            var netInput = IncrementalOracle.NetTable(allEvents, "t");
            var oracle = GroupBySumOracle(netInput, groupIndex: 0, valueIndex: 1);

            return accumulated.Equals(oracle);
        }, iter: 1200);
    }

    // ---- Joined group-by ----

    [Fact]
    public void JoinedGroupBySum_IncrementalEqualsBatch()
    {
        // orders (cust, amount) JOIN customers (id, region) GROUP BY region SUM(amount)
        // Keep the event-space small so the oracle remains tractable.
        var genOrder = Gen.Select(GenKey, GenValue, GenWeight)
            .Select(t => new InputEvent("orders", [t.Item1, t.Item2], t.Item3));
        var genCustomer = Gen.Select(GenKey, Gen.Int[0, 2], GenWeight)
            .Select(t => new InputEvent("customers", [t.Item1, "r" + t.Item2], t.Item3));
        var genEvent = Gen.OneOf(genOrder, genCustomer);

        GenTickPlan(genEvent).Sample(ticks =>
        {
            var q = IncrementalOracle.CompileQuery(
                [
                    "CREATE TABLE orders (cust INT NOT NULL, amount INT NOT NULL)",
                    "CREATE TABLE customers (id INT NOT NULL, region VARCHAR NOT NULL)",
                ],
                "SELECT c.region, SUM(o.amount) AS total " +
                "FROM orders o JOIN customers c ON o.cust = c.id " +
                "GROUP BY c.region");

            var accumulated = IncrementalOracle.RunAndAccumulate(q, ticks);

            var allEvents = ticks.SelectMany(t => t).ToArray();
            var netOrders = IncrementalOracle.NetTable(allEvents, "orders");
            var netCustomers = IncrementalOracle.NetTable(allEvents, "customers");

            var joined = JoinOracle(
                netOrders, r => r[0], 0,
                netCustomers, r => r[0], 0,
                (o, c) => new StructuralRow(o[0], o[1], c[0], c[1]));

            // Group by region (column 3 in joined), SUM amount (column 1).
            var oracle = GroupBySumOracle(joined, groupIndex: 3, valueIndex: 1);

            return accumulated.Equals(oracle);
        }, iter: 1000);
    }
}
