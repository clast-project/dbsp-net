// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Persistence;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

namespace DbspNet.Tests.Sql;

/// <summary>
/// The trace-free linear aggregate of docs/design-state-representation.md §4G.
/// Differential: the typed path with <c>LinearAggregateOp</c> must emit exactly the
/// same output delta as the trace-backed <c>IncrementalAggregateOp</c> on every tick
/// of random well-formed insert/delete streams (NULLs, FILTERs, COUNT(DISTINCT),
/// DECIMAL / DOUBLE / BIGINT accumulators, groups emptying and refilling), including
/// across a snapshot restore mid-stream.
/// </summary>
public sealed class LinearAggregateTests : IDisposable
{
    private static readonly string[] Ddl =
        ["CREATE TABLE t (g INT NOT NULL, id INT NOT NULL, v INT, w BIGINT NOT NULL, p INT NOT NULL, " +
         "s VARCHAR, d DECIMAL(10, 2) NOT NULL, dn DECIMAL(10, 2), x DOUBLE NOT NULL, xn DOUBLE)"];

    private const string Query =
        @"SELECT g,
                 COUNT(*) AS n,
                 COUNT(v) AS nv,
                 COUNT(*) FILTER (WHERE p < 3) AS n_lo,
                 SUM(w) AS sw,
                 SUM(p) AS sp,
                 AVG(v) AS av,
                 AVG(w) AS aw,
                 SUM(d) AS sd,
                 COUNT(dn) AS cdn,
                 AVG(dn) AS adn,
                 SUM(x) AS sx,
                 AVG(xn) AS axn,
                 COUNT(DISTINCT v) AS dv,
                 COUNT(DISTINCT v) FILTER (WHERE p < 3) AS dv_lo,
                 COUNT(DISTINCT v) FILTER (WHERE p >= 6) AS dv_hi,
                 COUNT(DISTINCT s) AS ds,
                 COUNT(DISTINCT v + p) AS dvp,
                 COUNT(DISTINCT d) AS dd,
                 SUM(v) AS sv,
                 SUM(xn) AS sxn,
                 SUM(dn) AS sdn
          FROM t
          GROUP BY g";

    // No DOUBLE accumulators: a trace-backed snapshot carries no aggregator state
    // on the typed path, so restoring it re-folds the trace, which is bit-exact only
    // for integer/DECIMAL sums (the §7.2 float caveat). Used for legacy -> linear.
    private const string ExactQuery =
        @"SELECT g, COUNT(*) AS n, COUNT(v) AS nv, COUNT(*) FILTER (WHERE p < 3) AS n_lo,
                 SUM(w) AS sw, SUM(d) AS sd, AVG(dn) AS adn,
                 COUNT(DISTINCT v) AS dv, COUNT(DISTINCT v) FILTER (WHERE p < 3) AS dv_lo,
                 COUNT(DISTINCT s) AS ds, COUNT(DISTINCT d) AS dd, SUM(v) AS sv, SUM(dn) AS sdn
          FROM t GROUP BY g";

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "dbspnet-lagg-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        if (Directory.Exists(_dir)) Directory.Delete(_dir, recursive: true);
    }

    private static CompiledQuery Compile(string query, bool linear, CompileOptions? options = null)
    {
        var prior = LinearAggregateMode.Enabled;
        LinearAggregateMode.Enabled = linear;
        try
        {
            var catalog = new Catalog();
            var resolver = new Resolver(catalog);
            foreach (var s in Ddl) resolver.Resolve(Parser.ParseStatement(s));
            var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(query))).Query;
            return PlanToCircuit.Compile(plan, ArrowSqlSnapshotCodecs.Instance, options);
        }
        finally
        {
            LinearAggregateMode.Enabled = prior;
        }
    }

    private static string AggOp(CompiledQuery q)
    {
        var names = q.Circuit.Operators.Select(o => o.GetType().Name)
            .Where(n => n.Contains("Aggregate", StringComparison.Ordinal)).ToList();
        return Assert.Single(names).Split('`')[0];
    }

    private static List<string> Delta(CompiledQuery q)
        => q.Current
            .Select(e => string.Join("|", Enumerable.Range(0, e.Key.Count).Select(i => Format(e.Key[i]))) + "#" + e.Value.Value)
            .OrderBy(x => x, StringComparer.Ordinal)
            .ToList();

    // Doubles by bit pattern: the linear fold must be bit-identical, not just close.
    private static string Format(object? v) => v switch
    {
        null => "NULL",
        double dbl => BitConverter.DoubleToInt64Bits(dbl).ToString(System.Globalization.CultureInfo.InvariantCulture),
        _ => v.ToString() ?? string.Empty,
    };

    private static object?[] RandomRow(Random rng, int groups, int range, int id)
        =>
        [
            rng.Next(groups), id,
            rng.Next(6) == 0 ? null : rng.Next(range),
            (long)rng.Next(-1000, 1000),
            rng.Next(9),
            rng.Next(6) == 0 ? null : "s" + rng.Next(range),
            (rng.Next(-5000, 5000) / 100m).ToString("0.00", System.Globalization.CultureInfo.InvariantCulture),
            rng.Next(5) == 0 ? null : (rng.Next(0, 300) / 100m).ToString("0.00", System.Globalization.CultureInfo.InvariantCulture),
            rng.NextDouble() * 1e3 - 500,
            rng.Next(5) == 0 ? null : rng.NextDouble(),
        ];

    /// <summary>
    /// Drives <paramref name="ticks"/> ticks of a random well-formed stream (deletes
    /// only of live rows, duplicates allowed, bursts that empty whole groups) into
    /// every query, asserting the output deltas agree tick by tick.
    /// </summary>
    private static void Drive(Random rng, CompiledQuery[] queries, List<object?[]> live, ref int nextId,
        int ticks, int groups, int range)
    {
        for (var tick = 0; tick < ticks; tick++)
        {
            if (rng.Next(7) == 0 && live.Count > 0)
            {
                // Empty one group entirely (it may refill later).
                var g = live[rng.Next(live.Count)][0];
                foreach (var row in live.Where(r => Equals(r[0], g)).ToList())
                {
                    live.Remove(row);
                    foreach (var q in queries) q.Table("t").Delete(row);
                }
            }

            var ops = rng.Next(0, 25);
            for (var op = 0; op < ops; op++)
            {
                if (live.Count > 0 && rng.Next(3) == 0)
                {
                    var idx = rng.Next(live.Count);
                    var row = live[idx];
                    live.RemoveAt(idx);
                    foreach (var q in queries) q.Table("t").Delete(row);
                }
                else
                {
                    // Re-insert a live row now and then: integrated weight 2.
                    var row = live.Count > 0 && rng.Next(8) == 0
                        ? live[rng.Next(live.Count)]
                        : RandomRow(rng, groups, range, nextId++);
                    live.Add(row);
                    foreach (var q in queries) q.Table("t").Insert(row);
                }
            }

            foreach (var q in queries) q.Step();
            var expected = Delta(queries[0]);
            for (var i = 1; i < queries.Length; i++)
            {
                Assert.Equal(expected, Delta(queries[i]));
            }
        }
    }

    [Fact]
    public void Selection_LinearShapesTakeTheTraceFreeOperator()
    {
        Assert.Equal("LinearAggregateOp", AggOp(Compile(Query, linear: true)));
        Assert.Equal("IncrementalAggregateOp", AggOp(Compile(Query, linear: false)));

        // NULLable SUM (BIGINT, DOUBLE, DECIMAL accumulators) is linear too.
        Assert.Equal("LinearAggregateOp", AggOp(Compile("SELECT g, SUM(v) FROM t GROUP BY g", true)));
        Assert.Equal("LinearAggregateOp", AggOp(Compile("SELECT g, SUM(xn) FROM t GROUP BY g", true)));
        Assert.Equal("LinearAggregateOp", AggOp(Compile("SELECT g, SUM(dn) FROM t GROUP BY g", true)));

        // Non-linear members and the spine family keep the trace.
        Assert.Equal("IncrementalAggregateOp", AggOp(Compile("SELECT g, COUNT(*), MIN(v) FROM t GROUP BY g", true)));
        Assert.Equal(
            "SpineIncrementalAggregateOp",
            AggOp(Compile("SELECT g, COUNT(*) FROM t GROUP BY g", true, new CompileOptions { TraceFamily = TraceFamily.Spine })));
    }

    [Theory]
    [InlineData(20261028, 4, 12)]
    [InlineData(7, 2, 40)]
    [InlineData(99, 9, 3)]
    [InlineData(5, 1, 6)]
    public void Linear_EqualsTraceBacked_EveryTick(int seed, int groups, int range)
    {
        var rng = new Random(seed);
        var queries = new[] { Compile(Query, linear: false), Compile(Query, linear: true) };
        Assert.Equal("LinearAggregateOp", AggOp(queries[1]));
        var live = new List<object?[]>();
        var nextId = 0;
        Drive(rng, queries, live, ref nextId, 80, groups, range);
    }

    [Theory]
    [InlineData(1, 4, 12, true)]
    [InlineData(2, 2, 5, true)]
    [InlineData(3, 3, 8, false)]
    public async Task RestoreMidStream_ContinuesExactly(int seed, int groups, int range, bool producerLinear)
    {
        // producerLinear=false restores a trace-backed snapshot into the linear
        // operator (the fingerprint alias + trace fold).
        var rng = new Random(seed);
        var query = producerLinear ? Query : ExactQuery;
        var reference = Compile(query, linear: false);
        var producer = Compile(query, linear: producerLinear);
        var live = new List<object?[]>();
        var nextId = 0;
        Drive(rng, [reference, producer], live, ref nextId, 30, groups, range);

        await Snapshot.WriteAsync(producer.Circuit, _dir);
        var restored = Compile(query, linear: true);
        Assert.Equal("LinearAggregateOp", AggOp(restored));
        await Snapshot.ReadAsync(restored.Circuit, _dir);

        Drive(rng, [reference, restored], live, ref nextId, 40, groups, range);
    }

    [Theory]
    [InlineData(11)]
    [InlineData(12)]
    [InlineData(13)]
    public void NonDistinctMembers_MatchTraceBacked_OnArbitraryZSetStreams(int seed)
    {
        // Arbitrary +-1 streams (deletes of rows never inserted, so integrated row
        // weights go negative and groups cancel to weight 0 while non-empty). Every
        // non-DISTINCT linear member is exact here: its state is a linear function of
        // the integrated group, and the op keeps a weight-0 group whose sums have not
        // cancelled (dropping it would lose them). NULLable SUM is deliberately absent:
        // its NULL rule follows summed weight, which diverges here (see
        // Corner_SumNullableUnderNegativeRowWeights_FollowsSummedNonNullWeight).
        const string q =
            @"SELECT g, COUNT(*) AS n, COUNT(v) AS nv, COUNT(*) FILTER (WHERE p < 3) AS n_lo,
                     SUM(w) AS sw, SUM(d) AS sd, AVG(v) AS av, AVG(dn) AS adn, SUM(x) AS sx, AVG(xn) AS axn
              FROM t GROUP BY g";
        var rng = new Random(seed);
        var queries = new[] { Compile(q, linear: false), Compile(q, linear: true) };
        Assert.Equal("LinearAggregateOp", AggOp(queries[1]));
        var pool = Enumerable.Range(0, 12).Select(i => RandomRow(rng, 3, 5, i)).ToList();
        for (var tick = 0; tick < 200; tick++)
        {
            for (var op = rng.Next(0, 5); op > 0; op--)
            {
                var row = pool[rng.Next(pool.Count)];
                var insert = rng.Next(2) == 0;
                foreach (var c in queries)
                {
                    if (insert) c.Table("t").Insert(row);
                    else c.Table("t").Delete(row);
                }
            }

            foreach (var c in queries) c.Step();
            Assert.Equal(Delta(queries[0]), Delta(queries[1]));
        }
    }

    [Fact]
    public void Corner_CountDistinctUnderNegativeRowWeights_CountsSummedValueWeight()
    {
        // Documented divergence (§4G): with an integrated input that holds a NEGATIVE
        // row weight, the trace-backed COUNT(DISTINCT) counts values held by some
        // positive-weight row, the linear one values whose summed weight is positive.
        // Rows id=1 (+1) and id=2 (-1) both carry v=5 and cancel; id=3 carries v=7.
        var queries = new[]
        {
            Compile("SELECT g, COUNT(DISTINCT v) AS dv FROM t GROUP BY g", linear: false),
            Compile("SELECT g, COUNT(DISTINCT v) AS dv FROM t GROUP BY g", linear: true),
        };
        foreach (var q in queries)
        {
            q.Table("t").Insert(0, 1, 5, 0L, 0, null, "0.00", null, 0.0, null);
            q.Table("t").Delete(0, 2, 5, 0L, 0, null, "0.00", null, 0.0, null);
            q.Table("t").Insert(0, 3, 7, 0L, 0, null, "0.00", null, 0.0, null);
            q.Step();
        }

        Assert.Equal(["0|2#1"], Delta(queries[0]));
        Assert.Equal(["0|1#1"], Delta(queries[1]));
    }

    [Fact]
    public void Corner_SumNullableUnderNegativeRowWeights_FollowsSummedNonNullWeight()
    {
        // Documented divergence (§4G, "summed-weight presence"): SUM over a NULLable
        // argument is NULL iff the summed weight of the group's non-NULL rows is 0.
        // The trace-backed operator asks instead whether some non-NULL row has
        // non-zero weight. With rows id=1 (+1, v=4) and id=2 (-1, v=2) the non-NULL
        // weight sums to 0 although both rows are held; id=3 (v NULL) keeps the group
        // present. Trace: 4*1 + 2*(-1) = 2. Linear: NULL. Same for every accumulator.
        const string q = "SELECT g, SUM(v) AS sv, SUM(xn) AS sxn, SUM(dn) AS sdn FROM t GROUP BY g";
        var queries = new[] { Compile(q, linear: false), Compile(q, linear: true) };
        Assert.Equal("LinearAggregateOp", AggOp(queries[1]));
        foreach (var c in queries)
        {
            c.Table("t").Insert(0, 1, 4, 0L, 0, null, "0.00", "4.00", 0.0, 4.0);
            c.Table("t").Delete(0, 2, 2, 0L, 0, null, "0.00", "2.00", 0.0, 2.0);
            c.Table("t").Insert(0, 3, null, 0L, 0, null, "0.00", null, 0.0, null);
            c.Step();
        }

        Assert.Equal(["0|2|" + Format(2.0) + "|200#1"], Delta(queries[0]));
        Assert.Equal(["0|NULL|NULL|NULL#1"], Delta(queries[1]));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task NullableSum_AllNullThenValueThenNull_MatchesTraceBacked(bool restoreBetweenTicks)
    {
        // A group of only NULL arguments sums to NULL; it turns non-NULL when a
        // non-NULL row arrives and back to NULL when that row leaves, while the group
        // stays present throughout. BIGINT, DOUBLE and DECIMAL accumulators, with an
        // optional snapshot/restore of the linear op before every tick.
        const string q = "SELECT g, COUNT(*) AS n, SUM(v) AS sv, SUM(xn) AS sxn, SUM(dn) AS sdn FROM t GROUP BY g";
        var legacy = Compile(q, linear: false);
        var linear = Compile(q, linear: true);
        object?[] null1 = [0, 1, null, 0L, 0, null, "0.00", null, 0.0, null];
        object?[] null2 = [0, 2, null, 0L, 0, null, "0.00", null, 0.0, null];
        object?[] val = [0, 3, 7, 0L, 0, null, "0.00", "1.25", 0.0, 0.5];
        var half = Format(0.5);
        var script = new (object?[] Row, bool Insert, string Expected)[]
        {
            (null1, true, "0|1|NULL|NULL|NULL#1"),
            (null2, true, "0|1|NULL|NULL|NULL#-1;0|2|NULL|NULL|NULL#1"),
            (val, true, "0|2|NULL|NULL|NULL#-1;0|3|7|" + half + "|125#1"),
            (val, false, "0|2|NULL|NULL|NULL#1;0|3|7|" + half + "|125#-1"),
            (null1, false, "0|1|NULL|NULL|NULL#1;0|2|NULL|NULL|NULL#-1"),
            (val, true, "0|1|NULL|NULL|NULL#-1;0|2|7|" + half + "|125#1"),
            (null2, false, "0|1|7|" + half + "|125#1;0|2|7|" + half + "|125#-1"),
            (val, false, "0|1|7|" + half + "|125#-1"),
            (null1, true, "0|1|NULL|NULL|NULL#1"),
        };

        var tick = 0;
        foreach (var (row, insert, expected) in script)
        {
            if (restoreBetweenTicks)
            {
                var dir = Path.Combine(_dir, "t" + tick);
                await Snapshot.WriteAsync(linear.Circuit, dir);
                linear = Compile(q, linear: true);
                await Snapshot.ReadAsync(linear.Circuit, dir);
            }

            tick++;
            foreach (var c in new[] { legacy, linear })
            {
                if (insert) c.Table("t").Insert(row);
                else c.Table("t").Delete(row);
                c.Step();
            }

            Assert.Equal(expected.Split(';'), Delta(legacy));
            Assert.Equal(Delta(legacy), Delta(linear));
        }
    }

    [Fact]
    public void AvgDecimalNotNull_GroupEmptying_RetractsInsteadOfDividingByZero()
    {
        // The trace-backed TypedAvgDecimalAggregator used to divide Sum / Count on
        // every Update, so emptying a group of AVG(DECIMAL NOT NULL) threw
        // DivideByZeroException. It now yields null for an emptied group; the
        // linear op asks for a result only while the group is present. Both
        // must retract the group's row.
        const string q = "SELECT g, AVG(d) AS ad FROM t GROUP BY g";
        var legacy = Compile(q, linear: false);
        var linear = Compile(q, linear: true);
        object?[] row = [0, 1, null, 0L, 0, null, "1.50", null, 0.0, null];
        foreach (var c in new[] { legacy, linear })
        {
            c.Table("t").Insert(row);
            c.Step();
            c.Table("t").Delete(row);
            c.Step();
            Assert.Equal(-1, Assert.Single(c.Current).Value.Value);
        }

        Assert.Equal(Delta(legacy), Delta(linear));
    }

    [Fact]
    public void DistinctWeightTable_MatchesDictionaryUnderChurn()
    {
        var rng = new Random(17);
        const int k = 3;
        var table = new DistinctWeightTable<long>(k);
        var oracle = new Dictionary<long, long[]>();
        for (var step = 0; step < 200_000; step++)
        {
            var key = (long)rng.Next(4_000) * (rng.Next(2) == 0 ? 1 : 1L << 32);
            var mask = (ulong)rng.Next(1, 1 << k);
            var w = (long)rng.Next(-2, 3);
            table.Add(key, mask, w);
            var ws = oracle.GetValueOrDefault(key) ?? (oracle[key] = new long[k]);
            for (var j = 0; j < k; j++) if ((mask & (1UL << j)) != 0) ws[j] += w;
            if (ws.All(x => x == 0)) oracle.Remove(key);

            if (step % 991 == 0 || step > 199_000)
            {
                Assert.Equal(oracle.Count, table.Count);
                for (var j = 0; j < k; j++)
                {
                    Assert.Equal(oracle.Values.Count(c => c[j] > 0), table.Distinct[j]);
                }
            }
        }

        // Round-trip through the persisted form.
        var writer = new System.Buffers.ArrayBufferWriter<byte>();
        table.Write(writer);
        var copy = new DistinctWeightTable<long>(k);
        ReadOnlySpan<byte> span = writer.WrittenSpan;
        copy.ReadInto(ref span);
        Assert.Equal(0, span.Length);
        Assert.Equal(table.Count, copy.Count);
        Assert.Equal(table.Distinct, copy.Distinct);
    }
}
