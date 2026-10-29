// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Persistence;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

namespace DbspNet.Tests.Sql;

/// <summary>
/// The compact aggregator sub-state of docs/design-state-representation.md §4:
/// unboxed, FILTER-shared COUNT(DISTINCT) tables and the (best, count) MIN/MAX
/// that rescans the trace — rescan-only, or the hybrid that promotes a group to
/// a sorted (value, count) run the first time its best is retracted.
/// Differential: the typed path with each MIN/MAX state (hybrid, rescan-only,
/// legacy sorted set) and the structural path must integrate to the same view
/// after every tick of random insert/delete streams with duplicated values,
/// NULLs and retractions of the current extreme.
/// </summary>
public sealed class CompactAggregatorStateTests : IDisposable
{
    private static readonly string[] Ddl =
        ["CREATE TABLE t (g INT NOT NULL, id INT NOT NULL, v INT, p INT NOT NULL, s VARCHAR)"];

    private const string Query =
        @"SELECT g,
                 COUNT(*) AS n,
                 COUNT(DISTINCT v) AS dv,
                 COUNT(DISTINCT v) FILTER (WHERE p < 3) AS dv_lo,
                 COUNT(DISTINCT v) FILTER (WHERE p >= 3 AND p < 6) AS dv_mid,
                 COUNT(*) FILTER (WHERE p >= 6) AS n_hi,
                 COUNT(DISTINCT v) FILTER (WHERE p >= 6) AS dv_hi,
                 COUNT(DISTINCT p) AS dp,
                 COUNT(DISTINCT p) FILTER (WHERE v IS NULL) AS dp_nullv,
                 COUNT(DISTINCT s) AS ds,
                 COUNT(DISTINCT v + p) AS dvp,
                 MIN(v) AS minv, MAX(v) AS maxv,
                 MIN(p) AS minp, MAX(p) FILTER (WHERE v > 10) AS maxp_v
          FROM t
          GROUP BY g";

    // MIN/MAX-only query for the sliding-window streams: plain, FILTERed and a
    // computed argument, so each group carries several independent states.
    private const string MinMaxQuery =
        @"SELECT g,
                 MIN(v) AS minv, MAX(v) AS maxv,
                 MIN(v) FILTER (WHERE p < 3) AS minv_lo,
                 MAX(v) FILTER (WHERE p >= 6) AS maxv_hi,
                 MIN(v - p) AS minvp,
                 MAX(p) AS maxp
          FROM t
          GROUP BY g";

    private readonly string _snapshotDir =
        Path.Combine(Path.GetTempPath(), "dbspnet-minmax-snap-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        if (Directory.Exists(_snapshotDir))
        {
            Directory.Delete(_snapshotDir, recursive: true);
        }
    }

    private static CompiledQuery Compile(string query, bool structural, bool snapshots = false)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in Ddl)
        {
            resolver.Resolve(Parser.ParseStatement(s));
        }

        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(query))).Query;
        return structural
            ? PlanToCircuit.Compile(plan, EmittedEqualityCodec.Instance)
            : snapshots ? PlanToCircuit.Compile(plan, ArrowSqlSnapshotCodecs.Instance) : PlanToCircuit.Compile(plan);
    }

    private static CompiledQuery CompileTyped(
        bool compactDistinct, MinMaxStateKind minMax, string query = Query, bool snapshots = false)
    {
        var (d, m) = (CompactAggregatorStateMode.CompactDistinct, CompactAggregatorStateMode.MinMax);
        CompactAggregatorStateMode.CompactDistinct = compactDistinct;
        CompactAggregatorStateMode.MinMax = minMax;
        try
        {
            return Compile(query, structural: false, snapshots);
        }
        finally
        {
            CompactAggregatorStateMode.CompactDistinct = d;
            CompactAggregatorStateMode.MinMax = m;
        }
    }

    private static void Accumulate(Dictionary<string, long> view, CompiledQuery q)
    {
        foreach (var (row, w) in q.Current)
        {
            var key = string.Join("|", Enumerable.Range(0, row.Count).Select(i => row[i]?.ToString() ?? "NULL"));
            var n = view.GetValueOrDefault(key) + w.Value;
            if (n == 0) view.Remove(key);
            else view[key] = n;
        }
    }

    private static List<KeyValuePair<string, long>> Sorted(Dictionary<string, long> view)
        => view.OrderBy(kv => kv.Key, StringComparer.Ordinal).ToList();

    [Fact]
    public void Hybrid_IsTheDefaultMinMaxState()
    {
        Assert.Equal(MinMaxStateKind.Hybrid, CompactAggregatorStateMode.MinMaxDefault);
    }

    [Theory]
    [InlineData(20261028, 4, 12)]
    [InlineData(7, 2, 40)]
    [InlineData(99, 9, 3)]
    public void Compact_Fallback_And_Structural_IntegrateToTheSameView(int seed, int groups, int valueRange)
    {
        var rng = new Random(seed);
        var queries = new[]
        {
            CompileTyped(compactDistinct: true, MinMaxStateKind.Hybrid),
            CompileTyped(compactDistinct: true, MinMaxStateKind.RescanOnly),
            CompileTyped(compactDistinct: false, MinMaxStateKind.Legacy),
            Compile(Query, structural: true),
        };
        var views = queries.Select(_ => new Dictionary<string, long>()).ToArray();

        var live = new List<object?[]>();
        var nextId = 0;
        for (var tick = 0; tick < 60; tick++)
        {
            var ops = rng.Next(1, 30);
            for (var op = 0; op < ops; op++)
            {
                // Bias deletes toward the extremes so MIN/MAX lose their best often.
                if (live.Count > 0 && rng.Next(3) == 0)
                {
                    var idx = rng.Next(2) == 0
                        ? live.IndexOf(live.OrderBy(r => (int?)r[2] ?? int.MaxValue).First())
                        : rng.Next(live.Count);
                    var row = live[idx];
                    live.RemoveAt(idx);
                    foreach (var q in queries) q.Table("t").Delete(row);
                }
                else
                {
                    int? v = rng.Next(8) == 0 ? null : rng.Next(valueRange);
                    var s = rng.Next(6) == 0 ? null : "s" + rng.Next(valueRange);
                    var row = new object?[] { rng.Next(groups), nextId++, v, rng.Next(9), s };
                    live.Add(row);
                    foreach (var q in queries) q.Table("t").Insert(row);
                }
            }

            for (var i = 0; i < queries.Length; i++)
            {
                queries[i].Step();
                Accumulate(views[i], queries[i]);
            }

            var reference = Sorted(views[^1]);
            for (var i = 0; i < queries.Length - 1; i++)
            {
                Assert.Equal(reference, Sorted(views[i]));
            }
        }

        // And a from-scratch batch over the survivors agrees with the incremental view.
        var batch = CompileTyped(compactDistinct: true, MinMaxStateKind.Hybrid);
        foreach (var row in live) batch.Table("t").Insert(row);
        batch.Step();
        var batchView = new Dictionary<string, long>();
        Accumulate(batchView, batch);
        Assert.Equal(Sorted(batchView), Sorted(views[0]));
    }

    /// <summary>
    /// A sliding-window stream: each tick every group appends rows and drops its
    /// oldest, so MIN loses its best on most ticks and groups promote mid-run.
    /// Also: rows inserted with weight 2 and retracted one copy at a time (the
    /// value stays held), random middle deletes, NULLs, duplicate values, and
    /// groups emptied completely (a promoted group demotes) and refilled.
    /// </summary>
    private sealed class SlidingStream(int seed, int groups)
    {
        private readonly Random _rng = new(seed);
        private readonly List<object?[]>[] _live = Enumerable.Range(0, groups).Select(_ => new List<object?[]>()).ToArray();
        private int _nextId;
        private int _clock;

        public List<(object?[] Row, long Weight)> NextTick()
        {
            var ops = new List<(object?[], long)>();
            _clock += 3;
            for (var g = 0; g < _live.Length; g++)
            {
                var live = _live[g];
                if (_rng.Next(40) == 0)
                {
                    // Empty the group.
                    foreach (var r in live) ops.Add((r, -1));
                    live.Clear();
                    continue;
                }

                var inserts = _rng.Next(0, 5);
                for (var i = 0; i < inserts; i++)
                {
                    int? v = _rng.Next(10) == 0 ? null : _clock + _rng.Next(-4, 5);
                    var row = new object?[] { g, _nextId++, v, _rng.Next(9), null };
                    live.Add(row);
                    ops.Add((row, 1));
                    if (_rng.Next(6) == 0)
                    {
                        // A second copy of the same row: weight 2 until one is retracted.
                        live.Add(row);
                        ops.Add((row, 1));
                    }
                }

                var deletes = live.Count > 12 ? _rng.Next(1, 5) : _rng.Next(0, 2);
                for (var i = 0; i < deletes && live.Count > 0; i++)
                {
                    var idx = _rng.Next(5) == 0 ? _rng.Next(live.Count) : 0;   // mostly the oldest
                    ops.Add((live[idx], -1));
                    live.RemoveAt(idx);
                }
            }

            return ops;
        }
    }

    private static void Push(CompiledQuery q, List<(object?[] Row, long Weight)> ops)
    {
        foreach (var (row, w) in ops)
        {
            if (w > 0) q.Table("t").Insert(row);
            else q.Table("t").Delete(row);
        }
    }

    [Theory]
    [InlineData(1, 3)]
    [InlineData(2026, 8)]
    [InlineData(42, 1)]
    public void SlidingWindow_PromotesMidRun_AndMatchesLegacyAndStructural(int seed, int groups)
    {
        var stream = new SlidingStream(seed, groups);
        var queries = new[]
        {
            CompileTyped(compactDistinct: true, MinMaxStateKind.Hybrid, MinMaxQuery),
            CompileTyped(compactDistinct: true, MinMaxStateKind.RescanOnly, MinMaxQuery),
            CompileTyped(compactDistinct: true, MinMaxStateKind.Legacy, MinMaxQuery),
            Compile(MinMaxQuery, structural: true),
        };
        var views = queries.Select(_ => new Dictionary<string, long>()).ToArray();
        for (var tick = 0; tick < 150; tick++)
        {
            var ops = stream.NextTick();
            for (var i = 0; i < queries.Length; i++)
            {
                Push(queries[i], ops);
                queries[i].Step();
                Accumulate(views[i], queries[i]);
            }

            var reference = Sorted(views[^1]);
            for (var i = 0; i < queries.Length - 1; i++)
            {
                Assert.Equal(reference, Sorted(views[i]));
            }
        }
    }

    private List<List<KeyValuePair<string, long>>> RunSliding(MinMaxStateKind kind, int snapshotAfter)
    {
        var stream = new SlidingStream(seed: 77, groups: 5);
        var q = CompileTyped(compactDistinct: true, kind, MinMaxQuery, snapshots: true);
        var view = new Dictionary<string, long>();
        var perTick = new List<List<KeyValuePair<string, long>>>();
        for (var tick = 0; tick < 120; tick++)
        {
            Push(q, stream.NextTick());
            q.Step();
            Accumulate(view, q);
            perTick.Add(Sorted(view));
            if (tick == snapshotAfter)
            {
                Snapshot.WriteAsync(q.Circuit, _snapshotDir).AsTask().GetAwaiter().GetResult();
                q = CompileTyped(compactDistinct: true, kind, MinMaxQuery, snapshots: true);
                Snapshot.ReadAsync(q.Circuit, _snapshotDir).AsTask().GetAwaiter().GetResult();
            }
        }

        return perTick;
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Snapshot_RestoresPromotedGroups_AndContinuesLikeLegacy(bool hybrid)
    {
        // By tick 60 the sliding stream has promoted every group (the hybrid's
        // first loss of the best happens within the first few ticks). Restore
        // folds a fresh compact state from the trace; the run must continue —
        // re-promoting on the next loss — exactly like an uninterrupted legacy run.
        var reference = RunSliding(MinMaxStateKind.Legacy, snapshotAfter: -1);
        var restarted = RunSliding(hybrid ? MinMaxStateKind.Hybrid : MinMaxStateKind.RescanOnly, snapshotAfter: 60);
        Assert.Equal(reference.Count, restarted.Count);
        for (var i = 0; i < reference.Count; i++)
        {
            Assert.Equal(reference[i], restarted[i]);
        }
    }

    private static ZSet<long, Z64> ZSetOf(IEnumerable<(long Value, long Weight)> entries)
    {
        var b = new ZSetBuilder<long, Z64>();
        foreach (var (v, w) in entries) b.Add(v, new Z64(w));
        return b.Build();
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Hybrid_PromotesOnFirstLossOfBest_AndNeverOnAppendOnly(bool wantMin)
    {
        // The row is the value itself (a set of longs), so the trace is the multiset.
        var agg = new TypedSqlMinMaxCompactAggregator<long, long>(r => r, wantMin, promote: true);
        var rescan = new TypedSqlMinMaxCompactAggregator<long, long>(r => r, wantMin, promote: false);
        var trace = new Dictionary<long, long>();
        object? state = null;
        object? rescanState = null;

        object? Step(IEnumerable<(long, long)> changes, ref object? s, TypedSqlMinMaxCompactAggregator<long, long> a)
        {
            var delta = ZSetOf(changes);
            var after = ZSetOf(trace.Select(kv => (kv.Key, kv.Value)));
            return a.Update(ref s, delta, after);
        }

        object? Apply((long, long)[] changes)
        {
            foreach (var (v, w) in changes)
            {
                var n = trace.GetValueOrDefault(v) + w;
                if (n == 0) trace.Remove(v);
                else trace[v] = n;
            }

            var r1 = Step(changes, ref state, agg);
            var r2 = Step(changes, ref rescanState, rescan);
            Assert.Equal(r2, r1);
            return r1;
        }

        // Append-only: 200 ticks of new values, half of them a new extreme. No promotion.
        for (long i = 0; i < 200; i++)
        {
            var v = wantMin ? -i : i;
            Assert.Equal(v, Apply([(v, 1), (v + (wantMin ? 1000 : -1000), 1)]));
            Assert.IsNotType<SortedValueCounts<long>>(state);
        }

        // Retract the best: one rescan promotes the group; the next-best comes out.
        var best = wantMin ? -199L : 199L;
        Assert.Equal(wantMin ? -198L : 198L, Apply([(best, -1)]));
        var sorted = Assert.IsType<SortedValueCounts<long>>(state);
        Assert.Equal(trace.Count, sorted.DistinctCount);
        Assert.Equal(trace.Keys.Order().Select(k => (k, 1L)), sorted.Entries());
        Assert.IsNotType<SortedValueCounts<long>>(rescanState);

        // Keep sliding the extreme away: stays promoted, answers from the sorted state.
        for (long i = 198; i > 100; i--)
        {
            var gone = wantMin ? -i : i;
            Assert.Equal(wantMin ? -(i - 1) : i - 1, Apply([(gone, -1)]));
            Assert.Same(sorted, state);
        }

        // Empty the group: a promoted group demotes to "no state".
        Assert.Null(Apply(trace.Keys.ToList().Select(k => (k, -1L)).ToArray()));
        Assert.Null(state);
        Assert.Equal(5L, Apply([(5, 1)]));
        Assert.IsNotType<SortedValueCounts<long>>(state);
    }

    [Fact]
    public void SortedValueCounts_MatchesSortedDictionaryUnderChurn()
    {
        // Random adds/removes over a small key space (duplicates, splits, merges,
        // emptied chunks), then a sliding phase (append at the top, drain the
        // bottom); Min/Max/entries must match a SortedDictionary oracle.
        var rng = new Random(7);
        var sorted = new SortedValueCounts<long>();
        var oracle = new SortedDictionary<long, long>();

        void Check()
        {
            Assert.Equal(oracle.Count, sorted.DistinctCount);
            if (oracle.Count > 0)
            {
                Assert.Equal(oracle.First().Key, sorted.Min);
                Assert.Equal(oracle.Last().Key, sorted.Max);
            }
        }

        for (var step = 0; step < 200_000; step++)
        {
            var key = (long)rng.Next(step < 100_000 ? 3_000 : 300);
            if (rng.Next(2) == 0 && oracle.TryGetValue(key, out var c))
            {
                Assert.True(sorted.Remove(key));
                if (c == 1) oracle.Remove(key);
                else oracle[key] = c - 1;
            }
            else if (rng.Next(50) == 0 && !oracle.ContainsKey(key))
            {
                Assert.False(sorted.Remove(key));   // never held: reported, nothing changes
            }
            else
            {
                sorted.Add(key);
                oracle[key] = oracle.GetValueOrDefault(key) + 1;
            }

            if (step % 991 == 0) Check();
            if (step % 9_973 == 0) Assert.Equal(oracle.Select(kv => (kv.Key, kv.Value)), sorted.Entries());
        }

        for (long top = 10_000; top < 40_000; top++)
        {
            sorted.Add(top);
            oracle[top] = 1;
            var low = oracle.First().Key;
            Assert.True(sorted.Remove(low));
            if (--oracle[low] == 0) oracle.Remove(low);
            if (top % 500 == 0) Check();
        }

        // Descending phase: prepend below the bottom (MAX over a window sliding down).
        for (long bottom = -1; bottom > -3_000; bottom--)
        {
            sorted.Add(bottom);
            oracle[bottom] = 1;
            if (bottom % 7 == 0)
            {
                var high = oracle.Last().Key;
                Assert.True(sorted.Remove(high));
                if (--oracle[high] == 0) oracle.Remove(high);
            }

            if (bottom % 250 == 0) Check();
        }

        Assert.Equal(oracle.Select(kv => (kv.Key, kv.Value)), sorted.Entries());

        // Bulk-built state agrees with incrementally built state.
        var values = oracle.SelectMany(kv => Enumerable.Repeat(kv.Key, (int)kv.Value)).Reverse().ToArray();
        var built = SortedValueCounts<long>.FromUnsorted(values, values.Length);
        Assert.Equal(sorted.Entries(), built.Entries());

        foreach (var (k, n) in oracle.ToList())
        {
            for (var i = 0; i < n; i++) Assert.True(sorted.Remove(k));
        }

        Assert.True(sorted.IsEmpty);
        sorted.Add(3);
        Assert.Equal(3, sorted.Min);
        Assert.Equal(3, sorted.Max);
    }

    [Fact]
    public void DistinctValueTable_MatchesDictionaryUnderChurn()
    {
        // Heavy insert/remove churn over a small key space exercises probe chains,
        // growth and backward-shift deletion; the per-member distinct counts must
        // match a Dictionary oracle after every operation.
        var rng = new Random(42);
        const int k = 3;
        var table = new DistinctValueTable<long>(k);
        var oracle = new Dictionary<long, int[]>();
        for (var step = 0; step < 200_000; step++)
        {
            var key = (long)rng.Next(5_000) * (rng.Next(2) == 0 ? 1 : 1L << 32);
            var mask = (ulong)rng.Next(1, 1 << k);
            var counts = oracle.GetValueOrDefault(key);
            var canRemove = counts is not null
                && Enumerable.Range(0, k).All(j => (mask & (1UL << j)) == 0 || counts[j] > 0);
            if (canRemove && rng.Next(2) == 0)
            {
                table.Remove(key, mask);
                for (var j = 0; j < k; j++) if ((mask & (1UL << j)) != 0) counts![j]--;
                if (counts!.All(c => c == 0)) oracle.Remove(key);
            }
            else
            {
                table.Insert(key, mask);
                counts ??= oracle[key] = new int[k];
                for (var j = 0; j < k; j++) if ((mask & (1UL << j)) != 0) counts[j]++;
            }

            if (step % 997 == 0 || step > 199_000)
            {
                Assert.Equal(oracle.Count, table.Count);
                for (var j = 0; j < k; j++)
                {
                    Assert.Equal(oracle.Values.Count(c => c[j] > 0), table.Distinct[j]);
                }
            }
        }

        // Drain everything; the table must end empty.
        foreach (var (key, counts) in oracle.ToList())
        {
            for (var j = 0; j < k; j++)
            {
                while (counts[j]-- > 0) table.Remove(key, 1UL << j);
            }
        }

        Assert.Equal(0, table.Count);
        Assert.All(table.Distinct, d => Assert.Equal(0, d));
    }
}
