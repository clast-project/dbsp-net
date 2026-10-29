// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Tests.Sql;

/// <summary>
/// The structural port of the hybrid MIN/MAX state (docs/design-state-representation.md
/// §4B.3): a group starts as (best, count) and promotes to the legacy
/// Dictionary + SortedSet the first time its best is retracted. Differential against the
/// legacy structural aggregator after every tick, and a batch oracle, on the program
/// path ivm-bench runs.
/// </summary>
public sealed class StructuralMinMaxHybridTests
{
    private static readonly string[] Program =
    [
        "CREATE TABLE t (g INT NOT NULL, id INT NOT NULL, v INT, p INT NOT NULL, s VARCHAR, ts TIMESTAMP, a VARCHAR)",
        @"CREATE VIEW m AS SELECT g,
                 MIN(v) AS minv, MAX(v) AS maxv,
                 MIN(v) FILTER (WHERE p < 3) AS minv_lo,
                 MAX(v) FILTER (WHERE p >= 6) AS maxv_hi,
                 MIN(v - p) AS minvp, MAX(p) AS maxp,
                 MIN(s) AS mins, MAX(s) AS maxs,
                 MIN(CASE WHEN a = 'Activate' THEN ts ELSE NULL END) AS placed,
                 MAX(CASE WHEN a = 'Cancelled' THEN ts ELSE NULL END) AS removed,
                 COUNT(*) AS n
          FROM t GROUP BY g",
    ];

    private static readonly HashSet<string> Outputs = new(StringComparer.Ordinal) { "m" };

    private static CompiledProgram Compile(StructuralMinMaxStateKind kind)
    {
        var prev = StructuralCompactAggregatorStateMode.MinMax;
        StructuralCompactAggregatorStateMode.MinMax = kind;
        try
        {
            return SqlProgram.Compile(Program, Outputs);
        }
        finally
        {
            StructuralCompactAggregatorStateMode.MinMax = prev;
        }
    }

    private static readonly Timestamp Epoch = Timestamp.Parse("2024-01-01 00:00:00");

    /// <summary>
    /// Random insert/delete stream. Rows are drawn from a small value domain so values
    /// repeat within a group and the same row recurs (weights above 1); deletes are
    /// biased toward rows holding a group's current extreme so promotions happen early;
    /// <paramref name="arbitrary"/> also deletes rows that are not present, so the
    /// integrated input carries negative weights.
    /// </summary>
    private sealed class Stream(int seed, int groups, bool arbitrary)
    {
        private readonly Random _rng = new(seed);
        private readonly List<object?[]> _present = [];
        private int _nextId;

        private object?[] NewRow()
        {
            var g = _rng.Next(groups);
            var v = _rng.Next(6) == 0 ? null : (object)_rng.Next(-5, 20);
            var p = _rng.Next(10);
            var s = _rng.Next(5) == 0 ? null : (object)Utf8String.Of("s" + _rng.Next(8));
            var ts = _rng.Next(5) == 0 ? null : (object)new Timestamp(Epoch.Microseconds + (_rng.Next(30) * 1_000_000L));
            var a = Utf8String.Of(_rng.Next(3) switch { 0 => "Activate", 1 => "Cancelled", _ => "Other" });
            // Reuse ids within a small range so identical rows recur.
            var id = _rng.Next(3) == 0 ? _rng.Next(4) : _nextId++;
            return [g, id, v, p, s, ts, a];
        }

        public List<(object?[] Row, long Weight)> NextTick()
        {
            var ops = new List<(object?[] Row, long Weight)>();
            var n = _rng.Next(1, 12);
            for (var i = 0; i < n; i++)
            {
                var r = _rng.Next(10);
                if (r < 5 || _present.Count == 0)
                {
                    var row = NewRow();
                    _present.Add(row);
                    ops.Add((row, 1));
                }
                else if (r < 9)
                {
                    // Bias: delete a row with an extreme v (or any row) of a random group.
                    int idx;
                    if (_rng.Next(2) == 0)
                    {
                        var g = (int)_present[_rng.Next(_present.Count)][0]!;
                        var cands = Enumerable.Range(0, _present.Count).Where(k => (int)_present[k][0]! == g && _present[k][2] is not null).ToList();
                        idx = cands.Count == 0
                            ? _rng.Next(_present.Count)
                            : (_rng.Next(2) == 0
                                ? cands.MinBy(k => (int)_present[k][2]!)
                                : cands.MaxBy(k => (int)_present[k][2]!));
                    }
                    else
                    {
                        idx = _rng.Next(_present.Count);
                    }

                    ops.Add((_present[idx], -1));
                    _present.RemoveAt(idx);
                }
                else if (arbitrary)
                {
                    ops.Add((NewRow(), -1));   // delete a row that was never inserted
                }
            }

            return ops;
        }
    }

    private static void Push(CompiledProgram q, List<(object?[] Row, long Weight)> ops)
        => q.Table("t").Push(ops);

    [Theory]
    [InlineData(1, 3, false)]
    [InlineData(7, 1, false)]
    [InlineData(2026, 10, false)]
    [InlineData(99, 4, false)]
    [InlineData(5, 3, true)]
    [InlineData(31, 6, true)]
    public void Hybrid_EqualsLegacy_EveryTick_AndBatchOracle(int seed, int groups, bool arbitrary)
    {
        var stream = new Stream(seed, groups, arbitrary);
        var hybrid = Compile(StructuralMinMaxStateKind.Hybrid);
        var legacy = Compile(StructuralMinMaxStateKind.Legacy);
        var input = new ZSetBuilder<StructuralRow, Z64>();
        for (var tick = 0; tick < 200; tick++)
        {
            var ops = stream.NextTick();
            Push(hybrid, ops);
            Push(legacy, ops);
            hybrid.Step();
            legacy.Step();
            foreach (var (row, w) in ops)
            {
                input.Add(new StructuralRow(row), new Z64(w));
            }

            Assert.Equal(legacy.Outputs["m"].CurrentView, hybrid.Outputs["m"].CurrentView);
        }

        // Batch oracle over the net input.
        var plan = SqlProgram.Resolve(Program, Outputs).Views.Single(v => v.ViewName == "m").Query;
        var ctx = new BatchEvalContext(
            new Dictionary<string, ZSet<StructuralRow, Z64>>(StringComparer.Ordinal) { ["t"] = input.Build() },
            new Dictionary<CteRef, ZSet<StructuralRow, Z64>>());
        var batch = BatchPlanEvaluator.Evaluate(plan, ctx);
        Assert.Equal(batch, hybrid.Outputs["m"].CurrentView);
    }

    [Fact]
    public void Hybrid_IsTheStructuralDefault()
        => Assert.Equal(StructuralMinMaxStateKind.Hybrid, StructuralCompactAggregatorStateMode.MinMaxDefault);

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Hybrid_PromotesOnFirstLossOfBest_AndNeverOnAppendOnly(bool wantMin)
    {
        var agg = new SqlMinMaxHybridAggregator(r => r[0], wantMin);
        object? state = null;
        var trace = new Dictionary<StructuralRow, long>();

        object? Tick(params (long V, long W)[] changes)
        {
            var b = new ZSetBuilder<StructuralRow, Z64>();
            foreach (var (v, w) in changes)
            {
                var row = new StructuralRow([v]);
                b.Add(row, new Z64(w));
                trace[row] = trace.GetValueOrDefault(row) + w;
                if (trace[row] == 0) trace.Remove(row);
            }

            var after = new ZSetBuilder<StructuralRow, Z64>();
            foreach (var (row, w) in trace) after.Add(row, new Z64(w));
            return agg.Update(ref state, null, b.Build(), after.Build());
        }

        // Append-only: never promotes.
        for (var i = 0; i < 50; i++)
        {
            Tick((i, 1));
            Assert.IsNotType<Dictionary<object, long>>(state);
            Assert.Equal("Compact", state!.GetType().Name);
        }

        // Retracting a non-extreme value stays compact.
        Tick((25, -1));
        Assert.Equal("Compact", state!.GetType().Name);

        // A second row holding the best (a distinct row, same value): losing one holder
        // keeps the group compact — the count, not a rescan, carries it.
        var bestVal = wantMin ? 0L : 49L;
        var twin = new StructuralRow([bestVal, "twin"]);
        {
            var b = new ZSetBuilder<StructuralRow, Z64>();
            b.Add(twin, new Z64(1));
            trace[twin] = 1;
            var after = new ZSetBuilder<StructuralRow, Z64>();
            foreach (var (row, w) in trace) after.Add(row, new Z64(w));
            agg.Update(ref state, null, b.Build(), after.Build());
            b = new ZSetBuilder<StructuralRow, Z64>();
            b.Add(twin, new Z64(-1));
            trace.Remove(twin);
            after = new ZSetBuilder<StructuralRow, Z64>();
            foreach (var (row, w) in trace) after.Add(row, new Z64(w));
            Assert.Equal(bestVal, agg.Update(ref state, null, b.Build(), after.Build()));
            Assert.Equal("Compact", state!.GetType().Name);
        }

        // Retracting the extreme promotes and yields the next best.
        var best = wantMin ? 0L : 49L;
        var next = wantMin ? 1L : 48L;
        Assert.Equal(next, Tick((best, -1)));
        Assert.NotEqual("Compact", state!.GetType().Name);

        // Emptying the group demotes.
        var rest = trace.Keys.Select(r => ((long)r[0]!, -trace[r])).ToArray();
        Assert.Null(Tick(rest));
        Assert.Null(state);
    }
}
