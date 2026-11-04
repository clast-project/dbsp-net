// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
// In-step delta spilling, D1–D3 (docs/design-bounded-memory.md §5.14). Required:
//   1. a block-backed Z-set (D1) answers every read — Count, WeightOf/Contains, enumeration, equality —
//      exactly as the managed set of the same adds, and every derived set (sum, difference, negation,
//      filter, scale, map, in-place merge, clone) equals the managed result;
//   2. the spilling builder (D2) builds managed below the threshold and block backed above it (or
//      always, forced), consolidating across frozen chunks, cancellations included;
//   3. with the knob on, compiled circuits (D3) give the same output every tick as a twin with it off,
//      across the random-query generator, and actually build block-backed deltas;
//   4. a fan-out join past the threshold emits a block-backed delta; below it, a managed one;
//   5. the left join's cross-product guard trips only on a blowup (output far past both groups) into a
//      managed builder: not on a big 1:N lookup, and not when the output spills (§5.16).
using CsCheck;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful.Blocks;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Tests.EndToEnd;
using DbspNet.Tests.Persistence;
using DbspNet.Tests.Sql;
using Xunit;

namespace DbspNet.Tests.Operators.Stateful;

[Collection(nameof(BlockedCheckpointTests))] // the guard tests set a process-wide limit
public class BlockedDeltaTests
{
    private static readonly Schema RowSchema = new(
    [
        new SchemaColumn("k", new SqlIntegerType(false)),
        new SchemaColumn("s", new SqlVarcharType(null, true)),
    ]);

    private static readonly ITraceBlockFormat Format =
        TraceBlockFormats.Create(RowSchema, Schema.Empty) ?? throw new InvalidOperationException("no block format: Persistence not loaded");

    private static readonly string?[] Strings = [null, "a", "bb", new string('x', 300)];

    private static StructuralRow Row(int k, int s) => new(SqlTestHelpers.EncodeStrings([k, Strings[s]]));

    // (k, s, weight) adds: few keys, so duplicates and cancellations are common.
    private static readonly Gen<(int K, int S, long W)[]> GenAdds =
        Gen.Select(Gen.Int[0, 12], Gen.Int[0, 3], Gen.OneOfConst(-2L, -1L, 1L, 1L, 2L)).Array[0, 120];

    private static ZSet<StructuralRow, Z64> Managed((int K, int S, long W)[] adds)
    {
        var b = new ZSetBuilder<StructuralRow, Z64>();
        foreach (var (k, s, w) in adds)
        {
            b.Add(Row(k, s), new Z64(w));
        }

        return b.Build();
    }

    // Built through the spilling builder under threshold rows (0 = forced) freezing every chunk rows.
    private static ZSet<StructuralRow, Z64> Spilled((int K, int S, long W)[] adds, long rows, int chunk)
    {
        using var _ = SpillSettings.UseStepSpill(rows, chunk);
        var b = DeltaSpill.Rows(Format, adds.Length);
        foreach (var (k, s, w) in adds)
        {
            b.Add(Row(k, s), new Z64(w));
        }

        return b.Build();
    }

    private static void AssertSame(ZSet<StructuralRow, Z64> expected, ZSet<StructuralRow, Z64> actual)
    {
        Assert.Equal(expected.Count, actual.Count);
        Assert.Equal(expected.IsEmpty, actual.IsEmpty);
        Assert.True(expected.Equals(actual), $"expected {expected} got {actual}");
        Assert.True(actual.Equals(expected));
        for (var k = 0; k <= 12; k++)
        {
            for (var s = 0; s < Strings.Length; s++)
            {
                var row = Row(k, s);
                Assert.Equal(expected.WeightOf(row), actual.WeightOf(row));
                Assert.Equal(expected.Contains(row), actual.Contains(row));
            }
        }

        var listed = actual.Select(e => e.Key + "#" + e.Value.Value).Order(StringComparer.Ordinal).ToList();
        Assert.Equal(expected.Select(e => e.Key + "#" + e.Value.Value).Order(StringComparer.Ordinal), listed);
        Assert.Equal(listed.Count, listed.Distinct(StringComparer.Ordinal).Count());
        Assert.Equal(expected.Keys.Count(), actual.Keys.Count());
        Assert.Equal(expected.SumWeights(), actual.SumWeights());
    }

    [Fact]
    public void Block_backed_sets_read_and_derive_as_managed_ones()
    {
        var multiRun = false;
        Gen.Select(GenAdds, GenAdds, Gen.Int[1, 9], Gen.Bool).Sample((a, b, chunk, forced) =>
        {
            var rows = forced ? 0L : chunk;
            var ma = Managed(a);
            var mb = Managed(b);
            var sa = Spilled(a, rows, chunk);
            var sb = Spilled(b, rows, chunk);
            AssertSame(ma, sa);

            // Forced: every non-empty set is blocks. Thresholded: blocks once a chunk froze.
            if (forced && !ma.IsEmpty)
            {
                Assert.True(sa.IsExternal);
            }

            if (!forced && a.Length < chunk)
            {
                Assert.False(sa.IsExternal);
            }

            // Multi-chunk sets keep their chunks as separate runs (§5.17: never merged).
            if (sa.External is BlockedRows br && a.Length > 2 * chunk && chunk < 4)
            {
                multiRun |= br.RunCount > 1;
            }

            using var _ = SpillSettings.UseStepSpill(rows, chunk);
            AssertSame(ma + mb, sa + sb);
            AssertSame(ma + mb, sa + mb);
            AssertSame(ma + mb, ma + sb);
            AssertSame(ma - mb, sa - sb);
            AssertSame(ma - mb, ma - sb);
            AssertSame(ma - mb, sa - mb);
            AssertSame(-ma, -sa);
            AssertSame(ma.ScalarMultiply(new Z64(3)), sa.ScalarMultiply(new Z64(3)));
            AssertSame(ma.ScalarMultiply(Z64.Zero), sa.ScalarMultiply(Z64.Zero));
            AssertSame(ma.Filter(r => (int)r[0]! % 2 == 0), sa.Filter(r => (int)r[0]! % 2 == 0));
            AssertSame(ma.MapKeys(r => Row((int)r[0]! / 3, 1)), sa.MapKeys(r => Row((int)r[0]! / 3, 1)));
            AssertSame(ma, sa.Clone());

            // In place, both ways: a block-backed delta into managed state, and (the fallback) into a
            // block-backed set, which moves into a managed map first.
            var state = ma.Clone();
            state.MergeInPlace(sb);
            AssertSame(ma + mb, state);
            var blockedState = sa.Clone();
            blockedState.MergeInPlace(mb);
            AssertSame(ma + mb, blockedState);
            AssertSame(ma, sa); // the shared store was not mutated
            var dropped = Spilled(a, rows, chunk);
            dropped.RemoveKeysBelow(6, r => (int)r[0]!);
            AssertSame(ma.Filter(r => (int)r[0]! >= 6), dropped);
        }, iter: 400);
        Assert.True(multiRun, "no case built a set of several runs");
    }

    [Fact]
    public void Builder_count_and_cancellation_across_chunks()
    {
        using var _ = SpillSettings.UseStepSpill(4, 4);
        var b = DeltaSpill.Rows(Format, 1_000_000);
        for (var k = 0; k < 10; k++)
        {
            b.Add(Row(k, 1), Z64.One);
        }

        Assert.True(b.Count >= 10); // an upper bound once chunks froze
        for (var k = 0; k < 10; k++)
        {
            b.Add(Row(k, 1), new Z64(-1));
        }

        var built = b.Build();
        Assert.True(built.IsEmpty);
        Assert.Equal(0, built.Count);
        Assert.Throws<InvalidOperationException>(() => b.Add(Row(1, 1), Z64.One));
    }

    [Fact]
    public void Knob_off_builds_the_plain_builder()
    {
        using var _ = SpillSettings.UseStepSpill(null);
        var b = DeltaSpill.Rows(Format, 0);
        for (var k = 0; k < 10_000; k++)
        {
            b.Add(Row(k, 0), Z64.One);
        }

        Assert.False(b.Build().IsExternal);
        Assert.False(DeltaSpill.For<StructuralRow, Z64>(null, 0).Build().IsExternal);
    }

    private static readonly string[] JoinDdl =
    [
        "CREATE TABLE f (k INT NOT NULL, x BIGINT NOT NULL, s VARCHAR)",
        "CREATE TABLE d (k INT NOT NULL, y BIGINT NOT NULL)",
    ];

    private const string JoinSql = "SELECT f.k, f.x + d.y AS z, f.s FROM f JOIN d ON f.k = d.k WHERE f.x >= 0";

    private static readonly CompileOptions Structural = CompileOptions.Default with { ShareArrangements = true };

    private static CompiledQuery CompileJoin(bool blocks)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var ddl in JoinDdl)
        {
            resolver.Resolve(Parser.ParseStatement(ddl));
        }

        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(JoinSql))).Query;
        using var mode = TraceBlocksMode.Use(join: blocks);
        return PlanToCircuit.Compile(plan, Structural, StructuralRowCodec.Instance);
    }

    // Tick 0: 20 fact rows on key 1, then a dimension tick with 30 rows on key 1 — a 600-row fan-out.
    private static List<ZSet<StructuralRow, Z64>> RunJoin(CompiledQuery q)
    {
        var outputs = new List<ZSet<StructuralRow, Z64>>();
        for (var i = 0; i < 20; i++)
        {
            q.Table("f").Insert(1, (long)i, "s" + (i % 3));
        }

        q.Step();
        outputs.Add(q.Current);
        for (var i = 0; i < 30; i++)
        {
            q.Table("d").Insert(1, (long)(100 * i));
        }

        q.Step();
        outputs.Add(q.Current);
        q.Table("f").Delete(1, 3L, "s0");
        q.Table("d").Insert(2, 7L);
        q.Step();
        outputs.Add(q.Current);
        return outputs;
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void A_fan_out_join_past_the_threshold_emits_blocks(bool blockedTraces)
    {
        var expected = RunJoin(CompileJoin(blockedTraces));
        using var _ = SpillSettings.UseStepSpill(100, 1);
        var actual = RunJoin(CompileJoin(blockedTraces));
        for (var t = 0; t < expected.Count; t++)
        {
            AssertSameRows(expected[t], actual[t]);
        }

        Assert.Equal(600, actual[1].Count);
        Assert.True(actual[1].IsExternal, "the 600-row tick is past the threshold");
        Assert.False(actual[2].IsExternal, "a 30-row tick is below it");
    }

    private const string LeftJoinSql = "SELECT f.k, f.x, d.y FROM f LEFT JOIN d ON f.k = d.k";

    private static CompiledQuery CompileLeftJoin()
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var ddl in JoinDdl)
        {
            resolver.Resolve(Parser.ParseStatement(ddl));
        }

        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(LeftJoinSql))).Query;
        return PlanToCircuit.Compile(plan, Structural, StructuralRowCodec.Instance);
    }

    // One key with `left` fact rows and `right` dimension rows, in one tick.
    private static ZSet<StructuralRow, Z64> LeftJoinOneKey(int left, int right)
    {
        var q = CompileLeftJoin();
        for (var i = 0; i < left; i++)
        {
            q.Table("f").Insert(1, (long)i, "s");
        }

        for (var i = 0; i < right; i++)
        {
            q.Table("d").Insert(1, (long)i);
        }

        q.Step();
        return q.Current;
    }

    [Fact]
    public void Cross_product_guard_trips_only_on_a_blowup_into_a_managed_builder()
    {
        var saved = IncrementalLeftJoinOp<StructuralRow, StructuralRow, StructuralRow, StructuralRow, Z64>.CrossProductGuardRows;
        IncrementalLeftJoinOp<StructuralRow, StructuralRow, StructuralRow, StructuralRow, Z64>.CrossProductGuardRows = 100;
        try
        {
            // Managed output for the first two cases, whatever the environment (a forced suite run spills).
            using var managed = SpillSettings.UseStepSpill(null);

            // A 1:N lookup past the limit: no more output than input.
            Assert.Equal(300, LeftJoinOneKey(300, 1).Count);

            // A real blowup (400 rows from 40) trips it ...
            var ex = Assert.Throws<InvalidOperationException>(() => LeftJoinOneKey(20, 20));
            Assert.Contains("oversized per-key cross product", ex.Message, StringComparison.Ordinal);

            // ... unless the output spills.
            ZSet<StructuralRow, Z64> spilled;
            using (SpillSettings.UseStepSpill(50, 1))
            {
                spilled = LeftJoinOneKey(20, 20);
            }

            Assert.Equal(400, spilled.Count);
            Assert.True(spilled.IsExternal);
        }
        finally
        {
            IncrementalLeftJoinOp<StructuralRow, StructuralRow, StructuralRow, StructuralRow, Z64>.CrossProductGuardRows = saved;
        }
    }

    private static void AssertSameRows(ZSet<StructuralRow, Z64> expected, ZSet<StructuralRow, Z64> actual)
    {
        Assert.Equal(expected.Count, actual.Count);
        Assert.True(expected.Equals(actual), $"expected {expected} got {actual}");
    }

    [Fact]
    public void Random_queries_match_a_twin_every_tick_with_every_delta_forced_to_blocks()
    {
        var blocked0 = Interlocked.Read(ref DeltaSpill.BlockedBuilds);
        Gen.Select(RandomQuery.GenQuery, RandomQuery.GenTicks, Gen.Int[1, 3], Gen.Bool)
            .Sample((sql, ticks, chunk, blocks) => Twin(sql, ticks, chunk, blocks), iter: 600);
        Assert.True(Interlocked.Read(ref DeltaSpill.BlockedBuilds) > blocked0, "no delta was built as blocks");
    }

    private static bool Twin(string sql, IReadOnlyList<IReadOnlyList<InputEvent>> ticks, int chunk, bool blocks)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var ddl in RandomQuery.FixedDdl)
        {
            resolver.Resolve(Parser.ParseStatement(ddl));
        }

        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(sql))).Query;
        CompiledQuery plain, spilled;
        using (TraceBlocksMode.Use(join: blocks))
        {
            plain = PlanToCircuit.Compile(plan, Structural, StructuralRowCodec.Instance);
            using var _ = SpillSettings.UseStepSpill(0, chunk);
            spilled = PlanToCircuit.Compile(plan, Structural, StructuralRowCodec.Instance);
        }

        using var run = SpillSettings.UseStepSpill(0, chunk);
        foreach (var tick in ticks)
        {
            foreach (var q in new[] { plain, spilled })
            {
                foreach (var ev in tick.Where(e => q.Inputs.ContainsKey(e.Table)))
                {
                    q.Table(ev.Table).Push([(ev.Row, ev.Weight)]);
                }

                q.Step();
            }

            if (!plain.Current.Equals(spilled.Current))
            {
                Console.Error.WriteLine($"SQL: {sql}\nplain:   {plain.Current}\nspilled: {spilled.Current}");
                return false;
            }
        }

        return true;
    }
}
