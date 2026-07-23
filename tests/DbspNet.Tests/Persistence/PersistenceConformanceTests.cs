// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
// The conformance harness recommended by docs/design-layering-review.md §8.1.
//
// Every stateful operator has two ways of arriving at its state: the live path (fold deltas tick
// by tick) and the restore path (rebuild from a snapshot). Nothing in the type system says those
// must agree, and when they silently disagreed the result was a shipped bug that corrupted a
// materialised view — see docs/design-incremental-persistence.md §7.2. The window/rank/offset
// operators escaped it only because they happen to reload by calling the same function the live
// path calls; the aggregate reconstructed by a different process and was wrong.
//
// So assert the contract directly, for every operator kind and every trace family:
//
//     save -> restore -> step   ==   the same steps, uninterrupted
//
// by VALUE, not merely by shape. Two properties are checked per case:
//
//   1. at the snapshot boundary, the restored program's output views equal the producer's
//      (this is what a naive "restore works" test checks, and it passed even while §7.2 was live)
//   2. after driving MORE ticks post-restore, the outputs still equal an uninterrupted run
//      (this is the one that caught §7.2 — a retraction that fails to cancel only shows up on
//      the first step AFTER a restore)
//
// CoverageGuard is the part that makes this harness self-extending: it reflects over Core for
// ISnapshotable implementations and fails if any is missing from the case table without an
// explicit, reasoned exemption. A new stateful operator therefore cannot be added without either
// a conformance case or a deliberate note saying why not.
using System.Reflection;
using DbspNet.Core.Circuit;
using DbspNet.Persistence;
using DbspNet.Sql.Compiler;

namespace DbspNet.Tests.Persistence;

public class PersistenceConformanceTests : IDisposable
{
    private readonly string _dir;

    public PersistenceConformanceTests()
    {
        _dir = Path.Combine(Path.GetTempPath(), "dbspnet-conformance-" + Guid.NewGuid().ToString("N"));
    }

    public void Dispose()
    {
        if (Directory.Exists(_dir))
        {
            Directory.Delete(_dir, recursive: true);
        }

        GC.SuppressFinalize(this);
    }

    private static readonly string[] Tables =
    [
        "CREATE TABLE t (k INT NOT NULL, g INT NOT NULL, v DOUBLE NOT NULL)",
        "CREATE TABLE u (k INT NOT NULL, w BIGINT NOT NULL)",
    ];

    // One view per stateful operator kind. Named `out` throughout so the driver is shape-agnostic.
    private static readonly Dictionary<string, string> Shapes = new(StringComparer.Ordinal)
    {
        ["distinct"] = "SELECT DISTINCT g FROM t",
        ["agg_exact"] = "SELECT k, SUM(w) AS s, COUNT(*) AS c FROM u GROUP BY k",
        ["agg_minmax"] = "SELECT k, MIN(w) AS lo, MAX(w) AS hi FROM u GROUP BY k",
        // The §7.2 shape: float accumulators whose live and reloaded folds can disagree.
        ["agg_float"] = "SELECT g, AVG(v) AS a, STDDEV(v) AS sd, SUM(v) AS s FROM t GROUP BY g",
        ["agg_distinct"] = "SELECT g, COUNT(DISTINCT v) AS cd FROM t GROUP BY g",
        ["join_inner"] = "SELECT t.k AS k, t.v AS v, u.w AS w FROM t JOIN u ON t.k = u.k",
        ["join_left"] = "SELECT t.k AS k, t.v AS v, u.w AS w FROM t LEFT JOIN u ON t.k = u.k",
        ["join_full"] = "SELECT t.k AS k, t.v AS v, u.w AS w FROM t FULL OUTER JOIN u ON t.k = u.k",
        ["window_agg"] = "SELECT k, g, SUM(v) OVER (PARTITION BY g ORDER BY k) AS rs FROM t",
        ["window_offset"] = "SELECT k, g, LAG(v) OVER (PARTITION BY g ORDER BY k, v) AS prv, " +
                            "LEAD(v) OVER (PARTITION BY g ORDER BY k, v) AS nxt FROM t",
        ["rank_partitioned"] = "SELECT k, g, RANK() OVER (PARTITION BY g ORDER BY v DESC, k) AS r FROM t",
        // The market_volatility shape: a GLOBAL rank, no PARTITION BY, which re-ranks everyone
        // whenever anything changes — the case where a failed retraction doubles the whole view.
        ["rank_global"] = "SELECT k, RANK() OVER (ORDER BY v DESC, k) AS r FROM t",
        ["topk_global"] = "SELECT k, v FROM t ORDER BY v DESC, k LIMIT 3",
        ["topk_partitioned"] = "SELECT k, g, v FROM (" +
                               "SELECT k, g, v, ROW_NUMBER() OVER (PARTITION BY g ORDER BY v DESC, k) AS rn FROM t" +
                               ") AS ranked WHERE rn <= 2",
    };

    public static TheoryData<string, string> Cases()
    {
        var data = new TheoryData<string, string>();
        foreach (var shape in Shapes.Keys.OrderBy(k => k, StringComparer.Ordinal))
        {
            foreach (var family in new[] { "flat", "spine" })
            {
                data.Add(shape, family);
            }
        }

        return data;
    }

    private static CompiledProgram Compile(string shape, string family)
    {
        var stmts = new List<string>(Tables) { "CREATE VIEW out AS " + Shapes[shape] };
        return SqlProgram.Compile(
            stmts,
            new HashSet<string>(["out"], StringComparer.Ordinal),
            ArrowSqlSnapshotCodecs.Instance,
            new CompileOptions
            {
                TraceFamily = family == "spine" ? TraceFamily.Spine : TraceFamily.Flat,
            });
    }

    // Ticks 0..5 build state; 6..9 run after the restore boundary.
    //
    // Two properties of this schedule are load-bearing, and a mutation test proved it: an earlier
    // version inserted only DISTINCT rows and failed to catch the §7.2 bug even with the fix
    // reverted, because the trace then holds weight-1 entries and a bulk reload fold matches the
    // incremental one exactly.
    //
    //   * the SAME row is inserted on several ticks, so its trace weight coalesces to >1. A reload
    //     that re-derives state folds `x * weight` in ONE operation where the live path folded
    //     `x` once per tick — which is where a non-associative accumulator drifts.
    //   * a large magnitude (1e16) sits alongside those unit values, so the drift is representable:
    //     ulp(1e16) is 2, so ten separate +1.0 are each absorbed while one +10.0 is not.
    //
    // Retractions matter too: a retraction forces an operator to emit against its cached previous
    // value, which is the move that turns a mis-restored cache into a visible corruption.
    private const int SnapshotAfterTick = 6;
    private const int TotalTicks = 10;

    private static void DriveTick(CompiledProgram p, int tick)
    {
        switch (tick)
        {
            case 0:
                p.Table("t").Insert(1, 10, 1.5);
                p.Table("t").Insert(2, 10, 2.5);
                p.Table("u").Insert(1, 100L);
                break;
            case 1:
                p.Table("t").Insert(3, 20, 3.5);
                p.Table("u").Insert(2, 200L);
                break;
            case 2:
                // The large magnitude that makes a fold-order difference representable.
                p.Table("t").Insert(4, 10, 1e16);
                break;
            case 3:
                p.Table("t").Insert(5, 10, 1.0);      // same row, weight -> 1
                break;
            case 4:
                p.Table("t").Insert(5, 10, 1.0);      // same row, weight -> 2
                p.Table("u").Insert(3, 300L);
                break;
            case 5:
                p.Table("t").Insert(5, 10, 1.0);      // same row, weight -> 3
                p.Table("t").Delete(2, 10, 2.5);      // retraction
                p.Table("u").Delete(2, 200L);         // retraction on the join's right side
                break;
            case 6:
                // First step after the restore boundary: forces every operator to emit against
                // whatever it believes the previous value was.
                p.Table("t").Insert(5, 10, 1.0);      // same row, weight -> 4
                break;
            case 7:
                p.Table("t").Insert(9, 20, 0.5);
                p.Table("u").Insert(9, 900L);
                break;
            case 8:
                p.Table("t").Delete(4, 10, 1e16);     // retract the large value post-restore
                break;
            case 9:
                p.Table("t").Insert(10, 30, 4.5);
                p.Table("u").Insert(10, 1000L);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(tick));
        }

        p.Step();
    }

    /// <summary>Order-independent, value-exact rendering of the output view.</summary>
    private static string Render(CompiledProgram p)
    {
        var output = p.Outputs["out"];
        var rows = new List<string>();
        foreach (var (row, weight) in output.CurrentView)
        {
            var cells = new string[output.Schema.Count];
            for (var i = 0; i < cells.Length; i++)
            {
                // "R" round-trips a double exactly, so a last-bit difference is not hidden.
                cells[i] = row[i] switch
                {
                    null => "<null>",
                    double d => d.ToString("R", System.Globalization.CultureInfo.InvariantCulture),
                    var o => o.ToString() ?? "<null>",
                };
            }

            rows.Add(string.Join("|", cells) + " => " + weight.Value.ToString(System.Globalization.CultureInfo.InvariantCulture));
        }

        rows.Sort(StringComparer.Ordinal);
        return string.Join("\n", rows);
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task RestoreThenStep_EqualsUninterrupted(string shape, string family)
    {
        // Reference: every tick, no interruption.
        var reference = Compile(shape, family);
        for (var i = 0; i < TotalTicks; i++)
        {
            DriveTick(reference, i);
        }

        var expectedFinal = Render(reference);

        // Producer: stop at the boundary and snapshot.
        var producer = Compile(shape, family);
        for (var i = 0; i < SnapshotAfterTick; i++)
        {
            DriveTick(producer, i);
        }

        var expectedAtBoundary = Render(producer);
        var dir = Path.Combine(_dir, shape + "-" + family);
        await Snapshot.WriteAsync(producer.Circuit, dir);

        // Property 1: the restore itself is faithful. This passed even while §7.2 was live, so it
        // is necessary but nowhere near sufficient.
        var restored = Compile(shape, family);
        await Snapshot.ReadAsync(restored.Circuit, dir);
        Assert.Equal(expectedAtBoundary, Render(restored));

        // Property 2: and it stays faithful once it starts stepping again. This is the one that
        // catches a cache restored to a value the downstream view never held.
        for (var i = SnapshotAfterTick; i < TotalTicks; i++)
        {
            DriveTick(restored, i);
        }

        Assert.Equal(expectedFinal, Render(restored));
    }

    /// <summary>
    /// The harness must not quietly stop covering things. Every <see cref="ISnapshotable"/> in
    /// Core has to be exercised by some case above, or be listed as a deliberate exemption with a
    /// reason. Adding a stateful operator without doing one of those fails here.
    /// </summary>
    [Fact]
    public void CoverageGuard_EverySnapshotableOperatorIsExercisedOrExempt()
    {
        // Operators that exist but cannot be reached through the SQL shapes above. Each needs a
        // reason, not just an entry.
        var exempt = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["LatenessOperator"] = "needs a LATENESS declaration; covered by LatenessSnapshotTests",
            ["TemporalFilterOp"] = "needs NOW()-relative predicates; covered by LogicalClockSnapshotTests",
            ["FixpointOperator"] = "recursive-CTE machinery; RecursiveCteSnapshotTests asserts " +
                                   "restore-then-step in both trace families",
            ["SemiNaiveFixpointOperator"] = "recursive-CTE machinery; RecursiveCteSnapshotTests " +
                                            "asserts restore-then-step in both trace families",
            ["PartitionedTopKNarrowOp"] = "a narrowed variant the planner picks by shape; not selectable from SQL directly",
        };

        var snapshotable = typeof(RootCircuit).Assembly
            .GetTypes()
            .Where(t => t is { IsAbstract: false, IsInterface: false }
                        && typeof(ISnapshotable).IsAssignableFrom(t))
            .Select(t => Strip(t.Name))
            .Distinct(StringComparer.Ordinal)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToList();

        Assert.NotEmpty(snapshotable);

        var covered = new HashSet<string>(StringComparer.Ordinal);
        foreach (var shape in Shapes.Keys)
        {
            foreach (var family in new[] { "flat", "spine" })
            {
                var program = Compile(shape, family);
                foreach (var op in program.Circuit.Operators)
                {
                    if (op is ISnapshotable)
                    {
                        covered.Add(Strip(op.GetType().Name));
                    }
                }
            }
        }

        var uncovered = snapshotable
            .Where(n => !covered.Contains(n) && !exempt.ContainsKey(n))
            .ToList();

        Assert.True(
            uncovered.Count == 0,
            "these ISnapshotable operators have no conformance case and no exemption — add a SQL " +
            "shape to Shapes, or an entry to `exempt` with a reason:\n  " + string.Join("\n  ", uncovered) +
            "\n(covered: " + string.Join(", ", covered.OrderBy(x => x, StringComparer.Ordinal)) + ")");

        // Exemptions must stay honest: one that no longer names a real operator is dead weight.
        var stale = exempt.Keys.Where(k => !snapshotable.Contains(k)).ToList();
        Assert.True(
            stale.Count == 0,
            "these exemptions no longer name an ISnapshotable operator and should be removed:\n  "
            + string.Join("\n  ", stale));
    }

    private static string Strip(string name)
    {
        var tick = name.IndexOf('`');
        return tick >= 0 ? name[..tick] : name;
    }
}
