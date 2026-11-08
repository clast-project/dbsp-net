// Scratch (§5.36): a local stand-in for tick 18's ops 2135-2175 on extended TPC-DS, the view
// fact_engagement_status_channel_eligibility_28d_rolling_hist_12m over int_hist_unpivoted_28d (a
// three-way UNION ALL unpivot). Synthetic rows of the same columns; per-op allocation per input row.
// Run with XT_MICRO=1 and the xt run's spill env (DBSPNET_SPILL=on, DBSPNET_STEP_SPILL_ROWS, ...).
using System.Globalization;
using DbspNet.Persistence;
using DbspNet.Sql.TypeSystem;
using DbspNet.Sql.Compiler;
using Xunit;
using Xunit.Abstractions;

namespace DbspNet.Tests.Scratch;

public class RollingEligibilityRepro(ITestOutputHelper output)
{
    private const string Active =
        "CREATE TABLE int_hist_active_28d (report_as_of_date DATE NOT NULL, engagement_registry_key INT NOT NULL, " +
        "household_key INT NOT NULL, allocated_units DOUBLE, product_tier_key INT NOT NULL, product_version_key INT NOT NULL, " +
        "preference_key INT NOT NULL, service_role VARCHAR, customer_path VARCHAR, parent_capacity INT, parent_limit INT, " +
        "reported_platform VARCHAR, policy_type VARCHAR, order_line_count BIGINT, household_id INT, " +
        "store_eligibility VARCHAR, catalog_eligibility VARCHAR, web_eligibility VARCHAR)";

    private static string Unpivot(string channel) =>
        "SELECT d.report_as_of_date, d.household_id, d.household_key, d.product_version_key, d.product_tier_key, " +
        "d.preference_key, d.service_role, d.reported_platform, d.policy_type, d.engagement_registry_key, d.allocated_units, " +
        "d.customer_path, d.parent_capacity, d.parent_limit, d.order_line_count, '" + channel + "' AS channel, " +
        "CASE WHEN d." + channel + "_eligibility = 'ready' THEN 'ready' WHEN d." + channel + "_eligibility = 'notready' " +
        "THEN 'not ready' ELSE 'not assessed' END AS eligibility_status FROM int_hist_active_28d AS d";

    private const string Keys =
        "report_as_of_date, household_id, channel, eligibility_status, household_key, product_version_key, " +
        "product_tier_key, preference_key, service_role, reported_platform, policy_type";

    private static readonly string Rolling =
        "CREATE VIEW rolling AS SELECT MD5(CONCAT_WS('-', COALESCE(TRY_CAST(a.report_as_of_date AS TEXT), ''), " +
        "COALESCE(TRY_CAST(a.household_id AS TEXT), ''), COALESCE(TRY_CAST(a.household_key AS TEXT), ''), " +
        "COALESCE(TRY_CAST(a.product_version_key AS TEXT), ''), COALESCE(TRY_CAST(a.product_tier_key AS TEXT), ''), " +
        "COALESCE(TRY_CAST(a.preference_key AS TEXT), ''), COALESCE(TRY_CAST(a.service_role AS TEXT), ''), " +
        "COALESCE(TRY_CAST(a.reported_platform AS TEXT), ''), COALESCE(TRY_CAST(a.policy_type AS TEXT), ''), " +
        "COALESCE(TRY_CAST(a.channel AS TEXT), ''), COALESCE(TRY_CAST(a.eligibility_status AS TEXT), ''))) AS primary_key, " +
        "a.report_as_of_date, a.household_id, a.channel, a.eligibility_status, TRY_CAST(a.engagement_count AS INT) AS engagement_count, " +
        "TRY_CAST(a.order_line_count AS INT) AS order_line_count, TRY_CAST(a.allocated_unit_count AS DOUBLE) AS allocated_unit_count, " +
        "a.household_key, a.product_version_key, a.product_tier_key, a.preference_key, a.service_role, a.reported_platform, a.policy_type, " +
        "TRY_CAST(COALESCE(m.parent_capacity_sum, 0) AS INT) AS parent_capacity_sum, TRY_CAST(COALESCE(m.parent_limit_sum, 0) AS INT) AS parent_limit_sum " +
        "FROM (SELECT " + Keys + ", COUNT(DISTINCT engagement_registry_key) AS engagement_count, SUM(allocated_units) AS allocated_unit_count, " +
        "SUM(order_line_count) AS order_line_count FROM unpivoted GROUP BY " + Keys + ") AS a " +
        "LEFT JOIN (SELECT " + Keys + ", SUM(parent_capacity) AS parent_capacity_sum, SUM(parent_limit) AS parent_limit_sum " +
        "FROM (SELECT DISTINCT " + Keys + ", customer_path, parent_capacity, parent_limit FROM unpivoted WHERE NOT customer_path IS NULL) AS dc " +
        "GROUP BY " + Keys + ") AS m " +
        "ON a.report_as_of_date = m.report_as_of_date AND a.household_id IS NOT DISTINCT FROM m.household_id AND a.channel = m.channel " +
        "AND a.eligibility_status = m.eligibility_status AND a.household_key = m.household_key AND a.product_version_key = m.product_version_key " +
        "AND a.product_tier_key = m.product_tier_key AND a.preference_key = m.preference_key " +
        "AND COALESCE(a.service_role, '') = COALESCE(m.service_role, '') AND COALESCE(a.reported_platform, '') = COALESCE(m.reported_platform, '') " +
        "AND COALESCE(a.policy_type, '') = COALESCE(m.policy_type, '')";

    [Fact]
    public void Profile()
    {
        if (Environment.GetEnvironmentVariable("XT_MICRO") is not "1")
        {
            output.WriteLine("XT_MICRO not set");
            return;
        }

        var n = int.Parse(Environment.GetEnvironmentVariable("XT_MICRO_ROWS") ?? "2000000", CultureInfo.InvariantCulture);
        var workers = int.Parse(Environment.GetEnvironmentVariable("XT_MICRO_W") ?? "12", CultureInfo.InvariantCulture);
        DbspNet.Sql.Expressions.BuiltinScalarFunctions.FusedConcatWs = Environment.GetEnvironmentVariable("XT_FUSED") is not "0";
        var statements = new[]
        {
            Active,
            "CREATE VIEW unpivoted AS " + Unpivot("store") + " UNION ALL " + Unpivot("catalog") + " UNION ALL " + Unpivot("web"),
            Rolling,
        };
        Assert.True(SqlProgram.TryCompileParallel(
            statements, new HashSet<string> { "unpivoted", "rolling" }, workers, out var p,
            snapshotCodecs: ArrowSqlSnapshotCodecs.Instance, numericStringCoercion: true, nullCollation: NullCollation.Low));
        using var program = p!;

        // Small first tick (prior state), then the big one, as tick 18 is.
        Push(program, new Random(1), 0, n / 20);
        program.Step();
        Push(program, new Random(2), n / 20, n);
        program.Circuit.ProfileOperators = true;
        program.Circuit.ResetOperatorProfile();
        var t0 = GC.GetTotalAllocatedBytes(true);
        var sw = System.Diagnostics.Stopwatch.StartNew();
        program.Step();
        sw.Stop();
        var total = GC.GetTotalAllocatedBytes(true) - t0;
        var rows = 3.0 * (n - n / 20);
        output.WriteLine(FormattableString.Invariant($"step {sw.Elapsed.TotalSeconds:F1}s alloc {total / (double)(1L << 30):F2} GiB = {total / rows:F0} B per unpivoted row ({rows:F0})"));
        foreach (var op in program.Circuit.CollectOperatorProfile().Where(o => o.AllocBytes > total / 200))
        {
            output.WriteLine(FormattableString.Invariant(
                $"[{op.Index}] {op.Name} <{op.Label}> {op.CumulativeMs / 1000:F1}s out={op.LastOutputRows} alloc={op.AllocBytes / (double)(1L << 30):F2} GiB {op.AllocBytes / rows:F0} B/row"));
        }
    }

    private static readonly string?[] Roles = ["admin", "member", "viewer", null];
    private static readonly string?[] Platforms = ["ios", "android", "web"];
    private static readonly string?[] Policies = ["standard", "premium", null];
    private static readonly string?[] Elig = ["ready", "notready", null];

    private static void Push(ParallelCompiledProgram program, Random rng, int from, int to)
    {
        var households = Math.Max(1, to / 6);
        var batch = new List<(object?[] Values, long Weight)>(to - from);
        for (var i = from; i < to; i++)
        {
            var h = rng.Next(households);
            var day = 19000 + (h * 7 + rng.Next(3)) % 365;
            var cp = h % 5 == 0 ? null : "customer/" + (h % 100_000).ToString(CultureInfo.InvariantCulture);
            batch.Add(([
                new Date32(day), i, h, rng.NextDouble() * 10, h % 3, h % 7, h % 5, Roles[h % 4], cp,
                cp is null ? null : h % 50, cp is null ? null : h % 30, Platforms[h % 3], Policies[h % 3],
                (long)rng.Next(10), h, Elig[rng.Next(3)], Elig[rng.Next(3)], Elig[rng.Next(3)],
            ], 1));
        }

        program.Table("int_hist_active_28d").Push(batch);
    }
}
