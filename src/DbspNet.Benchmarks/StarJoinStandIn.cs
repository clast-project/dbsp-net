// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Diagnostics;
using System.Globalization;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Core.Operators.Stateful.Blocks;
using DbspNet.Persistence;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Plan;

namespace DbspNet.Benchmarks;

/// <summary>
/// Stage 1 of the star join (docs/design-bounded-memory.md §5.48.6): a synthetic stand-in for
/// <c>int_engagement_status_transaction</c>'s broadcast LEFT-join runs. A fact of N rows (one SF25
/// worker's share is ~3M) LEFT-joins nine dimensions (eight of 1–4 rows, <c>dim_household</c> of
/// 72,002), every key computed from the fact row, in the real view's order. Three arms on the same
/// rows, each in a fresh process state: the compiled chain (today's plan at W=1: fused index passes,
/// folded projections, blocked traces), a baseline program with the same source and output but no
/// joins (what the chain pays that a star would pay too), and a prototype star pass: per fact row,
/// nine keys, nine dictionary lookups, the output row built once, plus one integrate of the fact into
/// a trace keyed by the tuple of the keys. Chain cost = chain step − baseline step; the star's cost is
/// its pass measured directly. Run with and without <c>DBSPNET_STEP_SPILL_ROWS=262144</c> (the xt
/// setting): with it every intermediate delta of the chain is block backed.
/// <c>dotnet run -c Release -- starjoin [factRows] [runs]</c>
/// </summary>
internal static class StarJoinStandIn
{
    private const int Households = 72_002;

    private static readonly string[] Ddl =
    [
        "CREATE TABLE f (event_time BIGINT NOT NULL, account_id BIGINT, engagement_path VARCHAR, customer_path VARCHAR, " +
            "platform_name VARCHAR, preference_type VARCHAR, preference_sku VARCHAR, product_tier VARCHAR, " +
            "product_version VARCHAR, mode_name VARCHAR, engagement_type_name VARCHAR, household_id BIGINT, " +
            "account_offer VARCHAR, account_usage VARCHAR)",
        "CREATE TABLE dim_customer_platform (platform_key INT NOT NULL, platform_name VARCHAR NOT NULL)",
        "CREATE TABLE dim_customer_preference (preference_key INT NOT NULL, preference_type VARCHAR NOT NULL, preference_sku VARCHAR NOT NULL)",
        "CREATE TABLE dim_product_tier (product_tier_key INT NOT NULL, product_tier VARCHAR NOT NULL)",
        "CREATE TABLE dim_product_version (product_version_key INT NOT NULL, product_version VARCHAR NOT NULL)",
        "CREATE TABLE dim_engagement_mode (mode_key INT NOT NULL, mode_name VARCHAR NOT NULL)",
        "CREATE TABLE dim_engagement_type (engagement_type_key INT NOT NULL, engagement_type_name VARCHAR NOT NULL)",
        "CREATE TABLE dim_household (household_key INT NOT NULL, household_id BIGINT NOT NULL)",
        "CREATE TABLE dim_account_offer (account_offer_key INT NOT NULL, account_offer VARCHAR NOT NULL)",
        "CREATE TABLE dim_account_usage (account_usage_key INT NOT NULL, account_usage VARCHAR NOT NULL)",
    ];

    // The real view's broadcast segment (840–887), keys from the fact row, in its order.
    private const string ChainView =
        "CREATE VIEW v AS SELECT f.event_time, " +
        "COALESCE(customer.household_key, 1) AS household_key, COALESCE(sotd.account_offer_key, 1) AS account_offer_key, " +
        "COALESCE(swtd.account_usage_key, 1) AS account_usage_key, COALESCE(infra.platform_key, 1) AS platform_key, " +
        "COALESCE(os.preference_key, 1) AS preference_key, COALESCE(product_tier.product_tier_key, 1) AS product_tier_key, " +
        "COALESCE(version.product_version_key, 1) AS product_version_key, COALESCE(type.engagement_type_key, 1) AS engagement_type_key, " +
        "COALESCE(mode.mode_key, 1) AS mode_key, f.engagement_path, f.customer_path " +
        "FROM f " +
        "LEFT JOIN dim_customer_platform AS infra ON infra.platform_name = COALESCE(f.platform_name, 'N/A') " +
        "LEFT JOIN dim_customer_preference AS os ON COALESCE(f.preference_type, 'unknown') = os.preference_type " +
        "  AND COALESCE(f.preference_sku, 'Unknown') = os.preference_sku " +
        "LEFT JOIN dim_product_tier AS product_tier ON f.product_tier = product_tier.product_tier " +
        "LEFT JOIN dim_product_version AS version ON f.product_version = version.product_version " +
        "LEFT JOIN dim_engagement_mode AS mode ON f.mode_name = mode.mode_name " +
        "LEFT JOIN dim_engagement_type AS type ON f.engagement_type_name = type.engagement_type_name " +
        "LEFT JOIN dim_household AS customer ON COALESCE(f.household_id, -2147483648) = customer.household_id " +
        "LEFT JOIN dim_account_offer AS sotd ON COALESCE(f.account_offer, 'Unknown') = sotd.account_offer " +
        "LEFT JOIN dim_account_usage AS swtd ON COALESCE(f.account_usage, 'Unknown') = swtd.account_usage";

    // Same source, same output width and row identity, no joins.
    private const string BaselineView =
        "CREATE VIEW v AS SELECT f.event_time, 1 AS household_key, 1 AS account_offer_key, 1 AS account_usage_key, " +
        "1 AS platform_key, 1 AS preference_key, 1 AS product_tier_key, 1 AS product_version_key, 1 AS engagement_type_key, " +
        "1 AS mode_key, f.engagement_path, f.customer_path FROM f";

    // Fact column indices.
    private const int CEventTime = 0, CEngPath = 2, CCustPath = 3, CPlatform = 4, CPrefType = 5, CPrefSku = 6,
        CTier = 7, CVersion = 8, CMode = 9, CEType = 10, CHousehold = 11, COffer = 12, CUsage = 13;

    private static readonly string[] Tiers = ["BASIC", "PLUS", "PRO", "ENTERPRISE"];
    private static readonly string[] Versions = ["V1", "V2", "V3", "V4"];
    private static readonly string[] Modes = ["ONLINE", "STORE", "PHONE"];
    private static readonly string[] Types = ["PURCHASE", "RETURN"];
    private static readonly (string, string)[] Prefs = [("unknown", "Unknown"), ("email", "A1"), ("sms", "B2")];
    private static readonly string[] Offers = ["Unknown", "TRIAL", "ANNUAL"];
    private static readonly string[] Usages = ["Unknown", "HEAVY"];

    public static int Run(string[] args)
    {
        var n = args.Length > 1 ? int.Parse(args[1], CultureInfo.InvariantCulture) : 3_000_000;
        var runs = args.Length > 2 ? int.Parse(args[2], CultureInfo.InvariantCulture) : 3;
        var arms = args.Length > 3 ? args[3].Split(',') : ["chain", "base", "star"];
        _ = ArrowSqlSnapshotCodecs.Instance; // loads Persistence: block formats (and spill, if on)
        Console.WriteLine(
            $"starjoin: fact={n:N0} runs={runs} STEP_SPILL_ROWS={SpillSettings.StepSpillRows?.ToString(CultureInfo.InvariantCulture) ?? "off"} " +
            $"blocks(join)={TraceBlocksMode.Join} server GC={System.Runtime.GCSettings.IsServerGC}");

        var facts = Facts(n);
        var results = new Dictionary<string, List<(double Ms, long Bytes)>>();
        for (var r = 0; r < runs; r++)
        {
            foreach (var arm in arms)
            {
                var m = arm switch
                {
                    "chain" => RunProgram(ChainView, facts, profile: r == 0),
                    "base" => RunProgram(BaselineView, facts, profile: false),
                    "star" => RunStar(facts, households: true),
                    "star8" => RunStar(facts, households: false),
                    _ => throw new ArgumentException(arm),
                };
                (results.TryGetValue(arm, out var l) ? l : results[arm] = []).Add(m);
                Console.WriteLine($"  run {r} {arm,-6} {m.Ms,9:F0} ms {m.Bytes / (1 << 20),8:N0} MiB  {m.Ms * 1e6 / n,7:F0} ns/row {m.Bytes / (double)n,7:F0} B/row");
            }
        }

        Console.WriteLine();
        foreach (var (arm, l) in results)
        {
            var ms = l.Select(x => x.Ms).Order().ElementAt(l.Count / 2);
            var b = l.Select(x => x.Bytes).Order().ElementAt(l.Count / 2);
            Console.WriteLine($"median {arm,-6} {ms,9:F0} ms {b / (1 << 20),8:N0} MiB  {ms * 1e6 / n,7:F0} ns/row {b / (double)n,7:F0} B/row");
        }

        return 0;
    }

    private static List<object?[]> Facts(int n)
    {
        var rng = new Random(42);
        var facts = new List<object?[]>(n);
        string? Pick(string[] xs, int nullPct) => rng.Next(100) < nullPct ? null : xs[rng.Next(xs.Length)];
        for (var i = 0; i < n; i++)
        {
            var pref = rng.Next(10) == 0 ? ((string?)null, (string?)null) : Prefs[rng.Next(Prefs.Length)];
            facts.Add(
            [
                1_700_000_000_000_000L + i * 1000L,
                (long)rng.Next(288_003),
                "engagement/" + rng.Next(9_000_000).ToString("D8", CultureInfo.InvariantCulture),
                "customer/" + rng.Next(2_000_000).ToString("D7", CultureInfo.InvariantCulture),
                rng.Next(3) == 0 ? "N/A" : null,
                pref.Item1, pref.Item2,
                rng.Next(20) == 0 ? "UNKNOWN" : Tiers[rng.Next(Tiers.Length)],
                rng.Next(20) == 0 ? "UNKNOWN" : Versions[rng.Next(Versions.Length)],
                rng.Next(20) == 0 ? "UNKNOWN" : Modes[rng.Next(Modes.Length)],
                Types[rng.Next(Types.Length)],
                rng.Next(10) == 0 ? null : (long)rng.Next(Households + 8_000),
                Pick(Offers, 20), Pick(Usages, 20),
            ]);
        }

        return facts;
    }

    private static void LoadDims(Func<string, TableInput> table)
    {
        table("dim_customer_platform").Insert(1, "N/A");
        for (var i = 0; i < Prefs.Length; i++)
        {
            table("dim_customer_preference").Insert(i + 2, Prefs[i].Item1, Prefs[i].Item2);
        }

        void Small(string t, string[] xs)
        {
            for (var i = 0; i < xs.Length; i++)
            {
                table(t).Insert(i + 2, xs[i]);
            }
        }

        Small("dim_product_tier", Tiers);
        Small("dim_product_version", Versions);
        Small("dim_engagement_mode", Modes);
        Small("dim_engagement_type", Types);
        Small("dim_account_offer", Offers);
        Small("dim_account_usage", Usages);
        table("dim_household").Push(Enumerable.Range(0, Households).Select(i => (new object?[] { i + 2, (long)i }, 1L)));
    }

    private static CompiledProgram Compile(string view) =>
        SqlProgram.Compile([.. Ddl, view], new HashSet<string>(["v"], StringComparer.Ordinal), ArrowSqlSnapshotCodecs.Instance);

    private static (double Ms, long Bytes) RunProgram(string view, List<object?[]> facts, bool profile)
    {
        var p = Compile(view);
        LoadDims(p.Table);
        p.Step();
        p.Table("f").Push(facts.Select(f => (f, 1L)));
        Settle();
        p.Circuit.ProfileOperators = profile;
        var (ms, bytes) = Measure(p.Step);
        if (profile)
        {
            var ops = p.Circuit.CollectOperatorProfile().Where(o => o.CumulativeMs > 1 || o.AllocBytes > 1 << 20).ToList();
            Console.WriteLine($"  chain per-op (tick 1, profiled step {ms:F0} ms; {ops.Count} ops shown):");
            foreach (var o in ops)
            {
                Console.WriteLine($"    [{o.Index,3}] {o.Name,-36} {o.CumulativeMs,8:F0} ms {o.AllocBytes / (1 << 20),7:N0} MiB  out={o.LastOutputRows}");
            }
        }

        var rows = p.Outputs["v"].CurrentView.Count;
        if (rows != facts.Count)
        {
            throw new InvalidOperationException($"view has {rows} rows, expected {facts.Count}");
        }

        return (ms, bytes);
    }

    private static (double Ms, long Bytes) Measure(Action a)
    {
        var b0 = GC.GetTotalAllocatedBytes(precise: true);
        var t0 = Stopwatch.GetTimestamp();
        a();
        var ms = Stopwatch.GetElapsedTime(t0).TotalMilliseconds;
        return (ms, GC.GetTotalAllocatedBytes(precise: true) - b0);
    }

    private static void Settle()
    {
        GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
        GC.WaitForPendingFinalizers();
    }

    // ---- The prototype star -------------------------------------------------------------------

    private sealed class Member(int[] keyCols, object?[] defaults, Dictionary<StructuralRow, List<(object? Out, long W)>> dim)
    {
        public int[] KeyCols { get; } = keyCols;

        // COALESCE literals for the key columns (null: the column is used as is).
        public object?[] Defaults { get; } = defaults;

        public Dictionary<StructuralRow, List<(object? Out, long W)>> Dim { get; } = dim;
    }

    /// <summary>Captures what a table pushes, so the star reads the exact cells the chain reads.</summary>
    private static ZSet<StructuralRow, Z64> Capture(TableInput t, Action push)
    {
        ZSet<StructuralRow, Z64>? got = null;
        void On(ZSet<StructuralRow, Z64> z) => got = got is null ? z : got + z;
        t.OnPushed += On;
        push();
        t.OnPushed -= On;
        return got ?? throw new InvalidOperationException("nothing pushed");
    }

    private static (double Ms, long Bytes) RunStar(List<object?[]> facts, bool households)
    {
        // A program only to encode the rows (and to own the output schema / formats).
        var chain = Compile(ChainView);
        var dims = new Dictionary<string, ZSet<StructuralRow, Z64>>(StringComparer.Ordinal);
        var handlers = new List<(TableInput T, Action<ZSet<StructuralRow, Z64>> H)>();
        foreach (var name in chain.Inputs.Keys.Where(k => k.StartsWith("dim_", StringComparison.Ordinal)))
        {
            Action<ZSet<StructuralRow, Z64>> h = z => dims[name] = dims.TryGetValue(name, out var o) ? o + z : z;
            chain.Table(name).OnPushed += h;
            handlers.Add((chain.Table(name), h));
        }

        LoadDims(chain.Table);
        foreach (var (t, h) in handlers)
        {
            t.OnPushed -= h;
        }

        var fTable = chain.Table("f");
        var fact = Capture(fTable, () => fTable.Push(facts.Select(f => (f, 1L))));
        var fSchema = fTable.Schema;
        var lit = BoundaryEncoder.Encode(fSchema,
            [0L, 0L, null, null, "N/A", "unknown", "Unknown", null, null, null, null, -2147483648L, "Unknown", "Unknown"]);
        var one = BoundaryEncoder.Encode(chain.Table("dim_product_tier").Schema, [1, "x"])[0];

        Member M(string dim, int[] factCols, int[] dimKeyCols, int outCol)
        {
            var d = new Dictionary<StructuralRow, List<(object?, long)>>();
            foreach (var (row, w) in dims[dim])
            {
                var key = new StructuralRow(dimKeyCols.Select(c => row[c]).ToArray());
                (d.TryGetValue(key, out var l) ? l : d[key] = []).Add((row[outCol], w.Value));
            }

            return new Member(factCols, factCols.Select(c => lit[c]).ToArray(), d);
        }

        // Output order: household, offer, usage, platform, preference, tier, version, type, mode.
        var household = M("dim_household", [CHousehold], [1], 0);
        var members = new[]
        {
            household,
            M("dim_account_offer", [COffer], [1], 0),
            M("dim_account_usage", [CUsage], [1], 0),
            M("dim_customer_platform", [CPlatform], [1], 0),
            M("dim_customer_preference", [CPrefType, CPrefSku], [1, 2], 0),
            M("dim_product_tier", [CTier], [1], 0),
            M("dim_product_version", [CVersion], [1], 0),
            M("dim_engagement_type", [CEType], [1], 0),
            M("dim_engagement_mode", [CMode], [1], 0),
        };
        var tupleCols = new List<int>();
        foreach (var m in members.Where(m => households || m != household))
        {
            tupleCols.AddRange(m.KeyCols);
        }

        var outSchema = chain.Outputs["v"].Schema;
        var outFormat = SpillSettings.StepSpillRows is null ? null : TraceBlockFormats.Create(outSchema, Schema.Empty);
        var keySchema = fSchema.SubsetByIndex(tupleCols);
        IGroupTrace<StructuralRow, StructuralRow, Z64> trace = TraceBlocksMode.Join && TraceBlockFormats.Create(keySchema, fSchema) is { } tf
            ? new BlockedGroupTrace(tf)
            : new ManagedGroupTrace<StructuralRow, StructuralRow, Z64>();
        IGroupTrace<StructuralRow, StructuralRow, Z64>? hhTrace = null;
        if (!households)
        {
            var hk = fSchema.SubsetByIndex([CHousehold]);
            hhTrace = TraceBlocksMode.Join && TraceBlockFormats.Create(hk, fSchema) is { } hf
                ? new BlockedGroupTrace(hf)
                : new ManagedGroupTrace<StructuralRow, StructuralRow, Z64>();
        }

        dims.Clear();
        Settle();

        long buildBytes = 0, integrateBytes = 0;
        double buildMs = 0, integrateMs = 0;
        var (ms, bytes) = Measure(() =>
        {
            var b0 = GC.GetTotalAllocatedBytes(precise: true);
            var t0 = Stopwatch.GetTimestamp();
            var output = DeltaSpill.For<StructuralRow, Z64>(outFormat, fact.Count);
            var keyBuf = new object?[2];
            var matches = new List<(object? Out, long W)>?[members.Length];
            foreach (var (r, w) in fact)
            {
                // Per member: the key (COALESCE'd), its matches, or a pad.
                for (var j = 0; j < members.Length; j++)
                {
                    var m = members[j];
                    var cols = m.KeyCols;
                    var null_ = false;
                    for (var c = 0; c < cols.Length; c++)
                    {
                        keyBuf[c] = r[cols[c]] ?? m.Defaults[c];
                        null_ |= keyBuf[c] is null;
                    }

                    matches[j] = null_ ? null : m.Dim.GetValueOrDefault(StructuralRow.Of(keyBuf.AsSpan(0, cols.Length)));
                }

                Expand(output, r, w.Value, members, matches, 0, new object?[12], 1, one);
            }

            var delta = output.Build();
            buildMs = Stopwatch.GetElapsedTime(t0).TotalMilliseconds;
            buildBytes = GC.GetTotalAllocatedBytes(precise: true) - b0;
            if (delta.Count != facts.Count)
            {
                throw new InvalidOperationException($"star built {delta.Count} rows, expected {facts.Count}");
            }

            // One integrate: the fact keyed by the tuple of the members' keys.
            b0 = GC.GetTotalAllocatedBytes(precise: true);
            t0 = Stopwatch.GetTimestamp();
            Integrate(trace, fact, tupleCols, members);
            if (hhTrace is not null)
            {
                Integrate(hhTrace, fact, [CHousehold], [household]);
            }

            integrateMs = Stopwatch.GetElapsedTime(t0).TotalMilliseconds;
            integrateBytes = GC.GetTotalAllocatedBytes(precise: true) - b0;
            GC.KeepAlive(delta);
        });
        Console.WriteLine(
            $"    star: build {buildMs:F0} ms {buildBytes / (1 << 20):N0} MiB, integrate {integrateMs:F0} ms {integrateBytes / (1 << 20):N0} MiB, groups={trace.GroupCount}");
        return (ms, bytes);
    }

    private static void Integrate(
        IGroupTrace<StructuralRow, StructuralRow, Z64> trace, ZSet<StructuralRow, Z64> fact, List<int> cols, Member[] members)
    {
        // The key columns COALESCE'd as the members evaluate them.
        var defaults = new Dictionary<int, object?>();
        foreach (var m in members)
        {
            for (var c = 0; c < m.KeyCols.Length; c++)
            {
                defaults[m.KeyCols[c]] = m.Defaults[c];
            }
        }

        var ix = new IndexedZSetBuilder<StructuralRow, StructuralRow, Z64>();
        var buf = new object?[cols.Count];
        foreach (var (r, w) in fact)
        {
            for (var i = 0; i < cols.Count; i++)
            {
                buf[i] = r[cols[i]] ?? defaults[cols[i]];
            }

            ix.Add(StructuralRow.Of(buf), r, w);
        }

        trace.Integrate(ix.Build());
    }

    // The cross product of the members' matches (here one each, or a pad), the row built once.
    private static void Expand(
        ZSetBuilder<StructuralRow, Z64> output, StructuralRow r, long w, Member[] members,
        List<(object? Out, long W)>?[] matches, int j, object?[] cells, long weight, object? one)
    {
        if (j == members.Length)
        {
            cells[0] = r[CEventTime];
            cells[10] = r[CEngPath];
            cells[11] = r[CCustPath];
            output.Add(StructuralRow.Of(cells), new Z64(w * weight));
            return;
        }

        if (matches[j] is not { Count: > 0 } ms)
        {
            cells[j + 1] = one;
            Expand(output, r, w, members, matches, j + 1, cells, weight, one);
            return;
        }

        foreach (var (o, mw) in ms)
        {
            cells[j + 1] = o ?? one;
            Expand(output, r, w, members, matches, j + 1, cells, weight * mw, one);
        }
    }
}
