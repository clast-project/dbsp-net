using System.Globalization;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

// Four canonical scenarios that together exercise every v1 SQL surface:
// filter, inner join, group-by, and a joined group-by. Each scenario builds
// a circuit, pushes a sequence of INSERT / DELETE deltas, prints the output
// delta at every step, and asserts at the end that the accumulated output
// matches a batch re-computation over the net input.
//
// The batch oracle is trivially correct (straight LINQ over the accumulated
// rows); the property we're testing is the equivalence
//     ∑ outputDeltas  ==  batch(∑ inputDeltas)
// which is the "incremental ≡ batch" guarantee DBSP gives us.

Scenario_Filter();
Scenario_InnerJoin();
Scenario_GroupBy();
Scenario_JoinedGroupBy();

Console.WriteLine();
Console.WriteLine("All four scenarios pass incremental == batch.");

static CompiledQuery Compile(string[] ddl, string query)
{
    var catalog = new Catalog();
    var resolver = new Resolver(catalog);
    foreach (var s in ddl)
    {
        resolver.Resolve(Parser.ParseStatement(s));
    }

    var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(query))).Query;
    return PlanToCircuit.Compile(plan);
}

static ZSet<StructuralRow, Z64> Accumulate(IEnumerable<ZSet<StructuralRow, Z64>> deltas)
{
    var acc = ZSet<StructuralRow, Z64>.Empty;
    foreach (var d in deltas)
    {
        acc += d;
    }

    return acc;
}

static void PrintBanner(string title)
{
    Console.WriteLine();
    Console.WriteLine(new string('=', 70));
    Console.WriteLine(title);
    Console.WriteLine(new string('=', 70));
}

static void PrintDelta(int step, ZSet<StructuralRow, Z64> z)
{
    if (z.IsEmpty)
    {
        Console.WriteLine($"  tick {step}: (no change)");
        return;
    }

    var entries = z
        .OrderBy(e => e.Key.ToString(), StringComparer.Ordinal)
        .Select(e => $"    {FormatWeight(e.Value)}  {e.Key}");
    Console.WriteLine($"  tick {step}:");
    foreach (var line in entries)
    {
        Console.WriteLine(line);
    }
}

static string FormatWeight(Z64 w)
{
    var v = w.Value;
    var sign = v < 0 ? '-' : '+';
    return $"{sign}{Math.Abs(v).ToString(CultureInfo.InvariantCulture),2}";
}

static void Assert(bool condition, string message)
{
    if (!condition)
    {
        throw new InvalidOperationException(message);
    }
}

static void AssertIncrementalEqualsBatch(
    CompiledQuery query,
    IEnumerable<ZSet<StructuralRow, Z64>> deltaHistory,
    ZSet<StructuralRow, Z64> batch)
{
    var incremental = Accumulate(deltaHistory);
    if (!incremental.Equals(batch))
    {
        Console.WriteLine("  MISMATCH!");
        Console.WriteLine("    incremental: " + incremental);
        Console.WriteLine("    batch:       " + batch);
        throw new InvalidOperationException("incremental != batch");
    }

    Console.WriteLine($"  incremental == batch  ({incremental.Count} rows, current net output)");
    _ = query;
}

static void Scenario_Filter()
{
    PrintBanner("Scenario 1 — filter: SELECT id, v FROM t WHERE v > 10");

    var q = Compile(
        new[] { "CREATE TABLE t (id INT NOT NULL, v INT NOT NULL)" },
        "SELECT id, v FROM t WHERE v > 10");

    var rows = new List<(int id, int v, long w)>();
    var deltas = new List<ZSet<StructuralRow, Z64>>();

    void StepAndRecord(int tick)
    {
        q.Step();
        deltas.Add(q.Current);
        PrintDelta(tick, q.Current);
    }

    // tick 1: bulk insert
    q.Table("t").Insert(1, 5);
    q.Table("t").Insert(2, 20);
    q.Table("t").Insert(3, 15);
    rows.AddRange(new[] { (1, 5, 1L), (2, 20, 1L), (3, 15, 1L) });
    StepAndRecord(1);

    // tick 2: one matching delete, one non-matching delete
    q.Table("t").Delete(2, 20);
    q.Table("t").Delete(1, 5);
    rows.AddRange(new[] { (2, 20, -1L), (1, 5, -1L) });
    StepAndRecord(2);

    // tick 3: add and immediately retract
    q.Table("t").Insert(4, 50);
    rows.Add((4, 50, 1L));
    StepAndRecord(3);

    q.Table("t").Delete(4, 50);
    rows.Add((4, 50, -1L));
    StepAndRecord(4);

    // Oracle: filter the accumulated input by v > 10.
    var oracle = rows
        .GroupBy(r => (r.id, r.v))
        .Select(g => (row: g.Key, weight: g.Sum(x => x.w)))
        .Where(x => x.row.v > 10 && x.weight != 0)
        .ToArray();
    var oracleZ = ZSet.FromEntries(oracle.Select(
        x => (new StructuralRow(x.row.id, x.row.v), new Z64(x.weight))));

    AssertIncrementalEqualsBatch(q, deltas, oracleZ);
}

static void Scenario_InnerJoin()
{
    PrintBanner("Scenario 2 — inner join: SELECT a.v, b.w FROM a JOIN b ON a.k = b.k");

    var q = Compile(
        new[]
        {
            "CREATE TABLE a (k INT NOT NULL, v INT NOT NULL)",
            "CREATE TABLE b (k INT NOT NULL, w INT NOT NULL)",
        },
        "SELECT a.v, b.w FROM a JOIN b ON a.k = b.k");

    var aRows = new List<(int k, int v, long w)>();
    var bRows = new List<(int k, int w, long weight)>();
    var deltas = new List<ZSet<StructuralRow, Z64>>();

    void StepAndRecord(int tick)
    {
        q.Step();
        deltas.Add(q.Current);
        PrintDelta(tick, q.Current);
    }

    // tick 1
    q.Table("a").Insert(1, 100);
    q.Table("a").Insert(2, 200);
    q.Table("b").Insert(1, 10);
    aRows.AddRange(new[] { (1, 100, 1L), (2, 200, 1L) });
    bRows.Add((1, 10, 1L));
    StepAndRecord(1);

    // tick 2 — arrive of a matching right-side row produces an output
    q.Table("b").Insert(2, 20);
    bRows.Add((2, 20, 1L));
    StepAndRecord(2);

    // tick 3 — extra row on left matches both b rows? no: it matches only k=1
    q.Table("a").Insert(1, 999);
    aRows.Add((1, 999, 1L));
    StepAndRecord(3);

    // tick 4 — retract one a row
    q.Table("a").Delete(1, 100);
    aRows.Add((1, 100, -1L));
    StepAndRecord(4);

    // Oracle.
    var aNet = aRows.GroupBy(r => (r.k, r.v)).Select(g => (r: g.Key, w: g.Sum(x => x.w)))
        .Where(x => x.w != 0).ToArray();
    var bNet = bRows.GroupBy(r => (r.k, r.w)).Select(g => (r: g.Key, weight: g.Sum(x => x.weight)))
        .Where(x => x.weight != 0).ToArray();

    var joined = new List<(StructuralRow Row, long W)>();
    foreach (var a in aNet)
    {
        foreach (var b in bNet)
        {
            if (a.r.k == b.r.k)
            {
                joined.Add((new StructuralRow(a.r.v, b.r.w), a.w * b.weight));
            }
        }
    }

    var oracleZ = ZSet.FromEntries(joined
        .GroupBy(x => x.Row)
        .Select(g => (g.Key, new Z64(g.Sum(x => x.W))))
        .Where(x => x.Item2.Value != 0));

    AssertIncrementalEqualsBatch(q, deltas, oracleZ);
}

static void Scenario_GroupBy()
{
    PrintBanner("Scenario 3 — group-by: SELECT dept, SUM(salary) FROM e GROUP BY dept");

    var q = Compile(
        new[] { "CREATE TABLE e (dept VARCHAR NOT NULL, salary INT NOT NULL)" },
        "SELECT dept, SUM(salary) AS total FROM e GROUP BY dept");

    var rows = new List<(string dept, int salary, long w)>();
    var deltas = new List<ZSet<StructuralRow, Z64>>();

    void StepAndRecord(int tick)
    {
        q.Step();
        deltas.Add(q.Current);
        PrintDelta(tick, q.Current);
    }

    q.Table("e").Insert("eng", 100);
    q.Table("e").Insert("eng", 200);
    q.Table("e").Insert("sales", 150);
    rows.AddRange(new[] { ("eng", 100, 1L), ("eng", 200, 1L), ("sales", 150, 1L) });
    StepAndRecord(1);

    // tick 2 — raise eng by adding 50; sales loses 50
    q.Table("e").Insert("eng", 50);
    q.Table("e").Delete("sales", 150);
    q.Table("e").Insert("sales", 100);
    rows.AddRange(new[] { ("eng", 50, 1L), ("sales", 150, -1L), ("sales", 100, 1L) });
    StepAndRecord(2);

    // Oracle.
    var net = rows
        .GroupBy(r => (r.dept, r.salary))
        .Select(g => (row: g.Key, w: g.Sum(x => x.w)))
        .Where(x => x.w != 0)
        .ToArray();
    var oracleZ = ZSet.FromEntries(
        net.GroupBy(x => x.row.dept)
           .Select(g => (new StructuralRow(g.Key, g.Sum(x => (long)x.row.salary * x.w)), new Z64(1))));

    AssertIncrementalEqualsBatch(q, deltas, oracleZ);
}

static void Scenario_JoinedGroupBy()
{
    PrintBanner("Scenario 4 — joined group-by: orders ⋈ customers, SUM by region");

    var q = Compile(
        new[]
        {
            "CREATE TABLE orders (cust INT NOT NULL, amount INT NOT NULL)",
            "CREATE TABLE customers (id INT NOT NULL, region VARCHAR NOT NULL)",
        },
        "SELECT c.region, SUM(o.amount) AS total " +
        "FROM orders o JOIN customers c ON o.cust = c.id " +
        "GROUP BY c.region");

    var orders = new List<(int cust, int amt, long w)>();
    var customers = new List<(int id, string region, long w)>();
    var deltas = new List<ZSet<StructuralRow, Z64>>();

    void StepAndRecord(int tick)
    {
        q.Step();
        deltas.Add(q.Current);
        PrintDelta(tick, q.Current);
    }

    q.Table("customers").Insert(1, "us");
    q.Table("customers").Insert(2, "us");
    q.Table("customers").Insert(3, "eu");
    customers.AddRange(new[] { (1, "us", 1L), (2, "us", 1L), (3, "eu", 1L) });

    q.Table("orders").Insert(1, 100);
    q.Table("orders").Insert(2, 50);
    q.Table("orders").Insert(3, 200);
    orders.AddRange(new[] { (1, 100, 1L), (2, 50, 1L), (3, 200, 1L) });
    StepAndRecord(1);

    q.Table("orders").Insert(3, 10);
    q.Table("orders").Delete(1, 100);
    orders.AddRange(new[] { (3, 10, 1L), (1, 100, -1L) });
    StepAndRecord(2);

    // Oracle.
    var oNet = orders.GroupBy(r => (r.cust, r.amt)).Select(g => (r: g.Key, w: g.Sum(x => x.w)))
        .Where(x => x.w != 0).ToArray();
    var cNet = customers.GroupBy(r => (r.id, r.region)).Select(g => (r: g.Key, w: g.Sum(x => x.w)))
        .Where(x => x.w != 0).ToArray();

    var joined = new List<((int cust, int amt, int id, string region), long W)>();
    foreach (var o in oNet)
    {
        foreach (var c in cNet)
        {
            if (o.r.cust == c.r.id)
            {
                joined.Add(((o.r.cust, o.r.amt, c.r.id, c.r.region), o.w * c.w));
            }
        }
    }

    var oracleZ = ZSet.FromEntries(
        joined.GroupBy(x => x.Item1.region)
              .Select(g => (new StructuralRow(g.Key, g.Sum(x => (long)x.Item1.amt * x.W)), new Z64(1))));

    AssertIncrementalEqualsBatch(q, deltas, oracleZ);
}

Assert(true, "unreachable");
