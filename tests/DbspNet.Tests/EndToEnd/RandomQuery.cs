// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using CsCheck;

namespace DbspNet.Tests.EndToEnd;

/// <summary>
/// Generators for random SQL query text + random tick plans, aimed at the
/// random-query PBT. All queries are over a fixed 2-table catalog:
/// <code>
///   CREATE TABLE t (k INT NOT NULL, v INT NOT NULL);
///   CREATE TABLE u (k INT NOT NULL, v INT NOT NULL);
/// </code>
/// Templates are parameterised by small random values (literal constants in
/// [-5, 15], column names from {k, v}, comparison operators). Every
/// template resolves without error under this fixed schema.
/// </summary>
internal static class RandomQuery
{
    public static readonly string[] FixedDdl =
    {
        "CREATE TABLE t (k INT NOT NULL, v INT NOT NULL)",
        "CREATE TABLE u (k INT NOT NULL, v INT NOT NULL)",
        // Nullable-column table — exercises NULL-key paths in joins, NULL-value
        // paths in aggregates, IS [NOT] NULL, COALESCE, NULLIF, etc.
        "CREATE TABLE n (k INT, v INT)",
    };

    // ---- Atom generators ----

    private static readonly Gen<int> GenLiteral = Gen.Int[-5, 15];
    private static readonly Gen<string> GenTable = Gen.OneOfConst("t", "u");
    private static readonly Gen<string> GenColumn = Gen.OneOfConst("k", "v");
    private static readonly Gen<string> GenCmp = Gen.OneOfConst("=", "<>", "<", "<=", ">", ">=");

    // ---- Query-shape generator ----
    //
    // Each template is a pure string builder over small random parameters.
    // Gen.OneOf picks uniformly. The last integer in each Select maps to
    // SQL text we produce.

    public static readonly Gen<string> GenQuery = Gen.OneOf(
        // 1. Simple scan
        GenTable.Select(t => $"SELECT k, v FROM {t}"),

        // 2. Filter: col cmp lit
        Gen.Select(GenTable, GenColumn, GenCmp, GenLiteral)
            .Select(p => $"SELECT k, v FROM {p.Item1} WHERE {p.Item2} {p.Item3} {p.Item4}"),

        // 3. Projection with arithmetic
        Gen.Select(GenTable, GenLiteral)
            .Select(p => $"SELECT k, v + {p.Item2} AS w FROM {p.Item1}"),

        // 4. Inner join
        Gen.Const("SELECT a.k, a.v, b.v FROM t a JOIN u b ON a.k = b.k"),

        // 5. Left outer join
        Gen.Const("SELECT a.k, a.v, b.v FROM t a LEFT JOIN u b ON a.k = b.k"),

        // 6. Right outer join
        Gen.Const("SELECT a.k, a.v, b.v FROM t a RIGHT JOIN u b ON a.k = b.k"),

        // 6b. Full outer join — symmetric match-presence tracking on both sides
        Gen.Const("SELECT a.k, a.v, b.k, b.v FROM t a FULL JOIN u b ON a.k = b.k"),

        // 6c. Full outer join over the nullable table — NULL-keyed rows on
        // either side bypass to their respective NULL-padded branch
        Gen.Const("SELECT a.k, a.v, b.k, b.v FROM n a FULL JOIN t b ON a.k = b.k"),

        // 7. GROUP BY SUM
        GenTable.Select(t => $"SELECT k, SUM(v) AS s FROM {t} GROUP BY k"),

        // 8. GROUP BY COUNT(*)
        GenTable.Select(t => $"SELECT k, COUNT(*) AS c FROM {t} GROUP BY k"),

        // 9. GROUP BY MIN/MAX
        GenTable.Select(t => $"SELECT k, MIN(v) AS mn, MAX(v) AS mx FROM {t} GROUP BY k"),

        // 9b. GROUP BY AVG / STDDEV / VARIANCE — the moment aggregators. Compared
        // against BatchPlanEvaluator over signed streams, so this exercises the
        // invertible sum/sum-of-squares accumulators under retraction. (AVG and
        // the STDDEV family had no PBT coverage before this.) SUM alongside pins
        // that the composite advances every sub-aggregator's state together.
        GenTable.Select(t =>
            $"SELECT k, SUM(v) AS s, AVG(v) AS a, STDDEV_POP(v) AS sp, VAR_SAMP(v) AS vs FROM {t} GROUP BY k"),

        // 9c. STDDEV over the nullable table — NULL args must be skipped, and an
        // all-NULL or single-non-NULL group must resolve to NULL correctly.
        Gen.Const("SELECT k, STDDEV_SAMP(v) AS ss, VAR_POP(v) AS vp FROM n GROUP BY k"),

        // 10. UNION ALL
        Gen.Const("SELECT k, v FROM t UNION ALL SELECT k, v FROM u"),

        // 11. Scalar subquery in WHERE
        GenTable.Select(t => $"SELECT k, v FROM {t} WHERE v > (SELECT MAX(v) FROM u)"),

        // 12. CTE
        Gen.Select(GenTable, GenLiteral)
            .Select(p => $"WITH c AS (SELECT k, v FROM {p.Item1} WHERE v > {p.Item2}) SELECT k, v FROM c"),

        // 13. CTE referenced twice (share-subcircuit)
        Gen.Const(
            "WITH c AS (SELECT k, v FROM t) " +
            "SELECT x.k, y.v FROM c x JOIN c y ON x.k = y.k"),

        // 14. Filter + group-by composition
        Gen.Select(GenTable, GenLiteral)
            .Select(p => $"SELECT k, SUM(v) AS s FROM {p.Item1} WHERE v > {p.Item2} GROUP BY k"),

        // 15. Join + group-by composition
        Gen.Const(
            "SELECT a.k, SUM(b.v) AS s " +
            "FROM t a JOIN u b ON a.k = b.k " +
            "GROUP BY a.k"),

        // 16. HAVING on GROUP BY
        Gen.Select(GenTable, GenLiteral)
            .Select(p => $"SELECT k, SUM(v) AS s FROM {p.Item1} GROUP BY k HAVING SUM(v) > {p.Item2}"),

        // 17. HAVING with COUNT(*)
        GenTable.Select(t => $"SELECT k FROM {t} GROUP BY k HAVING COUNT(*) > 1"),

        // 18. Nested CTEs
        Gen.Const(
            "WITH a AS (SELECT k, v FROM t WHERE v > 0), " +
            "     b AS (SELECT k, v FROM a WHERE k < 3) " +
            "SELECT k FROM b"),

        // 19. LEFT JOIN feeding a GROUP BY — retracts-on-match-flip interacting with aggregation
        Gen.Const(
            "SELECT a.k, SUM(COALESCE(b.v, 0)) AS s " +
            "FROM t a LEFT JOIN u b ON a.k = b.k " +
            "GROUP BY a.k"),

        // 20. UNION ALL feeding a GROUP BY (via a CTE since v1 lacks derived tables)
        Gen.Const(
            "WITH combined AS (SELECT k, v FROM t UNION ALL SELECT k, v FROM u) " +
            "SELECT k, SUM(v) AS s FROM combined GROUP BY k"),

        // ---- NULL-oriented templates over table `n` ----

        // 21. IS NULL filter
        Gen.Const("SELECT k, v FROM n WHERE v IS NULL"),

        // 22. IS NOT NULL filter
        Gen.Const("SELECT k, v FROM n WHERE k IS NOT NULL"),

        // 23. LEFT JOIN with NULL-keyed left rows — forces the
        // route-null-keys-through-the-bypass-branch path in IncrementalLeftJoinOp
        Gen.Const("SELECT a.k, a.v, b.v FROM n a LEFT JOIN t b ON a.k = b.k"),

        // 24. LEFT JOIN with nullable right table
        Gen.Const("SELECT a.k, a.v, b.v FROM t a LEFT JOIN n b ON a.k = b.k"),

        // 25. GROUP BY over nullable values — SUM should skip NULL v, NULL k
        // should form a single group (standard SQL semantics for GROUP BY NULL)
        Gen.Const("SELECT k, SUM(v) AS s FROM n GROUP BY k"),

        // 26. COUNT(col) vs COUNT(*) — differs exactly by NULL-valued rows
        Gen.Const("SELECT k, COUNT(v) AS cn, COUNT(*) AS ca FROM n GROUP BY k"),

        // 27. COALESCE over nullable column — scalar function with NULL handling
        GenLiteral.Select(p => $"SELECT COALESCE(v, {p}) AS w FROM n"),

        // 28. NULLIF between column and literal
        GenLiteral.Select(p => $"SELECT NULLIF(v, {p}) AS w FROM n"),

        // 29. Self-JOIN of nullable table on a nullable key
        Gen.Const("SELECT a.v, b.v FROM n a JOIN n b ON a.k = b.k"),

        // 30. Derived table in FROM (subquery with projection)
        GenLiteral.Select(p => $"SELECT x.v FROM (SELECT k, v FROM t WHERE v > {p}) x"),

        // 31. LEFT JOIN with a residual (non-equi ON conjunct). The circuit
        // lowers this to the anti-join rewrite; BatchPlanEvaluator implements
        // the semantics directly, so this comparison is a genuine differential
        // of two independent implementations — not a lowering against itself.
        //
        // The residual is CROSS-SIDE on purpose: that is what makes
        // match-presence a per-(key, left row) property rather than a per-key
        // one, which is the whole reason the operator can't express it. A left
        // row whose key matches but whose residual fails must still NULL-pad.
        GenLiteral.Select(p =>
            $"SELECT a.k, a.v, b.v FROM t a LEFT JOIN u b ON a.k = b.k AND a.v > b.v + {p}"),

        // 32. Same, right-side-only residual (the shape a filter pushdown could
        // in principle handle) — must agree with the general path.
        GenLiteral.Select(p =>
            $"SELECT a.k, a.v, b.v FROM t a LEFT JOIN u b ON a.k = b.k AND b.v > {p}"),

        // 33. Residual LEFT JOIN with a nullable PRESERVED side whose residual
        // references only the RIGHT columns. This is the shape that actually
        // reaches the NULL-safety hazard: a left row may carry a NULL value AND
        // still match (the residual doesn't read the NULL column), so it exercises
        // the anti-join's whole-row keying under NULLs. A residual referencing the
        // nullable column instead (as shape 34 does) can't reach it — NULL in the
        // residual is never TRUE, so a NULL-bearing row can't match. The anti-join
        // keys on the whole preserved row, NULLs included; dropping NULL rows (as
        // CompileSemiJoin does for probe keys) would let a matched row survive the
        // subtraction and emit a spurious NULL-pad beside its join output.
        GenLiteral.Select(p =>
            $"SELECT a.k, a.v, b.v FROM n a LEFT JOIN t b ON a.k = b.k AND b.v > {p}"),

        // 34. Residual over the nullable table on BOTH sides — the residual reads
        // the nullable column, so NULL rows fall out of matching naturally.
        GenLiteral.Select(p =>
            $"SELECT a.k, a.v, b.v FROM n a LEFT JOIN n b ON a.k = b.k AND a.v > b.v + {p}"),

        // 35. RIGHT JOIN with a residual — the preserved side is the right one.
        GenLiteral.Select(p =>
            $"SELECT a.k, a.v, b.v FROM t a RIGHT JOIN u b ON a.k = b.k AND a.v > b.v + {p}"),

        // 36. FULL JOIN with a residual — both sides preserved, independently.
        GenLiteral.Select(p =>
            $"SELECT a.k, a.v, b.k, b.v FROM t a FULL JOIN u b ON a.k = b.k AND a.v > b.v + {p}"),

        // 37. Keyless LEFT JOIN — no equi-key at all. Was rejected outright;
        // the rewrite lowers it as a unit-key cross product.
        Gen.Const("SELECT a.k, a.v, b.v FROM t a LEFT JOIN u b ON a.v > b.v"),

        // 38. Residual LEFT JOIN feeding a GROUP BY — pad/unpad flips must
        // propagate correctly into an aggregate.
        GenLiteral.Select(p =>
            $"SELECT a.k, COUNT(b.v) AS c, COUNT(*) AS ca " +
            $"FROM t a LEFT JOIN u b ON a.k = b.k AND a.v > b.v + {p} GROUP BY a.k"),

        // 31. Derived table with an aggregate inside
        Gen.Const("SELECT x.k, x.s FROM (SELECT k, SUM(v) AS s FROM t GROUP BY k) x WHERE x.s > 5"),

        // 32. Join of two derived tables
        Gen.Const(
            "SELECT x.v, y.v FROM " +
            "  (SELECT k, v FROM t) x " +
            "JOIN (SELECT k, v FROM u) y ON x.k = y.k"),

        // ---- Set ops (UNION / INTERSECT / EXCEPT) ----

        // 33. UNION (DISTINCT) — bag + dedup
        Gen.Const("SELECT k, v FROM t UNION SELECT k, v FROM u"),

        // 34. INTERSECT
        Gen.Const("SELECT k, v FROM t INTERSECT SELECT k, v FROM u"),

        // 35. EXCEPT
        Gen.Const("SELECT k, v FROM t EXCEPT SELECT k, v FROM u"),

        // 36. INTERSECT chain
        Gen.Const("SELECT v FROM t INTERSECT SELECT v FROM u INTERSECT SELECT k FROM t"),

        // 37. Nullable sides — NULL=NULL semantics
        Gen.Const("SELECT k, v FROM n INTERSECT SELECT k, v FROM n"),

        // 38. UNION with a filter on each side
        Gen.Select(GenLiteral, GenLiteral)
            .Select(p => $"SELECT v FROM t WHERE v > {p.Item1} UNION SELECT v FROM u WHERE v < {p.Item2}"),

        // 39. EXCEPT with a filter on the left
        GenLiteral.Select(p => $"SELECT k FROM t WHERE v > {p} EXCEPT SELECT k FROM u"),

        // 40. Precedence composition: UNION + INTERSECT
        Gen.Const(
            "SELECT v FROM t UNION SELECT v FROM u INTERSECT SELECT k FROM t"),

        // ---- CASE WHEN templates ----

        // 41. Searched CASE in projection — three-way bucketing, ELSE present
        // (non-nullable INT result).
        Gen.Select(GenTable, GenLiteral, GenLiteral)
            .Select(p =>
                $"SELECT k, CASE WHEN v > {p.Item2} THEN 1 WHEN v < {p.Item3} THEN -1 ELSE 0 END AS c " +
                $"FROM {p.Item1}"),

        // 42. Searched CASE with NO ELSE — unmatched rows yield NULL, so the
        // result column is nullable even though every branch is non-null.
        Gen.Select(GenTable, GenLiteral)
            .Select(p => $"SELECT k, CASE WHEN v > {p.Item2} THEN v END AS c FROM {p.Item1}"),

        // 43. Simple CASE (CASE operand WHEN val …) — desugars to `operand = val`
        // arms at parse time; ELSE references a column.
        GenTable.Select(t =>
            $"SELECT CASE k WHEN 0 THEN 100 WHEN 1 THEN 200 ELSE v END AS c FROM {t}"),

        // 44. Boolean-result CASE used as a WHERE predicate — exercises the
        // CompilePredicate (NULL→FALSE) edge over a CASE.
        Gen.Select(GenTable, GenLiteral, GenLiteral)
            .Select(p =>
                $"SELECT k, v FROM {p.Item1} " +
                $"WHERE CASE WHEN v > {p.Item2} THEN k < {p.Item3} ELSE k >= {p.Item3} END"),

        // 45. Conditional aggregation: CASE inside SUM — the classic pattern,
        // exercises aggregate roll-up through a CASE branch.
        Gen.Select(GenTable, GenLiteral)
            .Select(p =>
                $"SELECT k, SUM(CASE WHEN v > {p.Item2} THEN v ELSE 0 END) AS s " +
                $"FROM {p.Item1} GROUP BY k"),

        // 46. CASE over the nullable table — a NULL condition (v > lit when v is
        // NULL) must fall through to ELSE (SQL three-valued semantics).
        GenLiteral.Select(p => $"SELECT k, CASE WHEN v > {p} THEN 1 ELSE 0 END AS c FROM n"),

        // ---- Nullable-operand IN / NOT IN in non-WHERE positions ----
        // The probe (n.v / n.k) is nullable, so these resolve through the full
        // three-valued CASE rewrite (match/total/null hidden counts). The PBT
        // checks the resulting plan executes identically incrementally vs
        // batch across the flat / optimized / spine paths.

        // 47. Uncorrelated nullable IN in the SELECT list.
        Gen.Const("SELECT k, v IN (SELECT v FROM t) AS m FROM n"),

        // 48. Uncorrelated nullable NOT IN in the SELECT list.
        Gen.Const("SELECT k, v NOT IN (SELECT v FROM u) AS m FROM n"),

        // 49. Correlated nullable IN in the SELECT list (per-group counts).
        Gen.Const("SELECT k, v IN (SELECT v FROM t WHERE t.k = n.k) AS m FROM n"),

        // ---- IIF / DECODE (parse-time CASE desugar) ----

        // 50. IIF in projection.
        Gen.Select(GenTable, GenLiteral)
            .Select(p => $"SELECT k, IIF(v > {p.Item2}, 1, 0) AS f FROM {p.Item1}"),

        // 51. DECODE in projection (over the nullable table — exercises the
        // NULL-safe equality desugar against NULL keys).
        Gen.Const("SELECT k, DECODE(k, 0, 100, 1, 200, -1) AS d FROM n"),

        // ---- BETWEEN (parse-time comparison-conjunction desugar) ----

        // 52. BETWEEN as a WHERE range filter.
        Gen.Select(GenTable, GenLiteral, GenLiteral)
            .Select(p => $"SELECT k, v FROM {p.Item1} WHERE v BETWEEN {p.Item2} AND {p.Item3}"),

        // 53. NOT BETWEEN as a boolean projection over the nullable table
        // (3VL: a NULL probe yields NULL).
        Gen.Select(GenLiteral, GenLiteral)
            .Select(p => $"SELECT k, v NOT BETWEEN {p.Item1} AND {p.Item2} AS r FROM n"),

        // ---- || string concatenation (NULL-propagating) ----
        // The tables are INT-only, so cast to VARCHAR first.

        // 54. Concatenation over non-null columns.
        GenTable.Select(t =>
            $"SELECT CAST(k AS VARCHAR) || CAST(v AS VARCHAR) AS s FROM {t}"),

        // 55. Concatenation over the nullable table — a NULL operand makes the
        // whole result NULL (|| propagates, unlike CONCAT).
        Gen.Const("SELECT CAST(k AS VARCHAR) || CAST(v AS VARCHAR) AS s FROM n"),

        // ---- IS [NOT] DISTINCT FROM (NULL-safe (in)equality) ----

        // 56. NULL-safe equality as a WHERE filter over two nullable columns
        // (keeps both-NULL rows that `=` would drop).
        Gen.Const("SELECT k, v FROM n WHERE v IS NOT DISTINCT FROM k"),

        // 57. IS DISTINCT FROM as a (always-definite) boolean projection.
        Gen.Const("SELECT k, k IS DISTINCT FROM v AS d FROM n"),

        // ---- Non-equi / CROSS inner join (keyless unit-key nested loop) ----

        // 58. Non-equi inner join — the whole ON predicate becomes the residual,
        // filtered over the unit-key cross product.
        Gen.Const("SELECT a.k, a.v, b.v FROM t a JOIN u b ON a.v > b.v"),

        // 59. CROSS JOIN — full cartesian product (keyless inner, no residual).
        Gen.Const("SELECT a.k, a.v, b.v FROM t a CROSS JOIN u b"),

        // 60. Comma-join (implicit cross join) filtered by WHERE — the classic
        // pre-ANSI inner-join spelling.
        Gen.Const("SELECT a.k, a.v, b.v FROM t a, u b WHERE a.k = b.k"));

    // ---- Data generator ----

    // A nullable int drawn from [-2, 5]: values < 0 fold to NULL so ~25% of
    // draws are null, matching roughly how nulls appear in real SQL data.
    private static readonly Gen<object?> GenNullableInt =
        Gen.Int[-2, 5].Select(i => i < 0 ? null : (object?)i);

    private static readonly Gen<long> GenWeight = Gen.OneOfConst(1L, -1L);

    private static readonly Gen<InputEvent> GenEventT =
        Gen.Select(Gen.Int[0, 5], Gen.Int[0, 5], GenWeight)
            .Select(p => new InputEvent("t", [p.Item1, p.Item2], p.Item3));

    private static readonly Gen<InputEvent> GenEventU =
        Gen.Select(Gen.Int[0, 5], Gen.Int[0, 5], GenWeight)
            .Select(p => new InputEvent("u", [p.Item1, p.Item2], p.Item3));

    // Nullable-table events: each column is independently nullable.
    private static readonly Gen<InputEvent> GenEventN =
        Gen.Select(GenNullableInt, GenNullableInt, GenWeight)
            .Select(p => new InputEvent("n", [p.Item1, p.Item2], p.Item3));

    /// <summary>Generates a single INSERT/DELETE event on one of the three tables.</summary>
    public static readonly Gen<InputEvent> GenEvent = Gen.OneOf(GenEventT, GenEventU, GenEventN);

    /// <summary>
    /// A "tick plan" for the PBT — up to 8 ticks of up to 6 events. Longer
    /// sequences stress state accumulation in the incremental join/aggregate
    /// operators; shorter ones trigger common shrinks.
    /// </summary>
    public static readonly Gen<IReadOnlyList<IReadOnlyList<InputEvent>>> GenTicks =
        GenEvent.Array[0, 6]
            .Select(arr => (IReadOnlyList<InputEvent>)arr)
            .Array[1, 8]
            .Select(arr => (IReadOnlyList<IReadOnlyList<InputEvent>>)arr);
}
