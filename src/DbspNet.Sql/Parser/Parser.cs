// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System;
using System.Collections.Generic;
using DbspNet.Sql.Parser.Ast;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Sql.Parser;

/// <summary>
/// Recursive-descent statement parser with a Pratt expression parser. Input
/// is the token stream from <see cref="Lexer"/>; output is a
/// <see cref="SqlStatement"/> tree. The parser is strict about the v1 subset
/// (see the plan / <c>docs/skipped.md</c>); features outside the subset
/// produce a <see cref="ParseException"/> with source position.
/// </summary>
public sealed class Parser
{
    private readonly IReadOnlyList<Token> _tokens;
    private int _pos;

    public Parser(IReadOnlyList<Token> tokens)
    {
        ArgumentNullException.ThrowIfNull(tokens);
        _tokens = tokens;
    }

    public static SqlStatement ParseStatement(string source)
    {
        var tokens = Lexer.Tokenize(source);
        var p = new Parser(tokens);
        var stmt = p.ParseStatement();
        p.ExpectEndOfStatement();
        return stmt;
    }

    public static IReadOnlyList<SqlStatement> ParseStatements(string source)
    {
        var tokens = Lexer.Tokenize(source);
        var p = new Parser(tokens);
        var list = new List<SqlStatement>();
        while (p.Peek().Kind != TokenKind.EndOfInput)
        {
            list.Add(p.ParseStatement());
            while (p.Peek().Kind == TokenKind.Semicolon)
            {
                p.Advance();
            }
        }

        return list;
    }

    public SqlStatement ParseStatement()
    {
        var t = Peek();
        return t.Kind switch
        {
            TokenKind.With => ParseQuery(),
            TokenKind.Create => ParseCreate(),
            TokenKind.Select => ParseQuery(),
            _ => throw Error(t, $"expected WITH, CREATE or SELECT, got {Describe(t)}"),
        };
    }

    private void ExpectEndOfStatement()
    {
        while (Peek().Kind == TokenKind.Semicolon)
        {
            Advance();
        }

        var t = Peek();
        if (t.Kind != TokenKind.EndOfInput)
        {
            throw Error(t, $"expected end of input, got {Describe(t)}");
        }
    }

    // ---------------- CREATE TABLE / CREATE VIEW ----------------

    private SqlStatement ParseCreate()
    {
        Expect(TokenKind.Create);
        var t = Peek();
        return t.Kind switch
        {
            TokenKind.Table => ParseCreateTable(),
            TokenKind.View => ParseCreateView(),
            _ => throw Error(t, "expected TABLE or VIEW after CREATE"),
        };
    }

    private CreateTableStatement ParseCreateTable()
    {
        Expect(TokenKind.Table);
        var name = ExpectIdentifier("table name");
        Expect(TokenKind.LParen);
        var columns = new List<ColumnDefinition>();
        columns.Add(ParseColumnDefinition());
        while (Peek().Kind == TokenKind.Comma)
        {
            Advance();
            columns.Add(ParseColumnDefinition());
        }

        Expect(TokenKind.RParen);
        return new CreateTableStatement(name, columns);
    }

    private ColumnDefinition ParseColumnDefinition()
    {
        var name = ExpectIdentifier("column name");
        var type = ParseTypeSpec();
        bool? explicitNullability = null;
        var primaryKey = false;
        long? lateness = null;
        while (true)
        {
            var t = Peek();
            if (t.Kind == TokenKind.Not)
            {
                Advance();
                Expect(TokenKind.Null);
                explicitNullability = false;
            }
            else if (t.Kind == TokenKind.Null)
            {
                Advance();
                explicitNullability = true;
            }
            else if (t.Kind == TokenKind.Primary)
            {
                Advance();
                Expect(TokenKind.Key);
                primaryKey = true;
            }
            else if (t.Kind == TokenKind.Lateness)
            {
                Advance();
                // Bound is an integer in the column's native units (microseconds
                // for temporal columns, a raw offset for integer logical-time
                // columns). Duration-literal sugar (INTERVAL '1' HOUR) is deferred.
                lateness = ExpectLongLiteral("LATENESS bound");
            }
            else
            {
                break;
            }
        }

        var notNull = explicitNullability == false;
        return new ColumnDefinition(name, type, notNull, primaryKey, lateness);
    }

    private SqlTypeSpec ParseTypeSpec()
    {
        var t = Peek();
        switch (t.Kind)
        {
            case TokenKind.Int:
            case TokenKind.Integer:
                Advance();
                return new SqlTypeSpec("INTEGER");
            case TokenKind.BigInt:
                Advance();
                return new SqlTypeSpec("BIGINT");
            case TokenKind.TinyInt:
            case TokenKind.SmallInt:
                // No native 8/16-bit type; widen to INTEGER (lossy, like CHAR→VARCHAR).
                // Benchmark values are small flags/tiers where this is exact.
                Advance();
                return new SqlTypeSpec("INTEGER");
            case TokenKind.Real:
                Advance();
                return new SqlTypeSpec("REAL");
            case TokenKind.Double:
                Advance();
                // Optional PRECISION
                if (Peek().Kind == TokenKind.Precision)
                {
                    Advance();
                }

                return new SqlTypeSpec("DOUBLE PRECISION");
            case TokenKind.Decimal:
            case TokenKind.Numeric:
                Advance();
                int? p1 = null, p2 = null;
                if (Peek().Kind == TokenKind.LParen)
                {
                    Advance();
                    p1 = ExpectIntegerLiteral("precision");
                    if (Peek().Kind == TokenKind.Comma)
                    {
                        Advance();
                        p2 = ExpectIntegerLiteral("scale");
                    }

                    Expect(TokenKind.RParen);
                }

                return new SqlTypeSpec("DECIMAL", p1, p2);
            case TokenKind.Varchar:
                Advance();
                int? vlen = null;
                if (Peek().Kind == TokenKind.LParen)
                {
                    Advance();
                    vlen = ExpectIntegerLiteral("length");
                    Expect(TokenKind.RParen);
                }

                return new SqlTypeSpec("VARCHAR", vlen);
            case TokenKind.Char:
                Advance();
                int? clen = null;
                if (Peek().Kind == TokenKind.LParen)
                {
                    Advance();
                    clen = ExpectIntegerLiteral("length");
                    Expect(TokenKind.RParen);
                }

                return new SqlTypeSpec("CHAR", clen);
            case TokenKind.Text:
                Advance();
                return new SqlTypeSpec("VARCHAR");
            case TokenKind.Boolean:
            case TokenKind.Bool:
                Advance();
                return new SqlTypeSpec("BOOLEAN");
            case TokenKind.Date:
                Advance();
                return new SqlTypeSpec("DATE");
            case TokenKind.Time:
                Advance();
                return new SqlTypeSpec("TIME");
            case TokenKind.Timestamp:
                Advance();
                return new SqlTypeSpec("TIMESTAMP");
            case TokenKind.Interval:
                Advance();
                return new SqlTypeSpec("INTERVAL", IntervalQualifier: ParseIntervalQualifier());
            case TokenKind.Row:
                return ParseRowTypeSpec();
            default:
                throw Error(t, $"expected SQL type, got {Describe(t)}");
        }
    }

    /// <summary>
    /// Parse a <c>ROW( field TYPE [NULL | NOT NULL], … )</c> struct type spec,
    /// recursively (fields may themselves be <c>ROW</c>). The resolver flattens
    /// these into dotted-name scalar leaf columns — there is no runtime struct
    /// value. See <c>docs/design-nested-types.md</c>.
    /// </summary>
    private SqlTypeSpec ParseRowTypeSpec()
    {
        Expect(TokenKind.Row);
        Expect(TokenKind.LParen);
        var fields = new List<RowFieldSpec>();
        while (true)
        {
            var fieldName = ExpectFieldName("ROW field name");
            var fieldType = ParseTypeSpec();

            // Per-field nullability. Fields default to nullable (the ivm-bench /
            // Parquet convention); an explicit NOT NULL tightens it.
            var nullable = true;
            if (Peek().Kind == TokenKind.Not)
            {
                Advance();
                Expect(TokenKind.Null);
                nullable = false;
            }
            else if (Peek().Kind == TokenKind.Null)
            {
                Advance();
            }

            fields.Add(new RowFieldSpec(fieldName, fieldType, nullable));

            if (Peek().Kind == TokenKind.Comma)
            {
                Advance();
                continue;
            }

            break;
        }

        Expect(TokenKind.RParen);
        return new SqlTypeSpec("ROW", Fields: fields);
    }

    /// <summary>
    /// Parse an interval field qualifier — a single field (<c>DAY</c>,
    /// <c>SECOND</c>, …) or one of the supported compound forms
    /// (<c>YEAR TO MONTH</c>, <c>DAY TO SECOND</c>). The field words are
    /// non-reserved (lexed as identifiers), so only <c>INTERVAL</c> itself is
    /// a keyword — a column named <c>day</c> still parses.
    /// </summary>
    private IntervalQualifier ParseIntervalQualifier()
    {
        var leadTok = Peek();
        var lead = ExpectIdentifier("interval field");
        var field = IntervalQualifiers.ParseField(lead)
            ?? throw Error(leadTok, $"unknown interval field '{lead}'");

        if (Peek().Kind == TokenKind.Identifier
            && string.Equals(Peek().Text, "to", StringComparison.OrdinalIgnoreCase))
        {
            Advance();
            var trailTok = Peek();
            var trail = ExpectIdentifier("interval field");
            var trailField = IntervalQualifiers.ParseField(trail)
                ?? throw Error(trailTok, $"unknown interval field '{trail}'");

            return (field, trailField) switch
            {
                (IntervalQualifier.Year, IntervalQualifier.Month) => IntervalQualifier.YearToMonth,
                (IntervalQualifier.Day, IntervalQualifier.Second) => IntervalQualifier.DayToSecond,
                _ => throw Error(leadTok, $"unsupported interval qualifier '{lead} TO {trail}'"),
            };
        }

        return field;
    }

    private CreateViewStatement ParseCreateView()
    {
        Expect(TokenKind.View);
        var name = ExpectIdentifier("view name");
        Expect(TokenKind.As);
        var q = ParseQuery();
        return new CreateViewStatement(name, q);
    }

    // ---------------- Query expressions (SELECT / UNION ALL) ----------------

    /// <summary>
    /// Parse a full query expression: an optional <c>WITH</c> clause
    /// followed by one or more SELECTs chained by set operations
    /// (<c>UNION</c>, <c>UNION ALL</c>, <c>INTERSECT</c>, <c>EXCEPT</c>).
    /// INTERSECT binds tighter than UNION / EXCEPT per SQL standard.
    /// Same-kind chains collapse to a single N-branch <see cref="SetOpQuery"/>.
    /// </summary>
    private SqlQuery ParseQuery()
    {
        IReadOnlyList<CteDefinition> ctes = Array.Empty<CteDefinition>();
        if (Peek().Kind == TokenKind.With)
        {
            ctes = ParseWithClause();
        }

        var query = ParseUnionLevel();
        query = ParseOrderByLimit(query);
        return AttachCtes(query, ctes);
    }

    // Parse an optional trailing `ORDER BY` and/or `LIMIT`/`OFFSET`/`FETCH FIRST`
    // tail. These bind to the whole query expression (per SQL grammar), so they
    // sit above any set-op chain. Returns <paramref name="input"/> unchanged when
    // no clause is present.
    private SqlQuery ParseOrderByLimit(SqlQuery input)
    {
        IReadOnlyList<SortItem> orderBy = Array.Empty<SortItem>();
        if (Peek().Kind == TokenKind.Order)
        {
            Advance();
            Expect(TokenKind.By);
            var items = new List<SortItem> { ParseSortItem() };
            while (Peek().Kind == TokenKind.Comma)
            {
                Advance();
                items.Add(ParseSortItem());
            }

            orderBy = items;
        }

        long? limit = null;
        long? offset = null;
        var sawLimit = false;
        var sawOffset = false;
        while (true)
        {
            var t = Peek();
            if (t.Kind == TokenKind.Limit)
            {
                if (sawLimit)
                {
                    throw Error(t, "duplicate LIMIT/FETCH clause");
                }

                Advance();
                sawLimit = true;
                if (Peek().Kind == TokenKind.All)
                {
                    Advance(); // LIMIT ALL — no bound.
                }
                else
                {
                    limit = ParseNonNegativeLong("LIMIT count");
                }
            }
            else if (t.Kind == TokenKind.Fetch)
            {
                if (sawLimit)
                {
                    throw Error(t, "duplicate LIMIT/FETCH clause");
                }

                // FETCH { FIRST | NEXT } [n] { ROW | ROWS } ONLY  (n defaults to 1).
                Advance();
                sawLimit = true;
                if (Peek().Kind is TokenKind.First or TokenKind.Next)
                {
                    Advance();
                }
                else
                {
                    throw Error(Peek(), $"expected FIRST or NEXT after FETCH, got {Describe(Peek())}");
                }

                limit = Peek().Kind == TokenKind.IntegerLiteral ? ParseNonNegativeLong("FETCH count") : 1L;
                if (Peek().Kind is TokenKind.Row or TokenKind.Rows)
                {
                    Advance();
                }
                else
                {
                    throw Error(Peek(), $"expected ROW or ROWS in FETCH clause, got {Describe(Peek())}");
                }

                Expect(TokenKind.Only);
            }
            else if (t.Kind == TokenKind.Offset)
            {
                if (sawOffset)
                {
                    throw Error(t, "duplicate OFFSET clause");
                }

                Advance();
                sawOffset = true;
                offset = ParseNonNegativeLong("OFFSET count");
                if (Peek().Kind is TokenKind.Row or TokenKind.Rows)
                {
                    Advance();
                }
            }
            else
            {
                break;
            }
        }

        if (orderBy.Count == 0 && limit is null && offset is null)
        {
            return input;
        }

        return new OrderLimitQuery(input, orderBy, limit, offset, Array.Empty<CteDefinition>());
    }

    private SortItem ParseSortItem()
    {
        var expr = ParseExpression();
        var direction = SortDirection.Ascending;
        if (Peek().Kind == TokenKind.Asc)
        {
            Advance();
        }
        else if (Peek().Kind == TokenKind.Desc)
        {
            Advance();
            direction = SortDirection.Descending;
        }

        var nulls = NullOrdering.Default;
        if (Peek().Kind == TokenKind.Nulls)
        {
            Advance();
            if (Peek().Kind == TokenKind.First)
            {
                Advance();
                nulls = NullOrdering.NullsFirst;
            }
            else if (Peek().Kind == TokenKind.Last)
            {
                Advance();
                nulls = NullOrdering.NullsLast;
            }
            else
            {
                throw Error(Peek(), $"expected FIRST or LAST after NULLS, got {Describe(Peek())}");
            }
        }

        return new SortItem(expr, direction, nulls);
    }

    private long ParseNonNegativeLong(string what)
    {
        var t = Peek();
        var value = ExpectLongLiteral(what);
        if (value < 0)
        {
            throw Error(t, $"{what} must be non-negative");
        }

        return value;
    }

    // union_level ::= intersect_level ( (UNION [ALL] | EXCEPT) intersect_level )*
    private SqlQuery ParseUnionLevel()
    {
        var left = ParseIntersectLevel();
        while (Peek().Kind is TokenKind.Union or TokenKind.Except)
        {
            var opTok = Peek();
            Advance();

            SetOpKind kind;
            if (opTok.Kind == TokenKind.Union)
            {
                if (Peek().Kind == TokenKind.All)
                {
                    Advance();
                    kind = SetOpKind.UnionAll;
                }
                else
                {
                    kind = SetOpKind.Union;
                }
            }
            else
            {
                if (Peek().Kind == TokenKind.All)
                {
                    throw Error(Peek(), "EXCEPT ALL is not supported in v1; use plain EXCEPT");
                }

                kind = SetOpKind.Except;
            }

            var right = ParseIntersectLevel();
            left = Append(left, kind, right);
        }

        return left;
    }

    // intersect_level ::= simple_select ( INTERSECT simple_select )*
    private SqlQuery ParseIntersectLevel()
    {
        SqlQuery left = ParseSimpleSelect();
        while (Peek().Kind == TokenKind.Intersect)
        {
            Advance();
            if (Peek().Kind == TokenKind.All)
            {
                throw Error(Peek(), "INTERSECT ALL is not supported in v1; use plain INTERSECT");
            }

            var right = ParseSimpleSelect();
            left = Append(left, SetOpKind.Intersect, right);
        }

        return left;
    }

    private static SqlQuery Append(SqlQuery left, SetOpKind kind, SqlQuery right)
    {
        // Flatten same-kind chains so `a UNION ALL b UNION ALL c` produces a
        // single 3-branch SetOpQuery.
        if (left is SetOpQuery existing && existing.Kind == kind && existing.Ctes.Count == 0)
        {
            var combined = new List<SqlQuery>(existing.Branches.Count + 1);
            combined.AddRange(existing.Branches);
            combined.Add(right);
            return new SetOpQuery(kind, combined, Array.Empty<CteDefinition>());
        }

        return new SetOpQuery(kind, new List<SqlQuery> { left, right }, Array.Empty<CteDefinition>());
    }

    private static SqlQuery AttachCtes(SqlQuery q, IReadOnlyList<CteDefinition> ctes)
    {
        if (ctes.Count == 0)
        {
            return q;
        }

        return q switch
        {
            SelectStatement s => s with { Ctes = ctes },
            SetOpQuery set => set with { Ctes = ctes },
            OrderLimitQuery o => o with { Ctes = ctes },
            _ => throw new InvalidOperationException($"cannot attach CTEs to {q.GetType().Name}"),
        };
    }

    private SelectStatement ParseSimpleSelect()
    {
        Expect(TokenKind.Select);

        // Optional set quantifier. DISTINCT dedups the projected rows; ALL is
        // the (default) bag-semantics no-op.
        var distinct = false;
        if (Peek().Kind == TokenKind.Distinct)
        {
            Advance();
            distinct = true;
        }
        else if (Peek().Kind == TokenKind.All)
        {
            Advance();
        }

        var items = ParseSelectItems();
        Expect(TokenKind.From);
        var from = ParseFromClause();
        Expression? where = null;
        if (Peek().Kind == TokenKind.Where)
        {
            Advance();
            where = ParseExpression();
        }

        var groupBy = new List<Expression>();
        if (Peek().Kind == TokenKind.Group)
        {
            Advance();
            Expect(TokenKind.By);
            groupBy.Add(ParseExpression());
            while (Peek().Kind == TokenKind.Comma)
            {
                Advance();
                groupBy.Add(ParseExpression());
            }
        }

        Expression? having = null;
        if (Peek().Kind == TokenKind.Having)
        {
            Advance();
            having = ParseExpression();
        }

        // Optional named-window clause: WINDOW w AS ( … ), w2 AS ( … ). Parsed
        // after the SELECT list that references it (`… OVER w`), so references are
        // substituted here, once the definitions are known. Purely syntactic — the
        // resolver never sees a named window.
        if (Peek().Kind == TokenKind.Window)
        {
            Advance();
            var windows = new Dictionary<string, WindowSpec>(StringComparer.Ordinal);
            while (true)
            {
                var name = ExpectIdentifier("window name");
                Expect(TokenKind.As);
                if (!windows.TryAdd(name, ParseWindowSpec()))
                {
                    throw Error(Peek(), $"window '{name}' is defined more than once");
                }

                if (Peek().Kind == TokenKind.Comma)
                {
                    Advance();
                    continue;
                }

                break;
            }

            items = items.Select(it => it is ExpressionSelectItem esi
                ? esi with { Expression = SubstituteNamedWindows(esi.Expression, windows) }
                : it).ToList();
        }

        return new SelectStatement(items, from, where, groupBy, having, Array.Empty<CteDefinition>(), distinct);
    }

    /// <summary>
    /// Replace every <c>OVER w</c> named-window reference (a placeholder
    /// <see cref="WindowSpec"/> with <see cref="WindowSpec.Name"/> set) in an
    /// expression tree with the definition from <paramref name="windows"/>.
    /// Recurses through the same node shapes as the window-lifting walkers; a
    /// window function's own arguments are also visited. An unknown name is a
    /// parse error.
    /// </summary>
    private Expression SubstituteNamedWindows(Expression e, Dictionary<string, WindowSpec> windows)
    {
        switch (e)
        {
            case WindowFunctionExpression w:
            {
                var args = w.Arguments.Select(a => SubstituteNamedWindows(a, windows)).ToList();
                var over = w.Over;
                if (over.Name is { } name)
                {
                    if (!windows.TryGetValue(name, out var def))
                    {
                        throw Error(Peek(), $"window '{name}' is not defined");
                    }

                    over = def;
                }

                return w with { Arguments = args, Over = over };
            }

            case BinaryExpression b:
                return b with
                {
                    Left = SubstituteNamedWindows(b.Left, windows),
                    Right = SubstituteNamedWindows(b.Right, windows),
                };
            case UnaryExpression u:
                return u with { Operand = SubstituteNamedWindows(u.Operand, windows) };
            case IsNullExpression isn:
                return isn with { Operand = SubstituteNamedWindows(isn.Operand, windows) };
            case CastExpression c:
                return c with { Operand = SubstituteNamedWindows(c.Operand, windows) };
            case FunctionCallExpression f:
                return f with { Arguments = f.Arguments.Select(a => SubstituteNamedWindows(a, windows)).ToList() };
            case InListExpression il:
                return il with
                {
                    Probe = SubstituteNamedWindows(il.Probe, windows),
                    Values = il.Values.Select(v => SubstituteNamedWindows(v, windows)).ToList(),
                };
            case CaseExpression ce:
                return ce with
                {
                    Whens = ce.Whens.Select(wc => new CaseWhenClause(
                        SubstituteNamedWindows(wc.Condition, windows),
                        SubstituteNamedWindows(wc.Result, windows))).ToList(),
                    ElseResult = ce.ElseResult is null ? null : SubstituteNamedWindows(ce.ElseResult, windows),
                };
            default:
                return e;
        }
    }

    private List<CteDefinition> ParseWithClause()
    {
        Expect(TokenKind.With);
        // `WITH RECURSIVE` sets the flag on every CTE in this clause. SQL
        // standard: RECURSIVE permits self-reference; whether a CTE is
        // actually recursive is determined later by inspection.
        var recursive = false;
        if (Peek().Kind == TokenKind.Recursive)
        {
            Advance();
            recursive = true;
        }

        var ctes = new List<CteDefinition> { ParseCteDefinition(recursive) };
        while (Peek().Kind == TokenKind.Comma)
        {
            Advance();
            ctes.Add(ParseCteDefinition(recursive));
        }

        return ctes;
    }

    private CteDefinition ParseCteDefinition(bool recursive)
    {
        var name = ExpectIdentifier("CTE name");
        Expect(TokenKind.As);
        Expect(TokenKind.LParen);
        var inner = ParseQuery();
        Expect(TokenKind.RParen);
        return new CteDefinition(name, inner, recursive);
    }

    private List<SelectItem> ParseSelectItems()
    {
        var items = new List<SelectItem> { ParseSelectItem() };
        while (Peek().Kind == TokenKind.Comma)
        {
            Advance();
            items.Add(ParseSelectItem());
        }

        return items;
    }

    private SelectItem ParseSelectItem()
    {
        // SELECT *
        if (Peek().Kind == TokenKind.Star)
        {
            Advance();
            return new StarSelectItem(TableQualifier: null);
        }

        // SELECT t.*
        if (Peek().Kind == TokenKind.Identifier
            && _pos + 1 < _tokens.Count
            && _tokens[_pos + 1].Kind == TokenKind.Dot
            && _pos + 2 < _tokens.Count
            && _tokens[_pos + 2].Kind == TokenKind.Star)
        {
            var qual = Advance().Text;
            Advance(); // .
            Advance(); // *
            return new StarSelectItem(qual);
        }

        var expr = ParseExpression();
        string? alias = null;
        if (Peek().Kind == TokenKind.As)
        {
            Advance();
            alias = ExpectIdentifier("column alias");
        }
        else if (Peek().Kind == TokenKind.Identifier)
        {
            alias = Advance().Text;
        }

        return new ExpressionSelectItem(expr, alias);
    }

    private FromClause ParseFromClause()
    {
        // Comma-separated table references are implicit cross joins, with the
        // lowest precedence (explicit JOINs inside each reference bind tighter).
        // `FROM a, b` ≡ `FROM a CROSS JOIN b`: desugar each comma to an INNER
        // JOIN with a literal-true ON so it rides the keyless (unit-key)
        // inner-join path, exactly like the CROSS JOIN keyword.
        var result = ParseJoinExpression();
        while (Peek().Kind == TokenKind.Comma)
        {
            Advance();
            var next = ParseJoinExpression();
            var onTrue = new LiteralExpression(LiteralKind.Boolean, true);
            result = new JoinClause(result, next, JoinType.Inner, onTrue);
        }

        return result;
    }

    private FromClause ParseJoinExpression()
    {
        FromClause left = ParsePrimaryTableRef();
        while (true)
        {
            var t = Peek();
            JoinType joinType;

            // CROSS JOIN: unconditional pairing, no ON/USING. Desugar to an
            // INNER JOIN with a literal-true ON predicate so it rides the
            // existing keyless (unit-key) inner-join path in the resolver.
            if (t.Kind == TokenKind.Cross)
            {
                Advance();
                Expect(TokenKind.Join);
                var crossRight = ParsePrimaryTableRef();
                var onTrue = new LiteralExpression(LiteralKind.Boolean, true);
                left = new JoinClause(left, crossRight, JoinType.Inner, onTrue);
                continue;
            }

            if (t.Kind == TokenKind.Inner)
            {
                Advance();
                Expect(TokenKind.Join);
                joinType = JoinType.Inner;
            }
            else if (t.Kind == TokenKind.Left)
            {
                Advance();
                if (Peek().Kind == TokenKind.Outer)
                {
                    Advance();
                }

                Expect(TokenKind.Join);
                joinType = JoinType.LeftOuter;
            }
            else if (t.Kind == TokenKind.Right)
            {
                Advance();
                if (Peek().Kind == TokenKind.Outer)
                {
                    Advance();
                }

                Expect(TokenKind.Join);
                joinType = JoinType.RightOuter;
            }
            else if (t.Kind == TokenKind.Full)
            {
                Advance();
                if (Peek().Kind == TokenKind.Outer)
                {
                    Advance();
                }

                Expect(TokenKind.Join);
                joinType = JoinType.FullOuter;
            }
            else if (t.Kind == TokenKind.Join)
            {
                // Bare JOIN is INNER.
                Advance();
                joinType = JoinType.Inner;
            }
            else
            {
                break;
            }

            var right = ParsePrimaryTableRef();

            // The join condition is either `ON <predicate>` or
            // `USING (col, …)`. USING is desugared by the resolver into an
            // equi-join on the named columns plus a merged-column projection.
            if (Peek().Kind == TokenKind.Using)
            {
                Advance();
                Expect(TokenKind.LParen);
                var cols = new List<string> { ExpectIdentifier("USING column name") };
                while (Peek().Kind == TokenKind.Comma)
                {
                    Advance();
                    cols.Add(ExpectIdentifier("USING column name"));
                }

                Expect(TokenKind.RParen);
                left = new JoinClause(left, right, joinType, OnCondition: null, cols);
            }
            else
            {
                Expect(TokenKind.On);
                var cond = ParseExpression();
                left = new JoinClause(left, right, joinType, cond);
            }
        }

        return left;
    }

    private FromClause ParsePrimaryTableRef()
    {
        // `TABLE(TUMBLE|HOP(TABLE src, DESCRIPTOR(col), …))` — a streaming
        // windowing table-valued function (Feldera / Flink syntax).
        if (Peek().Kind == TokenKind.Table)
        {
            return ParseWindowTableFunction();
        }

        // `(SELECT …)` / `(WITH … SELECT …)` / `(…UNION ALL…)` in FROM
        // position is a derived table. SQL requires an alias for these.
        if (Peek().Kind == TokenKind.LParen)
        {
            var openParen = Peek();
            Advance();
            var inner = ParseQuery();
            Expect(TokenKind.RParen);

            string? derivedAlias = null;
            if (Peek().Kind == TokenKind.As)
            {
                Advance();
                derivedAlias = ExpectIdentifier("derived table alias");
            }
            else if (Peek().Kind == TokenKind.Identifier)
            {
                derivedAlias = Advance().Text;
            }

            if (derivedAlias is null)
            {
                throw new ParseException(
                    "derived table (subquery in FROM) requires an alias", openParen.Position);
            }

            return new DerivedTableReference(inner, derivedAlias);
        }

        var name = ExpectIdentifier("table name");
        string? alias = null;
        if (Peek().Kind == TokenKind.As)
        {
            Advance();
            alias = ExpectIdentifier("table alias");
        }
        else if (Peek().Kind == TokenKind.Identifier)
        {
            alias = Advance().Text;
        }

        return new TableReference(name, alias);
    }

    /// <summary>
    /// Parse a windowing table-valued function:
    /// <c>TABLE ( {TUMBLE|HOP} ( TABLE &lt;name&gt;, DESCRIPTOR(&lt;col&gt;), &lt;interval&gt;[, &lt;interval&gt;] ) ) [AS alias]</c>.
    /// TUMBLE takes one size INTERVAL; HOP takes a slide then a size INTERVAL.
    /// <c>TUMBLE</c> / <c>HOP</c> / <c>DESCRIPTOR</c> are contextual (matched by
    /// identifier text), so they stay usable as ordinary names.
    /// </summary>
    private FromClause ParseWindowTableFunction()
    {
        Expect(TokenKind.Table);
        Expect(TokenKind.LParen);

        var fnTok = Peek();
        var kind = ExpectIdentifier("windowing function (TUMBLE or HOP)");
        if (kind is not ("tumble" or "hop"))
        {
            throw Error(fnTok, $"unsupported windowing table function '{kind}' (expected TUMBLE or HOP)");
        }

        Expect(TokenKind.LParen);

        // Data argument: `TABLE <name>`. (A `(SELECT …)` subquery source is not
        // yet supported — wrap the windowing TVF over a base table.)
        if (Peek().Kind != TokenKind.Table)
        {
            throw Error(Peek(), $"{kind.ToUpperInvariant()} data argument must be `TABLE <name>`");
        }

        Advance();
        var srcName = ExpectIdentifier("table name");
        FromClause source = new TableReference(srcName, null);
        Expect(TokenKind.Comma);

        // DESCRIPTOR(timecol).
        if (!IsContextualKeyword("descriptor"))
        {
            throw Error(Peek(), $"{kind.ToUpperInvariant()} requires DESCRIPTOR(<time column>) as its second argument");
        }

        Advance();
        Expect(TokenKind.LParen);
        var timeColumn = ExpectIdentifier("descriptor time column");
        Expect(TokenKind.RParen);
        Expect(TokenKind.Comma);

        // Size args: one (TUMBLE) or two (HOP slide, size) INTERVAL expressions.
        var args = new List<Expression> { ParseExpression() };
        while (Peek().Kind == TokenKind.Comma)
        {
            Advance();
            args.Add(ParseExpression());
        }

        var expected = kind == "hop" ? 2 : 1;
        if (args.Count != expected)
        {
            throw Error(fnTok,
                $"{kind.ToUpperInvariant()} takes {expected} INTERVAL argument(s) (got {args.Count})");
        }

        Expect(TokenKind.RParen); // close windowing function
        Expect(TokenKind.RParen); // close TABLE(...)

        string? alias = null;
        if (Peek().Kind == TokenKind.As)
        {
            Advance();
            alias = ExpectIdentifier("table alias");
        }
        else if (Peek().Kind == TokenKind.Identifier)
        {
            alias = Advance().Text;
        }

        return new WindowTableFunction(kind, source, timeColumn, args, alias);
    }

    // ---------------- Pratt expression parser ----------------
    //
    // Precedence (lowest -> highest):
    //   OR          (infix, left)
    //   AND         (infix, left)
    //   NOT         (prefix)
    //   = <> < <= > >=   (infix, left)
    //   IS [NOT] NULL   (postfix)
    //   + -         (infix, left)
    //   * / %       (infix, left)
    //   unary + -   (prefix)
    //   primary
    //
    // We implement the standard "climb" technique: each level calls into the
    // next higher level and then loops while the current token binds at its
    // own level.

    public Expression ParseExpression() => ParseOr();

    private Expression ParseOr()
    {
        var left = ParseAnd();
        while (Peek().Kind == TokenKind.Or)
        {
            Advance();
            var right = ParseAnd();
            left = new BinaryExpression(BinaryOperator.Or, left, right);
        }

        return left;
    }

    private Expression ParseAnd()
    {
        var left = ParseNot();
        while (Peek().Kind == TokenKind.And)
        {
            Advance();
            var right = ParseNot();
            left = new BinaryExpression(BinaryOperator.And, left, right);
        }

        return left;
    }

    private Expression ParseNot()
    {
        if (Peek().Kind == TokenKind.Not)
        {
            Advance();
            return new UnaryExpression(UnaryOperator.Not, ParseNot());
        }

        return ParseComparison();
    }

    private Expression ParseComparison()
    {
        var left = ParseIsNull();
        while (true)
        {
            var t = Peek();

            // [NOT] IN (...): two-token lookahead for the negated form. The
            // `NOT` here is part of the IN operator, not the unary-not at
            // ParseNot (which sits at a higher precedence level and binds
            // an entire boolean expression).
            if (t.Kind == TokenKind.In)
            {
                Advance();
                left = ParseInRhs(left, isNegated: false, t.Position);
                continue;
            }

            if (t.Kind == TokenKind.Not
                && _pos + 1 < _tokens.Count
                && _tokens[_pos + 1].Kind == TokenKind.In)
            {
                Advance(); // NOT
                Advance(); // IN
                left = ParseInRhs(left, isNegated: true, t.Position);
                continue;
            }

            // [NOT] BETWEEN lo AND hi — same precedence level as IN /
            // comparison. The `AND` between the bounds is consumed by
            // ParseBetweenRhs, so it never reaches the boolean-AND level.
            if (t.Kind == TokenKind.Between)
            {
                Advance();
                left = ParseBetweenRhs(left, isNegated: false);
                continue;
            }

            if (t.Kind == TokenKind.Not
                && _pos + 1 < _tokens.Count
                && _tokens[_pos + 1].Kind == TokenKind.Between)
            {
                Advance(); // NOT
                Advance(); // BETWEEN
                left = ParseBetweenRhs(left, isNegated: true);
                continue;
            }

            // value [NOT] {LIKE | ILIKE | SIMILAR TO} pattern [ESCAPE esc].
            // LIKE / ILIKE / SIMILAR / TO / ESCAPE are contextual keywords
            // (never reserved), matched by identifier text, so they stay usable
            // as ordinary names everywhere else — mirroring OVER / PARTITION.
            if (IsContextualKeyword("like"))
            {
                Advance();
                left = ParseLikeRhs(left, "like", isNegated: false);
                continue;
            }

            if (IsContextualKeyword("ilike"))
            {
                Advance();
                left = ParseLikeRhs(left, "ilike", isNegated: false);
                continue;
            }

            // `value [NOT] RLIKE pattern` — the Spark/Hive infix spelling of a
            // POSIX substring regex match; desugars to REGEXP_LIKE.
            if (IsContextualKeyword("rlike"))
            {
                Advance();
                left = ParseLikeRhs(left, "regexp_like", isNegated: false);
                continue;
            }

            if (IsContextualKeyword("similar") && IsContextualKeywordAt(1, "to"))
            {
                Advance(); // SIMILAR
                Advance(); // TO
                left = ParseLikeRhs(left, "similar_to", isNegated: false);
                continue;
            }

            if (t.Kind == TokenKind.Not && IsContextualKeywordAt(1, "like"))
            {
                Advance(); // NOT
                Advance(); // LIKE
                left = ParseLikeRhs(left, "like", isNegated: true);
                continue;
            }

            if (t.Kind == TokenKind.Not && IsContextualKeywordAt(1, "ilike"))
            {
                Advance(); // NOT
                Advance(); // ILIKE
                left = ParseLikeRhs(left, "ilike", isNegated: true);
                continue;
            }

            if (t.Kind == TokenKind.Not && IsContextualKeywordAt(1, "rlike"))
            {
                Advance(); // NOT
                Advance(); // RLIKE
                left = ParseLikeRhs(left, "regexp_like", isNegated: true);
                continue;
            }

            if (t.Kind == TokenKind.Not
                && IsContextualKeywordAt(1, "similar")
                && IsContextualKeywordAt(2, "to"))
            {
                Advance(); // NOT
                Advance(); // SIMILAR
                Advance(); // TO
                left = ParseLikeRhs(left, "similar_to", isNegated: true);
                continue;
            }

            BinaryOperator op;
            switch (t.Kind)
            {
                case TokenKind.Eq: op = BinaryOperator.Equal; break;
                case TokenKind.NotEq: op = BinaryOperator.NotEqual; break;
                case TokenKind.Less: op = BinaryOperator.Less; break;
                case TokenKind.LessEq: op = BinaryOperator.LessEqual; break;
                case TokenKind.Greater: op = BinaryOperator.Greater; break;
                case TokenKind.GreaterEq: op = BinaryOperator.GreaterEqual; break;
                default: return left;
            }

            Advance();
            var right = ParseIsNull();
            left = new BinaryExpression(op, left, right);
        }
    }

    /// <summary>
    /// <c>probe [NOT] BETWEEN lo AND hi</c> — desugared to a comparison
    /// conjunction. <c>BETWEEN</c> ≡ <c>probe &gt;= lo AND probe &lt;= hi</c>;
    /// <c>NOT BETWEEN</c> ≡ <c>probe &lt; lo OR probe &gt; hi</c> (the De Morgan
    /// dual, which agrees under SQL three-valued logic). The bounds are parsed
    /// at <see cref="ParseIsNull"/> level so the separating <c>AND</c> binds to
    /// BETWEEN rather than the boolean-AND above this precedence level.
    /// </summary>
    /// <summary>
    /// <c>value [NOT] {LIKE|ILIKE|SIMILAR TO} pattern [ESCAPE esc]</c> — a
    /// parse-time desugar to a boolean scalar-function call (<c>like</c> /
    /// <c>ilike</c> / <c>similar_to</c>), with the negated form wrapped in a
    /// unary <c>NOT</c>. The wrap inherits SQL three-valued NULL handling from
    /// the unary-NOT lowering (NULL stays NULL), exactly as the comparison ops
    /// do, so <c>x NOT LIKE NULL</c> is NULL rather than TRUE. Resolution and
    /// lowering live in <c>ScalarFunctionRegistry</c>; only the keyword syntax
    /// stays here. The pattern (and optional escape) parse at
    /// <see cref="ParseIsNull"/> level so a trailing boolean <c>AND</c>/<c>OR</c>
    /// closes the predicate rather than being swallowed.
    /// </summary>
    private Expression ParseLikeRhs(Expression value, string functionName, bool isNegated)
    {
        var pattern = ParseIsNull();
        var args = new List<Expression> { value, pattern };

        if (IsContextualKeyword("escape"))
        {
            Advance();
            args.Add(ParseIsNull());
        }

        Expression call = new FunctionCallExpression(functionName, args, IsStar: false);
        return isNegated ? new UnaryExpression(UnaryOperator.Not, call) : call;
    }

    private Expression ParseBetweenRhs(Expression probe, bool isNegated)
    {
        // The probe is fanned out across both bounds, so a subquery there
        // would be reference-duplicated — reject it, as simple CASE / DECODE do.
        if (probe is SubqueryExpression)
        {
            throw Error(Peek(), "a scalar subquery is not supported as a BETWEEN operand");
        }

        var lo = ParseIsNull();
        Expect(TokenKind.And);
        var hi = ParseIsNull();

        if (!isNegated)
        {
            return new BinaryExpression(
                BinaryOperator.And,
                new BinaryExpression(BinaryOperator.GreaterEqual, probe, lo),
                new BinaryExpression(BinaryOperator.LessEqual, probe, hi));
        }

        return new BinaryExpression(
            BinaryOperator.Or,
            new BinaryExpression(BinaryOperator.Less, probe, lo),
            new BinaryExpression(BinaryOperator.Greater, probe, hi));
    }

    private Expression ParseInRhs(Expression probe, bool isNegated, SourcePosition inPos)
    {
        Expect(TokenKind.LParen);
        var next = Peek().Kind;
        if (next == TokenKind.Select || next == TokenKind.With)
        {
            var sq = ParseQuery();
            Expect(TokenKind.RParen);
            _ = inPos;
            return new InSubqueryExpression(probe, new SubqueryExpression(sq), isNegated);
        }

        var values = new List<Expression> { ParseExpression() };
        while (Peek().Kind == TokenKind.Comma)
        {
            Advance();
            values.Add(ParseExpression());
        }

        Expect(TokenKind.RParen);
        _ = inPos;
        return new InListExpression(probe, values, isNegated);
    }

    /// <summary>
    /// Parse the <c>||</c> string-concatenation level, between IS-NULL /
    /// comparison (looser) and additive (tighter). A run of <c>||</c> is
    /// collected into a single flat <c>FunctionCallExpression("||", …)</c>
    /// rather than a left-leaning binary chain, so the recursive walkers stay
    /// shallow and each operand is compiled exactly once. The <c>"||"</c>
    /// builtin propagates NULL (any NULL operand → NULL result), which is how
    /// SQL <c>||</c> differs from this engine's PG-style NULL-skipping CONCAT.
    /// </summary>
    private Expression ParseConcat()
    {
        var left = ParseAdditive();
        if (Peek().Kind != TokenKind.BarBar)
        {
            return left;
        }

        var operands = new List<Expression> { left };
        while (Peek().Kind == TokenKind.BarBar)
        {
            Advance();
            operands.Add(ParseAdditive());
        }

        return new FunctionCallExpression("||", operands, IsStar: false);
    }

    private Expression ParseIsNull()
    {
        var left = ParseConcat();
        while (Peek().Kind == TokenKind.Is)
        {
            Advance();
            var negated = false;
            if (Peek().Kind == TokenKind.Not)
            {
                Advance();
                negated = true;
            }

            // IS [NOT] DISTINCT FROM rhs — NULL-safe (in)equality — vs the
            // plain IS [NOT] NULL test. The `negated` flag captured above
            // means "NOT DISTINCT" (i.e. NULL-safe equal) here.
            if (Peek().Kind == TokenKind.Distinct)
            {
                Advance();
                Expect(TokenKind.From);
                var rhs = ParseConcat();
                left = BuildIsDistinctFrom(left, rhs, isNotDistinct: negated);
                continue;
            }

            // IS [NOT] TRUE / FALSE / UNKNOWN — boolean tests yielding a
            // definite boolean (never NULL), per the SQL three-valued rules.
            if (Peek().Kind is TokenKind.True or TokenKind.False)
            {
                var wantTrue = Peek().Kind == TokenKind.True;
                Advance();
                left = BuildIsBoolTest(left, wantTrue, negated);
                continue;
            }

            if (Peek().Kind == TokenKind.Identifier && Peek().Text == "unknown")
            {
                // `b IS UNKNOWN` ≡ `b IS NULL`; `b IS NOT UNKNOWN` ≡ `b IS NOT NULL`.
                Advance();
                left = new IsNullExpression(left, negated);
                continue;
            }

            Expect(TokenKind.Null);
            left = new IsNullExpression(left, negated);
        }

        return left;
    }

    /// <summary>
    /// <c>a IS [NOT] DISTINCT FROM b</c> — NULL-safe (in)equality, always a
    /// definite boolean (never NULL). Desugared to existing nodes:
    /// <c>a IS NOT DISTINCT FROM b</c> ≡
    /// <c>(a IS NULL AND b IS NULL) OR (a IS NOT NULL AND b IS NOT NULL AND a = b)</c>.
    /// The <c>IS NOT NULL</c> guards make three-valued <c>FALSE AND (a = b)</c>
    /// collapse to FALSE, so a one-sided NULL yields FALSE rather than leaking
    /// NULL. <c>IS DISTINCT FROM</c> is the logical negation of that.
    /// </summary>
    private Expression BuildIsDistinctFrom(Expression a, Expression b, bool isNotDistinct)
    {
        // a and b are each referenced several times by the desugar, so a
        // subquery operand would be reference-duplicated — reject it, as the
        // other operand-fanning desugars do.
        if (a is SubqueryExpression || b is SubqueryExpression)
        {
            throw Error(Peek(), "a scalar subquery is not supported as an IS [NOT] DISTINCT FROM operand");
        }

        var bothNull = new BinaryExpression(
            BinaryOperator.And,
            new IsNullExpression(a, Negated: false),
            new IsNullExpression(b, Negated: false));

        var bothPresentAndEqual = new BinaryExpression(
            BinaryOperator.And,
            new BinaryExpression(
                BinaryOperator.And,
                new IsNullExpression(a, Negated: true),
                new IsNullExpression(b, Negated: true)),
            new BinaryExpression(BinaryOperator.Equal, a, b));

        var notDistinct = new BinaryExpression(BinaryOperator.Or, bothNull, bothPresentAndEqual);

        return isNotDistinct
            ? notDistinct
            : new UnaryExpression(UnaryOperator.Not, notDistinct);
    }

    /// <summary>
    /// <c>b IS [NOT] TRUE</c> / <c>b IS [NOT] FALSE</c> — a definite boolean
    /// test (never NULL). Desugared via <c>COALESCE</c> so the operand is
    /// referenced exactly once (subquery operands are fine):
    /// <c>b IS TRUE</c> ≡ <c>COALESCE(b, FALSE)</c> and
    /// <c>b IS FALSE</c> ≡ <c>NOT COALESCE(b, TRUE)</c>; the <c>IS NOT</c>
    /// forms are the logical negations. <c>COALESCE(b, &lt;bool&gt;)</c> also
    /// pins <c>b</c> to BOOLEAN at resolve time.
    /// </summary>
    private static Expression BuildIsBoolTest(Expression b, bool wantTrue, bool negated)
    {
        // IS TRUE falls back to FALSE; IS FALSE falls back to TRUE.
        var fallback = new LiteralExpression(LiteralKind.Boolean, !wantTrue);
        var coalesce = new FunctionCallExpression(
            "coalesce", new Expression[] { b, fallback }, IsStar: false);
        var baseExpr = wantTrue
            ? (Expression)coalesce
            : new UnaryExpression(UnaryOperator.Not, coalesce);
        return negated ? new UnaryExpression(UnaryOperator.Not, baseExpr) : baseExpr;
    }

    private Expression ParseAdditive()
    {
        var left = ParseMultiplicative();
        while (true)
        {
            var t = Peek();
            BinaryOperator op;
            if (t.Kind == TokenKind.Plus)
            {
                op = BinaryOperator.Add;
            }
            else if (t.Kind == TokenKind.Minus)
            {
                op = BinaryOperator.Subtract;
            }
            else
            {
                return left;
            }

            Advance();
            var right = ParseMultiplicative();
            left = new BinaryExpression(op, left, right);
        }
    }

    private Expression ParseMultiplicative()
    {
        var left = ParseUnary();
        while (true)
        {
            var t = Peek();
            BinaryOperator op;
            if (t.Kind == TokenKind.Star)
            {
                op = BinaryOperator.Multiply;
            }
            else if (t.Kind == TokenKind.Slash)
            {
                op = BinaryOperator.Divide;
            }
            else if (t.Kind == TokenKind.Percent)
            {
                op = BinaryOperator.Modulo;
            }
            else
            {
                return left;
            }

            Advance();
            var right = ParseUnary();
            left = new BinaryExpression(op, left, right);
        }
    }

    private Expression ParseUnary()
    {
        if (Peek().Kind == TokenKind.Minus)
        {
            Advance();
            return new UnaryExpression(UnaryOperator.Negate, ParseUnary());
        }

        if (Peek().Kind == TokenKind.Plus)
        {
            Advance();
            return ParseUnary();
        }

        return ParsePrimary();
    }

    private Expression ParsePrimary()
    {
        var t = Peek();
        switch (t.Kind)
        {
            case TokenKind.IntegerLiteral:
                Advance();
                return new LiteralExpression(LiteralKind.Integer, t.IntegerValue);
            case TokenKind.DecimalLiteral:
                Advance();
                return new LiteralExpression(
                    LiteralKind.Decimal,
                    new Clast.DatabaseDecimal.Values.Decimal128(t.DecimalMantissa))
                {
                    DecimalScale = t.DecimalScale,
                };
            case TokenKind.FloatLiteral:
                Advance();
                return new LiteralExpression(LiteralKind.Float, t.FloatValue);
            case TokenKind.StringLiteral:
                Advance();
                return new LiteralExpression(LiteralKind.String, t.Text);
            case TokenKind.True:
                Advance();
                return new LiteralExpression(LiteralKind.Boolean, true);
            case TokenKind.False:
                Advance();
                return new LiteralExpression(LiteralKind.Boolean, false);
            case TokenKind.Null:
                Advance();
                return new LiteralExpression(LiteralKind.Null, null);
            case TokenKind.LParen:
                Advance();
                // Parenthesised SELECT (or WITH … SELECT) is a scalar
                // subquery; anything else is a grouped expression.
                if (Peek().Kind is TokenKind.Select or TokenKind.With)
                {
                    var subqueryAst = ParseQuery();
                    Expect(TokenKind.RParen);
                    return new SubqueryExpression(subqueryAst);
                }

                var inner = ParseExpression();
                Expect(TokenKind.RParen);
                return inner;
            case TokenKind.Interval:
                return ParseIntervalLiteral();
            case TokenKind.Date when PeekAt(1).Kind == TokenKind.StringLiteral:
            case TokenKind.Time when PeekAt(1).Kind == TokenKind.StringLiteral:
            case TokenKind.Timestamp when PeekAt(1).Kind == TokenKind.StringLiteral:
                return ParseTypedTemporalLiteral();
            case TokenKind.Extract:
                return ParseExtractExpression();
            case TokenKind.Cast:
                return ParseCastExpression();
            case TokenKind.Coalesce:
                return ParseCoalesceExpression();
            case TokenKind.Case:
                return ParseCaseExpression();
            case TokenKind.Exists:
                return ParseExistsExpression();
            case TokenKind.Identifier:
                return ParseIdentifierExpression();
            default:
                throw Error(t, $"expected expression, got {Describe(t)}");
        }
    }

    /// <summary>
    /// <c>EXISTS (subquery)</c> — parses to an
    /// <see cref="ExistsExpression"/> AST node. The resolver routes
    /// uncorrelated cases via a synthesised
    /// <c>COALESCE((SELECT COUNT(*) FROM (sq)), 0) &gt; 0</c> desugar (same
    /// shape the parser used to emit before this AST node existed), and
    /// correlated cases via a <see cref="Plan.SemiJoinPlan"/> lift with
    /// the correlation columns as equi-keys.
    /// </summary>
    private Expression ParseExistsExpression()
    {
        Expect(TokenKind.Exists);
        Expect(TokenKind.LParen);
        if (Peek().Kind is not TokenKind.Select and not TokenKind.With)
        {
            throw Error(Peek(), "EXISTS requires a parenthesised subquery");
        }

        var inner = ParseQuery();
        Expect(TokenKind.RParen);

        var original = new SubqueryExpression(inner);
        // Cache the (SELECT COUNT(*) FROM (inner) AS __exists_inner)
        // subquery used by the resolver's COALESCE-desugar. Computed once
        // so reference-equality dedup in WrapWithScalarSubqueries works.
        var derived = new DerivedTableReference(inner, Alias: "__exists_inner");
        var countStar = new FunctionCallExpression("count", Array.Empty<Expression>(), IsStar: true);
        var countSelect = new SelectStatement(
            Items: [new ExpressionSelectItem(countStar, Alias: null)],
            From: derived,
            Where: null,
            GroupBy: Array.Empty<Expression>(),
            Having: null,
            Ctes: Array.Empty<CteDefinition>());
        var countSubquery = new SubqueryExpression(countSelect);
        return new ExistsExpression(original, countSubquery);
    }

    /// <summary>
    /// Parse a <c>CASE … END</c> expression. Both the searched form
    /// (<c>CASE WHEN cond THEN r …</c>) and the simple form
    /// (<c>CASE operand WHEN val THEN r …</c>) are accepted; the simple form
    /// is desugared here into the searched <see cref="CaseExpression"/> by
    /// rewriting each arm's test to <c>operand = val</c>.
    /// </summary>
    private CaseExpression ParseCaseExpression()
    {
        Expect(TokenKind.Case);

        // Simple form: a comparand sits between CASE and the first WHEN.
        Expression? operand = null;
        if (Peek().Kind != TokenKind.When)
        {
            operand = ParseExpression();

            // The simple form fans the operand out across every arm
            // (operand = vK). Re-referencing a subquery operand would
            // double-count it in the resolver's reference-keyed subquery
            // map, so reject that narrow case rather than miscompile it.
            if (operand is SubqueryExpression)
            {
                throw Error(Peek(), "a scalar subquery is not supported as a simple CASE operand");
            }
        }

        var whens = new List<CaseWhenClause>();
        while (Peek().Kind == TokenKind.When)
        {
            Advance();
            var test = ParseExpression();
            Expect(TokenKind.Then);
            var result = ParseExpression();

            var condition = operand is null
                ? test
                : new BinaryExpression(BinaryOperator.Equal, operand, test);
            whens.Add(new CaseWhenClause(condition, result));
        }

        if (whens.Count == 0)
        {
            throw Error(Peek(), "CASE requires at least one WHEN branch");
        }

        Expression? elseResult = null;
        if (Peek().Kind == TokenKind.Else)
        {
            Advance();
            elseResult = ParseExpression();
        }

        Expect(TokenKind.End);
        return new CaseExpression(whens, elseResult);
    }

    /// <summary>
    /// <c>IIF(condition, a, b)</c> ≡ <c>CASE WHEN condition THEN a ELSE b END</c>.
    /// The resolver enforces that <c>condition</c> is BOOLEAN (via the CASE arm).
    /// </summary>
    private CaseExpression BuildIifExpression(IReadOnlyList<Expression> args, Token nameToken)
    {
        if (args.Count != 3)
        {
            throw Error(nameToken, $"IIF takes exactly 3 arguments, got {args.Count}");
        }

        return new CaseExpression(
            new[] { new CaseWhenClause(args[0], args[1]) },
            args[2]);
    }

    /// <summary>
    /// <c>DECODE(expr, s1, r1 [, s2, r2 …] [, default])</c> ≡
    /// <c>CASE WHEN expr ≡ s1 THEN r1 [WHEN expr ≡ s2 THEN r2 …] [ELSE default] END</c>,
    /// where <c>≡</c> is NULL-safe equality. DECODE (Oracle) treats
    /// <c>NULL = NULL</c> as a match, unlike <c>=</c> / simple CASE, so each
    /// arm tests <c>(expr = sK) OR (expr IS NULL AND sK IS NULL)</c> — which
    /// under CASE three-valued fall-through selects the arm exactly when
    /// DECODE would match.
    /// </summary>
    private CaseExpression BuildDecodeExpression(IReadOnlyList<Expression> args, Token nameToken)
    {
        if (args.Count < 3)
        {
            throw Error(nameToken, $"DECODE takes at least 3 arguments, got {args.Count}");
        }

        var expr = args[0];
        // expr (and each search) is fanned out across the arms, so a subquery
        // there would be reference-duplicated and double-counted in the
        // subquery map — reject it, as simple CASE does for its operand.
        if (expr is SubqueryExpression)
        {
            throw Error(nameToken, "a scalar subquery is not supported as the DECODE expression");
        }

        var whens = new List<CaseWhenClause>();
        var i = 1;
        while (i + 1 < args.Count)
        {
            var search = args[i];
            if (search is SubqueryExpression)
            {
                throw Error(nameToken, "a scalar subquery is not supported as a DECODE search value");
            }

            whens.Add(new CaseWhenClause(NullSafeEqual(expr, search), args[i + 1]));
            i += 2;
        }

        // A final unpaired argument is the ELSE default; otherwise none (NULL).
        var elseResult = i < args.Count ? args[i] : null;
        return new CaseExpression(whens, elseResult);

        static Expression NullSafeEqual(Expression a, Expression b)
        {
            var eq = new BinaryExpression(BinaryOperator.Equal, a, b);
            var bothNull = new BinaryExpression(
                BinaryOperator.And,
                new IsNullExpression(a, Negated: false),
                new IsNullExpression(b, Negated: false));
            return new BinaryExpression(BinaryOperator.Or, eq, bothNull);
        }
    }

    private CastExpression ParseCastExpression()
    {
        Expect(TokenKind.Cast);
        Expect(TokenKind.LParen);
        var expr = ParseExpression();
        Expect(TokenKind.As);
        var type = ParseTypeSpec();
        Expect(TokenKind.RParen);
        return new CastExpression(expr, type);
    }

    /// <summary>
    /// Parse an <c>INTERVAL '&lt;value&gt;' &lt;qualifier&gt;</c> literal.
    /// Desugared to <c>CAST('&lt;value&gt;' AS INTERVAL &lt;qualifier&gt;)</c>
    /// so every downstream AST walker (resolver, aggregate detection,
    /// optimizer) handles it without a bespoke node; the resolver constant-
    /// folds the cast of the literal string into a typed interval value, so
    /// there is no per-row re-parse.
    /// </summary>
    /// <summary>
    /// Parse a typed temporal literal — <c>DATE '…'</c>, <c>TIME '…'</c>,
    /// <c>TIMESTAMP '…'</c> — as a constant CAST of the string to that type,
    /// mirroring <see cref="ParseIntervalLiteral"/>. Only reached when the type
    /// keyword is immediately followed by a string literal (guarded by the
    /// caller), so the keywords stay usable as type names elsewhere.
    /// </summary>
    private Expression ParseTypedTemporalLiteral()
    {
        var typeName = Peek().Kind switch
        {
            TokenKind.Date => "DATE",
            TokenKind.Time => "TIME",
            _ => "TIMESTAMP",
        };
        Advance();
        var strTok = Advance();   // the caller guaranteed a StringLiteral here
        return new CastExpression(
            new LiteralExpression(LiteralKind.String, strTok.Text),
            new SqlTypeSpec(typeName));
    }

    private Expression ParseIntervalLiteral()
    {
        Expect(TokenKind.Interval);
        var strTok = Peek();
        if (strTok.Kind != TokenKind.StringLiteral)
        {
            throw Error(strTok, $"expected interval string literal, got {Describe(strTok)}");
        }

        Advance();
        var qualifier = ParseIntervalQualifier();
        return new CastExpression(
            new LiteralExpression(LiteralKind.String, strTok.Text),
            new SqlTypeSpec("INTERVAL", IntervalQualifier: qualifier));
    }

    /// <summary>
    /// Parse <c>EXTRACT(field FROM source)</c>. The <c>field</c> is a bare
    /// identifier (YEAR, MONTH, …, non-reserved) lowered to a string-literal
    /// first argument, so the call shares the <c>extract</c> registry entry
    /// with <c>DATE_PART('field', source)</c>.
    /// </summary>
    private Expression ParseExtractExpression()
    {
        Expect(TokenKind.Extract);
        Expect(TokenKind.LParen);
        var field = ExpectIdentifier("extract field");
        Expect(TokenKind.From);
        var source = ParseExpression();
        Expect(TokenKind.RParen);
        return new FunctionCallExpression(
            "extract",
            new Expression[] { new LiteralExpression(LiteralKind.String, field), source },
            IsStar: false);
    }

    private FunctionCallExpression ParseCoalesceExpression()
    {
        Expect(TokenKind.Coalesce);
        Expect(TokenKind.LParen);
        var args = new List<Expression> { ParseExpression() };
        while (Peek().Kind == TokenKind.Comma)
        {
            Advance();
            args.Add(ParseExpression());
        }

        Expect(TokenKind.RParen);
        return new FunctionCallExpression("coalesce", args, IsStar: false);
    }

    /// <summary>
    /// Parse an optional <c>FILTER (WHERE predicate)</c> clause following an
    /// aggregate call. Returns the predicate, or <c>null</c> when no FILTER
    /// follows. <c>filter</c> is recognised contextually so it stays usable as an
    /// ordinary identifier elsewhere.
    /// </summary>
    private Expression? TryParseAggregateFilter()
    {
        if (!IsContextualKeyword("filter"))
        {
            return null;
        }

        Advance(); // filter
        Expect(TokenKind.LParen);
        Expect(TokenKind.Where);
        var predicate = ParseExpression();
        Expect(TokenKind.RParen);
        return predicate;
    }

    private Expression ParseIdentifierExpression()
    {
        var first = Advance();
        // Possible shapes:
        //   ident                       -> ColumnReference(null, ident)
        //   ident . ident               -> ColumnReference(ident, ident)
        //   ident ( ... )               -> FunctionCallExpression
        //   ident ( * )                 -> FunctionCallExpression (COUNT(*))

        // NOW() / CURRENT_TIMESTAMP / CURRENT_DATE / CURRENT_TIME — the advancing
        // logical clock. A dedicated NowExpression node, never a function call:
        // the clock is not a pure scalar and must not reach the scalar-function
        // registry. Recognised contextually so existing identifiers aren't
        // broken — `now` only becomes the clock when written with its empty arg
        // list (a column literally named "now" still resolves), and the
        // parenless CURRENT_* spellings only when used bare (a
        // `current_timestamp.col` qualifier stays a column).
        if (first.Text == "now" && Peek().Kind == TokenKind.LParen)
        {
            Advance();
            Expect(TokenKind.RParen);
            return new NowExpression(NowFunction.Now);
        }

        if (first.Text is "current_timestamp" or "current_date" or "current_time"
            && Peek().Kind is not TokenKind.Dot and not TokenKind.LParen)
        {
            return new NowExpression(first.Text switch
            {
                "current_date" => NowFunction.CurrentDate,
                "current_time" => NowFunction.CurrentTime,
                _ => NowFunction.CurrentTimestamp,
            });
        }

        if (Peek().Kind == TokenKind.Dot)
        {
            // Dotted path. The first segment is the qualifier (table alias);
            // the remaining segments join with '.' into the column name. A plain
            // `t.k` yields ColumnReference("t", "k") as before; a nested-struct
            // access `cm.Customer.Name.C_L_NAME` yields
            // ColumnReference("cm", "Customer.Name.C_L_NAME"), which resolves
            // against the flattened dotted-name leaf columns produced at CREATE
            // TABLE. See docs/design-nested-types.md.
            Advance();
            var name = ExpectFieldName("column name");
            while (Peek().Kind == TokenKind.Dot)
            {
                Advance();
                name = name + "." + ExpectFieldName("field name");
            }

            return new ColumnReference(first.Text, name);
        }

        if (Peek().Kind == TokenKind.LParen)
        {
            Advance();
            if (Peek().Kind == TokenKind.Star)
            {
                Advance();
                Expect(TokenKind.RParen);

                // COUNT(*) FILTER (WHERE p) ≡ COUNT(CASE WHEN p THEN 1 END):
                // count only the rows where p holds (the CASE is non-NULL there).
                var starFilter = TryParseAggregateFilter();
                if (starFilter is not null)
                {
                    if (IsContextualKeyword("over"))
                    {
                        throw Error(first, "FILTER is not supported with a window function");
                    }

                    var oneArm = new CaseExpression(
                        new[] { new CaseWhenClause(starFilter, new LiteralExpression(LiteralKind.Integer, 1L)) },
                        ElseResult: null);
                    return new FunctionCallExpression(first.Text, new Expression[] { oneArm }, IsStar: false);
                }

                if (IsContextualKeyword("over"))
                {
                    Advance();
                    return new WindowFunctionExpression(
                        first.Text, Array.Empty<Expression>(), IsStar: true, ParseOverSpec());
                }

                return new FunctionCallExpression(first.Text, Array.Empty<Expression>(), IsStar: true);
            }

            // POSITION(substring IN string) — the standard spelling uses the
            // IN keyword as the argument separator. Lower to a two-argument
            // FunctionCallExpression(needle, haystack); STRPOS(string, sub) is
            // the comma-form alias handled by the general arg path below.
            if (first.Text == "position")
            {
                // The needle is parsed below the comparison level so the
                // separating IN binds to POSITION rather than being consumed
                // as an `x IN (…)` membership test.
                var needle = ParseConcat();
                Expect(TokenKind.In);
                var haystack = ParseExpression();
                Expect(TokenKind.RParen);
                return new FunctionCallExpression("position", new[] { needle, haystack }, IsStar: false);
            }

            // DISTINCT argument modifier — COUNT(DISTINCT x). Captured here for
            // any function spelling; the resolver accepts it only on COUNT.
            var distinct = false;
            if (Peek().Kind == TokenKind.Distinct)
            {
                Advance();
                distinct = true;
            }

            var args = new List<Expression>();
            if (Peek().Kind != TokenKind.RParen)
            {
                args.Add(ParseExpression());
                while (Peek().Kind == TokenKind.Comma)
                {
                    Advance();
                    args.Add(ParseExpression());
                }
            }

            Expect(TokenKind.RParen);

            // PERCENTILE_CONT(f) / PERCENTILE_DISC(f) WITHIN GROUP (ORDER BY x):
            // the ANSI ordered-set spelling of an approximate quantile. Lower to
            // the same value-first, fraction-second call shape the resolver uses
            // for APPROX_PERCENTILE(x, f) so both spellings share one DDSketch
            // aggregator (no separate downstream support needed).
            if (first.Text is "percentile_cont" or "percentile_disc"
                && IsContextualKeyword("within"))
            {
                return ParseWithinGroup(first, args);
            }

            // IIF / DECODE are conditional functions; desugar to CASE here so
            // they flow through resolution, aggregation, and both compilers
            // exactly like a hand-written CASE (no downstream support needed).
            if (first.Text == "iif")
            {
                return BuildIifExpression(args, first);
            }

            if (first.Text == "decode")
            {
                return BuildDecodeExpression(args, first);
            }

            // agg(x) FILTER (WHERE p) ≡ agg(CASE WHEN p THEN x END): unmatched
            // rows become NULL, which every aggregate ignores. Pure sugar — it
            // flows through the CASE + aggregate machinery unchanged (a DISTINCT
            // modifier is preserved). The window form is out of scope.
            var filter = TryParseAggregateFilter();
            if (filter is not null)
            {
                if (IsContextualKeyword("over"))
                {
                    throw Error(first, "FILTER is not supported with a window function");
                }

                if (args.Count != 1)
                {
                    throw Error(first, "FILTER requires a single-argument aggregate");
                }

                var arm = new CaseExpression(
                    new[] { new CaseWhenClause(filter, args[0]) }, ElseResult: null);
                return new FunctionCallExpression(
                    first.Text, new Expression[] { arm }, IsStar: false, Distinct: distinct);
            }

            // A trailing OVER (...) turns the call into a window function. The
            // ranking functions (RANK/ROW_NUMBER/DENSE_RANK, no args) ride the
            // TOP-K pattern; the aggregate functions (SUM/COUNT/AVG/MIN/MAX,
            // with an argument) ride the window-aggregate path. The resolver
            // routes each and rejects anything else. `args` is carried so the
            // window-aggregate resolver can read the aggregate argument.
            if (IsContextualKeyword("over"))
            {
                if (distinct)
                {
                    throw Error(first, "DISTINCT is not supported in a window function");
                }

                Advance();
                return new WindowFunctionExpression(first.Text, args, IsStar: false, ParseOverSpec());
            }

            // Event-time tumbling-window functions. TUMBLE(t, size) (a GROUP BY
            // auxiliary) and TUMBLE_START(t, size) both name the window's start —
            // the floor of t to its size-bucket; TUMBLE_END(t, size) is start +
            // size. Desugar all three to the internal `tumble_start` scalar (and
            // TUMBLE_END to `tumble_start(t, size) + size`) so a query GROUPed BY
            // TUMBLE matches the TUMBLE_START / TUMBLE_END it SELECTs via the
            // resolver's AstEqual group-key match, and the whole family rides the
            // existing GROUP BY-expression-key + monotonicity-GC machinery with no
            // new plan node or operator. TUMBLE* are contextual (matched by text),
            // so they stay usable as ordinary identifiers.
            if (!distinct && first.Text is "tumble" or "tumble_start" or "tumble_end")
            {
                if (args.Count != 2)
                {
                    throw Error(first,
                        $"{first.Text.ToUpperInvariant()} takes a time column and a window-size INTERVAL");
                }

                var start = new FunctionCallExpression(
                    "tumble_start", new[] { args[0], args[1] }, IsStar: false);
                return first.Text == "tumble_end"
                    ? new BinaryExpression(BinaryOperator.Add, start, args[1])
                    : start;
            }

            return new FunctionCallExpression(first.Text, args, IsStar: false, Distinct: distinct);
        }

        return new ColumnReference(Qualifier: null, first.Text);
    }

    /// <summary>
    /// Parse the <c>WITHIN GROUP (ORDER BY x)</c> tail of an ordered-set
    /// aggregate. The caller has parsed <c>name ( fraction )</c> into
    /// <paramref name="args"/> and the next token is the <c>within</c> contextual
    /// keyword. Returns a <c>FunctionCallExpression(name, [x, fraction])</c>.
    /// </summary>
    private Expression ParseWithinGroup(Token nameToken, List<Expression> args)
    {
        if (args.Count != 1)
        {
            throw Error(nameToken,
                $"{nameToken.Text.ToUpperInvariant()} WITHIN GROUP takes a single fraction argument");
        }

        Advance(); // `within`
        Expect(TokenKind.Group);
        Expect(TokenKind.LParen);
        Expect(TokenKind.Order);
        Expect(TokenKind.By);
        var sort = ParseSortItem();
        Expect(TokenKind.RParen);

        // DESC inverts the quantile: the f-th percentile measured from the top
        // is the (1 − f)-th from the bottom. Lower to `1 − f` so the resolver and
        // the sketch stay ordering-agnostic (they always measure from the
        // bottom); the resolver constant-folds the subtraction.
        var fraction = sort.Direction == SortDirection.Descending
            ? new BinaryExpression(
                BinaryOperator.Subtract, new LiteralExpression(LiteralKind.Integer, 1L), args[0])
            : args[0];

        return new FunctionCallExpression(
            nameToken.Text, new[] { sort.Expression, fraction }, IsStar: false);
    }

    /// <summary>
    /// Parse a window specification <c>( [PARTITION BY e, …] [ORDER BY s, …] )</c>;
    /// the caller has already consumed the <c>OVER</c> token. <c>PARTITION</c> is
    /// a contextual keyword (not reserved); <c>ORDER BY</c> reuses
    /// <see cref="ParseSortItem"/>.
    /// </summary>
    /// <summary>
    /// Parse what follows <c>OVER</c>: either an inline <c>(PARTITION BY … )</c>
    /// spec, or a bare identifier naming a window defined in the query's
    /// <c>WINDOW</c> clause. The named form is a placeholder <see cref="WindowSpec"/>
    /// carrying only <see cref="WindowSpec.Name"/>; <see cref="SubstituteNamedWindows"/>
    /// replaces it once the <c>WINDOW</c> clause has been parsed.
    /// </summary>
    private WindowSpec ParseOverSpec()
    {
        if (Peek().Kind == TokenKind.LParen)
        {
            return ParseWindowSpec();
        }

        var name = ExpectIdentifier("window name or '('");
        return new WindowSpec(Array.Empty<Expression>(), Array.Empty<SortItem>(), Frame: null, Name: name);
    }

    private WindowSpec ParseWindowSpec()
    {
        Expect(TokenKind.LParen);

        IReadOnlyList<Expression> partitionBy = Array.Empty<Expression>();
        if (IsContextualKeyword("partition"))
        {
            Advance();
            Expect(TokenKind.By);
            var parts = new List<Expression> { ParseExpression() };
            while (Peek().Kind == TokenKind.Comma)
            {
                Advance();
                parts.Add(ParseExpression());
            }

            partitionBy = parts;
        }

        IReadOnlyList<SortItem> orderBy = Array.Empty<SortItem>();
        if (Peek().Kind == TokenKind.Order)
        {
            Advance();
            Expect(TokenKind.By);
            var items = new List<SortItem> { ParseSortItem() };
            while (Peek().Kind == TokenKind.Comma)
            {
                Advance();
                items.Add(ParseSortItem());
            }

            orderBy = items;
        }

        var frame = ParseWindowFrameOrNull();

        Expect(TokenKind.RParen);
        return new WindowSpec(partitionBy, orderBy, frame);
    }

    /// <summary>
    /// Parse an optional window frame clause —
    /// <c>{RANGE|ROWS|GROUPS} {BETWEEN start AND end | start}</c>. The framing
    /// keywords (<c>RANGE</c>, <c>GROUPS</c>, <c>PRECEDING</c>, <c>FOLLOWING</c>,
    /// <c>UNBOUNDED</c>, <c>CURRENT</c>) are contextual; <c>ROWS</c>/<c>ROW</c>/
    /// <c>BETWEEN</c>/<c>AND</c> are reserved tokens. The single-bound form
    /// <c>RANGE start</c> implies an end bound of <c>CURRENT ROW</c>. Returns
    /// <c>null</c> when no frame clause is present.
    /// </summary>
    private WindowFrame? ParseWindowFrameOrNull()
    {
        WindowFrameMode mode;
        if (IsContextualKeyword("range"))
        {
            mode = WindowFrameMode.Range;
            Advance();
        }
        else if (Peek().Kind == TokenKind.Rows)
        {
            mode = WindowFrameMode.Rows;
            Advance();
        }
        else if (IsContextualKeyword("groups"))
        {
            mode = WindowFrameMode.Groups;
            Advance();
        }
        else
        {
            return null;
        }

        if (Peek().Kind == TokenKind.Between)
        {
            Advance();
            var start = ParseFrameBound();
            Expect(TokenKind.And);
            var end = ParseFrameBound();
            return new WindowFrame(mode, start, end);
        }

        // Single-bound shorthand: the lone bound is the start; the end is
        // implicitly CURRENT ROW.
        var only = ParseFrameBound();
        return new WindowFrame(mode, only, new FrameBound(FrameBoundKind.CurrentRow, null));
    }

    /// <summary>
    /// Parse one window-frame bound — <c>UNBOUNDED PRECEDING</c>,
    /// <c>UNBOUNDED FOLLOWING</c>, <c>CURRENT ROW</c>, or
    /// <c>&lt;offset&gt; {PRECEDING|FOLLOWING}</c> (the offset is a constant or
    /// <c>INTERVAL</c> literal).
    /// </summary>
    private FrameBound ParseFrameBound()
    {
        if (IsContextualKeyword("unbounded"))
        {
            Advance();
            if (IsContextualKeyword("preceding"))
            {
                Advance();
                return new FrameBound(FrameBoundKind.UnboundedPreceding, null);
            }

            if (IsContextualKeyword("following"))
            {
                Advance();
                return new FrameBound(FrameBoundKind.UnboundedFollowing, null);
            }

            throw Error(Peek(), $"expected PRECEDING or FOLLOWING after UNBOUNDED, got {Describe(Peek())}");
        }

        if (IsContextualKeyword("current"))
        {
            Advance();
            Expect(TokenKind.Row);
            return new FrameBound(FrameBoundKind.CurrentRow, null);
        }

        var offset = ParseExpression();
        if (IsContextualKeyword("preceding"))
        {
            Advance();
            return new FrameBound(FrameBoundKind.Preceding, offset);
        }

        if (IsContextualKeyword("following"))
        {
            Advance();
            return new FrameBound(FrameBoundKind.Following, offset);
        }

        throw Error(Peek(), $"expected PRECEDING or FOLLOWING after frame offset, got {Describe(Peek())}");
    }

    /// <summary>A non-reserved word matched by text in a position where it acts
    /// as a keyword (e.g. <c>OVER</c>, <c>PARTITION</c>). Quoted identifiers
    /// never match, so <c>"over"</c> stays usable as an ordinary name.</summary>
    private bool IsContextualKeyword(string word)
    {
        var t = Peek();
        return t.Kind == TokenKind.Identifier && !t.QuotedIdentifier && t.Text == word;
    }

    /// <summary>
    /// <see cref="IsContextualKeyword"/> with a lookahead offset — used for the
    /// two-token <c>NOT LIKE</c> / <c>SIMILAR TO</c> forms.
    /// </summary>
    private bool IsContextualKeywordAt(int offset, string word)
    {
        var i = _pos + offset;
        if (i >= _tokens.Count)
        {
            return false;
        }

        var t = _tokens[i];
        return t.Kind == TokenKind.Identifier && !t.QuotedIdentifier && t.Text == word;
    }

    // ---------------- Token helpers ----------------

    private Token Peek() => _tokens[_pos];

    private Token PeekAt(int offset)
    {
        var i = _pos + offset;
        return i < _tokens.Count ? _tokens[i] : _tokens[^1];
    }

    private Token Advance()
    {
        var t = _tokens[_pos];
        if (t.Kind != TokenKind.EndOfInput)
        {
            _pos++;
        }

        return t;
    }

    private Token Expect(TokenKind kind)
    {
        var t = Peek();
        if (t.Kind != kind)
        {
            throw Error(t, $"expected {kind}, got {Describe(t)}");
        }

        return Advance();
    }

    private string ExpectIdentifier(string what)
    {
        var t = Peek();
        if (t.Kind != TokenKind.Identifier)
        {
            throw Error(t, $"expected {what}, got {Describe(t)}");
        }

        return Advance().Text;
    }

    /// <summary>
    /// Expect a member name — an identifier, or a reserved word used as a name.
    /// Struct field names and dotted field-access segments (<c>ROW(Name …)</c>,
    /// <c>x.Value</c>) legitimately collide with keywords; in these positions the
    /// grammar is unambiguous, so a keyword is accepted by its (folded) text.
    /// </summary>
    private string ExpectFieldName(string what)
    {
        var t = Peek();
        if (t.Kind != TokenKind.Identifier && !Lexer.IsKeywordKind(t.Kind))
        {
            throw Error(t, $"expected {what}, got {Describe(t)}");
        }

        return Advance().Text;
    }

    private int ExpectIntegerLiteral(string what)
    {
        var t = Peek();
        if (t.Kind != TokenKind.IntegerLiteral)
        {
            throw Error(t, $"expected integer {what}, got {Describe(t)}");
        }

        if (t.IntegerValue > int.MaxValue || t.IntegerValue < int.MinValue)
        {
            throw Error(t, $"{what} out of range");
        }

        Advance();
        return (int)t.IntegerValue;
    }

    private long ExpectLongLiteral(string what)
    {
        var t = Peek();
        if (t.Kind != TokenKind.IntegerLiteral)
        {
            throw Error(t, $"expected integer {what}, got {Describe(t)}");
        }

        Advance();
        return t.IntegerValue;
    }

    private static string Describe(Token t) =>
        t.Kind == TokenKind.EndOfInput ? "end of input" :
        t.Kind == TokenKind.Identifier ? $"identifier '{t.Text}'" :
        t.Kind == TokenKind.StringLiteral ? $"string '{t.Text}'" :
        $"'{t.Text}'";

    private static ParseException Error(Token t, string message) =>
        new($"{message}", t.Position);
}

public sealed class ParseException : Exception
{
    public SourcePosition Position { get; }

    public ParseException(string message, SourcePosition position)
        : base($"{message} (at {position})")
    {
        Position = position;
    }
}
