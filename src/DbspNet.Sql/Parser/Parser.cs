// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections.Generic;
using DbspNet.Sql.Parser.Ast;

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
            default:
                throw Error(t, $"expected SQL type, got {Describe(t)}");
        }
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
        return AttachCtes(query, ctes);
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

        return new SelectStatement(items, from, where, groupBy, having, Array.Empty<CteDefinition>(), distinct);
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

    private Expression ParseIdentifierExpression()
    {
        var first = Advance();
        // Possible shapes:
        //   ident                       -> ColumnReference(null, ident)
        //   ident . ident               -> ColumnReference(ident, ident)
        //   ident ( ... )               -> FunctionCallExpression
        //   ident ( * )                 -> FunctionCallExpression (COUNT(*))
        if (Peek().Kind == TokenKind.Dot)
        {
            Advance();
            var colName = ExpectIdentifier("column name");
            return new ColumnReference(first.Text, colName);
        }

        if (Peek().Kind == TokenKind.LParen)
        {
            Advance();
            if (Peek().Kind == TokenKind.Star)
            {
                Advance();
                Expect(TokenKind.RParen);
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

            return new FunctionCallExpression(first.Text, args, IsStar: false);
        }

        return new ColumnReference(Qualifier: null, first.Text);
    }

    // ---------------- Token helpers ----------------

    private Token Peek() => _tokens[_pos];

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
