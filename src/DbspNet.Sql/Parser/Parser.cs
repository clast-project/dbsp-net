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

        return new SelectStatement(items, from, where, groupBy, having, Array.Empty<CteDefinition>());
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
            Expect(TokenKind.On);
            var cond = ParseExpression();
            left = new JoinClause(left, right, joinType, cond);
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

    private Expression ParseIsNull()
    {
        var left = ParseAdditive();
        while (Peek().Kind == TokenKind.Is)
        {
            Advance();
            var negated = false;
            if (Peek().Kind == TokenKind.Not)
            {
                Advance();
                negated = true;
            }

            Expect(TokenKind.Null);
            left = new IsNullExpression(left, negated);
        }

        return left;
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
            case TokenKind.Exists:
                return ParseExistsExpression();
            case TokenKind.Identifier:
                return ParseIdentifierExpression();
            default:
                throw Error(t, $"expected expression, got {Describe(t)}");
        }
    }

    /// <summary>
    /// <c>EXISTS (subquery)</c> — desugar at parse time to
    /// <c>COALESCE((SELECT COUNT(*) FROM (subquery)), 0) &gt; 0</c>. The
    /// synthesized scalar subquery rides on the existing
    /// <see cref="Plan.Resolver.WrapWithScalarSubqueries"/> machinery; no
    /// new resolver case, no new operator. <c>NOT EXISTS (sq)</c> falls
    /// out naturally as <c>NOT (count &gt; 0)</c> via the existing unary-NOT
    /// handling at <see cref="ParseNot"/>.
    /// </summary>
    /// <remarks>
    /// The <c>COALESCE(..., 0)</c> wrap is load-bearing for NOT EXISTS over
    /// an empty subquery: DbspNet's incremental aggregate emits no row for
    /// an empty input (its observable behaviour, see
    /// <c>WhereScalarSubquery_EmptySubquery_ComparisonYieldsNull_FiltersOut</c>),
    /// so the bare <c>(SELECT COUNT(*) FROM (sq))</c> evaluates to NULL on
    /// empty <c>sq</c>. Without COALESCE, <c>NOT (NULL &gt; 0)</c> stays NULL
    /// and WHERE would drop the row instead of passing it. Standard SQL has
    /// <c>COUNT(*)</c> always return one row, so the COALESCE is a no-op
    /// against any conformant input; it's the engine-quirk shim.
    /// </remarks>
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
        var zero = new LiteralExpression(LiteralKind.Integer, 0L);
        var coalesced = new FunctionCallExpression(
            "coalesce",
            new Expression[] { countSubquery, zero },
            IsStar: false);
        return new BinaryExpression(BinaryOperator.Greater, coalesced, zero);
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
