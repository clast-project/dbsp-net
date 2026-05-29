// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Globalization;
using System.Text;

namespace DbspNet.Sql.Parser;

/// <summary>
/// Hand-rolled PostgreSQL-flavored SQL lexer. Unquoted identifiers are
/// case-folded to lowercase; double-quoted identifiers preserve case and
/// may contain any character (with <c>""</c> as an embedded quote). String
/// literals are single-quoted with <c>''</c> as an embedded apostrophe.
/// <c>--</c> starts a line comment; <c>/* ... */</c> brackets a block comment.
/// </summary>
public sealed class Lexer
{
    private static readonly Dictionary<string, TokenKind> Keywords = new(StringComparer.Ordinal)
    {
        ["select"] = TokenKind.Select,
        ["from"] = TokenKind.From,
        ["where"] = TokenKind.Where,
        ["group"] = TokenKind.Group,
        ["by"] = TokenKind.By,
        ["having"] = TokenKind.Having,
        ["as"] = TokenKind.As,
        ["with"] = TokenKind.With,
        ["recursive"] = TokenKind.Recursive,
        ["create"] = TokenKind.Create,
        ["table"] = TokenKind.Table,
        ["view"] = TokenKind.View,
        ["inner"] = TokenKind.Inner,
        ["left"] = TokenKind.Left,
        ["right"] = TokenKind.Right,
        ["outer"] = TokenKind.Outer,
        ["join"] = TokenKind.Join,
        ["on"] = TokenKind.On,
        ["union"] = TokenKind.Union,
        ["all"] = TokenKind.All,
        ["intersect"] = TokenKind.Intersect,
        ["except"] = TokenKind.Except,
        ["and"] = TokenKind.And,
        ["or"] = TokenKind.Or,
        ["not"] = TokenKind.Not,
        ["is"] = TokenKind.Is,
        ["null"] = TokenKind.Null,
        ["true"] = TokenKind.True,
        ["false"] = TokenKind.False,
        ["in"] = TokenKind.In,
        ["exists"] = TokenKind.Exists,
        ["cast"] = TokenKind.Cast,
        ["coalesce"] = TokenKind.Coalesce,
        ["primary"] = TokenKind.Primary,
        ["key"] = TokenKind.Key,
        ["lateness"] = TokenKind.Lateness,
        ["int"] = TokenKind.Int,
        ["integer"] = TokenKind.Integer,
        ["bigint"] = TokenKind.BigInt,
        ["real"] = TokenKind.Real,
        ["double"] = TokenKind.Double,
        ["precision"] = TokenKind.Precision,
        ["decimal"] = TokenKind.Decimal,
        ["numeric"] = TokenKind.Numeric,
        ["varchar"] = TokenKind.Varchar,
        ["char"] = TokenKind.Char,
        ["text"] = TokenKind.Text,
        ["boolean"] = TokenKind.Boolean,
        ["bool"] = TokenKind.Bool,
        ["date"] = TokenKind.Date,
        ["time"] = TokenKind.Time,
        ["timestamp"] = TokenKind.Timestamp,
    };

    private readonly string _source;
    private int _pos;
    private int _line = 1;
    private int _lineStart;

    public Lexer(string source)
    {
        ArgumentNullException.ThrowIfNull(source);
        _source = source;
    }

    public static IReadOnlyList<Token> Tokenize(string source)
    {
        var lexer = new Lexer(source);
        var tokens = new List<Token>();
        while (true)
        {
            var t = lexer.Next();
            tokens.Add(t);
            if (t.Kind == TokenKind.EndOfInput)
            {
                break;
            }
        }

        return tokens;
    }

    public Token Next()
    {
        SkipWhitespaceAndComments();
        if (_pos >= _source.Length)
        {
            return new Token(TokenKind.EndOfInput, string.Empty, CurrentPosition());
        }

        var start = CurrentPosition();
        var c = _source[_pos];

        if (IsIdentStart(c))
        {
            return LexIdentifierOrKeyword(start);
        }

        if (c == '"')
        {
            return LexQuotedIdentifier(start);
        }

        if (char.IsDigit(c))
        {
            return LexNumber(start);
        }

        if (c == '\'')
        {
            return LexString(start);
        }

        return LexPunctuationOrOperator(start);
    }

    private void SkipWhitespaceAndComments()
    {
        while (_pos < _source.Length)
        {
            var c = _source[_pos];
            if (c == '\n')
            {
                _pos++;
                _line++;
                _lineStart = _pos;
            }
            else if (c == '\r')
            {
                _pos++;
                if (_pos < _source.Length && _source[_pos] == '\n')
                {
                    _pos++;
                }

                _line++;
                _lineStart = _pos;
            }
            else if (char.IsWhiteSpace(c))
            {
                _pos++;
            }
            else if (c == '-' && _pos + 1 < _source.Length && _source[_pos + 1] == '-')
            {
                _pos += 2;
                while (_pos < _source.Length && _source[_pos] != '\n' && _source[_pos] != '\r')
                {
                    _pos++;
                }
            }
            else if (c == '/' && _pos + 1 < _source.Length && _source[_pos + 1] == '*')
            {
                var start = CurrentPosition();
                _pos += 2;
                while (_pos + 1 < _source.Length && !(_source[_pos] == '*' && _source[_pos + 1] == '/'))
                {
                    if (_source[_pos] == '\n')
                    {
                        _line++;
                        _lineStart = _pos + 1;
                    }

                    _pos++;
                }

                if (_pos + 1 >= _source.Length)
                {
                    throw new LexException("unterminated block comment", start);
                }

                _pos += 2;
            }
            else
            {
                break;
            }
        }
    }

    private Token LexIdentifierOrKeyword(SourcePosition start)
    {
        var begin = _pos;
        while (_pos < _source.Length && IsIdentPart(_source[_pos]))
        {
            _pos++;
        }

        var raw = _source.Substring(begin, _pos - begin);
        var folded = raw.ToLowerInvariant();
        if (Keywords.TryGetValue(folded, out var kw))
        {
            return new Token(kw, folded, start);
        }

        return new Token(TokenKind.Identifier, folded, start);
    }

    private Token LexQuotedIdentifier(SourcePosition start)
    {
        _pos++; // opening quote
        var sb = new StringBuilder();
        while (_pos < _source.Length)
        {
            var c = _source[_pos];
            if (c == '"')
            {
                if (_pos + 1 < _source.Length && _source[_pos + 1] == '"')
                {
                    sb.Append('"');
                    _pos += 2;
                    continue;
                }

                _pos++;
                return new Token(TokenKind.Identifier, sb.ToString(), start) { QuotedIdentifier = true };
            }

            if (c == '\n')
            {
                _line++;
                _lineStart = _pos + 1;
            }

            sb.Append(c);
            _pos++;
        }

        throw new LexException("unterminated quoted identifier", start);
    }

    private Token LexString(SourcePosition start)
    {
        _pos++; // opening quote
        var sb = new StringBuilder();
        while (_pos < _source.Length)
        {
            var c = _source[_pos];
            if (c == '\'')
            {
                if (_pos + 1 < _source.Length && _source[_pos + 1] == '\'')
                {
                    sb.Append('\'');
                    _pos += 2;
                    continue;
                }

                _pos++;
                return new Token(TokenKind.StringLiteral, sb.ToString(), start);
            }

            if (c == '\n')
            {
                _line++;
                _lineStart = _pos + 1;
            }

            sb.Append(c);
            _pos++;
        }

        throw new LexException("unterminated string literal", start);
    }

    private Token LexNumber(SourcePosition start)
    {
        var begin = _pos;
        var hasDot = false;
        var hasExp = false;

        while (_pos < _source.Length && char.IsDigit(_source[_pos]))
        {
            _pos++;
        }

        if (_pos < _source.Length && _source[_pos] == '.')
        {
            // Only consume the dot if followed by digits (else it could be "1.foo" membership
            // syntax — but SQL doesn't have that; still guard to avoid swallowing a stray dot).
            if (_pos + 1 < _source.Length && char.IsDigit(_source[_pos + 1]))
            {
                hasDot = true;
                _pos++;
                while (_pos < _source.Length && char.IsDigit(_source[_pos]))
                {
                    _pos++;
                }
            }
        }

        if (_pos < _source.Length && (_source[_pos] == 'e' || _source[_pos] == 'E'))
        {
            var expStart = _pos;
            _pos++;
            if (_pos < _source.Length && (_source[_pos] == '+' || _source[_pos] == '-'))
            {
                _pos++;
            }

            if (_pos >= _source.Length || !char.IsDigit(_source[_pos]))
            {
                throw new LexException("malformed numeric literal: missing exponent digits", start);
            }

            while (_pos < _source.Length && char.IsDigit(_source[_pos]))
            {
                _pos++;
            }

            hasExp = true;
            _ = expStart;
        }

        var text = _source.Substring(begin, _pos - begin);

        if (hasExp)
        {
            var dv = double.Parse(text, NumberStyles.Float, CultureInfo.InvariantCulture);
            return new Token(TokenKind.FloatLiteral, text, start) { FloatValue = dv };
        }

        if (hasDot)
        {
            var (mantissa, scale) = ParseDecimalLiteral(text, start);
            return new Token(TokenKind.DecimalLiteral, text, start)
            {
                DecimalMantissa = mantissa,
                DecimalScale = scale,
            };
        }

        if (long.TryParse(text, NumberStyles.None, CultureInfo.InvariantCulture, out var lv))
        {
            return new Token(TokenKind.IntegerLiteral, text, start) { IntegerValue = lv };
        }

        // Integer out of long range: parse as Int128 with scale 0.
        var (bigMantissa, bigScale) = ParseDecimalLiteral(text, start);
        return new Token(TokenKind.DecimalLiteral, text, start)
        {
            DecimalMantissa = bigMantissa,
            DecimalScale = bigScale,
        };
    }

    /// <summary>
    /// Parse a decimal literal exactly — preserving the natural scale (digits
    /// after the decimal point, if any). Mantissa is signed Int128. Throws
    /// <see cref="LexException"/> if the digit count exceeds Int128 capacity.
    /// </summary>
    private (Int128 Mantissa, byte Scale) ParseDecimalLiteral(string text, SourcePosition start)
    {
        var mantissa = Int128.Zero;
        var scale = 0;
        var inFraction = false;
        var digitCount = 0;
        for (var i = 0; i < text.Length; i++)
        {
            var c = text[i];
            if (c == '.')
            {
                inFraction = true;
                continue;
            }

            // Digits only — exponent / sign cases handled separately upstream.
            digitCount++;
            if (digitCount > 38)
            {
                throw new LexException(
                    $"decimal literal exceeds 38-digit Int128 capacity",
                    start);
            }

            mantissa = mantissa * 10 + (c - '0');
            if (inFraction)
            {
                scale++;
            }
        }

        if (scale > byte.MaxValue)
        {
            throw new LexException("decimal scale exceeds 255", start);
        }

        return (mantissa, (byte)scale);
    }

    private Token LexPunctuationOrOperator(SourcePosition start)
    {
        var c = _source[_pos];
        _pos++;
        switch (c)
        {
            case '(': return new Token(TokenKind.LParen, "(", start);
            case ')': return new Token(TokenKind.RParen, ")", start);
            case ',': return new Token(TokenKind.Comma, ",", start);
            case ';': return new Token(TokenKind.Semicolon, ";", start);
            case '.': return new Token(TokenKind.Dot, ".", start);
            case '*': return new Token(TokenKind.Star, "*", start);
            case '+': return new Token(TokenKind.Plus, "+", start);
            case '-': return new Token(TokenKind.Minus, "-", start);
            case '/': return new Token(TokenKind.Slash, "/", start);
            case '%': return new Token(TokenKind.Percent, "%", start);
            case '=': return new Token(TokenKind.Eq, "=", start);
            case '<':
                if (_pos < _source.Length && _source[_pos] == '=')
                {
                    _pos++;
                    return new Token(TokenKind.LessEq, "<=", start);
                }

                if (_pos < _source.Length && _source[_pos] == '>')
                {
                    _pos++;
                    return new Token(TokenKind.NotEq, "<>", start);
                }

                return new Token(TokenKind.Less, "<", start);
            case '>':
                if (_pos < _source.Length && _source[_pos] == '=')
                {
                    _pos++;
                    return new Token(TokenKind.GreaterEq, ">=", start);
                }

                return new Token(TokenKind.Greater, ">", start);
            case '!':
                if (_pos < _source.Length && _source[_pos] == '=')
                {
                    _pos++;
                    return new Token(TokenKind.NotEq, "!=", start);
                }

                throw new LexException($"unexpected character '{c}'", start);
            default:
                throw new LexException($"unexpected character '{c}'", start);
        }
    }

    private SourcePosition CurrentPosition() => new(_line, _pos - _lineStart + 1, _pos);

    private static bool IsIdentStart(char c) => char.IsLetter(c) || c == '_';

    private static bool IsIdentPart(char c) => char.IsLetterOrDigit(c) || c == '_';
}

public sealed class LexException : Exception
{
    public SourcePosition Position { get; }

    public LexException(string message, SourcePosition position)
        : base($"{message} (at {position})")
    {
        Position = position;
    }
}
