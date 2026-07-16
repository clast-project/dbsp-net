// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
namespace DbspNet.Sql.Parser;

/// <summary>
/// Position of a token inside a source string. Line/Column are 1-based;
/// Offset is the 0-based index into the original source.
/// </summary>
public readonly record struct SourcePosition(int Line, int Column, int Offset)
{
    public override string ToString() => $"line {Line}, column {Column}";
}

public enum TokenKind
{
    EndOfInput,

    // Literals
    IntegerLiteral,
    DecimalLiteral,
    FloatLiteral,
    StringLiteral,

    // Names
    Identifier,

    // Statement / clause keywords
    Select, From, Where, Group, By, Having, As, With, Recursive,
    Create, Table, View, Inner, Left, Right, Full, Outer, Cross, Join, On, Using,
    Union, All, Intersect, Except,

    // ORDER BY / LIMIT / OFFSET / FETCH keywords
    Order, Limit, Offset, Fetch, First, Next, Row, Rows, Only, Asc, Desc, Nulls, Last,

    // Boolean / nullability keywords
    And, Or, Not, Is, Null, True, False, In, Exists, Between, Distinct,

    // Functional keywords
    Cast, Coalesce, Primary, Key, Lateness,

    // Conditional keywords
    Case, When, Then, Else, End,

    // Type keywords
    Int, Integer, BigInt, TinyInt, SmallInt, Real, Double, Precision,
    Decimal, Numeric, Varchar, Char, Text, Boolean, Bool,
    Date, Time, Timestamp, Interval, Extract,

    // Punctuation
    LParen, RParen, Comma, Semicolon, Dot, Star,

    // Operators
    Plus, Minus, Slash, Percent, BarBar,
    Eq, NotEq, Less, LessEq, Greater, GreaterEq,
}

/// <summary>
/// A single token produced by <see cref="Lexer"/>. For numeric literals the
/// parsed value is stored in <see cref="IntegerValue"/>, <see cref="DecimalMantissa"/>
/// + <see cref="DecimalScale"/>, or <see cref="FloatValue"/> depending on
/// <see cref="Kind"/>; for string literals the decoded (unescaped) payload
/// is in <see cref="Text"/>. For identifiers, <see cref="Text"/> holds the
/// canonical spelling (unquoted identifiers are lower-cased; double-quoted
/// identifiers preserve case).
/// </summary>
/// <remarks>
/// Decimal literals are carried as a fixed-point pair (mantissa, scale) —
/// scale-in-type model, matching <see cref="Clast.DatabaseDecimal.Values.Decimal128"/>
/// downstream. <c>1.5</c> → (mantissa=15, scale=1); <c>1.50</c> →
/// (mantissa=150, scale=2). Mantissa width up to 128-bit (~38 digits).
/// </remarks>
public sealed record Token(
    TokenKind Kind,
    string Text,
    SourcePosition Position)
{
    public long IntegerValue { get; init; }
    public Int128 DecimalMantissa { get; init; }
    public byte DecimalScale { get; init; }
    public double FloatValue { get; init; }
    public bool QuotedIdentifier { get; init; }
}
