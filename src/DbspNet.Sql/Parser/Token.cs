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
    Create, Table, View, Inner, Left, Right, Outer, Join, On,
    Union, All, Intersect, Except,

    // Boolean / nullability keywords
    And, Or, Not, Is, Null, True, False,

    // Functional keywords
    Cast, Coalesce, Primary, Key,

    // Type keywords
    Int, Integer, BigInt, Real, Double, Precision,
    Decimal, Numeric, Varchar, Char, Text, Boolean, Bool,

    // Punctuation
    LParen, RParen, Comma, Semicolon, Dot, Star,

    // Operators
    Plus, Minus, Slash, Percent,
    Eq, NotEq, Less, LessEq, Greater, GreaterEq,
}

/// <summary>
/// A single token produced by <see cref="Lexer"/>. For numeric literals the
/// parsed value is stored in <see cref="IntegerValue"/>, <see cref="DecimalValue"/>,
/// or <see cref="FloatValue"/> depending on <see cref="Kind"/>; for string
/// literals the decoded (unescaped) payload is in <see cref="Text"/>. For
/// identifiers, <see cref="Text"/> holds the canonical spelling (unquoted
/// identifiers are lower-cased; double-quoted identifiers preserve case).
/// </summary>
public sealed record Token(
    TokenKind Kind,
    string Text,
    SourcePosition Position)
{
    public long IntegerValue { get; init; }
    public decimal DecimalValue { get; init; }
    public double FloatValue { get; init; }
    public bool QuotedIdentifier { get; init; }
}
