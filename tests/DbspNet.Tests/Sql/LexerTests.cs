using DbspNet.Sql.Parser;

namespace DbspNet.Tests.Sql;

public class LexerTests
{
    [Fact]
    public void Keywords_AreCaseInsensitive()
    {
        var tokens = Lexer.Tokenize("SELECT select Select sELeCt");
        Assert.Equal(TokenKind.Select, tokens[0].Kind);
        Assert.Equal(TokenKind.Select, tokens[1].Kind);
        Assert.Equal(TokenKind.Select, tokens[2].Kind);
        Assert.Equal(TokenKind.Select, tokens[3].Kind);
        Assert.Equal(TokenKind.EndOfInput, tokens[^1].Kind);
    }

    [Fact]
    public void UnquotedIdentifier_IsLowercased()
    {
        var tokens = Lexer.Tokenize("Foo BAR baz_qux");
        Assert.Equal(TokenKind.Identifier, tokens[0].Kind);
        Assert.Equal("foo", tokens[0].Text);
        Assert.Equal("bar", tokens[1].Text);
        Assert.Equal("baz_qux", tokens[2].Text);
    }

    [Fact]
    public void QuotedIdentifier_PreservesCase_AndEscapesDoubledQuote()
    {
        var tokens = Lexer.Tokenize("\"Foo\" \"has \"\"inner\"\" quotes\"");
        Assert.Equal(TokenKind.Identifier, tokens[0].Kind);
        Assert.True(tokens[0].QuotedIdentifier);
        Assert.Equal("Foo", tokens[0].Text);
        Assert.Equal("has \"inner\" quotes", tokens[1].Text);
    }

    [Fact]
    public void QuotedIdentifier_NeverMatchesKeyword()
    {
        var tokens = Lexer.Tokenize("\"select\"");
        Assert.Equal(TokenKind.Identifier, tokens[0].Kind);
        Assert.Equal("select", tokens[0].Text);
    }

    [Fact]
    public void StringLiteral_EscapesDoubledQuote()
    {
        var tokens = Lexer.Tokenize("'it''s ok' 'simple'");
        Assert.Equal(TokenKind.StringLiteral, tokens[0].Kind);
        Assert.Equal("it's ok", tokens[0].Text);
        Assert.Equal("simple", tokens[1].Text);
    }

    [Fact]
    public void IntegerLiteral_Parses()
    {
        var tokens = Lexer.Tokenize("42 0 1000000");
        Assert.Equal(TokenKind.IntegerLiteral, tokens[0].Kind);
        Assert.Equal(42, tokens[0].IntegerValue);
        Assert.Equal(0, tokens[1].IntegerValue);
        Assert.Equal(1_000_000, tokens[2].IntegerValue);
    }

    [Fact]
    public void DecimalLiteral_HasFractionNoExponent()
    {
        var tokens = Lexer.Tokenize("3.14 0.5");
        Assert.Equal(TokenKind.DecimalLiteral, tokens[0].Kind);
        Assert.Equal((Int128)314, tokens[0].DecimalMantissa);
        Assert.Equal(2, tokens[0].DecimalScale);
        Assert.Equal((Int128)5, tokens[1].DecimalMantissa);
        Assert.Equal(1, tokens[1].DecimalScale);
    }

    [Fact]
    public void FloatLiteral_HasExponent()
    {
        var tokens = Lexer.Tokenize("1e10 2.5e-3 6E+2");
        Assert.Equal(TokenKind.FloatLiteral, tokens[0].Kind);
        Assert.Equal(1e10, tokens[0].FloatValue);
        Assert.Equal(2.5e-3, tokens[1].FloatValue);
        Assert.Equal(6e2, tokens[2].FloatValue);
    }

    [Fact]
    public void Operators_IncludeLessEqGreaterEqNotEq()
    {
        var tokens = Lexer.Tokenize("<= >= <> != < > = * /");
        var kinds = new[]
        {
            TokenKind.LessEq, TokenKind.GreaterEq, TokenKind.NotEq, TokenKind.NotEq,
            TokenKind.Less, TokenKind.Greater, TokenKind.Eq, TokenKind.Star, TokenKind.Slash,
        };
        for (var i = 0; i < kinds.Length; i++)
        {
            Assert.Equal(kinds[i], tokens[i].Kind);
        }
    }

    [Fact]
    public void LineComment_IsSkipped()
    {
        var tokens = Lexer.Tokenize("SELECT -- comment here\n*");
        Assert.Equal(TokenKind.Select, tokens[0].Kind);
        Assert.Equal(TokenKind.Star, tokens[1].Kind);
    }

    [Fact]
    public void BlockComment_IsSkipped_SpansMultipleLines()
    {
        var tokens = Lexer.Tokenize("SELECT /* block\n  still inside */ *");
        Assert.Equal(TokenKind.Select, tokens[0].Kind);
        Assert.Equal(TokenKind.Star, tokens[1].Kind);
    }

    [Fact]
    public void SourcePosition_IsOneBased()
    {
        var tokens = Lexer.Tokenize("SELECT\n   x");
        Assert.Equal(1, tokens[0].Position.Line);
        Assert.Equal(1, tokens[0].Position.Column);
        Assert.Equal(2, tokens[1].Position.Line);
        Assert.Equal(4, tokens[1].Position.Column);
    }

    [Fact]
    public void UnterminatedString_Throws()
    {
        Assert.Throws<LexException>(() => Lexer.Tokenize("'nope"));
    }

    [Fact]
    public void UnterminatedBlockComment_Throws()
    {
        Assert.Throws<LexException>(() => Lexer.Tokenize("/* no end"));
    }
}
