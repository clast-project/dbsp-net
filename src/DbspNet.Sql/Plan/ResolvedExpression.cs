// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections.Generic;
using DbspNet.Sql.Parser.Ast;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Sql.Plan;

/// <summary>
/// A fully name- and type-resolved scalar expression. Column references have
/// been replaced by positional <see cref="ResolvedColumn"/>s; every node
/// carries its inferred <see cref="SqlType"/>. The expression compiler
/// (<c>DbspNet.Sql.Expressions.ExpressionCompiler</c>) walks this tree and
/// emits a <see cref="System.Linq.Expressions.LambdaExpression"/>.
/// </summary>
public abstract record ResolvedExpression(SqlType Type);

public sealed record ResolvedLiteral(LiteralKind Kind, object? Value, SqlType Type) : ResolvedExpression(Type);

public sealed record ResolvedColumn(int Index, SqlType Type) : ResolvedExpression(Type);

public sealed record ResolvedBinary(
    BinaryOperator Operator,
    ResolvedExpression Left,
    ResolvedExpression Right,
    SqlType Type) : ResolvedExpression(Type);

public sealed record ResolvedUnary(
    UnaryOperator Operator,
    ResolvedExpression Operand,
    SqlType Type) : ResolvedExpression(Type);

public sealed record ResolvedIsNull(ResolvedExpression Operand, bool Negated, SqlType Type) : ResolvedExpression(Type);

public sealed record ResolvedCast(ResolvedExpression Operand, SqlType Type) : ResolvedExpression(Type);

/// <summary>
/// A scalar (non-aggregate) function call: <c>COALESCE</c> in v1. The
/// function name has been lower-cased.
/// </summary>
public sealed record ResolvedFunctionCall(
    string FunctionName,
    IReadOnlyList<ResolvedExpression> Arguments,
    SqlType Type) : ResolvedExpression(Type);

/// <summary>
/// <c>probe [NOT] IN (v1, ..., vN)</c> — literal-list form. Probe and every
/// value have been promoted to the same common comparable type by the
/// resolver; equality at compile time is on that common type. Three-valued
/// NULL semantics; see <see cref="DbspNet.Sql.Parser.Ast.InListExpression"/>
/// for the algebra.
/// </summary>
public sealed record ResolvedInList(
    ResolvedExpression Probe,
    IReadOnlyList<ResolvedExpression> Values,
    bool IsNegated,
    SqlType Type) : ResolvedExpression(Type);
