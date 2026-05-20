// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
namespace DbspNet.Core.Circuit.Operators;

internal sealed class ApplyOp<TIn, TOut> : IOperator
{
    private readonly Stream<TIn> _input;
    private readonly Stream<TOut> _output;
    private readonly Func<TIn, TOut> _transform;

    public ApplyOp(Stream<TIn> input, Stream<TOut> output, Func<TIn, TOut> transform)
    {
        _input = input;
        _output = output;
        _transform = transform;
    }

    public void Step()
    {
        _output.SetCurrent(_transform(_input.Current));
    }
}

internal sealed class Apply2Op<TIn1, TIn2, TOut> : IOperator
{
    private readonly Stream<TIn1> _left;
    private readonly Stream<TIn2> _right;
    private readonly Stream<TOut> _output;
    private readonly Func<TIn1, TIn2, TOut> _transform;

    public Apply2Op(Stream<TIn1> left, Stream<TIn2> right, Stream<TOut> output, Func<TIn1, TIn2, TOut> transform)
    {
        _left = left;
        _right = right;
        _output = output;
        _transform = transform;
    }

    public void Step()
    {
        _output.SetCurrent(_transform(_left.Current, _right.Current));
    }
}
