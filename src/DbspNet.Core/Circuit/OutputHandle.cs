// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
namespace DbspNet.Core.Circuit;

/// <summary>
/// Handle used by callers to read values from a circuit output stream. After
/// <see cref="RootCircuit.Step"/>, <see cref="Current"/> exposes the value
/// emitted during that tick.
/// </summary>
public sealed class OutputHandle<T>
{
    private readonly Stream<T> _stream;

    internal OutputHandle(Stream<T> stream)
    {
        _stream = stream;
    }

    /// <summary>The value emitted on this output during the most recent step.</summary>
    public T Current => _stream.Current;
}
