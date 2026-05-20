// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
namespace DbspNet.Core.Circuit;

/// <summary>
/// Represents a single node in a DBSP circuit. Operators are scheduled in
/// topological order and fire exactly once per tick.
/// </summary>
internal interface IOperator
{
    void Step();
}
