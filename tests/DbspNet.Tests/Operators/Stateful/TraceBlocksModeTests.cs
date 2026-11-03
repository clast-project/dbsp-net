// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Operators.Stateful.Blocks;
using Xunit;

namespace DbspNet.Tests.Operators.Stateful;

/// <summary><c>DBSPNET_TRACE_BLOCKS</c>: blocks are on unless turned off (docs/design-flat-trace-state.md §3.25).</summary>
public class TraceBlocksModeTests
{
    [Theory]
    [InlineData(null, new[] { "all" })]
    [InlineData("", new[] { "all" })]
    [InlineData("  ", new[] { "all" })]
    [InlineData("off", new string[0])]
    [InlineData("NONE", new string[0])]
    [InlineData("join,agg", new[] { "join", "agg" })]
    [InlineData("all", new[] { "all" })]
    public void Setting_ParsesToKinds(string? setting, string[] expected) =>
        Assert.Equal(expected, TraceBlocksMode.ParseKinds(setting));
}
