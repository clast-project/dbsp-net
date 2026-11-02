// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Runtime.CompilerServices;

namespace DbspNet.Tests;

/// <summary>Runs before any test: loads DbspNet.Persistence so its module initializer installs the
/// trace block format (docs/design-flat-trace-state.md §3) before a test compiles a circuit — with
/// DBSPNET_TRACE_BLOCKS=join, every test that compiles an inner join should get the blocked one,
/// not only those that happen to touch a Persistence type first.</summary>
internal static class TestAssemblyInit
{
#pragma warning disable CA2255
    [ModuleInitializer]
#pragma warning restore CA2255
    internal static void Init() =>
        RuntimeHelpers.RunModuleConstructor(typeof(DbspNet.Persistence.Blocks.BlockSchema).Module.ModuleHandle);
}
