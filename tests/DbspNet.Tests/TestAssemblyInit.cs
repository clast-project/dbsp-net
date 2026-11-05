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
    internal static void Init()
    {
        RuntimeHelpers.RunModuleConstructor(typeof(DbspNet.Persistence.Blocks.BlockSchema).Module.ModuleHandle);

        // A forced in-step spill run (DBSPNET_STEP_SPILL_ROWS=0) writes how many deltas it built as
        // blocks, so a green suite is known not to be vacuous (docs/design-bounded-memory.md §5.14). Appended:
        // a test that loads a second copy of this assembly (its own AssemblyLoadContext) adds a zero line.
        if (Environment.GetEnvironmentVariable("DBSPNET_STEP_SPILL_STATS") is { Length: > 0 } path)
        {
            AppDomain.CurrentDomain.ProcessExit += (_, _) => File.AppendAllText(
                path,
                FormattableString.Invariant(
                    $"blocked deltas {DbspNet.Core.Operators.Stateful.Blocks.DeltaSpill.BlockedBuilds}, rows {DbspNet.Core.Operators.Stateful.Blocks.DeltaSpill.BlockedRows}, indexed {DbspNet.Core.Operators.Stateful.Blocks.DeltaSpill.BlockedGroupBuilds} ({DbspNet.Core.Operators.Stateful.Blocks.DeltaSpill.BlockedGroupEntries} entries), adopted {DbspNet.Core.Operators.Stateful.Blocks.DeltaSpill.Adopted}\n"));
        }
    }
}
