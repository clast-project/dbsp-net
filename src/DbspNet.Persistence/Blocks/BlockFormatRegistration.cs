// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Runtime.CompilerServices;
using DbspNet.Sql.Compiler;

namespace DbspNet.Persistence.Blocks;

/// <summary>Installs the v3 run block as the compiler's trace block format when this assembly
/// loads (see <see cref="TraceBlockFormats"/>).</summary>
internal static class BlockFormatRegistration
{
    // A library module initializer on purpose: the compiler (DbspNet.Sql) cannot reference this
    // assembly, and the format must be in place before any circuit that might use it compiles.
#pragma warning disable CA2255
    [ModuleInitializer]
#pragma warning restore CA2255
    internal static void Register()
    {
        TraceBlockFormats.Factory = static (key, value) => BlockSchema.TryCreate(key, value);

        // Mid-batch spilling (docs/design-bounded-memory.md §5.9), opt-in: DBSPNET_SPILL=on.
        if (Core.Operators.Stateful.Blocks.SpillSettings.Spill)
        {
            Core.Operators.Stateful.Blocks.BlockAccess.Spiller = RunSpiller.CreateDefault();
        }
    }
}
