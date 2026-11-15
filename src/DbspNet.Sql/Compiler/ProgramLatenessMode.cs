// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Sql.Plan;

namespace DbspNet.Sql.Compiler;

/// <summary>
/// What the program compilers (<see cref="PlanToCircuit.CompileProgram"/>,
/// <see cref="PlanToCircuit.TryCompileProgramParallel"/>) do with a table that declares
/// <c>LATENESS</c>. They do not enforce it: no late rows are dropped and no state is collected,
/// so a program would silently give different results than the same SQL compiled as a single
/// query (issue #3). Until cross-view frontiers are wired, such a program is rejected.
/// </summary>
/// <remarks>
/// Default: reject. <c>DBSPNET_PROGRAM_LATENESS=ignore</c> (or the thread-static
/// <see cref="Ignore"/>) restores the old behaviour, compiling the program with the declaration
/// ignored. Read at compile time.
/// </remarks>
internal static class ProgramLatenessMode
{
    private static readonly bool EnvironmentIgnore = string.Equals(
        Environment.GetEnvironmentVariable("DBSPNET_PROGRAM_LATENESS")?.Trim(), "ignore", StringComparison.OrdinalIgnoreCase);

    [ThreadStatic]
    private static bool? _ignore;

    internal static bool Ignore
    {
        get => _ignore ?? EnvironmentIgnore;
        set => _ignore = value;
    }

    /// <summary>Throw if any table declares <c>LATENESS</c>, unless <see cref="Ignore"/>.</summary>
    internal static void Check(IReadOnlyList<CreateTablePlan> tables)
    {
        if (Ignore)
        {
            return;
        }

        foreach (var table in tables)
        {
            foreach (var column in table.Schema.Columns)
            {
                if (column.Lateness is not null)
                {
                    throw new NotSupportedException(
                        $"LATENESS on {table.TableName}.{column.Name} is not supported in a multi-view program yet: " +
                        "the program path would neither drop late rows nor collect state, so results would " +
                        "differ from the same query compiled on its own " +
                        "(https://github.com/clast-project/dbsp-net/issues/3). Remove the LATENESS clause, " +
                        "compile the query with PlanToCircuit.Compile, or set DBSPNET_PROGRAM_LATENESS=ignore " +
                        "to compile with the declaration ignored.");
                }
            }
        }
    }
}
