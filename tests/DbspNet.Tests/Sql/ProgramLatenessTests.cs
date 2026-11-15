// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Sql.Compiler;
using Xunit;

namespace DbspNet.Tests.Sql;

/// <summary>
/// The program path does not enforce LATENESS (issue #3), so it rejects a declaration rather than
/// silently diverge from the single-query path. <c>DBSPNET_PROGRAM_LATENESS=ignore</c> restores the
/// old behaviour.
/// </summary>
public class ProgramLatenessTests
{
    private static readonly string[] Sql =
    [
        "CREATE TABLE e (ts BIGINT NOT NULL LATENESS 5, v INT NOT NULL)",
        "CREATE VIEW per_ts AS SELECT ts, SUM(v) AS s FROM e GROUP BY ts",
    ];

    private static readonly HashSet<string> Views = new(["per_ts"], StringComparer.Ordinal);

    [Fact]
    public void SerialProgram_RejectsLateness_NamingTheColumnAndTheIssue()
    {
        var ex = Assert.Throws<NotSupportedException>(() => SqlProgram.Compile(Sql, Views));
        Assert.Contains("e.ts", ex.Message, StringComparison.Ordinal);
        Assert.Contains("issues/3", ex.Message, StringComparison.Ordinal);
        Assert.Contains("DBSPNET_PROGRAM_LATENESS=ignore", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ParallelProgram_RejectsLateness()
    {
        Assert.Throws<NotSupportedException>(() => SqlProgram.TryCompileParallel(Sql, Views, 2, out _));
    }

    [Fact]
    public void ProgramWithoutLateness_StillCompiles()
    {
        string[] sql =
        [
            "CREATE TABLE e (ts BIGINT NOT NULL, v INT NOT NULL)",
            "CREATE VIEW per_ts AS SELECT ts, SUM(v) AS s FROM e GROUP BY ts",
        ];
        var program = SqlProgram.Compile(sql, Views);
        program.Table("e").Insert(1L, 2);
        program.Step();
        Assert.Equal(1, program.Outputs["per_ts"].RowCount);
    }

    [Fact]
    public void Ignore_CompilesWithTheDeclarationIgnored_LateRowsKept()
    {
        ProgramLatenessMode.Ignore = true;
        try
        {
            var program = SqlProgram.Compile(Sql, Views);
            program.Table("e").Insert(100L, 1);
            program.Step();

            // 10 is more than 5 below the max seen: the single-query path would drop it as late.
            // Ignored on the program path, it is kept. This is the divergence the check guards.
            program.Table("e").Insert(10L, 1);
            program.Step();
            Assert.Equal(2, program.Outputs["per_ts"].RowCount);
        }
        finally
        {
            ProgramLatenessMode.Ignore = false;
        }
    }
}
