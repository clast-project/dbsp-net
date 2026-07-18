// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System;
using System.Collections.Generic;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Tests.Sql;

/// <summary>
/// Default NULL placement under a bare <c>ORDER BY</c> (no explicit NULLS clause),
/// switchable via <see cref="NullCollation"/>. This drove the ivm-bench
/// <c>broker_performance</c> rank divergence: Feldera (Calcite) sorts NULLs
/// <see cref="NullCollation.Low"/> (last under DESC), so an all-NULL group ranks
/// last; DbspNet's High default put it first and shifted every rank below it.
/// </summary>
public class NullCollationTests
{
    private static CompiledProgram RankProgram(NullCollation collation) =>
        SqlProgram.Compile(
            [
                "CREATE TABLE t (id INT NOT NULL, v DOUBLE PRECISION)",
                "CREATE VIEW r AS SELECT id, DENSE_RANK() OVER (ORDER BY v DESC) AS rnk FROM t",
            ],
            new HashSet<string>(StringComparer.Ordinal) { "r" },
            nullCollation: collation);

    private static Dictionary<int, long> RunRanks(NullCollation collation)
    {
        var prog = RankProgram(collation);
        prog.Table("t").Insert(1, 100.0);
        prog.Table("t").Insert(2, 90.0);
        prog.Table("t").Insert(3, (object?)null);
        prog.Step();

        var ranks = new Dictionary<int, long>();
        foreach (var (row, w) in prog.Outputs["r"].CurrentView)
        {
            if (w.Value != 0)
            {
                ranks[(int)row[0]!] = (long)row[1]!;
            }
        }

        return ranks;
    }

    [Fact]
    public void Low_DescPlacesNullLast_FelderaDefault()
    {
        var ranks = RunRanks(NullCollation.Low);
        // v: 100, 90, NULL under DESC with nulls low → NULL sorts last.
        Assert.Equal(1, ranks[1]);   // 100
        Assert.Equal(2, ranks[2]);   // 90
        Assert.Equal(3, ranks[3]);   // NULL last
    }

    [Fact]
    public void High_DescPlacesNullFirst_PostgresDefault()
    {
        var ranks = RunRanks(NullCollation.High);
        // Nulls high → NULL is the "largest", sorts first under DESC.
        Assert.Equal(1, ranks[3]);   // NULL first
        Assert.Equal(2, ranks[1]);   // 100
        Assert.Equal(3, ranks[2]);   // 90
    }

    [Fact]
    public void High_IsTheDefaultWhenUnspecified()
    {
        // Omitting nullCollation must preserve the long-standing behaviour (High).
        var prog = SqlProgram.Compile(
            [
                "CREATE TABLE t (id INT NOT NULL, v DOUBLE PRECISION)",
                "CREATE VIEW r AS SELECT id, DENSE_RANK() OVER (ORDER BY v DESC) AS rnk FROM t",
            ],
            new HashSet<string>(StringComparer.Ordinal) { "r" });

        prog.Table("t").Insert(1, 100.0);
        prog.Table("t").Insert(2, (object?)null);
        prog.Step();

        long nullRank = 0;
        foreach (var (row, w) in prog.Outputs["r"].CurrentView)
        {
            if (w.Value != 0 && (int)row[0]! == 2)
            {
                nullRank = (long)row[1]!;
            }
        }

        Assert.Equal(1, nullRank);   // High default → NULL first under DESC
    }
}
