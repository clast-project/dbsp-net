// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Circuit;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Optimizer;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;
using Xunit;

namespace DbspNet.Tests.EndToEnd;

/// <summary>
/// §22.7 per-operator selection gate: under the default
/// <see cref="PartitionedTopKNarrowing.Auto"/> override,
/// <c>CircuitBuilder.PartitionedTopK</c> must build the narrow-key
/// <see cref="PartitionedTopKNarrowOp{TRow,TKey}"/> for a single-column TOP-K with
/// <c>limit &gt; <see cref="PartitionedTopKNarrowingMode.AutoLimitThreshold"/></c>
/// (q19's accumulating window) and keep the whole-row
/// <see cref="PartitionedTopKOp{TRow,TKey}"/> for every TOP-1 dedup (q18/q9) — the
/// crossover finding that the narrow prize tracks per-partition accumulation while the
/// only cheap static signal is the limit (§22.6/§22.7). The PBT proves the two operators
/// are value-equivalent; this proves the compiler routes to the right one.
/// </summary>
public class PartitionedTopKSelectionTests
{
    private const string Ddl = "CREATE TABLE t (g INT NOT NULL, v INT NOT NULL, id INT NOT NULL)";

    private static string TopKSql(int limit) =>
        "SELECT g, v, id FROM (SELECT g, v, id, ROW_NUMBER() OVER " +
        $"(PARTITION BY g ORDER BY v ASC) AS rn FROM t) s WHERE rn <= {limit}";

    // Multi-column ORDER BY: no single-column extractor is plumbed, so the narrow path
    // never applies regardless of limit (falls back to whole-row).
    private static string MultiColSql(int limit) =>
        "SELECT g, v, id FROM (SELECT g, v, id, ROW_NUMBER() OVER " +
        $"(PARTITION BY g ORDER BY v ASC, id DESC) AS rn FROM t) s WHERE rn <= {limit}";

    [Theory]
    [InlineData(1, false)]  // TOP-1 dedup (q18/q9 shape) — whole-row
    [InlineData(2, true)]   // first genuine TOP-K — narrow
    [InlineData(3, true)]
    [InlineData(10, true)]  // q19 shape — narrow
    public void Auto_SelectsNarrow_WhenLimitExceedsThreshold(int limit, bool expectNarrow)
    {
        WithOverride(PartitionedTopKNarrowing.Auto, () =>
        {
            Assert.Equal(expectNarrow, FlatPathIsNarrow(TopKSql(limit)));
            Assert.Equal(expectNarrow, TypedPathIsNarrow(TopKSql(limit)));
        });
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(10)]
    public void Auto_KeepsWholeRow_ForMultiColumnOrderBy(int limit)
    {
        WithOverride(PartitionedTopKNarrowing.Auto, () =>
        {
            Assert.False(FlatPathIsNarrow(MultiColSql(limit)));
            Assert.False(TypedPathIsNarrow(MultiColSql(limit)));
        });
    }

    [Fact]
    public void ForceOverrides_PinTheArm_RegardlessOfLimit()
    {
        // ForceNarrow narrows even a TOP-1 (the §22 A/B "on" arm); ForceWholeRow keeps
        // the whole-row op even for a big limit (the baseline arm).
        WithOverride(PartitionedTopKNarrowing.ForceNarrow, () =>
        {
            Assert.True(FlatPathIsNarrow(TopKSql(1)));
            Assert.True(TypedPathIsNarrow(TopKSql(1)));
        });
        WithOverride(PartitionedTopKNarrowing.ForceWholeRow, () =>
        {
            Assert.False(FlatPathIsNarrow(TopKSql(10)));
            Assert.False(TypedPathIsNarrow(TopKSql(10)));
        });
    }

    private static void WithOverride(PartitionedTopKNarrowing mode, Action body)
    {
        var prev = PartitionedTopKNarrowingMode.Override;
        PartitionedTopKNarrowingMode.Override = mode;
        try
        {
            body();
        }
        finally
        {
            PartitionedTopKNarrowingMode.Override = prev;
        }
    }

    private static bool FlatPathIsNarrow(string sql)
        => HasNarrowTopK(PlanToCircuit.Compile(Plan(sql)).Circuit.Operators);

    private static bool TypedPathIsNarrow(string sql)
    {
        Assert.True(TypedPlanCompiler.TryCompile(Plan(sql), out var typed));
        return HasNarrowTopK(typed!.Circuit.Operators);
    }

    private static LogicalPlan Plan(string sql)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        resolver.Resolve(Parser.ParseStatement(Ddl));
        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(sql))).Query;
        return PlanOptimizer.Optimize(plan);
    }

    // Exactly one partitioned-TOP-K operator is built per query; assert on which variant.
    private static bool HasNarrowTopK(IReadOnlyList<IOperator> operators)
    {
        var topK = operators
            .Where(o => o.GetType().Name.StartsWith("PartitionedTopK", StringComparison.Ordinal))
            .ToList();
        Assert.Single(topK);
        return topK[0].GetType().Name.StartsWith("PartitionedTopKNarrowOp", StringComparison.Ordinal);
    }
}
