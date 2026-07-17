// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System;
using System.Collections.Generic;
using System.Linq;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Plan;

namespace DbspNet.Tests.Sql;

/// <summary>
/// The multi-view program compiler: a DAG of CREATE TABLE sources + CREATE VIEW
/// definitions lowered into one circuit where views are shared streams (an internal
/// view referenced by two outputs is computed once) with an integrated output per
/// designated view. The incremental outputs must equal a dependency-order batch
/// re-computation. This is the engine shape ivm-bench expects (sources → … → gold as
/// one incrementally-maintained circuit).
/// </summary>
public class ProgramCompilerTests
{
    // A small TPC-DI-shaped DAG: an SCD2 "current record" internal view shared by two
    // outputs (a fact enrichment join and a per-key aggregate).
    private static readonly string[] Program =
    [
        "CREATE TABLE company (company_id INT NOT NULL, name_code INT NOT NULL, effective_ts INT NOT NULL)",
        "CREATE TABLE trade (trade_id INT NOT NULL, company_id INT NOT NULL, amount INT NOT NULL)",
        "CREATE VIEW company_current AS " +
        "  SELECT company_id, name_code FROM (" +
        "    SELECT company_id, name_code, " +
        "      CASE WHEN ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY effective_ts DESC) = 1 THEN 1 ELSE 0 END AS is_current " +
        "    FROM company" +
        "  ) s WHERE is_current = 1",
        "CREATE VIEW trade_enriched AS " +
        "  SELECT t.trade_id, t.amount, c.name_code " +
        "  FROM trade t JOIN company_current c ON t.company_id = c.company_id",
        "CREATE VIEW company_totals AS " +
        "  SELECT c.name_code, SUM(t.amount) AS total " +
        "  FROM trade t JOIN company_current c ON t.company_id = c.company_id " +
        "  GROUP BY c.name_code",
    ];

    private static readonly HashSet<string> Outputs = new(StringComparer.Ordinal) { "trade_enriched", "company_totals" };

    private static StructuralRow Row(params object?[] values) => new(values);

    // Evaluate every view in dependency order, threading each result in as a "table" so
    // downstream views that reference it resolve — the batch oracle for the program.
    private static Dictionary<string, ZSet<StructuralRow, Z64>> BatchAll(
        ResolvedProgram program,
        ZSet<StructuralRow, Z64> company,
        ZSet<StructuralRow, Z64> trade)
    {
        var tableStates = new Dictionary<string, ZSet<StructuralRow, Z64>>(StringComparer.Ordinal)
        {
            ["company"] = company,
            ["trade"] = trade,
        };

        var results = new Dictionary<string, ZSet<StructuralRow, Z64>>(StringComparer.Ordinal);
        foreach (var v in program.Views)
        {
            var ctx = new BatchEvalContext(tableStates, new Dictionary<CteRef, ZSet<StructuralRow, Z64>>());
            var result = BatchPlanEvaluator.Evaluate(v.Query, ctx);
            tableStates[v.ViewName] = result;
            results[v.ViewName] = result;
        }

        return results;
    }

    [Fact]
    public void MultiView_SharedView_TwoOutputs_MatchBatch()
    {
        var resolved = SqlProgram.Resolve(Program, Outputs);
        var program = PlanToCircuit.CompileProgram(resolved.Tables, resolved.Views);

        Assert.Equal(2, program.Inputs.Count);
        Assert.Equal(2, program.Outputs.Count);
        Assert.True(program.Outputs.ContainsKey("trade_enriched"));
        Assert.True(program.Outputs.ContainsKey("company_totals"));

        // company: SCD2 feed (company 1 updated to name_code 101, company 2 → 201).
        program.Table("company").Insert(1, 100, 1);
        program.Table("company").Insert(2, 200, 1);
        program.Table("company").Insert(1, 101, 2);
        program.Table("company").Insert(2, 201, 3);
        // trade: facts.
        program.Table("trade").Insert(10, 1, 50);
        program.Table("trade").Insert(11, 2, 30);
        program.Table("trade").Insert(12, 1, 20);
        program.Step();

        var company = new ZSetBuilder<StructuralRow, Z64>();
        foreach (var r in new[] { Row(1, 100, 1), Row(2, 200, 1), Row(1, 101, 2), Row(2, 201, 3) })
        {
            company.Add(r, Z64.One);
        }

        var trade = new ZSetBuilder<StructuralRow, Z64>();
        foreach (var r in new[] { Row(10, 1, 50), Row(11, 2, 30), Row(12, 1, 20) })
        {
            trade.Add(r, Z64.One);
        }

        var batch = BatchAll(resolved, company.Build(), trade.Build());

        Assert.True(program.Outputs["trade_enriched"].CurrentView.Equals(batch["trade_enriched"]),
            $"trade_enriched\n  view={program.Outputs["trade_enriched"].CurrentView}\n  batch={batch["trade_enriched"]}");
        Assert.True(program.Outputs["company_totals"].CurrentView.Equals(batch["company_totals"]),
            $"company_totals\n  view={program.Outputs["company_totals"].CurrentView}\n  batch={batch["company_totals"]}");

        // Concrete check: current company 1 = name_code 101 (trades 50+20=70), company 2 = 201 (30).
        Assert.Equal(1, program.Outputs["company_totals"].CurrentView.WeightOf(Row(101, 70L)).Value);
        Assert.Equal(1, program.Outputs["company_totals"].CurrentView.WeightOf(Row(201, 30L)).Value);
        Assert.Equal(3, program.Outputs["trade_enriched"].CurrentView.Count); // 3 trades enriched
    }

    [Fact]
    public void MultiView_IncrementalUpdate_MatchesBatch()
    {
        var resolved = SqlProgram.Resolve(Program, Outputs);
        var program = PlanToCircuit.CompileProgram(resolved.Tables, resolved.Views);

        // Tick 1: initial state.
        program.Table("company").Insert(1, 100, 1);
        program.Table("trade").Insert(10, 1, 50);
        program.Step();

        // Tick 2: a later company record (SCD2 update) + a new trade — exercises the
        // shared current-record view re-ranking flowing into both outputs.
        program.Table("company").Insert(1, 101, 2);
        program.Table("trade").Insert(11, 1, 20);
        program.Step();

        var company = new ZSetBuilder<StructuralRow, Z64>();
        company.Add(Row(1, 100, 1), Z64.One);
        company.Add(Row(1, 101, 2), Z64.One);
        var trade = new ZSetBuilder<StructuralRow, Z64>();
        trade.Add(Row(10, 1, 50), Z64.One);
        trade.Add(Row(11, 1, 20), Z64.One);

        var batch = BatchAll(resolved, company.Build(), trade.Build());

        Assert.True(program.Outputs["trade_enriched"].CurrentView.Equals(batch["trade_enriched"]));
        Assert.True(program.Outputs["company_totals"].CurrentView.Equals(batch["company_totals"]));
        // Both trades now map to the current name_code 101, total 70.
        Assert.Equal(1, program.Outputs["company_totals"].CurrentView.WeightOf(Row(101, 70L)).Value);
    }

    [Fact]
    public void DeadView_NotReachableFromAnyOutput_IsNotCompiled()
    {
        // `dead` is neither an output nor referenced by one. If it were compiled, stepping it
        // would evaluate 100 / (id - id) = divide-by-zero and throw. Dead-view elimination
        // skips it, so the Step runs and only the live output is materialised.
        var program = new[]
        {
            "CREATE TABLE t (id INT NOT NULL)",
            "CREATE VIEW live AS SELECT id FROM t",
            "CREATE VIEW dead AS SELECT id, 100 / (id - id) AS boom FROM t",
        };
        var outputs = new HashSet<string>(StringComparer.Ordinal) { "live" };

        var resolved = SqlProgram.Resolve(program, outputs);
        var compiled = PlanToCircuit.CompileProgram(resolved.Tables, resolved.Views);

        Assert.True(compiled.Outputs.ContainsKey("live"));
        Assert.False(compiled.Outputs.ContainsKey("dead"));

        compiled.Table("t").Insert(5);
        compiled.Step(); // would throw DivideByZeroException if `dead` were compiled + stepped

        Assert.Equal(1, compiled.Outputs["live"].CurrentView.WeightOf(Row(5)).Value);
    }

    [Fact]
    public void IndirectlyReferencedView_IsKept()
    {
        // `mid` is not an output, but the output `outv` references it, so it must be compiled;
        // pruning it would leave `outv` unable to resolve its scan (CompileProgram would throw).
        var program = new[]
        {
            "CREATE TABLE t (id INT NOT NULL, amt INT NOT NULL)",
            "CREATE VIEW mid AS SELECT id, amt FROM t WHERE amt > 0",
            "CREATE VIEW outv AS SELECT SUM(amt) AS total FROM mid",
        };
        var outputs = new HashSet<string>(StringComparer.Ordinal) { "outv" };

        var resolved = SqlProgram.Resolve(program, outputs);
        var compiled = PlanToCircuit.CompileProgram(resolved.Tables, resolved.Views);

        compiled.Table("t").Insert(1, 10);
        compiled.Table("t").Insert(2, 5);
        compiled.Table("t").Insert(3, 0); // filtered out by mid
        compiled.Step();

        Assert.Equal(1, compiled.Outputs["outv"].CurrentView.WeightOf(Row(15L)).Value);
    }
}
