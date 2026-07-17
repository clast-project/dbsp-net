// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using Clast.DatabaseDecimal.Values;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;
using Xunit;

namespace DbspNet.Tests.Sql;

/// <summary>
/// Reproduces the ivm-bench dim_account join shape with synthetic data where the answer is
/// known: <c>accounts JOIN dim_customer ON customer_id = customer_id AND ts BETWEEN eff AND end
/// JOIN dim_broker USING (broker_id)</c>. All keys match, so the result must be non-empty —
/// yet dim_account came back empty over correct real inputs.
/// </summary>
public sealed class DimAccountJoinShapeTests
{
    private static CompiledQuery Compile(string[] ddl, string query)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in ddl)
        {
            resolver.Resolve(Parser.ParseStatement(s));
        }

        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(query))).Query;
        return PlanToCircuit.Compile(plan);
    }

    [Fact]
    public void ResidualJoin_then_UsingJoin_matches()
    {
        var q = Compile(
            [
                "CREATE TABLE accounts (account_id BIGINT NOT NULL, customer_id BIGINT NOT NULL, broker_id BIGINT NOT NULL, ts BIGINT NOT NULL)",
                "CREATE TABLE dim_customer (customer_id BIGINT NOT NULL, eff BIGINT NOT NULL, endt BIGINT NOT NULL)",
                "CREATE TABLE dim_broker (broker_id BIGINT NOT NULL, bname VARCHAR NOT NULL)",
            ],
            "SELECT a.account_id, a.broker_id, b.bname " +
            "FROM accounts a " +
            "JOIN dim_customer c ON a.customer_id = c.customer_id AND a.ts BETWEEN c.eff AND c.endt " +
            "JOIN dim_broker b USING (broker_id)");

        // account 1 → customer 100 (ts 5 in [0,100]) → broker 10; account 2 → customer 100 → broker 20.
        q.Table("accounts").Insert(1L, 100L, 10L, 5L);
        q.Table("accounts").Insert(2L, 100L, 20L, 5L);
        q.Table("dim_customer").Insert(100L, 0L, 100L);
        q.Table("dim_broker").Insert(10L, "x");
        q.Table("dim_broker").Insert(20L, "y");
        q.Step();

        Assert.Equal(2, q.Current.Count);
    }

    [Fact]
    public void ResidualJoin_with_timestamp_and_9999_upper_bound_matches()
    {
        var q = Compile(
            [
                "CREATE TABLE accounts (account_id BIGINT NOT NULL, customer_id BIGINT NOT NULL, broker_id BIGINT NOT NULL, ts TIMESTAMP NOT NULL)",
                "CREATE TABLE dim_customer (customer_id BIGINT NOT NULL, eff TIMESTAMP NOT NULL, endt TIMESTAMP NOT NULL)",
                "CREATE TABLE dim_broker (broker_id BIGINT NOT NULL, bname VARCHAR NOT NULL)",
            ],
            "SELECT a.account_id, a.broker_id, b.bname " +
            "FROM accounts a " +
            "JOIN dim_customer c ON a.customer_id = c.customer_id AND a.ts BETWEEN c.eff AND c.endt " +
            "JOIN dim_broker b USING (broker_id)");

        var acctTs = Timestamp.Parse("2007-07-07 16:07:49");
        var custEff = Timestamp.Parse("2007-01-01 00:00:00");
        var custEnd = Timestamp.Parse("9999-12-31 23:59:59.999"); // the SCD "current" sentinel

        q.Table("accounts").Insert(1L, 100L, 10L, acctTs);
        q.Table("dim_customer").Insert(100L, custEff, custEnd);
        q.Table("dim_broker").Insert(10L, "x");
        q.Step();

        Assert.Equal(1, q.Current.Count);
    }

    [Fact]
    public void UsingJoin_bigint_vs_varchar_key_coerces_and_matches()
    {
        // The ivm-bench dim_account bug: accounts.broker_id is BIGINT (ca_b_id), dim_broker.broker_id
        // is VARCHAR (employee_id). Under numeric<->string coercion the values are equal, but the
        // join built its keys from the raw column types (BIGINT vs VARCHAR) and matched nothing.
        // Uses the structural program path (what the benchmark runs) with coercion enabled.
        var prog = SqlProgram.Compile(
            [
                "CREATE TABLE accounts (account_id BIGINT NOT NULL, broker_id BIGINT NOT NULL)",
                "CREATE TABLE dim_broker (broker_id VARCHAR NOT NULL, bname VARCHAR NOT NULL)",
                "CREATE VIEW joined AS SELECT a.account_id, b.bname FROM accounts a JOIN dim_broker b USING (broker_id)",
            ],
            new System.Collections.Generic.HashSet<string>(System.StringComparer.Ordinal) { "joined" },
            numericStringCoercion: true);

        prog.Table("accounts").Insert(1L, 10180L);
        prog.Table("accounts").Insert(2L, 12538L);
        prog.Table("dim_broker").Insert(Utf8String.Of("10180"), Utf8String.Of("x"));
        prog.Table("dim_broker").Insert(Utf8String.Of("99999"), Utf8String.Of("z"));
        prog.Step();

        // account 1 (broker 10180) matches dim_broker "10180"; account 2 (12538) has no match.
        Assert.Equal(1, prog.Outputs["joined"].CurrentView.Count);
    }
}
