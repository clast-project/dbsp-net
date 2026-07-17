// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections.Generic;
using Apache.Arrow;
using Apache.Arrow.Types;
using DbspNet.Connectors.Abstractions;
using Xunit;
using ArrowSchema = Apache.Arrow.Schema;

namespace DbspNet.Tests.Connectors;

/// <summary>
/// <see cref="NestedArrowResolver"/> — resolving a lowered ROW leaf (a dotted declared
/// name) into a nested Arrow struct path and extracting it, with parent-struct null
/// propagation. Mirrors the TPC-DI CustomerMgmt shape (Customer → Account → CA_B_ID).
/// </summary>
public sealed class NestedArrowResolverTests
{
    // Customer( Account( CA_B_ID: bigint ) ), plus a flat top-level `id: bigint`.
    private static (ArrowSchema Schema, RecordBatch Batch) BuildNested()
    {
        // Leaf values; row 1 and row 2 hold stale values that must be nulled out by
        // ancestor-struct nulls (Account null at row 1, Customer null at row 2).
        var caBId = new Int64Array.Builder().Append(10).Append(99).Append(77).Build();

        var accountType = new StructType(new List<Field> { new("CA_B_ID", Int64Type.Default, nullable: true) });
        var accountValidity = new ArrowBuffer.BitmapBuilder().Append(true).Append(false).Append(true).Build();
        var account = new StructArray(accountType, length: 3, new IArrowArray[] { caBId }, accountValidity, nullCount: 1);

        var customerType = new StructType(new List<Field> { new("Account", accountType, nullable: true) });
        var customerValidity = new ArrowBuffer.BitmapBuilder().Append(true).Append(true).Append(false).Build();
        var customer = new StructArray(customerType, length: 3, new IArrowArray[] { account }, customerValidity, nullCount: 1);

        var id = new Int64Array.Builder().Append(1).Append(2).Append(3).Build();

        var schema = new ArrowSchema.Builder()
            .Field(new Field("Customer", customerType, nullable: true))
            .Field(new Field("id", Int64Type.Default, nullable: false))
            .Build();

        var batch = new RecordBatch(schema, new IArrowArray[] { customer, id }, length: 3);
        return (schema, batch);
    }

    [Fact]
    public void Resolves_nested_path_case_insensitively()
    {
        var (schema, _) = BuildNested();

        Assert.True(NestedArrowResolver.TryResolve(schema, "customer.account.ca_b_id", out var path, out var leaf));
        Assert.Equal(new[] { 0, 0, 0 }, path);
        Assert.NotNull(leaf);
        Assert.Equal("CA_B_ID", leaf!.Name);
    }

    [Fact]
    public void Resolves_flat_top_level_column()
    {
        var (schema, _) = BuildNested();

        Assert.True(NestedArrowResolver.TryResolve(schema, "id", out var path, out var leaf));
        Assert.Equal(new[] { 1 }, path);
        Assert.Equal("id", leaf!.Name);
    }

    [Fact]
    public void Unresolvable_name_returns_false()
    {
        var (schema, _) = BuildNested();

        Assert.False(NestedArrowResolver.TryResolve(schema, "customer.account.missing", out _, out _));
        Assert.False(NestedArrowResolver.TryResolve(schema, "nope", out _, out _));
    }

    [Fact]
    public void Extract_propagates_ancestor_struct_nulls()
    {
        var (schema, batch) = BuildNested();
        Assert.True(NestedArrowResolver.TryResolve(schema, "Customer.Account.CA_B_ID", out var path, out _));

        var col = Assert.IsType<Int64Array>(NestedArrowResolver.Extract(batch, path));

        Assert.Equal(3, col.Length);
        Assert.True(col.IsValid(0));
        Assert.Equal(10L, col.GetValue(0));
        Assert.False(col.IsValid(1)); // Account null → leaf null (not the stale 99)
        Assert.False(col.IsValid(2)); // Customer null → leaf null (not the stale 77)
        Assert.Equal(2, col.NullCount);
    }

    [Fact]
    public void Extract_leaf_with_own_nulls_under_partly_null_parent_keeps_valid_values()
    {
        // Mimics parquet nested nulls (the ivm-bench dim_account bug): Customer always present,
        // Account null on some rows, and the leaf carries its OWN null bitmap matching Account.
        // Valid account rows must retain their value — the bug returned all-null.
        const int n = 5;
        var caId = new Int64Array.Builder().Append(100).AppendNull().Append(300).AppendNull().Append(500).Build();
        var accountType = new StructType(new List<Field> { new("_CA_ID", Int64Type.Default, nullable: true) });
        var acctValidity = new ArrowBuffer.BitmapBuilder()
            .Append(true).Append(false).Append(true).Append(false).Append(true).Build();
        var account = new StructArray(accountType, n, new IArrowArray[] { caId }, acctValidity, nullCount: 2);

        var customerType = new StructType(new List<Field> { new("Account", accountType, nullable: true) });
        var customer = new StructArray(customerType, n, new IArrowArray[] { account }, ArrowBuffer.Empty, nullCount: 0);

        var schema = new ArrowSchema.Builder().Field(new Field("Customer", customerType, nullable: true)).Build();
        var batch = new RecordBatch(schema, new IArrowArray[] { customer }, n);

        Assert.True(NestedArrowResolver.TryResolve(schema, "Customer.Account._CA_ID", out var path, out _));
        var col = Assert.IsType<Int64Array>(NestedArrowResolver.Extract(batch, path));

        Assert.Equal(100L, col.GetValue(0));
        Assert.False(col.IsValid(1));
        Assert.Equal(300L, col.GetValue(2));
        Assert.False(col.IsValid(3));
        Assert.Equal(500L, col.GetValue(4));
    }

    [Fact]
    public void Extract_flat_column_is_unchanged()
    {
        var (schema, batch) = BuildNested();
        Assert.True(NestedArrowResolver.TryResolve(schema, "id", out var path, out _));

        var col = Assert.IsType<Int64Array>(NestedArrowResolver.Extract(batch, path));
        Assert.Equal(new long?[] { 1L, 2L, 3L }, new[] { col.GetValue(0), col.GetValue(1), col.GetValue(2) });
    }
}
