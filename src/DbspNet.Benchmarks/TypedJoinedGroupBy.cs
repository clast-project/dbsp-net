// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Linear;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Core.Operators.Stateful.Aggregators;

namespace DbspNet.Benchmarks;

/// <summary>
/// Hand-wired typed-row version of the Joined GROUP BY benchmark query:
/// <c>SELECT c.region, SUM(o.amount) FROM orders o JOIN customers c ON
/// o.cust_id = c.id GROUP BY c.region</c>. Bypasses the SQL compiler and
/// <see cref="DbspNet.Core.Collections.StructuralRow"/> entirely — rows are
/// <c>readonly record struct</c>s carried through the generic Core operators.
/// The perf delta against the SQL-compiled baseline tells us the ceiling
/// multiplier a fully typed Phase 1 would deliver.
/// </summary>
internal static class TypedJoinedGroupBy
{
    public readonly record struct CustomerRow(int Id, string Region);

    public readonly record struct OrderRow(int CustId, int Amount);

    public readonly record struct JoinedRow(string Region, int Amount);

    /// <summary>
    /// Incremental SUM over <see cref="JoinedRow.Amount"/>. Mirrors the null-
    /// and retraction-aware semantics of <c>SqlSumAggregator</c>: tracks the
    /// running sum plus the count of distinct non-null rows currently present.
    /// </summary>
    private sealed class SumAmountAggregator : IAggregator<JoinedRow, long>
    {
        private sealed class SumState
        {
            public long Sum;
            public long DistinctRows;
        }

        public Optional<long> Compute(ZSet<JoinedRow, Z64> multiset)
        {
            if (multiset.IsEmpty)
            {
                return Optional<long>.None;
            }

            long sum = 0;
            var any = false;
            foreach (var (row, w) in multiset)
            {
                sum += row.Amount * w.Value;
                any = true;
            }

            return any ? Optional<long>.Some(sum) : Optional<long>.None;
        }

        public Optional<long> Update(
            ref object? state,
            Optional<long> oldValue,
            ZSet<JoinedRow, Z64> delta,
            ZSet<JoinedRow, Z64> after)
        {
            var s = state as SumState ?? new SumState();
            foreach (var (row, w) in delta)
            {
                var afterW = after.WeightOf(row);
                var beforeW = afterW.Value - w.Value;
                s.Sum += row.Amount * w.Value;
                if (beforeW == 0 && afterW.Value != 0)
                {
                    s.DistinctRows++;
                }
                else if (beforeW != 0 && afterW.Value == 0)
                {
                    s.DistinctRows--;
                }
            }

            state = s;
            return s.DistinctRows > 0 ? Optional<long>.Some(s.Sum) : Optional<long>.None;
        }
    }

    public sealed class Compiled
    {
        public required RootCircuit Circuit { get; init; }

        public required InputHandle<ZSet<CustomerRow, Z64>> Customers { get; init; }

        public required InputHandle<ZSet<OrderRow, Z64>> Orders { get; init; }

        public required OutputHandle<ZSet<(string Region, long Sum), Z64>> Output { get; init; }
    }

    public static Compiled Build()
    {
        InputHandle<ZSet<CustomerRow, Z64>>? custHandle = null;
        InputHandle<ZSet<OrderRow, Z64>>? orderHandle = null;
        OutputHandle<ZSet<(string, long), Z64>>? outHandle = null;

        var circuit = RootCircuit.Build(b =>
        {
            var (ch, cs) = b.ZSetInput<CustomerRow, Z64>();
            var (oh, os) = b.ZSetInput<OrderRow, Z64>();
            custHandle = ch;
            orderHandle = oh;

            var custByKey = b.GroupProject<int, CustomerRow, CustomerRow, Z64>(cs, c => c.Id, c => c);
            var orderByKey = b.GroupProject<int, OrderRow, OrderRow, Z64>(os, o => o.CustId, o => o);

            var joined = b.IncrementalInnerJoin(
                orderByKey,
                custByKey,
                (_, o, c) => new JoinedRow(c.Region, o.Amount));

            var joinedByRegion = b.GroupProject<string, JoinedRow, JoinedRow, Z64>(
                joined, j => j.Region, j => j);

            var aggregated = b.IncrementalAggregate(joinedByRegion, new SumAmountAggregator());

            outHandle = b.Output(aggregated);
        });

        return new Compiled
        {
            Circuit = circuit,
            Customers = custHandle!,
            Orders = orderHandle!,
            Output = outHandle!,
        };
    }

    public static ZSet<CustomerRow, Z64> BuildCustomersDelta(IReadOnlyList<(int Id, string Region)> customers)
    {
        var b = new ZSetBuilder<CustomerRow, Z64>();
        foreach (var c in customers)
        {
            b.Add(new CustomerRow(c.Id, c.Region), new Z64(1));
        }

        return b.Build();
    }

    public static ZSet<OrderRow, Z64> BuildOrdersDelta(IReadOnlyList<(int CustId, int Amount)> orders)
    {
        var b = new ZSetBuilder<OrderRow, Z64>();
        foreach (var o in orders)
        {
            b.Add(new OrderRow(o.CustId, o.Amount), new Z64(1));
        }

        return b.Build();
    }
}
