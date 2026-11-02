// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful.Blocks;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Persistence.Blocks;

/// <summary>
/// Where every live copy of every row an output sink has written sits, as one <c>long</c> address
/// per copy (the sink packs it, e.g. file id and position), so a retraction can be deleted where it
/// sits (docs/design-flat-trace-state.md §3.9).
/// </summary>
/// <remarks>
/// <para>Two representations. <b>Managed</b>: a dictionary keyed by the row object, which then stays
/// alive as long as a copy is live. <b>Blocked</b> (when the view itself is held as blocks,
/// <c>DBSPNET_TRACE_BLOCKS=view</c> or <c>all</c>, and every column has a block encoding): a
/// <see cref="BlockedIndexedTrace"/> keyed by the row with the address as its value, each copy an
/// entry of weight +1 and its removal the same entry at −1, so the index holds no reference to a
/// row. A blocked index beside a managed view would only add bytes, hence the coupling.</para>
/// <para>Changes are made in batches: <see cref="Take"/> and <see cref="Add"/>, then
/// <see cref="Commit"/>. Between commits a row may be taken from once, and is never both taken from
/// and added to (the runner's net change is consolidated); <see cref="Take"/> sees the state of the
/// last commit.</para>
/// </remarks>
public abstract class OutputRowIndex
{
    private OutputRowIndex()
    {
    }

    /// <summary>Live addresses (copies) held.</summary>
    public long Count { get; private protected set; }

    /// <summary>Whether this index is held as blocks.</summary>
    public abstract bool IsBlocked { get; }

    /// <summary>The index for a view of <paramref name="viewSchema"/>: blocked when stored views are
    /// blocks and every column has a block encoding, otherwise managed.</summary>
    public static OutputRowIndex Create(Schema viewSchema)
    {
        ArgumentNullException.ThrowIfNull(viewSchema);
        return TraceBlocksMode.View && BlockSchema.TryCreate(viewSchema, AddressSchema) is { } format
            ? new Blocked(format)
            : new Managed();
    }

    /// <summary>A managed index (tests).</summary>
    internal static OutputRowIndex CreateManaged() => new Managed();

    /// <summary>A blocked index for <paramref name="viewSchema"/> (tests).</summary>
    internal static OutputRowIndex CreateBlocked(Schema viewSchema, int freezeEntries, int blockEntries) =>
        new Blocked(BlockSchema.TryCreate(viewSchema, AddressSchema)
            ?? throw new ArgumentException("The schema has a column without a block encoding.", nameof(viewSchema)),
            freezeEntries, blockEntries);

    private static readonly Schema AddressSchema = new([new SchemaColumn("__address", new SqlBigintType(false))]);

    /// <summary>Remove <paramref name="count"/> copies of <paramref name="row"/>, appending their
    /// addresses to <paramref name="taken"/>; throws if fewer are live.</summary>
    public abstract void Take(object row, long count, List<long> taken);

    /// <summary>Record a new copy of <paramref name="row"/> at <paramref name="address"/>.</summary>
    public abstract void Add(object row, long address);

    /// <summary>End a batch of changes.</summary>
    public abstract void Commit();

    /// <summary>Forget everything (the sink was truncated).</summary>
    public abstract void Clear();

    /// <summary>The live addresses of <paramref name="row"/> as of the last commit (tests).</summary>
    public abstract IReadOnlyCollection<long> AddressesOf(object row);

    private static InvalidOperationException TooFew(long count, long live) =>
        new($"asked to delete {count} copies of a row it holds {live} of.");

    private sealed class Managed : OutputRowIndex
    {
        private readonly Dictionary<object, List<long>> _addresses = new();

        public override bool IsBlocked => false;

        public override void Take(object row, long count, List<long> taken)
        {
            if (!_addresses.TryGetValue(row, out var live) || live.Count < count)
            {
                throw TooFew(count, live?.Count ?? 0);
            }

            for (long i = 0; i < count; i++)
            {
                taken.Add(live[^1]);
                live.RemoveAt(live.Count - 1);
            }

            if (live.Count == 0)
            {
                _addresses.Remove(row);
            }

            Count -= count;
        }

        public override void Add(object row, long address)
        {
            if (!_addresses.TryGetValue(row, out var live))
            {
                _addresses[row] = live = new List<long>(1);
            }

            live.Add(address);
            Count++;
        }

        public override void Commit()
        {
        }

        public override void Clear()
        {
            _addresses.Clear();
            Count = 0;
        }

        public override IReadOnlyCollection<long> AddressesOf(object row) =>
            _addresses.TryGetValue(row, out var live) ? [.. live] : [];
    }

    private sealed class Blocked : OutputRowIndex
    {
        private readonly BlockSchema _format;
        private readonly int _freezeEntries;
        private readonly int _blockEntries;
        private BlockedIndexedTrace _trace;

        // The batch's entries, flat: integrated as one run at commit (sorted and encoded once; §3.10).
        private List<StructuralRow> _keys = new();
        private List<long> _addresses = new();
        private List<long> _weights = new();

        // Rows taken from since the last commit: Take reads the committed state, so a second Take of
        // the same row in one batch would hand out the same addresses again.
        private readonly HashSet<StructuralRow> _taken = new();

        public Blocked(BlockSchema format, int? freezeEntries = null, int? blockEntries = null)
        {
            _format = format;
            _freezeEntries = freezeEntries ?? TraceBlocksMode.FreezeEntries;
            _blockEntries = blockEntries ?? TraceBlocksMode.BlockEntries;
            _trace = NewTrace();
        }

        public override bool IsBlocked => true;

        private BlockedIndexedTrace NewTrace() => new(_format, _freezeEntries, blockEntries: _blockEntries);

        private static StructuralRow Row(object row) =>
            row as StructuralRow ?? throw new ArgumentException(
                $"A blocked output row index keys on {nameof(StructuralRow)}s, not {row?.GetType().Name ?? "null"}.", nameof(row));

        public override void Take(object row, long count, List<long> taken)
        {
            var key = Row(row);
            if (!_taken.Add(key))
            {
                throw new InvalidOperationException("A row was taken from twice in one batch.");
            }

            long found = 0;
            if (count > 0)
            {
                foreach (var (value, weight) in _trace.GroupView(key).Decoded())
                {
                    if (weight.Value <= 0)
                    {
                        continue;
                    }

                    if (weight.Value != 1)
                    {
                        throw new InvalidOperationException($"Output address {value[0]} is live {weight.Value} times.");
                    }

                    if (found < count)
                    {
                        var address = (long)value[0]!;
                        taken.Add(address);
                        Stage(key, address, -1);
                    }

                    found++;
                }
            }

            if (found < count)
            {
                throw TooFew(count, found);
            }

            Count -= count;
        }

        public override void Add(object row, long address)
        {
            Stage(Row(row), address, +1);
            Count++;
        }

        private void Stage(StructuralRow key, long address, long weight)
        {
            _keys.Add(key);
            _addresses.Add(address);
            _weights.Add(weight);
        }

        public override void Commit()
        {
            var keys = _keys;
            var addresses = _addresses;
            var weights = _weights;
            _trace.IntegrateEntries(keys.Count, i => keys[i], i => new StructuralRow(addresses[i]), i => weights[i]);
            ResetBatch();
        }

        private void ResetBatch()
        {
            // A large batch's lists are dropped, not cleared, so their capacity is not kept.
            if (_keys.Capacity > 1 << 16)
            {
                _keys = new();
                _addresses = new();
                _weights = new();
            }
            else
            {
                _keys.Clear();
                _addresses.Clear();
                _weights.Clear();
            }

            _taken.Clear();
        }

        public override void Clear()
        {
            _trace = NewTrace();
            ResetBatch();
            Count = 0;
        }

        public override IReadOnlyCollection<long> AddressesOf(object row)
        {
            var result = new List<long>();
            foreach (var (value, weight) in _trace.GroupView(Row(row)).Decoded())
            {
                for (var i = 0L; i < weight.Value; i++)
                {
                    result.Add((long)value[0]!);
                }
            }

            return result;
        }
    }
}
