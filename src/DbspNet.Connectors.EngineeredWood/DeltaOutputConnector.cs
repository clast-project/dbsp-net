// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using Apache.Arrow;
using Apache.Arrow.Types;
using DbspNet.Arrow;
using DbspNet.Connectors.Abstractions;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;
using ArrowSchema = Apache.Arrow.Schema;
using SqlSchema = DbspNet.Sql.Plan.Schema;

namespace DbspNet.Connectors.EngineeredWood;

/// <summary>
/// A <see cref="IOutputConnector"/> writing to a Delta Lake table via engineered-wood.
/// <see cref="OutputMode.Truncate"/> replaces the whole table with the current view each
/// tick (<c>DeltaWriteMode.Overwrite</c> — an atomic remove-all + add, naturally
/// idempotent; the ivm-bench full-state path). <see cref="OutputMode.Changelog"/> appends
/// each tick's delta rows tagged with <c>__op</c> (i/d) and <c>__ts</c> (the tick).
/// </summary>
/// <remarks>
/// Truncate writes each distinct view row once (set semantics). Bag multiplicity
/// (<c>weights &gt; 1</c>, e.g. under <c>UNION ALL</c>) and changelog exactly-once via
/// Delta's <c>txn</c> app-id are follow-ons; ivm-bench views are sets.
/// </remarks>
public sealed class DeltaOutputConnector : IOutputConnector
{
    private readonly ITableFileSystem _fs;
    private DeltaTable? _table;
    private ArrowSchema? _changelogArrow;

    public DeltaOutputConnector(string viewName, ITableFileSystem fs, OutputMode mode = OutputMode.Truncate)
    {
        ViewName = viewName ?? throw new ArgumentNullException(nameof(viewName));
        _fs = fs ?? throw new ArgumentNullException(nameof(fs));
        Mode = mode;
    }

    public DeltaOutputConnector(string viewName, string tableDirectory, OutputMode mode = OutputMode.Truncate)
        : this(viewName, new LocalTableFileSystem(tableDirectory), mode)
    {
    }

    public string ViewName { get; }

    public OutputMode Mode { get; }

    public async ValueTask BindSchemaAsync(SqlSchema viewSchema, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(viewSchema);
        var dataArrow = ArrowSchemaBridge.ToArrow(viewSchema);

        ArrowSchema tableArrow;
        if (Mode == OutputMode.Changelog)
        {
            var b = new ArrowSchema.Builder();
            foreach (var f in dataArrow.FieldsList)
            {
                b.Field(f);
            }

            b.Field(new Field("__op", StringType.Default, false));
            b.Field(new Field("__ts", Int64Type.Default, false));
            _changelogArrow = b.Build();
            tableArrow = _changelogArrow;
        }
        else
        {
            tableArrow = dataArrow;
        }

        _table = await DeltaTable.OpenOrCreateAsync(_fs, tableArrow, cancellationToken: cancellationToken)
            .ConfigureAwait(false);
    }

    public async ValueTask WriteViewAsync(RecordBatch view, long[] weights, long tick, CancellationToken cancellationToken)
    {
        if (Mode != OutputMode.Truncate)
        {
            throw new InvalidOperationException("WriteViewAsync is for Truncate mode.");
        }

        var table = Table();

        // Refresh so Overwrite computes remove-files against the latest committed
        // snapshot (its own prior write), then atomically replace the contents.
        await table.RefreshAsync(cancellationToken).ConfigureAwait(false);
        await table.WriteAsync(new[] { view }, DeltaWriteMode.Overwrite, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask WriteDeltaAsync(RecordBatch delta, long[] weights, long tick, CancellationToken cancellationToken)
    {
        if (Mode != OutputMode.Changelog)
        {
            throw new InvalidOperationException("WriteDeltaAsync is for Changelog mode.");
        }

        if (delta.Length == 0)
        {
            return; // nothing to append this tick
        }

        var table = Table();
        var n = delta.ColumnCount;
        var arrays = new IArrowArray[n + 2];
        for (var i = 0; i < n; i++)
        {
            arrays[i] = delta.Column(i);
        }

        var op = new StringArray.Builder();
        var ts = new Int64Array.Builder();
        for (var i = 0; i < delta.Length; i++)
        {
            op.Append(weights[i] >= 0 ? "i" : "d");
            ts.Append(tick);
        }

        arrays[n] = op.Build();
        arrays[n + 1] = ts.Build();

        var augmented = new RecordBatch(_changelogArrow!, arrays, delta.Length);
        await table.WriteAsync(new[] { augmented }, DeltaWriteMode.Append, cancellationToken).ConfigureAwait(false);
    }

    private DeltaTable Table() =>
        _table ?? throw new InvalidOperationException(
            $"DeltaOutputConnector '{ViewName}' not bound — call BindSchemaAsync first (the runner does this).");

    public ValueTask DisposeAsync()
    {
        _table?.Dispose();
        return ValueTask.CompletedTask;
    }
}
