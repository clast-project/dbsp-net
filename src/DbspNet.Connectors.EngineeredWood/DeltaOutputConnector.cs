// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using Apache.Arrow;
using Apache.Arrow.Types;
using DbspNet.Arrow;
using DbspNet.Connectors.Abstractions;
using EngineeredWood.DeltaLake.DeletionVectors;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using ArrowSchema = Apache.Arrow.Schema;
using SqlSchema = DbspNet.Sql.Plan.Schema;

namespace DbspNet.Connectors.EngineeredWood;

/// <summary>
/// A <see cref="IOutputConnector"/> writing to a Delta Lake table via engineered-wood.
/// <see cref="OutputMode.Truncate"/> replaces the whole table with the current view each
/// tick (<c>DeltaWriteMode.Overwrite</c> — an atomic remove-all + add, naturally
/// idempotent). <see cref="OutputMode.Changelog"/> appends each tick's delta rows tagged
/// with <c>__op</c> (i/d) and <c>__ts</c> (the tick). <see cref="OutputMode.Incremental"/>
/// keeps the table equal to the view but writes only each batch's net change, in one
/// commit: deletion vectors over the retracted rows plus a file of the inserted ones.
/// </summary>
/// <remarks>
/// Truncate honours multiplicity: a view row with weight <c>w</c> is written <c>w</c>
/// times (<see cref="ArrowExtensions.ToArrowView"/> expands it), so a bag view (e.g.
/// under <c>UNION ALL</c>) round-trips correctly. Changelog exactly-once via Delta's
/// <c>txn</c> app-id is a follow-on.
/// </remarks>
public sealed class DeltaOutputConnector : IOutputConnector
{
    private readonly ITableFileSystem _fs;
    private DeltaTable? _table;
    private ArrowSchema? _changelogArrow;

    // Incremental mode: where every live copy of every row sits (file, position), so a
    // retraction can be deleted by position with a deletion vector, as Feldera does.
    private readonly Dictionary<object, List<RowAddress>> _addresses = new();
    private bool _incrementalStarted;

    private readonly record struct RowAddress(string Path, long Position);

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

        // engineered-wood's ParquetWriteOptions.OmitPathInSchema defaults to true, which
        // drops the ColumnMetaData.path_in_schema field (required by the Parquet spec). The
        // thrift-generated readers in Apache Arrow / DuckDB / pyarrow enforce it and reject
        // such files with "TProtocolException: Invalid data". Force it on so DbspNet's output
        // is readable by standard tooling (the correctness-comparison harness, downstream
        // consumers).
        // EWPARQUET0002: OmitPathInSchema is marked [Experimental] in the released
        // package. Our use is deliberate and covered by the read-back tests — suppress
        // the experimental diagnostic for this one line rather than project-wide.
#pragma warning disable EWPARQUET0002
        var options = new DeltaTableOptions
        {
            ParquetWriteOptions = ParquetWriteOptions.Default with { OmitPathInSchema = false },
        };
#pragma warning restore EWPARQUET0002

        if (Mode == OutputMode.Incremental)
        {
            // Deleting by position needs deletion vectors, which only a create can turn on.
            var exists = await new TransactionLog(_fs).GetLatestVersionAsync(cancellationToken).ConfigureAwait(false) >= 0;
            if (!exists)
            {
                _table = await DeltaTable.CreateAsync(
                    _fs, tableArrow, options, enableDeletionVectors: true, cancellationToken: cancellationToken)
                    .ConfigureAwait(false);
                return;
            }

            _table = await DeltaTable.OpenAsync(_fs, options, cancellationToken).ConfigureAwait(false);
            if (!DeletionVectorConfig.IsEnabled(_table.CurrentSnapshot.Metadata.Configuration))
            {
                throw new InvalidOperationException(
                    $"DeltaOutputConnector '{ViewName}': the existing table at the output path does not have "
                    + "deletion vectors enabled, which Incremental mode needs. Remove it, or use Truncate mode.");
            }

            return;
        }

        _table = await DeltaTable.OpenOrCreateAsync(_fs, tableArrow, options, cancellationToken: cancellationToken)
            .ConfigureAwait(false);
    }

    public async ValueTask ApplyChangesAsync(
        RecordBatch inserts,
        IReadOnlyList<object> insertKeys,
        IReadOnlyList<(object Key, long Count)> deletes,
        long tick,
        CancellationToken cancellationToken)
    {
        if (Mode != OutputMode.Incremental)
        {
            throw new InvalidOperationException("ApplyChangesAsync is for Incremental mode.");
        }

        ArgumentNullException.ThrowIfNull(inserts);
        ArgumentNullException.ThrowIfNull(insertKeys);
        ArgumentNullException.ThrowIfNull(deletes);
        if (insertKeys.Count != inserts.Length)
        {
            throw new ArgumentException("insertKeys must have one key per inserted row.", nameof(insertKeys));
        }

        var first = !_incrementalStarted;
        if (!first && inserts.Length == 0 && deletes.Count == 0)
        {
            return; // nothing changed this batch
        }

        var table = Table();
        await table.RefreshAsync(cancellationToken).ConfigureAwait(false);
        await using var txn = table.StartTransaction();

        if (first)
        {
            // Truncate once, in the same commit as the first write (Feldera's truncate-on-start).
            // The positions of rows written by an earlier process are unknown, so they go whole.
            await txn.DeleteAsync(global::EngineeredWood.Expressions.Expressions.True, cancellationToken).ConfigureAwait(false);
            _addresses.Clear();
        }
        else if (deletes.Count > 0)
        {
            var positions = new Dictionary<string, IReadOnlyCollection<long>>(StringComparer.Ordinal);
            foreach (var (key, count) in deletes)
            {
                if (!_addresses.TryGetValue(key, out var live) || live.Count < count)
                {
                    throw new InvalidOperationException(
                        $"DeltaOutputConnector '{ViewName}': asked to delete {count} copies of a row it holds "
                        + $"{live?.Count ?? 0} of.");
                }

                for (long i = 0; i < count; i++)
                {
                    var at = live[^1];
                    live.RemoveAt(live.Count - 1);
                    if (!positions.TryGetValue(at.Path, out var list))
                    {
                        positions[at.Path] = list = new List<long>();
                    }

                    ((List<long>)list).Add(at.Position);
                }

                if (live.Count == 0)
                {
                    _addresses.Remove(key);
                }
            }

            await txn.StageRowDeletesAsync(RowSelection.ByPath(positions), RowDeleteMode.DeletionVector, cancellationToken)
                .ConfigureAwait(false);
        }

        if (inserts.Length > 0)
        {
            // The files are written uncommitted, in input order; row i of the input is at
            // its file's position (i - the rows in earlier files).
            var files = await table.WriteDataFilesAsync(new[] { inserts }, cancellationToken).ConfigureAwait(false);
            var row = 0;
            foreach (var file in files)
            {
                for (long position = 0; position < file.NumRecords; position++, row++)
                {
                    var key = insertKeys[row];
                    if (!_addresses.TryGetValue(key, out var live))
                    {
                        _addresses[key] = live = new List<RowAddress>(1);
                    }

                    live.Add(new RowAddress(file.RelativePath, position));
                }
            }

            if (row != inserts.Length)
            {
                throw new InvalidOperationException(
                    $"DeltaOutputConnector '{ViewName}': wrote {row} rows for {inserts.Length} inserted.");
            }

            txn.StageDataFiles(files);
        }

        await txn.CommitAsync(cancellationToken).ConfigureAwait(false);
        _incrementalStarted = true;
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
