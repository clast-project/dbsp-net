// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Runtime.CompilerServices;
using DbspNet.Arrow;
using DbspNet.Connectors.Abstractions;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;
using ArrowSchema = Apache.Arrow.Schema;
using SqlSchema = DbspNet.Sql.Plan.Schema;

namespace DbspNet.Connectors.EngineeredWood;

/// <summary>
/// A <see cref="IInputConnector"/> over a Delta Lake table, backed by engineered-wood.
/// Follows the table's change-data-feed one version per tick (Feldera "always"
/// semantics): <see cref="NextAsync"/> reads exactly one Delta version's changes via
/// <c>ReadChangesAsync</c> and maps <c>_change_type</c> to signed weights (insert /
/// update-postimage → <c>+1</c>, delete / update-preimage → <c>-1</c>). Version 0 (table
/// creation + initial rows) arrives as inserts, so there is no separate bulk-snapshot
/// mode — the whole history replays as one tick per version. Offset = Delta version.
/// </summary>
/// <remarks>
/// engineered-wood's CDF reader infers inserts/deletes from Add/Remove-file actions even
/// on tables that never enabled the change-data-feed table property, so this works over
/// plain append/overwrite tables (the common ETL source shape). Replayable: recovery
/// re-reads from the committed version, so no input write-ahead log is needed.
/// </remarks>
public sealed class DeltaInputConnector : IInputConnector
{
    private readonly ITableFileSystem _fs;
    private readonly ISchemaMapper _mapper;

    private DeltaTable? _table;
    private SqlSchema? _schema;
    private ArrowSchema? _resolvedArrow;

    /// <summary>Open a Delta source over an engineered-wood filesystem rooted at the
    /// table directory.</summary>
    public DeltaInputConnector(string name, ITableFileSystem fs, ISchemaMapper? mapper = null)
    {
        Name = name ?? throw new ArgumentNullException(nameof(name));
        _fs = fs ?? throw new ArgumentNullException(nameof(fs));
        _mapper = mapper ?? ArrowSchemaMapper.Instance;
    }

    /// <summary>Open a Delta source at a local directory path.</summary>
    public DeltaInputConnector(string name, string tableDirectory, ISchemaMapper? mapper = null)
        : this(name, new LocalTableFileSystem(tableDirectory), mapper)
    {
    }

    public string Name { get; }

    public IConnectorOffset InitialOffset => LongOffset.Before;

    public IConnectorOffset ParseOffset(string serialized) => LongOffset.Parse(serialized);

    public async ValueTask<SqlSchema> ResolveSchemaAsync(SqlSchema? declared, CancellationToken cancellationToken)
    {
        _table = await DeltaTable.OpenAsync(_fs, cancellationToken: cancellationToken).ConfigureAwait(false);
        var sourceArrow = _table.ArrowSchema;

        if (declared is null)
        {
            _schema = _mapper.Infer(sourceArrow);
        }
        else
        {
            _ = _mapper.Bind(declared, sourceArrow); // validate; throws if incompatible
            _schema = declared;
        }

        _resolvedArrow = ArrowSchemaBridge.ToArrow(_schema);
        return _schema;
    }

    public async ValueTask<IConnectorOffset?> LatestOffsetAsync(CancellationToken cancellationToken)
    {
        var table = Table();
        await table.RefreshAsync(cancellationToken).ConfigureAwait(false);
        return new LongOffset(table.CurrentSnapshot.Version);
    }

    public ValueTask<InputBatch?> NextAsync(IConnectorOffset from, CancellationToken cancellationToken)
    {
        var table = Table();
        var next = ((LongOffset)from).Value + 1;
        var latest = table.CurrentSnapshot.Version;
        if (next > latest)
        {
            return ValueTask.FromResult<InputBatch?>(null);
        }

        return ValueTask.FromResult<InputBatch?>(
            new InputBatch(StreamVersion(table, next, cancellationToken), new LongOffset(next), Completed: next == latest));
    }

    // Stream one version's changes lazily — one Arrow batch materialised at a time.
    private async IAsyncEnumerable<VersionBatch> StreamVersion(
        DeltaTable table, long version, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (var raw in table.ReadChangesAsync(version, version, cancellationToken).ConfigureAwait(false))
        {
            yield return ArrowProjection.Project(raw, _schema!, _resolvedArrow!, Name);
        }
    }

    private DeltaTable Table() =>
        _table ?? throw new InvalidOperationException(
            $"DeltaInputConnector '{Name}' not initialised — call ResolveSchemaAsync first (the runner does this).");

    public ValueTask DisposeAsync()
    {
        _table?.Dispose();
        return ValueTask.CompletedTask;
    }
}
