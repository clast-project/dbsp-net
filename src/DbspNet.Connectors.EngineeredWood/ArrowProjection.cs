// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using Apache.Arrow;
using DbspNet.Connectors.Abstractions;
using EngineeredWood.DeltaLake.ChangeDataFeed;
using ArrowSchema = Apache.Arrow.Schema;
using SqlSchema = DbspNet.Sql.Plan.Schema;

namespace DbspNet.Connectors.EngineeredWood;

/// <summary>
/// Projects a source Arrow batch (a Delta CDF batch or a plain snapshot/Parquet batch)
/// onto the resolved table schema — selecting the resolved columns by name and dropping
/// any extras (CDF metadata, unused source columns) — and derives signed per-row weights
/// from the <c>_change_type</c> column when present (insert / update-postimage → <c>+1</c>,
/// delete / update-preimage → <c>-1</c>); a batch without that column is all inserts.
/// </summary>
internal static class ArrowProjection
{
    public static VersionBatch Project(RecordBatch raw, SqlSchema schema, ArrowSchema resolvedArrow, string sourceName)
    {
        var byName = new Dictionary<string, int>(StringComparer.Ordinal);
        for (var i = 0; i < raw.Schema.FieldsList.Count; i++)
        {
            byName[raw.Schema.FieldsList[i].Name] = i;
        }

        var rowCount = raw.Length;
        var weights = new long[rowCount];
        if (byName.TryGetValue(CdfConfig.ChangeTypeColumn, out var changeIdx)
            && raw.Column(changeIdx) is StringArray changeCol)
        {
            for (var i = 0; i < rowCount; i++)
            {
                var kind = changeCol.GetString(i);
                weights[i] = kind is CdfConfig.Delete or CdfConfig.UpdatePreimage ? -1L : 1L;
            }
        }
        else
        {
            System.Array.Fill(weights, 1L);
        }

        var dataArrays = new IArrowArray[schema.Count];
        for (var d = 0; d < schema.Count; d++)
        {
            // Resolve each declared column to a top-level source field or a nested-struct
            // path (a lowered ROW leaf), then extract it — flattening the nested source and
            // propagating parent-struct nulls. Flat sources take the single-segment path.
            if (!NestedArrowResolver.TryResolve(raw.Schema, schema[d].Name, out var path, out _))
            {
                throw new InvalidOperationException(
                    $"source batch for '{sourceName}' has no column '{schema[d].Name}'");
            }

            dataArrays[d] = NestedArrowResolver.Extract(raw, path);
        }

        return new VersionBatch(new RecordBatch(resolvedArrow, dataArrays, rowCount), weights);
    }
}
