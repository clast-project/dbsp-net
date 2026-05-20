// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using DbspNet.Arrow;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

namespace DbspNet.Tests.Arrow;

public class ArrowIpcTests
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
    public void RoundTripSingleBatch_ThroughMemoryStream()
    {
        // Producer: build a Z-set and serialize the delta.
        var producer = Compile(
            ["CREATE TABLE t (id INT NOT NULL, name VARCHAR NOT NULL)"],
            "SELECT id, name FROM t");
        producer.Table("t").Insert(1, "alice");
        producer.Table("t").Insert(2, "bob");
        producer.Step();

        using var stream = new MemoryStream();
        producer.WriteArrowStream(stream, leaveOpen: true);

        // Consumer: read the stream back into a fresh table.
        stream.Position = 0;
        var consumer = Compile(
            ["CREATE TABLE t (id INT NOT NULL, name VARCHAR NOT NULL)"],
            "SELECT id, name FROM t");
        var rows = consumer.Table("t").ReadArrowStream(stream);
        consumer.Step();

        Assert.Equal(2, rows);
        Assert.Equal(2, consumer.Current.Count);
        Assert.Equal(1, consumer.WeightOf(1, "alice").Value);
        Assert.Equal(1, consumer.WeightOf(2, "bob").Value);
    }

    [Fact]
    public void RoundTrip_NegativeWeights_AreApplied()
    {
        // Producer reaches a state where the next delta is a retraction.
        var producer = Compile(
            ["CREATE TABLE t (id INT NOT NULL)"],
            "SELECT id FROM t");
        producer.Table("t").Insert(1);
        producer.Step();           // delta: {1: +1}
        producer.Table("t").Delete(1);
        producer.Step();           // delta: {1: −1}

        // Serialize the retraction delta.
        using var stream = new MemoryStream();
        producer.WriteArrowStream(stream, leaveOpen: true);
        stream.Position = 0;

        // Consumer ingests the streamed batch into a fresh table whose
        // current state matches the producer's pre-delete state.
        var consumer = Compile(
            ["CREATE TABLE t (id INT NOT NULL)"],
            "SELECT id FROM t");
        consumer.Table("t").Insert(1);
        consumer.Step();           // delta: {1: +1}
        consumer.Table("t").ReadArrowStream(stream);
        consumer.Step();           // delta should be {1: −1}

        var consumerDelta = consumer.ToArrowDelta();
        Assert.Equal(1, consumerDelta.Rows.Length);
        Assert.Equal(-1, consumerDelta.Weights[0]);
        Assert.Equal(1, ((Int32Array)consumerDelta.Rows.Column(0)).Values[0]);
    }

    [Fact]
    public void MultiBatchSession_StreamsPerStep()
    {
        // Produce three ticks' worth of deltas through one ArrowDeltaWriter.
        var producer = Compile(
            ["CREATE TABLE t (id INT NOT NULL)"],
            "SELECT id FROM t");

        using var stream = new MemoryStream();
        using (var writer = producer.OpenArrowDeltaWriter(stream, leaveOpen: true))
        {
            producer.Table("t").Insert(1);
            producer.Step();
            writer.WriteDelta(producer.ToArrowDelta());

            producer.Table("t").Insert(2);
            producer.Step();
            writer.WriteDelta(producer.ToArrowDelta());

            producer.Table("t").Insert(3);
            producer.Step();
            writer.WriteDelta(producer.ToArrowDelta());
        }

        // Consume all three batches into a fresh table.
        stream.Position = 0;
        var consumer = Compile(
            ["CREATE TABLE t (id INT NOT NULL)"],
            "SELECT id FROM t");
        var totalRows = consumer.Table("t").ReadArrowStream(stream);
        consumer.Step();

        Assert.Equal(3, totalRows);
        Assert.Equal(3, consumer.Current.Count);
        Assert.Equal(1, consumer.WeightOf(1).Value);
        Assert.Equal(1, consumer.WeightOf(2).Value);
        Assert.Equal(1, consumer.WeightOf(3).Value);
    }

    [Fact]
    public void StreamBatches_LazilyYieldsPerBatchRowCount()
    {
        var producer = Compile(
            ["CREATE TABLE t (id INT NOT NULL)"],
            "SELECT id FROM t");

        using var stream = new MemoryStream();
        using (var writer = producer.OpenArrowDeltaWriter(stream, leaveOpen: true))
        {
            producer.Table("t").Insert(10);
            producer.Table("t").Insert(20);
            producer.Step();
            writer.WriteDelta(producer.ToArrowDelta());

            producer.Table("t").Insert(30);
            producer.Step();
            writer.WriteDelta(producer.ToArrowDelta());
        }

        stream.Position = 0;
        var consumer = Compile(
            ["CREATE TABLE t (id INT NOT NULL)"],
            "SELECT id FROM t");

        // Consume each batch as its own tick; capture the per-tick delta
        // shape to verify lazy iteration interleaves with Step() correctly.
        var counts = new List<int>();
        var perTickRows = new List<int>();
        foreach (var rows in consumer.Table("t").ReadArrowStreamBatches(stream))
        {
            counts.Add(rows);
            consumer.Step();
            perTickRows.Add(consumer.Current.Count);
        }

        Assert.Equal(new[] { 2, 1 }, counts);
        Assert.Equal(new[] { 2, 1 }, perTickRows);
    }

    [Fact]
    public void IngestPlainArrowStream_NoWeightColumn_AllRowsAtPlusOne()
    {
        // Build an Arrow stream that does NOT have a __weight column —
        // simulates ingesting from a producer that doesn't know about
        // Z-set weights. The reader should default each row to weight +1.
        var consumer = Compile(
            ["CREATE TABLE t (id INT NOT NULL, name VARCHAR NOT NULL)"],
            "SELECT id, name FROM t");

        var arrowSchema = ArrowSchemaBridge.ToArrow(consumer.Table("t").Schema);
        var idBuilder = new Int32Array.Builder();
        idBuilder.Append(7).Append(8);
        var nameBuilder = new StringArray.Builder();
        nameBuilder.Append("seven").Append("eight");
        var batch = new RecordBatch(arrowSchema, new IArrowArray[]
        {
            idBuilder.Build(),
            nameBuilder.Build(),
        }, length: 2);

        using var stream = new MemoryStream();
        using (var writer = new ArrowStreamWriter(stream, arrowSchema, leaveOpen: true))
        {
            writer.WriteRecordBatch(batch);
            writer.WriteEnd();
        }

        stream.Position = 0;
        var rows = consumer.Table("t").ReadArrowStream(stream);
        consumer.Step();

        Assert.Equal(2, rows);
        Assert.Equal(1, consumer.WeightOf(7, "seven").Value);
        Assert.Equal(1, consumer.WeightOf(8, "eight").Value);
    }

    [Fact]
    public void RoundTrip_PreservesAllScalarTypes()
    {
        var producer = Compile(
            [
                "CREATE TABLE t (i INT NOT NULL, dec DECIMAL(10, 2) NOT NULL, " +
                "s VARCHAR NOT NULL, dt DATE NOT NULL)",
            ],
            "SELECT i, dec, s, dt FROM t");
        producer.Table("t").Insert(
            42, "12.34", "hello",
            DbspNet.Sql.TypeSystem.Date32.Parse("2026-05-04"));
        producer.Step();

        using var stream = new MemoryStream();
        producer.WriteArrowStream(stream, leaveOpen: true);
        stream.Position = 0;

        var consumer = Compile(
            [
                "CREATE TABLE t (i INT NOT NULL, dec DECIMAL(10, 2) NOT NULL, " +
                "s VARCHAR NOT NULL, dt DATE NOT NULL)",
            ],
            "SELECT i, dec, s, dt FROM t");
        consumer.Table("t").ReadArrowStream(stream);
        consumer.Step();

        Assert.Equal(1, consumer.Current.Count);
        Assert.Equal(1, consumer.WeightOf(
            42, "12.34", "hello",
            DbspNet.Sql.TypeSystem.Date32.Parse("2026-05-04")).Value);
    }
}
