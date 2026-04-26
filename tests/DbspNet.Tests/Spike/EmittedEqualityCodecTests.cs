using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Tests.Spike;

/// <summary>
/// Phase 1 spike (b) — smoke tests for <see cref="EmittedEqualityCodec"/>.
/// Validates the emitted-subclass mechanics: emitted instances behave as
/// <see cref="StructuralRow"/> for indexer / Count / equality, and cross-
/// type comparisons against base <see cref="StructuralRow"/> still produce
/// correct results.
/// </summary>
public class EmittedEqualityCodecTests
{
    private static Schema TwoIntSchema { get; } = new Schema(new[]
    {
        new SchemaColumn("a", new SqlIntegerType(false)),
        new SchemaColumn("b", new SqlIntegerType(false)),
    });

    private static Schema IntStringSchema { get; } = new Schema(new[]
    {
        new SchemaColumn("id", new SqlIntegerType(false)),
        new SchemaColumn("name", new SqlVarcharType(null, false)),
    });

    [Fact]
    public void EmittedRow_BehavesAsStructuralRow()
    {
        var codec = EmittedEqualityCodec.Instance;
        var row = codec.BuildRow(TwoIntSchema, new object?[] { 7, 42 });

        // It is a StructuralRow (subclass), not the base class itself.
        Assert.IsAssignableFrom<StructuralRow>(row);
        Assert.NotEqual(typeof(StructuralRow), row.GetType());

        Assert.Equal(2, row.Count);
        Assert.Equal(7, row[0]);
        Assert.Equal(42, row[1]);
    }

    [Fact]
    public void EmittedRow_EqualsAndHashesByValue()
    {
        var codec = EmittedEqualityCodec.Instance;
        var r1 = codec.BuildRow(TwoIntSchema, new object?[] { 7, 42 });
        var r2 = codec.BuildRow(TwoIntSchema, new object?[] { 7, 42 });
        var r3 = codec.BuildRow(TwoIntSchema, new object?[] { 7, 43 });

        Assert.True(r1.Equals(r2));
        Assert.Equal(r1.GetHashCode(), r2.GetHashCode());
        Assert.False(r1.Equals(r3));
    }

    [Fact]
    public void EmittedRow_CrossTypeEqualityWithStructuralRow()
    {
        // Critical invariant: an emitted row and a base StructuralRow with
        // the same logical values must compare equal and hash to the same
        // bucket. Otherwise a typed key probing a dictionary that holds
        // untyped values (or vice versa) silently misses.
        var emitted = EmittedEqualityCodec.Instance.BuildRow(TwoIntSchema, new object?[] { 7, 42 });
        var structural = StructuralRowCodec.Instance.BuildRow(TwoIntSchema, new object?[] { 7, 42 });

        Assert.Equal(emitted.GetHashCode(), structural.GetHashCode());
        Assert.True(emitted.Equals(structural));
        Assert.True(structural.Equals(emitted));
    }

    [Fact]
    public void EmittedRow_DifferentSchemas_GetDifferentTypes()
    {
        var codec = EmittedEqualityCodec.Instance;
        var twoInt = codec.BuildRow(TwoIntSchema, new object?[] { 1, 2 });
        var intString = codec.BuildRow(IntStringSchema, new object?[] { 1, "two" });

        Assert.NotEqual(twoInt.GetType(), intString.GetType());
        // Different schemas obviously hash differently — sanity check.
        Assert.NotEqual(twoInt.GetHashCode(), intString.GetHashCode());
    }

    [Fact]
    public void EmittedRow_StringField()
    {
        var codec = EmittedEqualityCodec.Instance;
        var r1 = codec.BuildRow(IntStringSchema, new object?[] { 5, "hello" });
        var r2 = codec.BuildRow(IntStringSchema, new object?[] { 5, "hello" });
        var r3 = codec.BuildRow(IntStringSchema, new object?[] { 5, "world" });

        Assert.True(r1.Equals(r2));
        Assert.False(r1.Equals(r3));
        Assert.Equal("hello", r1[1]);
    }

    [Fact]
    public void EmittedRow_DictionaryKeyDistinctness()
    {
        // Mirrors the spike-A invariant for the typed record-struct path.
        var codec = EmittedEqualityCodec.Instance;
        var dict = new Dictionary<StructuralRow, int>();
        for (var i = 0; i < 1000; i++)
        {
            var row = codec.BuildRow(TwoIntSchema, new object?[] { i % 10, i % 7 });
            dict[row] = dict.TryGetValue(row, out var c) ? c + 1 : 1;
        }

        Assert.Equal(70, dict.Count);
    }

    [Fact]
    public void EmittedRow_FallsBackToBase_WhenSchemaIsNull()
    {
        var row = EmittedEqualityCodec.Instance.BuildRow(null, new object?[] { 1, 2 });
        // Falls back to the base class — no subclass, type is exactly
        // StructuralRow.
        Assert.Equal(typeof(StructuralRow), row.GetType());
    }
}
