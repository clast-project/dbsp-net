// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
// Decoded rows that are retained together share storage (docs/design-durable-state.md, "Giant
// groups decoded without sharing"): a giant group's and a cold read's strings come from one arena
// per column, and with DBSPNET_DECODE_INTERN=1 equal cells share one box. Required:
//   1. sharing never changes a value: 0.0 / -0.0, NaN payloads and decimal scale survive;
//   2. a restored giant group with strings and signed-zero doubles joins exactly like its twin,
//      with sharing on and off.
using System.Globalization;
using DbspNet.Persistence;
using DbspNet.Persistence.IO;
using DbspNet.Persistence.Runs;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Tests.Persistence;

[Collection(nameof(CellInterner))] // toggles a process-wide switch
public class RestoreDecodeSharingTests
{
    [Fact]
    public void Interning_SharesEqualCells_AndNeverMergesDistinctRepresentations()
    {
        var was = (CellInterner.Enabled, CellInterner.MinRows);
        try
        {
            CellInterner.Enabled = true;
            CellInterner.MinRows = 2;
            var otherNaN = BitConverter.Int64BitsToDouble(BitConverter.DoubleToInt64Bits(double.NaN) ^ 1);
            object?[] column =
            [
                0.0, -0.0, 0.0, -0.0, double.NaN, otherNaN, double.NaN, 1.0m, 1.00m, 1.0m,
                Utf8String.Of("x"), Utf8String.Of("x"), 5L, 5L, 5, null, null,
            ];
            var before = column.ToArray();
            CellInterner.Intern(column);

            // Every cell keeps its exact representation.
            for (var i = 0; i < column.Length; i++)
            {
                Assert.Equal(Repr(before[i]), Repr(column[i]));
            }

            Assert.Same(column[0], column[2]); // 0.0
            Assert.Same(column[1], column[3]); // -0.0
            Assert.NotSame(column[0], column[1]);
            Assert.Same(column[4], column[6]); // the same NaN
            Assert.NotSame(column[4], column[5]); // another NaN payload
            Assert.NotSame(column[7], column[9]); // decimals are never shared
            Assert.Same(column[10], column[11]);
            Assert.Same(column[12], column[13]);
            Assert.NotSame(column[13], column[14]); // long 5 is not int 5
        }
        finally
        {
            (CellInterner.Enabled, CellInterner.MinRows) = was;
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ARestoredGiantGroupWithStrings_JoinsExactlyLikeItsTwin(bool intern)
    {
        var was = (CellInterner.Enabled, CellInterner.MinRows);
        CellInterner.Enabled = intern;
        try
        {
            using var layout = StateLayout.Use(true);
            string[] sql =
            [
                "CREATE TABLE t (a INT NOT NULL, s VARCHAR NOT NULL, d DOUBLE NOT NULL)",
                "CREATE TABLE u (a INT NOT NULL, x VARCHAR NOT NULL)",
                "CREATE VIEW j AS SELECT t.a, t.s, t.d, u.x FROM t JOIN u ON t.a = u.a",
            ];
            var views = new HashSet<string>(["j"], StringComparer.Ordinal);
            void Load(CompiledProgram p)
            {
                for (var i = 0; i < RunColdGroups.GiantGroupEntries + 500; i++)
                {
                    // Key 0 is the giant group: repeated and distinct strings, signed zeros.
                    p.Table("t").Insert(
                        0,
                        i % 3 == 0 ? "same" : "s" + i.ToString(CultureInfo.InvariantCulture),
                        i % 2 == 0 ? 0.0 : i % 5 == 0 ? -0.0 : i * 0.5);
                }

                for (var a = 1; a < 3000; a++)
                {
                    p.Table("t").Insert(a, "r" + a.ToString(CultureInfo.InvariantCulture), -0.0);
                    p.Table("u").Insert(a, "k");
                }

                p.Step();
            }

            var fs = new InMemoryTableFileSystem();
            var producer = SqlProgram.Compile(sql, views, ArrowSqlSnapshotCodecs.Instance);
            var twin = SqlProgram.Compile(sql, views, ArrowSqlSnapshotCodecs.Instance);
            Load(producer);
            Load(twin);
            await Snapshot.WriteAsync(producer.Circuit, fs);

            var restored = SqlProgram.Compile(sql, views, ArrowSqlSnapshotCodecs.Instance);
            await Snapshot.ReadAsync(restored.Circuit, fs);
            foreach (var p in new[] { restored, twin })
            {
                p.Table("u").Insert(0, "giant"); // reads the giant group
                p.Table("u").Insert(7, "small"); // a cold range read
                p.Table("t").Insert(5, "five", -0.0);
                p.Step();
            }

            static string View(CompiledProgram p) => string.Join(
                ";",
                p.Outputs["j"].CurrentView.Select(e => string.Join(",", e.Key.Select(Repr)) + "#" + e.Value.Value)
                    .Order(StringComparer.Ordinal));
            var expected = View(twin);
            Assert.Contains(Repr(-0.0), expected, StringComparison.Ordinal);
            Assert.Contains("Utf8String:same", expected, StringComparison.Ordinal);
            Assert.Equal(expected, View(restored));
        }
        finally
        {
            (CellInterner.Enabled, CellInterner.MinRows) = was;
        }
    }

    // A cell's exact representation: type, bits for floating point, scale for decimals.
    private static string Repr(object? v) => v switch
    {
        null => "null",
        double d => "d" + BitConverter.DoubleToInt64Bits(d).ToString(CultureInfo.InvariantCulture),
        float f => "f" + BitConverter.SingleToInt32Bits(f).ToString(CultureInfo.InvariantCulture),
        decimal m => "m" + m.ToString(CultureInfo.InvariantCulture),
        _ => v.GetType().Name + ":" + Convert.ToString(v, CultureInfo.InvariantCulture),
    };
}
