// Scratch (gated: RUN_SHAPE_DIR=<snapshot store>): the block-size shape of every run the current
// snapshot names — how much state sits in giant single-group blocks (docs/design-durable-state.md §1f).
using System.Globalization;
using DbspNet.Persistence.IO.Local;
using DbspNet.Persistence.Runs;
using Xunit.Abstractions;

namespace DbspNet.Tests.Scratch;

public class RunShapeProbe(ITestOutputHelper output)
{
    [Fact]
    public async Task Measure()
    {
        if (Environment.GetEnvironmentVariable("RUN_SHAPE_DIR") is not { Length: > 0 } dir)
        {
            return;
        }

        var fs = new LocalTableFileSystem(dir);
        var runs = Directory.EnumerateFiles(Path.Combine(dir, "runs"), "*", SearchOption.AllDirectories).ToList();
        long entries = 0, blocks = 0;
        var bySize = new long[6]; // block entries: <=1024, <=4096, <=16K, <=64K, <=256K, more
        var entriesBySize = new long[6];
        long giantGroupEntries = 0, giantGroups = 0;
        foreach (var path in runs)
        {
            var rel = Path.GetRelativePath(dir, path);
            await using var run = await RunReader.OpenAsync(fs, rel, r => r.GetHashCode());
            foreach (var b in run.Blocks)
            {
                blocks++;
                entries += b.Entries;
                var i = b.Entries <= 1024 ? 0 : b.Entries <= 4096 ? 1 : b.Entries <= 16384 ? 2 : b.Entries <= 65536 ? 3 : b.Entries <= 262144 ? 4 : 5;
                bySize[i]++;
                entriesBySize[i] += b.Entries;
                if (b.Groups == 1 && b.Entries > 4096)
                {
                    giantGroups++;
                    giantGroupEntries += b.Entries;
                }
            }
        }

        output.WriteLine(string.Create(CultureInfo.InvariantCulture, $"SHAPE runs={runs.Count} blocks={blocks} entries={entries} giant_single_group_blocks={giantGroups} giant_entries={giantGroupEntries} ({100.0 * giantGroupEntries / entries:F1}%)"));
        output.WriteLine("SHAPE blocks by entries [<=1K, <=4K, <=16K, <=64K, <=256K, more] = " + string.Join(" ", bySize));
        output.WriteLine("SHAPE entries by block  [<=1K, <=4K, <=16K, <=64K, <=256K, more] = " + string.Join(" ", entriesBySize));
    }
}
