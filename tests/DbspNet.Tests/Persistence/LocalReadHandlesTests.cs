// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Persistence.IO.Local;
using Xunit;

namespace DbspNet.Tests.Persistence;

/// <summary>
/// Kept-open read handles (docs/design-bounded-memory.md §3.6): a file changed through the store is
/// read fresh, and a handle evicted while leased stays readable until its lease ends.
/// </summary>
[Collection(nameof(LocalReadHandlesTests))]
public class LocalReadHandlesTests
{
    private static byte Read(LocalTableFileSystem fs, string path)
    {
        using var lease = LocalReadHandles.Rent(fs.FullPath(path));
        var b = new byte[1];
        lease.File.ReadExactly(0, b);
        return b[0];
    }

    [Fact]
    public async Task ChangedFiles_AreReadFresh()
    {
        var root = Directory.CreateTempSubdirectory("handles").FullName;
        try
        {
            var fs = new LocalTableFileSystem(root);
            await fs.WriteAllBytesAsync("a", new byte[] { 1 });
            Assert.Equal(1, Read(fs, "a"));

            await fs.DeleteAsync("a");
            await fs.WriteAllBytesAsync("a", new byte[] { 2 });
            Assert.Equal(2, Read(fs, "a"));

            await using (var f = await fs.CreateAsync("a", overwrite: true))
            {
                await f.WriteAsync(new byte[] { 3 });
            }

            Assert.Equal(3, Read(fs, "a"));

            await fs.WriteAllBytesAsync("b", new byte[] { 4 });
            await fs.DeleteAsync("a");
            Assert.True(await fs.RenameAsync("b", "a"));
            Assert.Equal(4, Read(fs, "a"));
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task EvictedWhileLeased_StaysReadable_AndTheCapHolds()
    {
        var root = Directory.CreateTempSubdirectory("handles").FullName;
        try
        {
            var fs = new LocalTableFileSystem(root);
            await fs.WriteAllBytesAsync("held", new byte[] { 7 });
            using var held = LocalReadHandles.Rent(fs.FullPath("held"));
            for (var i = 0; i < LocalReadHandles.Capacity + 10; i++)
            {
                await fs.WriteAllBytesAsync("f" + i, new byte[] { (byte)i });
                Assert.Equal((byte)i, Read(fs, "f" + i));
            }

            Assert.True(LocalReadHandles.Count <= LocalReadHandles.Capacity);
            var b = new byte[1];
            held.File.ReadExactly(0, b); // evicted from the cache, still open for its lease
            Assert.Equal(7, b[0]);
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }
}
