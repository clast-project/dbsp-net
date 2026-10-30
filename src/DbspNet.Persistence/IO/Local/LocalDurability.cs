// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Runtime.InteropServices;

namespace DbspNet.Persistence.IO.Local;

/// <summary>
/// fsync for files and directories. .NET has no API for syncing a directory, and a new file's
/// directory entry is not durable until its parent directory is synced, so on Unix this goes to
/// libc directly.
/// </summary>
/// <remarks>
/// On macOS, <c>fsync</c> pushes data to the drive but not through the drive's own cache; only
/// <c>F_FULLFSYNC</c> does, and it flushes the whole device. So files and directories get a plain
/// <c>fsync</c> each and the batch ends with one <see cref="Barrier"/>, rather than paying a device
/// flush per file. On Linux <c>fsync</c> is already the full guarantee and the barrier is a no-op.
/// On Windows, directory entries are journaled by NTFS and there is nothing to sync.
/// </remarks>
internal static class LocalDurability
{
    private const int ORdOnly = 0;
    private const int FFullFsync = 51; // <sys/fcntl.h> on Darwin
    private const int EIntr = 4;

    public static void SyncFile(string path)
    {
        if (OperatingSystem.IsWindows())
        {
            using var stream = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);
            stream.Flush(flushToDisk: true);
            return;
        }

        SyncPath(path);
    }

    public static void SyncDirectory(string path)
    {
        if (OperatingSystem.IsWindows())
        {
            return;
        }

        SyncPath(path);
    }

    /// <summary>
    /// Device-level flush after a batch of <see cref="SyncFile"/>/<see cref="SyncDirectory"/> calls.
    /// <paramref name="anyPath"/> is any existing file or directory on the same volume.
    /// </summary>
    public static void Barrier(string anyPath)
    {
        if (!OperatingSystem.IsMacOS())
        {
            return;
        }

        var fd = OpenOrThrow(anyPath);
        try
        {
            int rc;
            do
            {
                rc = Native.Fcntl(fd, FFullFsync);
            }
            while (rc < 0 && Marshal.GetLastPInvokeError() == EIntr);

            if (rc < 0)
            {
                // Some filesystems (network, FUSE) reject F_FULLFSYNC; fsync is the best remaining.
                FsyncOrThrow(fd, anyPath);
            }
        }
        finally
        {
            _ = Native.Close(fd);
        }
    }

    private static void SyncPath(string path)
    {
        var fd = OpenOrThrow(path);
        try
        {
            FsyncOrThrow(fd, path);
        }
        finally
        {
            _ = Native.Close(fd);
        }
    }

    private static int OpenOrThrow(string path)
    {
        int fd;
        do
        {
            fd = Native.Open(path, ORdOnly);
        }
        while (fd < 0 && Marshal.GetLastPInvokeError() == EIntr);

        if (fd < 0)
        {
            throw new IOException($"open('{path}') for fsync failed: errno {Marshal.GetLastPInvokeError()}");
        }

        return fd;
    }

    private static void FsyncOrThrow(int fd, string path)
    {
        int rc;
        do
        {
            rc = Native.Fsync(fd);
        }
        while (rc < 0 && Marshal.GetLastPInvokeError() == EIntr);

        if (rc < 0)
        {
            // A failed fsync means the data may never reach disk; the commit must not proceed.
            throw new IOException($"fsync('{path}') failed: errno {Marshal.GetLastPInvokeError()}");
        }
    }

    private static class Native
    {
        // Path passed as NUL-terminated UTF-8 bytes; no string marshalling involved.
        [DllImport("libc", EntryPoint = "open", SetLastError = true)]
        private static extern int Open(byte[] path, int flags);

        public static int Open(string path, int flags)
        {
            var bytes = new byte[System.Text.Encoding.UTF8.GetByteCount(path) + 1];
            System.Text.Encoding.UTF8.GetBytes(path, bytes);
            return Open(bytes, flags);
        }

        [DllImport("libc", EntryPoint = "fsync", SetLastError = true)]
        public static extern int Fsync(int fd);

        // fcntl is variadic; F_FULLFSYNC takes no third argument, so the fixed two-argument form
        // is ABI-safe (Apple arm64 passes variadic arguments on the stack, fixed ones in registers).
        [DllImport("libc", EntryPoint = "fcntl", SetLastError = true)]
        public static extern int Fcntl(int fd, int cmd);

        [DllImport("libc", EntryPoint = "close")]
        public static extern int Close(int fd);
    }
}
