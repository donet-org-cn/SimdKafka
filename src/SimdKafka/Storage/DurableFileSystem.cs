using System.ComponentModel;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;

namespace SimdKafka;

internal static partial class DurableFileSystem
{
    // .NET maps Unix EFBIG to ArgumentOutOfRangeException rather than IOException.
    internal static bool IsStorageError(Exception error) =>
        error is IOException or UnauthorizedAccessException or ArgumentOutOfRangeException;

    internal static void EnsureDirectory(string path, bool ownerOnly = false)
    {
        if (Directory.Exists(path)) return;
        string parent = Path.GetDirectoryName(path)!;
        EnsureDirectory(parent);
        if (ownerOnly && !OperatingSystem.IsWindows())
            Directory.CreateDirectory(path, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
        else Directory.CreateDirectory(path);
        FlushDirectory(parent);
    }

    internal static void Replace(string source, string destination)
    {
        if (OperatingSystem.IsWindows())
        {
            const int replaceExisting = 1, writeThrough = 8;
            if (MoveFile(source, destination, replaceExisting | writeThrough) == 0)
                throw IoError("replace catalog");
            return;
        }
        File.Move(source, destination, overwrite: true);
        FlushDirectory(Path.GetDirectoryName(destination)!);
    }

    internal static void FlushData(SafeFileHandle file)
    {
        if (!OperatingSystem.IsLinux())
        {
            RandomAccess.FlushToDisk(file);
            return;
        }
        while (FlushFileData(file) != 0)
            if (Marshal.GetLastPInvokeError() != 4) throw IoError("fdatasync partition");
    }

    internal static bool TryPunchHole(SafeFileHandle file, long position, long length)
    {
        if (OperatingSystem.IsLinux())
        {
            const int keepSize = 1, punchHole = 2;
            int result;
            do { result = PunchLinux(file, keepSize | punchHole, position, length); }
            while (result != 0 && Marshal.GetLastPInvokeError() == 4);
            return result == 0;
        }
        if (!OperatingSystem.IsWindows()) return false;
        const uint setSparse = 0x000900C4, setZeroData = 0x000980C8;
        var range = new ZeroDataRange { Start = position, Beyond = position + length };
        return SetSparse(file, setSparse, IntPtr.Zero, 0, IntPtr.Zero, 0, out _, IntPtr.Zero) != 0 &&
            SetZeroData(file, setZeroData, ref range, 16, IntPtr.Zero, 0, out _, IntPtr.Zero) != 0;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct ZeroDataRange { internal long Start, Beyond; }

    internal static void FlushDirectory(string path)
    {
        if (OperatingSystem.IsWindows()) return;
        int descriptor = OpenDirectory(path, 0);
        if (descriptor < 0) throw IoError("open directory for sync");
        try
        {
            while (Sync(descriptor) != 0)
                if (Marshal.GetLastPInvokeError() != 4) throw IoError("fsync directory");
        }
        finally
        {
            Close(descriptor);
        }
    }

    private static IOException IoError(string operation) =>
        new(operation, new Win32Exception(Marshal.GetLastPInvokeError()));

    [LibraryImport("libc", EntryPoint = "open", SetLastError = true, StringMarshalling = StringMarshalling.Utf8)]
    private static partial int OpenDirectory(string path, int flags);
    [LibraryImport("libc", EntryPoint = "close")]
    private static partial int Close(int descriptor);
    [LibraryImport("libc", EntryPoint = "fsync", SetLastError = true)]
    private static partial int Sync(int descriptor);
    [LibraryImport("libc", EntryPoint = "fdatasync", SetLastError = true)]
    private static partial int FlushFileData(SafeFileHandle file);
    [LibraryImport("libc", EntryPoint = "fallocate", SetLastError = true)]
    private static partial int PunchLinux(SafeFileHandle file, int mode, long offset, long length);
    [LibraryImport("kernel32.dll", EntryPoint = "DeviceIoControl", SetLastError = true)]
    private static partial int SetSparse(SafeFileHandle file, uint code, IntPtr input, uint inputBytes,
        IntPtr output, uint outputBytes, out uint bytes, IntPtr overlapped);
    [LibraryImport("kernel32.dll", EntryPoint = "DeviceIoControl", SetLastError = true)]
    private static partial int SetZeroData(SafeFileHandle file, uint code, ref ZeroDataRange input, uint inputBytes,
        IntPtr output, uint outputBytes, out uint bytes, IntPtr overlapped);
    [LibraryImport("kernel32.dll", EntryPoint = "MoveFileExW", SetLastError = true,
        StringMarshalling = StringMarshalling.Utf16)]
    private static partial int MoveFile(string source, string destination, int flags);
}
