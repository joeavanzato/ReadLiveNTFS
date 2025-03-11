using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Linq;
using System.Runtime.InteropServices;
using DiscUtils.Ntfs;
using RawNtfsAccess.Exceptions;
using RawNtfsAccess.IO;
using RawNtfsAccess.Models;

namespace RawNtfsAccess.Links
{
    /// <summary>
    /// Handles operations related to resolving NTFS links
    /// </summary>
    internal class LinkResolver
    {
        private readonly RawDiskReader _diskReader;
        private readonly HashSet<string> _visitedLinks;
        private int _currentDepth;

        // Constants for reparse point tags
        private const uint IO_REPARSE_TAG_MOUNT_POINT = 0xA0000003;
        private const uint IO_REPARSE_TAG_SYMLINK = 0xA000000C;
        private const uint IO_REPARSE_TAG_DEDUP = 0x80000013;

        // P/Invoke declarations for Windows API functions
        [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Auto)]
        private static extern bool DeviceIoControl(
            IntPtr hDevice,
            uint dwIoControlCode,
            IntPtr lpInBuffer,
            uint nInBufferSize,
            IntPtr lpOutBuffer,
            uint nOutBufferSize,
            out uint lpBytesReturned,
            IntPtr lpOverlapped);

        [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Auto)]
        private static extern IntPtr CreateFile(
            string lpFileName,
            uint dwDesiredAccess,
            uint dwShareMode,
            IntPtr lpSecurityAttributes,
            uint dwCreationDisposition,
            uint dwFlagsAndAttributes,
            IntPtr hTemplateFile);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool CloseHandle(IntPtr hObject);

        // Constants for the CreateFile function
        private const uint OPEN_EXISTING = 3;
        private const uint FILE_FLAG_BACKUP_SEMANTICS = 0x02000000;
        private const uint FILE_FLAG_OPEN_REPARSE_POINT = 0x00200000;

        // Constants for DeviceIoControl
        private const uint FSCTL_GET_REPARSE_POINT = 0x000900A8;

        // Access rights for CreateFile
        private const uint GENERIC_READ = 0x80000000;
        private const uint FILE_SHARE_READ = 0x00000001;
        private const uint FILE_SHARE_WRITE = 0x00000002;
        private const uint FILE_SHARE_DELETE = 0x00000004;

        /// <summary>
        /// Initializes a new instance of the LinkResolver class
        /// </summary>
        /// <param name="diskReader">The raw disk reader</param>
        public LinkResolver(RawDiskReader diskReader)
        {
            _diskReader = diskReader ?? throw new ArgumentNullException(nameof(diskReader));
            _visitedLinks = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            _currentDepth = 0;
        }

        /// <summary>
        /// Gets the link target from a reparse point
        /// </summary>
        /// <param name="filePath">The path to the file or directory</param>
        /// <returns>Tuple containing the link type and target path</returns>
        public (LinkType LinkType, string Target) GetLinkTarget(string filePath)
        {
            if (string.IsNullOrEmpty(filePath))
                throw new ArgumentNullException(nameof(filePath));

            try
            {
                var ntfsFs = _diskReader.NtfsFileSystem;
                
                // Check if it's a file or directory
                bool isDirectory = ntfsFs.DirectoryExists(filePath);
                bool isFile = ntfsFs.FileExists(filePath);
                
                if (!isDirectory && !isFile)
                    return (LinkType.None, null);
                
                // Get the attributes
                FileAttributes attributes;
                if (isDirectory)
                {
                    var dirInfo = ntfsFs.GetDirectoryInfo(filePath);
                    attributes = dirInfo.Attributes;
                }
                else
                {
                    var fileInfo = ntfsFs.GetFileInfo(filePath);
                    attributes = fileInfo.Attributes;
                }
                
                // Check if it's a reparse point
                if ((attributes & FileAttributes.ReparsePoint) != FileAttributes.ReparsePoint)
                    return (LinkType.None, null);
                
                // Try to get the target using DiscUtils
                string target = TryGetTargetWithDiscUtils(filePath, isDirectory);
                if (!string.IsNullOrEmpty(target))
                {
                    return (isDirectory ? LinkType.Junction : LinkType.SymbolicFile, target);
                }
                
                // If DiscUtils approach fails and we're on Windows, use the Windows API
                if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                {
                    return GetReparsePointTargetWithWinAPI(filePath, isDirectory);
                }
                
                // If we get here, neither method worked
                throw new NotSupportedException(
                    "Could not resolve reparse point target. On non-Windows platforms, ensure DiscUtils properly supports reparse point resolution.");
            }
            catch (Exception ex)
            {
                throw new LinkRecursionException(
                    filePath, 
                    _currentDepth, 
                    $"Error reading reparse point data: {ex.Message}");
            }
        }

        /// <summary>
        /// Try to get the target path using DiscUtils' APIs
        /// </summary>
        /// <param name="filePath">The path to the file or directory</param>
        /// <param name="isDirectory">Whether the path is a directory</param>
        /// <returns>The target path if successful; otherwise, null</returns>
        private string TryGetTargetWithDiscUtils(string filePath, bool isDirectory)
        {
            try
            {
                var ntfsFs = _diskReader.NtfsFileSystem;
                
                // DiscUtils.Ntfs has a ReparsePoints property on FileSystemInfo 
                // objects in newer versions
                if (isDirectory)
                {
                    var dirInfo = ntfsFs.GetDirectoryInfo(filePath);
                    
                    // Try to access the reparse target through reflection
                    // In newer versions, DirectoryInfo may have a method to get the target
                    try
                    {
                        if (dirInfo.Exists && (dirInfo.Attributes & FileAttributes.ReparsePoint) == FileAttributes.ReparsePoint)
                        {
                            // Some versions of DiscUtils have a GetReparsePoint() method
                            var type = dirInfo.GetType();
                            var method = type.GetMethod("GetReparsePoint") ?? 
                                         type.GetMethod("GetReparsePointData") ??
                                         type.GetMethod("GetReparseData");
                                
                            if (method != null)
                            {
                                var reparseData = method.Invoke(dirInfo, null);
                                if (reparseData != null)
                                {
                                    // Try to extract the target path from the reparse data
                                    // The exact property/method depends on the DiscUtils version
                                    var reparseType = reparseData.GetType();
                                    var targetProp = reparseType.GetProperty("Target") ?? 
                                                     reparseType.GetProperty("TargetPath") ??
                                                     reparseType.GetProperty("Path");
                                    
                                    if (targetProp != null)
                                    {
                                        var target = targetProp.GetValue(reparseData) as string;
                                        if (!string.IsNullOrEmpty(target))
                                        {
                                            return CleanupReparseTarget(target);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Reflection error accessing reparse point: {ex.Message}");
                        // Continue to try other methods
                    }
                }
                else
                {
                    var fileInfo = ntfsFs.GetFileInfo(filePath);
                    
                    // Similar approach for files
                    try
                    {
                        if (fileInfo.Exists && (fileInfo.Attributes & FileAttributes.ReparsePoint) == FileAttributes.ReparsePoint)
                        {
                            // Try the same reflection approach for files
                            var type = fileInfo.GetType();
                            var method = type.GetMethod("GetReparsePoint") ?? 
                                         type.GetMethod("GetReparsePointData") ??
                                         type.GetMethod("GetReparseData");
                                
                            if (method != null)
                            {
                                var reparseData = method.Invoke(fileInfo, null);
                                if (reparseData != null)
                                {
                                    var reparseType = reparseData.GetType();
                                    var targetProp = reparseType.GetProperty("Target") ?? 
                                                     reparseType.GetProperty("TargetPath") ??
                                                     reparseType.GetProperty("Path");
                                    
                                    if (targetProp != null)
                                    {
                                        var target = targetProp.GetValue(reparseData) as string;
                                        if (!string.IsNullOrEmpty(target))
                                        {
                                            return CleanupReparseTarget(target);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Reflection error accessing reparse point: {ex.Message}");
                        // Continue to try other methods
                    }
                }
                
                // In DiscUtils.Ntfs, the ReparsePoint class might be internal, so we need a different approach
                // Try to open the file and examine its attributes directly
                try
                {
                    // Get reparse point data directly from the file's attributes
                    // This requires knowledge of the NTFS file structure
                    // Since DiscUtils doesn't expose this directly in the public API,
                    // we'll rely on the Windows API method instead for production code
                }
                catch
                {
                    // If this fails, return null to fall back to Windows API
                }
                
                return null;
            }
            catch
            {
                return null;
            }
        }

        /// <summary>
        /// Get the reparse point target using Windows API (on Windows only)
        /// </summary>
        /// <param name="path">The path to the reparse point</param>
        /// <param name="isDirectory">Whether the path is a directory</param>
        /// <returns>Tuple containing the link type and target path</returns>
        private (LinkType LinkType, string Target) GetReparsePointTargetWithWinAPI(string path, bool isDirectory)
        {
            // Convert relative path to full path with drive letter
            string fullPath;
            if (path.Length >= 2 && path[1] == ':')
            {
                fullPath = path;
            }
            else
            {
                // Add drive letter and ensure path has leading slash
                char driveLetter = _diskReader.DriveLetter[0];
                
                if (path.StartsWith("/") || path.StartsWith("\\"))
                {
                    fullPath = $"{driveLetter}:{path}";
                }
                else
                {
                    fullPath = $"{driveLetter}:\\{path}";
                }
            }
            
            // Replace forward slashes with backslashes for Windows API
            fullPath = fullPath.Replace('/', '\\');
            
            IntPtr handle = CreateFile(
                fullPath,
                GENERIC_READ,
                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                IntPtr.Zero,
                OPEN_EXISTING,
                FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT,
                IntPtr.Zero);
                
            if (handle == IntPtr.Zero || handle.ToInt64() == -1)
            {
                int error = Marshal.GetLastWin32Error();
                throw new IOException($"Failed to open reparse point: Win32 error {error}");
            }
            
            try
            {
                // Buffer to store reparse data
                int bufferSize = 16 * 1024; // 16 KB should be enough for most cases
                IntPtr buffer = Marshal.AllocHGlobal(bufferSize);
                
                try
                {
                    // Get reparse point data
                    uint bytesReturned;
                    bool success = DeviceIoControl(
                        handle,
                        FSCTL_GET_REPARSE_POINT,
                        IntPtr.Zero,
                        0,
                        buffer,
                        (uint)bufferSize,
                        out bytesReturned,
                        IntPtr.Zero);
                        
                    if (!success)
                    {
                        int error = Marshal.GetLastWin32Error();
                        throw new IOException($"Failed to get reparse point data: Win32 error {error}");
                    }
                    
                    // Parse the reparse data
                    // The structure varies depending on the reparse tag
                    
                    // Read reparse tag
                    uint reparseTag = (uint)Marshal.ReadInt32(buffer);
                    
                    if (reparseTag == IO_REPARSE_TAG_MOUNT_POINT)
                    {
                        // Junction point
                        string target = ParseMountPointReparseData(buffer, bytesReturned);
                        return (LinkType.Junction, target);
                    }
                    else if (reparseTag == IO_REPARSE_TAG_SYMLINK)
                    {
                        // Symbolic link
                        string target = ParseSymlinkReparseData(buffer, bytesReturned);
                        return (isDirectory ? LinkType.SymbolicDirectory : LinkType.SymbolicFile, target);
                    }
                    else
                    {
                        throw new NotSupportedException($"Unsupported reparse tag: 0x{reparseTag:X}");
                    }
                }
                finally
                {
                    Marshal.FreeHGlobal(buffer);
                }
            }
            finally
            {
                CloseHandle(handle);
            }
        }

        /// <summary>
        /// Parse reparse data for a mount point (junction)
        /// </summary>
        /// <param name="buffer">Buffer containing the reparse data</param>
        /// <param name="bytesReturned">Number of bytes returned</param>
        /// <returns>The target path</returns>
        private string ParseMountPointReparseData(IntPtr buffer, uint bytesReturned)
        {
            // Skip the reparse tag and reparse data length (8 bytes total)
            int offset = 8;
            
            // Read the substitute name offset and length
            ushort substituteNameOffset = (ushort)Marshal.ReadInt16(buffer, offset);
            offset += 2;
            ushort substituteNameLength = (ushort)Marshal.ReadInt16(buffer, offset);
            offset += 2;
            
            // Skip the print name offset and length
            offset += 4;
            
            // The path buffer starts after the fixed header (usually at offset 16)
            int pathBufferOffset = 16;
            
            // Read the substitute name (target path)
            byte[] pathBuffer = new byte[substituteNameLength];
            Marshal.Copy(new IntPtr(buffer.ToInt64() + pathBufferOffset + substituteNameOffset), pathBuffer, 0, substituteNameLength);
            
            // Convert to string (Unicode)
            string target = Encoding.Unicode.GetString(pathBuffer);
            
            // Clean up the target path
            return CleanupReparseTarget(target);
        }

        /// <summary>
        /// Parse reparse data for a symbolic link
        /// </summary>
        /// <param name="buffer">Buffer containing the reparse data</param>
        /// <param name="bytesReturned">Number of bytes returned</param>
        /// <returns>The target path</returns>
        private string ParseSymlinkReparseData(IntPtr buffer, uint bytesReturned)
        {
            // Skip the reparse tag and reparse data length (8 bytes total)
            int offset = 8;
            
            // Read the substitute name offset and length
            ushort substituteNameOffset = (ushort)Marshal.ReadInt16(buffer, offset);
            offset += 2;
            ushort substituteNameLength = (ushort)Marshal.ReadInt16(buffer, offset);
            offset += 2;
            
            // Skip the print name offset and length
            offset += 4;
            
            // Read the flags
            uint flags = (uint)Marshal.ReadInt32(buffer, offset);
            offset += 4;
            
            // The path buffer starts after the fixed header (usually at offset 20)
            int pathBufferOffset = 20;
            
            // Read the substitute name (target path)
            byte[] pathBuffer = new byte[substituteNameLength];
            Marshal.Copy(new IntPtr(buffer.ToInt64() + pathBufferOffset + substituteNameOffset), pathBuffer, 0, substituteNameLength);
            
            // Convert to string (Unicode)
            string target = Encoding.Unicode.GetString(pathBuffer);
            
            // Clean up the target path
            return CleanupReparseTarget(target);
        }

        /// <summary>
        /// Resolves a link target to its final destination
        /// </summary>
        /// <param name="linkPath">Path to the link</param>
        /// <param name="options">NTFS access options</param>
        /// <returns>Path to the final target</returns>
        public string ResolveLinkTarget(string linkPath, RawNtfsOptions options)
        {
            if (string.IsNullOrEmpty(linkPath))
                throw new ArgumentException("Link path cannot be null or empty", nameof(linkPath));
                
            options ??= new RawNtfsOptions();
            
            _visitedLinks.Clear();
            _currentDepth = 0;
            
            return ResolveLink(linkPath, options);
        }

        /// <summary>
        /// Resolves a link recursively to its final destination
        /// </summary>
        /// <param name="linkPath">Path to the link</param>
        /// <param name="options">NTFS access options</param>
        /// <returns>Path to the final target</returns>
        private string ResolveLink(string linkPath, RawNtfsOptions options)
        {
            // Check for recursion limit
            if (_currentDepth >= options.MaxLinkDepth)
            {
                throw new LinkRecursionException(
                    linkPath, 
                    _currentDepth, 
                    $"Maximum link recursion depth reached ({options.MaxLinkDepth})");
            }
            
            // Check if we've already visited this link to prevent cycles
            string normalizedPath = linkPath.ToLowerInvariant();
            if (_visitedLinks.Contains(normalizedPath))
            {
                throw new LinkRecursionException(
                    linkPath, 
                    _currentDepth, 
                    "Circular link reference detected");
            }
            
            _visitedLinks.Add(normalizedPath);
            _currentDepth++;
            
            try
            {
                var ntfsFs = _diskReader.NtfsFileSystem;
                string path = NormalizePath(linkPath);
                
                // Check if it's a file or directory
                bool isDirectory = ntfsFs.DirectoryExists(path);
                bool isFile = ntfsFs.FileExists(path);
                
                if (!isDirectory && !isFile)
                    return linkPath;
                
                // Get the attributes
                FileAttributes attributes;
                if (isDirectory)
                {
                    var dirInfo = ntfsFs.GetDirectoryInfo(path);
                    attributes = dirInfo.Attributes;
                }
                else
                {
                    var fileInfo = ntfsFs.GetFileInfo(path);
                    attributes = fileInfo.Attributes;
                }
                
                // Check if it's a reparse point
                if ((attributes & FileAttributes.ReparsePoint) != FileAttributes.ReparsePoint)
                    return linkPath;
                
                // Get the link target
                var (linkType, target) = GetLinkTarget(path);
                
                // If not a link or no target, return the original path
                if (linkType == LinkType.None || string.IsNullOrEmpty(target))
                {
                    return linkPath;
                }
                
                // Check if we should follow this type of link
                bool isRelative = !Path.IsPathRooted(target);
                bool shouldFollow = isRelative ? options.FollowRelativeLinks : options.FollowAbsoluteLinks;
                
                if (!shouldFollow)
                {
                    return linkPath;
                }
                
                // For relative links, resolve relative to parent directory
                if (isRelative)
                {
                    string parentDir = Path.GetDirectoryName(path);
                    target = Path.GetFullPath(Path.Combine(parentDir ?? string.Empty, target));
                }
                
                // Recursively resolve the target
                return ResolveLink(target, options);
            }
            finally
            {
                _currentDepth--;
            }
        }

        /// <summary>
        /// Cleans up a reparse point target
        /// </summary>
        /// <param name="target">Raw target path</param>
        /// <returns>Cleaned up path</returns>
        private string CleanupReparseTarget(string target)
        {
            if (string.IsNullOrEmpty(target))
                return target;
                
            // Remove NT namespace prefix
            if (target.StartsWith(@"\??\"))
            {
                target = target.Substring(4);
            }
            
            // Also handle volume GUIDs like \??\Volume{guid}\path
            if (target.StartsWith(@"\??\Volume{"))
            {
                int endVolumeIndex = target.IndexOf('}');
                if (endVolumeIndex > 0 && endVolumeIndex + 1 < target.Length)
                {
                    // This is a volume GUID path, try to resolve it to a drive letter if possible
                    string volumeGuid = target.Substring(4, endVolumeIndex - 3);
                    string remainingPath = target.Substring(endVolumeIndex + 1);
                    
                    // For simplicity, we'll use the same drive as our current disk
                    // In a real implementation, you'd map the volume GUID to a drive letter
                    target = _diskReader.DriveLetter + remainingPath;
                }
            }
            
            // Clean up the path for Windows
            return target.Replace('/', '\\');
        }

        /// <summary>
        /// Normalizes a Windows path for use with DiscUtils.Ntfs
        /// </summary>
        /// <param name="path">The path to normalize</param>
        /// <returns>Normalized path</returns>
        private string NormalizePath(string path)
        {
            if (string.IsNullOrEmpty(path))
                return path;
                
            // Remove drive letter if present
            if (path.Length >= 2 && path[1] == ':')
            {
                path = path.Substring(2);
            }
            
            // Ensure path doesn't start with a separator (DiscUtils expects paths without leading slashes)
            if (path.Length > 0 && (path[0] == Path.DirectorySeparatorChar || path[0] == Path.AltDirectorySeparatorChar))
            {
                path = path.Substring(1);
            }
            
            return path;
        }
    }
}