using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Linq;
using System.Runtime.InteropServices;
using DiscUtils;
using DiscUtils.Ntfs;
using NtfsUtilities;
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
            char dl = _diskReader.DriveLetter.ToCharArray()[0];
            var finalPath = _diskReader.NtfsFileSystem.ResolvePath(filePath, dl);
            return finalPath;
            /*var reparseAttr = _diskReader.NtfsFileSystem.GetReparsePoint(filePath);
            if (reparseAttr != null && DiscUtils.Ntfs.AttributeType.ReparsePoint.Equals(reparseAttr.Tag))
            {
                // Cast the reparse data to the expected symbolic link buffer.
                var symLinkBuffer = (DiscUtils.Ntfs.AttributeType.SymbolicLinkReparseBuffer)reparseAttr.Buffer;
                string targetPath = symLinkBuffer.SubstituteName;
    
                // targetPath now holds the target of the symbolic link.
            }

            if (reparseAttr != null && reparseAttr.Tag == 0xA0000003)
            {
                // It’s likely a mount point (junction).
                var mountPointBuffer = (MountPointReparseBuffer)reparseAttr.Buffer;
                string targetPath = mountPointBuffer.SubstituteName;
    
                // targetPath holds the resolved target.
            }*/
            
            // Generate (mostly) by ChatGPT
            /*HashSet<string> visited = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            string currentPath = filePath;
            while (true)
            {
                var entry = _diskReader.NtfsFileSystem.GetDirectoryInfo(filePath);
                if (entry == null)
                {
                    // If the entry is not found, break (maybe the path is invalid or on a different FS).
                    return filePath;
                }
                // Check if this entry is a reparse point (indicating a symbolic link or junction in NTFS).
                if ((entry.Attributes & FileAttributes.ReparsePoint) != 0)
                {
                    // The file/directory has a reparse point attribute. Read the reparse data to find the target it points to.
                    DiscFileInfo file = _diskReader.NtfsFileSystem.GetFileInfo(currentPath);
                    NtfsStream reparseAttrStream = file.GetStream(AttributeType.ReparsePoint, null);
                    if (reparseAttrStream == null)
                    {
                        // No reparse data found even though flag is set (unexpected) - break out.
                        break;
                    }
                    byte[] reparseData = new byte[reparseAttrStream.ContentLength];
                    using (Stream s = reparseAttrStream.Open(FileAccess.Read))
                    {
                        s.Read(reparseData, 0, reparseData.Length);
                    }
                    // The reparse data buffer layout:
                    // [4 bytes Tag] [2 bytes DataLength] [2 bytes Reserved] [reparse-specific data]
                    uint tag = BitConverter.ToUInt32(reparseData, 0);
                    // We handle two specific tags: symlink (0xA000000C) and junction (mount point, 0xA0000003).
                    string targetPath = null;
                    if (tag == 0xA000000C) // IO_REPARSE_TAG_SYMLINK
                    {
                        // Symbolic link reparse data structure (after the first 8 bytes):
                        // [2 bytes SubstituteNameOffset] [2 bytes SubstituteNameLength]
                        // [2 bytes PrintNameOffset] [2 bytes PrintNameLength]
                        // [4 bytes Flags] (0 = absolute, 1 = relative)
                        // [variable-length PathBuffer] containing Unicode strings for substitute and print names.
                        ushort substituteNameOffset = BitConverter.ToUInt16(reparseData, 8);
                        ushort substituteNameLength = BitConverter.ToUInt16(reparseData, 10);
                        ushort printNameOffset = BitConverter.ToUInt16(reparseData, 12);
                        ushort printNameLength = BitConverter.ToUInt16(reparseData, 14);
                        uint symlinkFlags = BitConverter.ToUInt32(reparseData, 16);
                        int pathBufferOffset = 20;
                        string substituteName = System.Text.Encoding.Unicode.GetString(reparseData, pathBufferOffset + substituteNameOffset, substituteNameLength);
                        string printName = System.Text.Encoding.Unicode.GetString(reparseData, pathBufferOffset + printNameOffset, printNameLength);
                        // Prefer the print name for human-readable target if available, otherwise use the substitute name.
                        string rawTarget = !string.IsNullOrEmpty(printName) ? printName : substituteName;
                        if ((symlinkFlags & 0x1) != 0)
                        {
                            // Relative symlink: the target path is relative to the directory of the link.
                            string linkDirectory = Path.GetDirectoryName(currentPath);
                            if (string.IsNullOrEmpty(linkDirectory))
                            {
                                // If the link is at root (rare for relative symlink), treat base as root.
                                linkDirectory = currentPath;
                            }
                            targetPath = Path.GetFullPath(Path.Combine(linkDirectory, rawTarget));
                        }
                        else
                        {
                            // Absolute symlink: target is an absolute path.
                            targetPath = rawTarget;
                        }
                    }
                    else if (tag == 0xA0000003) // IO_REPARSE_TAG_MOUNT_POINT (junction)
                    {
                        // Junction reparse data structure (after first 8 bytes, similar to symlink but no flags):
                        // [2 bytes SubstituteNameOffset] [2 bytes SubstituteNameLength]
                        // [2 bytes PrintNameOffset] [2 bytes PrintNameLength]
                        // [variable-length PathBuffer] with Unicode strings for substitute and print names.
                        ushort substituteNameOffset = BitConverter.ToUInt16(reparseData, 8);
                        ushort substituteNameLength = BitConverter.ToUInt16(reparseData, 10);
                        ushort printNameOffset = BitConverter.ToUInt16(reparseData, 12);
                        ushort printNameLength = BitConverter.ToUInt16(reparseData, 14);
                        int pathBufferOffset = 16;
                        string substituteName = System.Text.Encoding.Unicode.GetString(reparseData, pathBufferOffset + substituteNameOffset, substituteNameLength);
                        string printName = System.Text.Encoding.Unicode.GetString(reparseData, pathBufferOffset + printNameOffset, printNameLength);
                        // Use print name if available, otherwise use substitute name.
                        targetPath = !string.IsNullOrEmpty(printName) ? printName : substituteName;
                    }
                    else
                    {
                        // A different type of reparse point (not handled here). Stop resolution.
                        break;
                    }

                    if (string.IsNullOrEmpty(targetPath))
                    {
                        // Could not determine the target path for the reparse point.
                        break;
                    }
                    // Clean up the target path from device prefixes if present (e.g., "\\?\").
                    if (targetPath.StartsWith("\\\\?\\", StringComparison.OrdinalIgnoreCase))
                    {
                        if (targetPath.StartsWith("\\\\?\\UNC\\", StringComparison.OrdinalIgnoreCase))
                        {
                            targetPath = "\\" + targetPath.Substring(8);
                        }
                        else
                        {
                            targetPath = targetPath.Substring(4);
                        }
                    }
                    // If target is a network path or on a different filesystem type, we will return it directly without further resolution.
                    if (targetPath.StartsWith("\\\\"))
                    {
                        return targetPath;
                    }
                    // Set up for the next iteration to resolve the target path.
                    currentPath = targetPath;
                    continue;
                }

                // Not a reparse point. Check if the file has multiple hard link names.
                {
                    DiscFileInfo file = _diskReader.NtfsFileSystem.GetFileInfo(entry.);
                    // DiscUtils provides all names (including alternate 8.3 names and hard links) for the file.
                    var allNames = file.FullName;
                    if (allNames != null && allNames.Count > 1)
                    {
                        string preferredName = null;
                        foreach (string name in allNames)
                        {
                            // Choose the first name that does not contain a tilde (which likely indicates it's not the short 8.3 name).
                            if (!name.Contains("~"))
                            {
                                preferredName = name;
                                break;
                            }
                        }
                        if (preferredName == null)
                        {
                            // In case all names contain '~' (very unlikely unless the actual name has '~'), just use the first name.
                            preferredName = allNames[0];
                        }
                        if (!string.Equals(preferredName, currentPath, StringComparison.OrdinalIgnoreCase))
                        {
                            // If the current path is not already the preferred name, switch to it and continue resolution from there.
                            currentPath = preferredName;
                            continue;
                        }
                    }
                }

                // If we get here, the path is neither a reparse point (symlink/junction) nor an alternate hard link name needing resolution.
                // We've resolved it to the final target.
                break;
            }*/

            return filePath;
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
                    return _diskReader.DriveLetter+":"+target;
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