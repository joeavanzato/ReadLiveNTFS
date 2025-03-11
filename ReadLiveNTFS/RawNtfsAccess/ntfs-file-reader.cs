using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using DiscUtils.Ntfs;
using Microsoft.Win32.SafeHandles;
using RawNtfsAccess.Exceptions;
using RawNtfsAccess.Models;
using RawNtfsAccess.Links;

namespace RawNtfsAccess.IO
{
    /// <summary>
    /// Provides methods for reading files from an NTFS volume using raw disk access
    /// </summary>
    internal class NtfsFileReader
    {
        private readonly RawDiskReader _diskReader;
        private readonly AlternateDataStreamHandler _adsHandler;
        private readonly LinkResolver _linkResolver;

        /// <summary>
        /// Initializes a new instance of the NtfsFileReader class
        /// </summary>
        /// <param name="diskReader">The raw disk reader</param>
        /// <param name="linkResolver">The link resolver</param>
        public NtfsFileReader(RawDiskReader diskReader, LinkResolver linkResolver)
        {
            _diskReader = diskReader ?? throw new ArgumentNullException(nameof(diskReader));
            _linkResolver = linkResolver ?? throw new ArgumentNullException(nameof(linkResolver));
            _adsHandler = new AlternateDataStreamHandler(_diskReader);
        }

        /// <summary>
        /// Gets the NTFS file system
        /// </summary>
        public NtfsFileSystem NtfsFileSystem => _diskReader.NtfsFileSystem;

        /// <summary>
        /// Checks if a file exists in the NTFS file system
        /// </summary>
        /// <param name="filePath">Full path to the file</param>
        /// <returns>True if the file exists; otherwise, false</returns>
        public bool FileExists(string filePath)
        {
            if (string.IsNullOrEmpty(filePath))
                throw new ArgumentException("File path cannot be null or empty", nameof(filePath));

            string normalizedPath = NormalizePath(filePath);
            
            try
            {
                return NtfsFileSystem.FileExists(normalizedPath);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error checking if file exists: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Gets information about a file
        /// </summary>
        /// <param name="filePath">Full path to the file</param>
        /// <param name="resolveLinks">Whether to resolve links</param>
        /// <param name="options">NTFS access options</param>
        /// <returns>File information</returns>
        public NtfsFileInfo GetFileInfo(string filePath, bool resolveLinks = true, RawNtfsOptions options = null)
        {
            options ??= new RawNtfsOptions();
            
            if (string.IsNullOrEmpty(filePath))
                throw new ArgumentException("File path cannot be null or empty", nameof(filePath));

            string normalizedPath = NormalizePath(filePath);
            
            // Check for alternate data stream in the path
            string streamName = null;
            if (normalizedPath.Contains(':'))
            {
                var parts = normalizedPath.Split(new[] { ':' }, 2);
                normalizedPath = parts[0];
                streamName = parts.Length > 1 ? parts[1] : null;
            }

            if (!NtfsFileSystem.FileExists(normalizedPath))
                throw new FileNotFoundException($"File not found: {normalizedPath}");

            try
            {
                // Get the file information from the NTFS file system
                var fileInfo = NtfsFileSystem.GetFileInfo(normalizedPath);
                var attributes = fileInfo.Attributes;
                
                var ntfsFileInfo = new NtfsFileInfo
                {
                    FullPath = filePath,
                    Size = fileInfo.Length,
                    CreationTime = fileInfo.CreationTime,
                    LastAccessTime = fileInfo.LastAccessTime,
                    LastWriteTime = fileInfo.LastWriteTime,
                    Attributes = attributes,
                    FileId = GetFileId(normalizedPath)
                };

                // Get alternate data streams
                ntfsFileInfo.AlternateDataStreams = _adsHandler.GetAlternateDataStreamNames(normalizedPath).ToList();

                // Check if it's a link and get the target
                if (IsReparsePoint(attributes))
                {
                    // Get reparse point tag and target
                    var (linkType, target) = _linkResolver.GetLinkTarget(normalizedPath);
                    ntfsFileInfo.LinkTarget = target;

                    // If requested, resolve the link target
                    if (resolveLinks && !string.IsNullOrEmpty(target))
                    {
                        bool isRelative = !Path.IsPathRooted(target);
                        
                        // Check if we should follow this type of link based on options
                        bool shouldFollow = isRelative ? options.FollowRelativeLinks : options.FollowAbsoluteLinks;
                        
                        if (shouldFollow)
                        {
                            // For relative links, resolve relative to parent directory
                            if (isRelative)
                            {
                                string parentDir = Path.GetDirectoryName(normalizedPath);
                                target = Path.GetFullPath(Path.Combine(parentDir ?? string.Empty, target));
                            }
                            
                            // If target exists, get info about it
                            if (NtfsFileSystem.FileExists(target))
                            {
                                return GetFileInfo(target, true, options);
                            }
                        }
                    }
                }

                return ntfsFileInfo;
            }
            catch (FileAttributeException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new NtfsAccessException($"Error getting file info for {normalizedPath}: {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Opens a read-only stream to a file
        /// </summary>
        /// <param name="filePath">Path to the file</param>
        /// <param name="options">NTFS access options</param>
        /// <returns>Stream for reading the file</returns>
        public Stream OpenFile(string filePath, RawNtfsOptions options = null)
        {
            options ??= new RawNtfsOptions();
            
            if (string.IsNullOrEmpty(filePath))
                throw new ArgumentException("File path cannot be null or empty", nameof(filePath));

            string streamName = null;
            string normalizedPath = NormalizePath(filePath);
            
            // Check for alternate data stream in the path
            if (normalizedPath.Contains(':'))
            {
                var parts = normalizedPath.Split(new[] { ':' }, 2);
                normalizedPath = parts[0];
                streamName = parts.Length > 1 ? parts[1] : null;
            }

            if (!NtfsFileSystem.FileExists(normalizedPath))
                throw new FileNotFoundException($"File not found: {normalizedPath}");

            try
            {
                var fileInfo = NtfsFileSystem.GetFileInfo(normalizedPath);
                var attributes = fileInfo.Attributes;
                
                // Check if it's a reparse point (symbolic link or junction)
                if (IsReparsePoint(attributes))
                {
                    var (linkType, target) = _linkResolver.GetLinkTarget(normalizedPath);
                    
                    if (!string.IsNullOrEmpty(target))
                    {
                        bool isRelative = !Path.IsPathRooted(target);
                        bool shouldFollow = isRelative ? options.FollowRelativeLinks : options.FollowAbsoluteLinks;
                        
                        if (shouldFollow)
                        {
                            // For relative links, resolve relative to parent directory
                            if (isRelative)
                            {
                                string parentDir = Path.GetDirectoryName(normalizedPath);
                                target = Path.GetFullPath(Path.Combine(parentDir ?? string.Empty, target));
                            }
                            
                            // If target exists, open it instead
                            if (NtfsFileSystem.FileExists(target))
                            {
                                return OpenFile(target, options);
                            }
                        }
                    }
                }
                
                // If requested, get an alternate data stream
                if (!string.IsNullOrEmpty(streamName))
                {
                    return _adsHandler.OpenAlternateDataStream(normalizedPath, streamName, IsSparseFile(attributes), NtfsFileSystem);
                }
                
                // Handle sparse files differently
                if (IsSparseFile(attributes))
                {
                    return new SparseFileStream(NtfsFileSystem, normalizedPath, options.BufferSize);
                }
                
                // For regular files, return a simple stream
                return NtfsFileSystem.OpenFile(normalizedPath, FileMode.Open, FileAccess.Read);
            }
            catch (Exception ex)
            {
                throw new NtfsAccessException($"Error opening file {normalizedPath}: {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Copies a file to a destination
        /// </summary>
        /// <param name="sourcePath">Source file path</param>
        /// <param name="destinationPath">Destination file path</param>
        /// <param name="overwrite">Whether to overwrite the destination file if it exists</param>
        /// <param name="options">NTFS access options</param>
        public void CopyFile(string sourcePath, string destinationPath, bool overwrite = false, RawNtfsOptions options = null)
        {
            options ??= new RawNtfsOptions();
            
            if (string.IsNullOrEmpty(sourcePath))
                throw new ArgumentException("Source path cannot be null or empty", nameof(sourcePath));
                
            if (string.IsNullOrEmpty(destinationPath))
                throw new ArgumentException("Destination path cannot be null or empty", nameof(destinationPath));

            if (File.Exists(destinationPath) && !overwrite)
                throw new IOException($"Destination file already exists: {destinationPath}");

            string sourceStreamName = null;
            string normalizedSourcePath = NormalizePath(sourcePath);
            
            // Check for alternate data stream in the source path
            if (normalizedSourcePath.Contains(':'))
            {
                var parts = normalizedSourcePath.Split(new[] { ':' }, 2);
                normalizedSourcePath = parts[0];
                sourceStreamName = parts.Length > 1 ? parts[1] : null;
            }

            if (!NtfsFileSystem.FileExists(normalizedSourcePath))
                throw new FileNotFoundException($"Source file not found: {normalizedSourcePath}");

            try
            {
                
                // Create destination directory if it doesn't exist
                string destinationDir = Path.GetDirectoryName(destinationPath);
                if (!string.IsNullOrEmpty(destinationDir) && !Directory.Exists(destinationDir))
                {
                    Directory.CreateDirectory(destinationDir);
                }
                
                // If a specific ADS is requested, only copy that stream
                /*if (!string.IsNullOrEmpty(sourceStreamName))
                {
                    SafeFileHandle handle = CreateFile(sourcePath, GENERIC_WRITE, FILE_SHARE_NONE, IntPtr.Zero, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, IntPtr.Zero);
                    using (var sourceStream = OpenFile(destinationPath, options))
                    using (var destStream = new FileStream(handle, FileAccess.Write))
                    {
                        CopyStream(sourceStream, destStream, options.BufferSize);
                    }
                    return;
                }*/
                
                if (!string.IsNullOrEmpty(sourceStreamName))
                {
                    // Construct the correct ADS path
                    string adsSourcePath = $"{normalizedSourcePath}:{sourceStreamName}";
    
                    // Or even better, directly use methods that clearly handle ADS:
                    //using (var sourceStream = _adsHandler.OpenAlternateDataStream(normalizedSourcePath, sourceStreamName))
                    
                    //using (var sourceStream = _diskReader.NtfsFileSystem.OpenFile(adsSourcePath, FileMode.Open, FileAccess.Read))
                    using (var sourceStream = OpenFile(adsSourcePath, options))
                    using (var destStream = File.Create(destinationPath))
                    {
                        CopyStream(sourceStream, destStream, options.BufferSize);
                    }
                    return;
                }
                
                // Get file info to check for special handling
                var fileInfo = GetFileInfo(normalizedSourcePath, true, options);
                
                // Copy the main data stream
                using (var sourceStream = OpenFile(normalizedSourcePath, options))
                using (var destStream = File.Create(destinationPath))
                {
                    CopyStream(sourceStream, destStream, options.BufferSize);
                }
                
                // Copy all alternate data streams if any
                foreach (var adsName in fileInfo.AlternateDataStreams)
                {
                    string sourceAdsPath = $"{normalizedSourcePath}:{adsName}";
                    string destAdsPath = $"{destinationPath}:{adsName}";
                    
                    /*using (var sourceStream = OpenFile(sourceAdsPath, options))
                    using (var destStream = File.Create(destAdsPath))
                    {
                        CopyStream(sourceStream, destStream, options.BufferSize);
                    }*/
                    SafeFileHandle handle = CreateFile(destAdsPath, GENERIC_WRITE, FILE_SHARE_NONE, IntPtr.Zero, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, IntPtr.Zero);
                    using (var sourceStream = OpenFile(sourceAdsPath, options))
                    using (var destStream = new FileStream(handle, FileAccess.Write))
                    {
                        CopyStream(sourceStream, destStream, options.BufferSize);
                    }
                }

                // Copy timestamps
                File.SetCreationTime(destinationPath, fileInfo.CreationTime);
                File.SetLastWriteTime(destinationPath, fileInfo.LastWriteTime);
                File.SetLastAccessTime(destinationPath, fileInfo.LastAccessTime);
                
                // Try to copy attributes where possible
                // Note: Some attributes like compressed may not be applicable to the destination
                try
                {
                    File.SetAttributes(destinationPath, fileInfo.Attributes);
                }
                catch
                {
                    // Ignore attribute setting failures
                    Console.WriteLine($"Warning: Failed to set attributes on {destinationPath}");
                }
            }
            catch (Exception ex)
            {
                throw new NtfsAccessException($"Error copying file from {sourcePath} to {destinationPath}: {ex.Message}", ex);
            }
        }
        
        [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
        static extern SafeFileHandle CreateFile(
            string lpFileName,
            uint dwDesiredAccess,
            uint dwShareMode,
            IntPtr lpSecurityAttributes,
            uint dwCreationDisposition,
            uint dwFlagsAndAttributes,
            IntPtr hTemplateFile);

        // Desired access flags
        const uint GENERIC_WRITE = 0x40000000;
        // Share mode: no sharing
        const uint FILE_SHARE_NONE = 0;
        // Creation disposition: create new or overwrite if exists
        const uint CREATE_ALWAYS = 2;
        // Flags and attributes: normal file
        const uint FILE_ATTRIBUTE_NORMAL = 0x80;
    

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
            
            // Replace backslashes with forward slashes if needed
            // path = path.Replace(Path.DirectorySeparatorChar, '/');
            
            return path;
        }

        /// <summary>
        /// Gets the file ID from the NTFS file system
        /// </summary>
        /// <param name="path">Path to the file</param>
        /// <returns>File ID</returns>
        private long GetFileId(string path)
        {
            try
            {
                // This is a workaround since we don't have direct access to the MFT record number
                // Use a unique identifier based on the file path
                return path.GetHashCode();
            }
            catch
            {
                return 0;
            }
        }

        /// <summary>
        /// Checks if a file has the reparse point attribute
        /// </summary>
        /// <param name="attributes">File attributes</param>
        /// <returns>True if the file is a reparse point; otherwise, false</returns>
        private bool IsReparsePoint(FileAttributes attributes)
        {
            return (attributes & FileAttributes.ReparsePoint) == FileAttributes.ReparsePoint;
        }

        /// <summary>
        /// Checks if a file has the sparse file attribute
        /// </summary>
        /// <param name="attributes">File attributes</param>
        /// <returns>True if the file is sparse; otherwise, false</returns>
        private bool IsSparseFile(FileAttributes attributes)
        {
            return (attributes & FileAttributes.SparseFile) == FileAttributes.SparseFile;
        }

        /// <summary>
        /// Copies data from one stream to another
        /// </summary>
        /// <param name="source">Source stream</param>
        /// <param name="destination">Destination stream</param>
        /// <param name="bufferSize">Buffer size in bytes</param>
        private void CopyStream(Stream source, Stream destination, int bufferSize)
        {
            byte[] buffer = new byte[bufferSize];
            int bytesRead;
            
            while ((bytesRead = source.Read(buffer, 0, buffer.Length)) > 0)
            {
                destination.Write(buffer, 0, bytesRead);
            }
        }
    }
}