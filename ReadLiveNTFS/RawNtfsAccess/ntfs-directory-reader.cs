using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using DiscUtils.Ntfs;
using RawNtfsAccess.Exceptions;
using RawNtfsAccess.Models;
using RawNtfsAccess.Links;

namespace RawNtfsAccess.IO
{
    /// <summary>
    /// Provides methods for reading directories from an NTFS volume using raw disk access
    /// </summary>
    internal class NtfsDirectoryReader
    {
        private readonly RawDiskReader _diskReader;
        private readonly LinkResolver _linkResolver;

        /// <summary>
        /// Initializes a new instance of the NtfsDirectoryReader class
        /// </summary>
        /// <param name="diskReader">The raw disk reader</param>
        /// <param name="linkResolver">The link resolver</param>
        public NtfsDirectoryReader(RawDiskReader diskReader, LinkResolver linkResolver)
        {
            _diskReader = diskReader ?? throw new ArgumentNullException(nameof(diskReader));
            _linkResolver = linkResolver ?? throw new ArgumentNullException(nameof(linkResolver));
        }

        /// <summary>
        /// Gets the NTFS file system
        /// </summary>
        public NtfsFileSystem NtfsFileSystem => _diskReader.NtfsFileSystem;

        /// <summary>
        /// Checks if a directory exists in the NTFS file system
        /// </summary>
        /// <param name="directoryPath">Full path to the directory</param>
        /// <returns>True if the directory exists; otherwise, false</returns>
        public bool DirectoryExists(string directoryPath)
        {
            if (string.IsNullOrEmpty(directoryPath))
                throw new ArgumentException("Directory path cannot be null or empty", nameof(directoryPath));

            string normalizedPath = NormalizePath(directoryPath);
            
            try
            {
                return NtfsFileSystem.DirectoryExists(normalizedPath);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error checking if directory exists: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Gets information about a directory
        /// </summary>
        /// <param name="directoryPath">Full path to the directory</param>
        /// <param name="resolveLinks">Whether to resolve links</param>
        /// <param name="options">NTFS access options</param>
        /// <returns>Directory information</returns>
        public NtfsDirectoryInfo GetDirectoryInfo(string directoryPath, bool resolveLinks = true, RawNtfsOptions options = null)
        {
            options ??= new RawNtfsOptions();
            
            if (string.IsNullOrEmpty(directoryPath))
                throw new ArgumentException("Directory path cannot be null or empty", nameof(directoryPath));

            string normalizedPath = NormalizePath(directoryPath);
            
            if (!NtfsFileSystem.DirectoryExists(normalizedPath))
                throw new DirectoryNotFoundException($"Directory not found: {normalizedPath}");

            try
            {
                // Get the directory from the NTFS file system
                var dirInfo = NtfsFileSystem.GetDirectoryInfo(normalizedPath);
                var attributes = dirInfo.Attributes;
                
                var ntfsDirInfo = new NtfsDirectoryInfo
                {
                    FullPath = directoryPath,
                    CreationTime = dirInfo.CreationTime,
                    LastAccessTime = dirInfo.LastAccessTime,
                    LastWriteTime = dirInfo.LastWriteTime,
                    Attributes = attributes,
                    DirectoryId = GetDirectoryId(normalizedPath)
                };

                // Check if it's a link and get the target
                if (IsReparsePoint(attributes))
                {
                    // Get reparse point tag and target
                    var (linkType, target) = _linkResolver.GetLinkTarget(normalizedPath);
                    ntfsDirInfo.LinkTarget = target;

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
                            if (NtfsFileSystem.DirectoryExists(target))
                            {
                                return GetDirectoryInfo(target, true, options);
                            }
                        }
                    }
                }

                return ntfsDirInfo;
            }
            catch (FileAttributeException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new NtfsAccessException($"Error getting directory info for {normalizedPath}: {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Gets all files in a directory
        /// </summary>
        /// <param name="directoryPath">Path to the directory</param>
        /// <param name="searchPattern">Search pattern</param>
        /// <param name="searchOption">Whether to include subdirectories</param>
        /// <param name="resolveLinks">Whether to resolve links</param>
        /// <param name="options">NTFS access options</param>
        /// <returns>List of file information objects</returns>
        public IEnumerable<NtfsFileInfo> GetFiles(
            string directoryPath, 
            string searchPattern = "*", 
            SearchOption searchOption = SearchOption.TopDirectoryOnly, 
            bool resolveLinks = true,
            RawNtfsOptions options = null)
        {
            options ??= new RawNtfsOptions();
            
            if (string.IsNullOrEmpty(directoryPath))
                throw new ArgumentException("Directory path cannot be null or empty", nameof(directoryPath));

            string normalizedPath = NormalizePath(directoryPath);
            
            if (!NtfsFileSystem.DirectoryExists(normalizedPath))
                throw new DirectoryNotFoundException($"Directory not found: {normalizedPath}");

            // Check if directory is a reparse point and resolve if needed
            try
            {
                var dirInfo = NtfsFileSystem.GetDirectoryInfo(normalizedPath);
                
                if (IsReparsePoint(dirInfo.Attributes) && resolveLinks)
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
                            
                            // If target exists, use it instead
                            if (NtfsFileSystem.DirectoryExists(target))
                            {
                                return GetFiles(target, searchPattern, searchOption, resolveLinks, options);
                            }
                        }
                    }
                }

                // Get files in the directory
                var files = new List<NtfsFileInfo>();
                
                // Use GetFiles from System.IO.Directory via DiscUtils
                var filePaths = NtfsFileSystem.GetFiles(normalizedPath, searchPattern);
                
                // Get info for each file
                foreach (var filePath in filePaths)
                {
                    try
                    {
                        string fullPath = Path.Combine(directoryPath, Path.GetFileName(filePath));
                        var fileInfo = GetFileInfo(filePath, resolveLinks, options);
                        
                        if (fileInfo != null)
                        {
                            // Update the full path to include the original directory path
                            fileInfo.FullPath = fullPath;
                            files.Add(fileInfo);
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Error getting file info for {filePath}: {ex.Message}");
                        // Continue with the next file
                    }
                }
                
                // Recursively get files from subdirectories if requested
                if (searchOption == SearchOption.AllDirectories)
                {
                    var subdirPaths = NtfsFileSystem.GetDirectories(normalizedPath);
                    
                    foreach (var subdirPath in subdirPaths)
                    {
                        try
                        {
                            string subdir = Path.Combine(directoryPath, Path.GetFileName(subdirPath));
                            var subdirFiles = GetFiles(subdir, searchPattern, searchOption, resolveLinks, options);
                            files.AddRange(subdirFiles);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Error processing subdirectory {subdirPath}: {ex.Message}");
                            // Continue with the next subdirectory
                        }
                    }
                }
                
                return files;
            }
            catch (Exception ex)
            {
                throw new NtfsAccessException($"Error getting files in directory {normalizedPath}: {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Gets all directories in a directory
        /// </summary>
        /// <param name="directoryPath">Path to the directory</param>
        /// <param name="searchPattern">Search pattern</param>
        /// <param name="searchOption">Whether to include subdirectories</param>
        /// <param name="resolveLinks">Whether to resolve links</param>
        /// <param name="options">NTFS access options</param>
        /// <returns>List of directory information objects</returns>
        public IEnumerable<NtfsDirectoryInfo> GetDirectories(
            string directoryPath, 
            string searchPattern = "*", 
            SearchOption searchOption = SearchOption.TopDirectoryOnly, 
            bool resolveLinks = true,
            RawNtfsOptions options = null)
        {
            options ??= new RawNtfsOptions();
            
            if (string.IsNullOrEmpty(directoryPath))
                throw new ArgumentException("Directory path cannot be null or empty", nameof(directoryPath));

            string normalizedPath = NormalizePath(directoryPath);
            
            if (!NtfsFileSystem.DirectoryExists(normalizedPath))
                throw new DirectoryNotFoundException($"Directory not found: {normalizedPath}");

            // Check if directory is a reparse point and resolve if needed
            try
            {
                var dirInfo = NtfsFileSystem.GetDirectoryInfo(normalizedPath);
                
                if (IsReparsePoint(dirInfo.Attributes) && resolveLinks)
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
                            
                            // If target exists, use it instead
                            if (NtfsFileSystem.DirectoryExists(target))
                            {
                                return GetDirectories(target, searchPattern, searchOption, resolveLinks, options);
                            }
                        }
                    }
                }

                // Get directories
                var directories = new List<NtfsDirectoryInfo>();
                
                // Use GetDirectories from System.IO.Directory via DiscUtils
                var subdirPaths = NtfsFileSystem.GetDirectories(normalizedPath, searchPattern);
                
                // Get info for each directory
                foreach (var subdirPath in subdirPaths)
                {
                    try
                    {
                        string fullPath = Path.Combine(directoryPath, Path.GetFileName(subdirPath));
                        var subdirInfo = GetDirectoryInfo(subdirPath, resolveLinks, options);
                        
                        if (subdirInfo != null)
                        {
                            // Update the full path to include the original directory path
                            subdirInfo.FullPath = fullPath;
                            directories.Add(subdirInfo);
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Error getting directory info for {subdirPath}: {ex.Message}");
                        // Continue with the next directory
                    }
                }
                
                // Recursively get directories from subdirectories if requested
                if (searchOption == SearchOption.AllDirectories)
                {
                    foreach (var subdir in directories.ToList()) // Create a copy to avoid modification during enumeration
                    {
                        try
                        {
                            var subdirs = GetDirectories(subdir.FullPath, searchPattern, searchOption, resolveLinks, options);
                            directories.AddRange(subdirs);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Error processing subdirectory {subdir.FullPath}: {ex.Message}");
                            // Continue with the next subdirectory
                        }
                    }
                }
                
                return directories;
            }
            catch (Exception ex)
            {
                throw new NtfsAccessException($"Error getting directories in directory {normalizedPath}: {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Gets information about a file
        /// </summary>
        /// <param name="filePath">Path to the file</param>
        /// <param name="resolveLinks">Whether to resolve links</param>
        /// <param name="options">NTFS access options</param>
        /// <returns>File information</returns>
        private NtfsFileInfo GetFileInfo(string filePath, bool resolveLinks, RawNtfsOptions options)
        {
            // Use full file path
            string normalizedPath = NormalizePath(filePath);

            try
            {
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

                // Check if it's a link and get the target
                if (IsReparsePoint(attributes))
                {
                    var (linkType, target) = _linkResolver.GetLinkTarget(normalizedPath);
                    ntfsFileInfo.LinkTarget = target;

                    // If requested, resolve the link target
                    if (resolveLinks && !string.IsNullOrEmpty(target))
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
            catch (Exception ex)
            {
                Console.WriteLine($"Error getting file info for {normalizedPath}: {ex.Message}");
                return null;
            }
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
            
            // Replace backslashes with forward slashes if needed
            // path = path.Replace(Path.DirectorySeparatorChar, '/');
            
            return path;
        }

        /// <summary>
        /// Gets the directory ID from the NTFS file system
        /// </summary>
        /// <param name="path">Path to the directory</param>
        /// <returns>Directory ID</returns>
        private long GetDirectoryId(string path)
        {
            try
            {
                // This is a workaround since we don't have direct access to the MFT record number
                // Use a unique identifier based on the directory path
                return path.GetHashCode();
            }
            catch
            {
                return 0;
            }
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
    }
}