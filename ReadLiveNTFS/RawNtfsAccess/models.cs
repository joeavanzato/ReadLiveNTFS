using System;
using System.Collections.Generic;
using System.IO;
using DiscUtils.Ntfs;

namespace RawNtfsAccess.Models
{
    /// <summary>
    /// Configuration options for raw NTFS access operations
    /// </summary>
    public class RawNtfsOptions
    {
        /// <summary>
        /// Size of the buffer used for reading data (in bytes)
        /// </summary>
        public int BufferSize { get; set; } = 4 * 1024 * 1024; // 4MB default

        /// <summary>
        /// Maximum depth for resolving symbolic links and junction points
        /// </summary>
        public int MaxLinkDepth { get; set; } = 10;

        /// <summary>
        /// Whether to follow relative symbolic links
        /// </summary>
        public bool FollowRelativeLinks { get; set; } = true;

        /// <summary>
        /// Whether to follow absolute symbolic links
        /// </summary>
        public bool FollowAbsoluteLinks { get; set; } = false;
    }

    /// <summary>
    /// Represents information about an NTFS file accessible via raw disk access
    /// </summary>
    public class NtfsFileInfo
    {
        /// <summary>
        /// Full path to the file
        /// </summary>
        public string FullPath { get; set; }

        /// <summary>
        /// File size in bytes
        /// </summary>
        public long Size { get; set; }

        /// <summary>
        /// File creation time
        /// </summary>
        public DateTime CreationTime { get; set; }

        /// <summary>
        /// Last access time
        /// </summary>
        public DateTime LastAccessTime { get; set; }

        /// <summary>
        /// Last write time
        /// </summary>
        public DateTime LastWriteTime { get; set; }

        /// <summary>
        /// File attributes
        /// </summary>
        public FileAttributes Attributes { get; set; }

        /// <summary>
        /// Whether the file is compressed
        /// </summary>
        public bool IsCompressed => (Attributes & FileAttributes.Compressed) == FileAttributes.Compressed;

        /// <summary>
        /// Whether the file is sparse
        /// </summary>
        public bool IsSparse => (Attributes & FileAttributes.SparseFile) == FileAttributes.SparseFile;

        /// <summary>
        /// Whether the file is a reparse point (symbolic link or junction)
        /// </summary>
        public bool IsReparsePoint => (Attributes & FileAttributes.ReparsePoint) == FileAttributes.ReparsePoint;

        /// <summary>
        /// Reference to the underlying file record
        /// </summary>
        public long FileId { get; set; }

        /// <summary>
        /// Names of the alternate data streams
        /// </summary>
        public List<string> AlternateDataStreams { get; set; } = new List<string>();

        /// <summary>
        /// Target path if the file is a link
        /// </summary>
        public string LinkTarget { get; set; }
    }

    /// <summary>
    /// Represents information about a directory in NTFS
    /// </summary>
    public class NtfsDirectoryInfo
    {
        /// <summary>
        /// Full path to the directory
        /// </summary>
        public string FullPath { get; set; }

        /// <summary>
        /// Directory creation time
        /// </summary>
        public DateTime CreationTime { get; set; }

        /// <summary>
        /// Last access time
        /// </summary>
        public DateTime LastAccessTime { get; set; }

        /// <summary>
        /// Last write time
        /// </summary>
        public DateTime LastWriteTime { get; set; }

        /// <summary>
        /// Directory attributes
        /// </summary>
        public FileAttributes Attributes { get; set; }

        /// <summary>
        /// Whether the directory is a reparse point (symbolic link or junction)
        /// </summary>
        public bool IsReparsePoint => (Attributes & FileAttributes.ReparsePoint) == FileAttributes.ReparsePoint;

        /// <summary>
        /// Reference to the underlying directory record
        /// </summary>
        public long DirectoryId { get; set; }

        /// <summary>
        /// Target path if the directory is a link
        /// </summary>
        public string LinkTarget { get; set; }
    }
}

namespace RawNtfsAccess.Links
{
    /// <summary>
    /// Types of NTFS links
    /// </summary>
    public enum LinkType
    {
        /// <summary>
        /// Not a link
        /// </summary>
        None,

        /// <summary>
        /// Junction point (directory symbolic link)
        /// </summary>
        Junction,

        /// <summary>
        /// Symbolic link to a file
        /// </summary>
        SymbolicFile,

        /// <summary>
        /// Symbolic link to a directory
        /// </summary>
        SymbolicDirectory,

        /// <summary>
        /// Hard link
        /// </summary>
        HardLink
    }
}
