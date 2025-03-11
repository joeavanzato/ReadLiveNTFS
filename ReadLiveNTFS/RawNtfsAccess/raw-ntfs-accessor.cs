using System;
using System.Collections.Generic;
using System.IO;
using RawNtfsAccess.Exceptions;
using RawNtfsAccess.IO;
using RawNtfsAccess.Links;
using RawNtfsAccess.Models;

namespace RawNtfsAccess
{
    /// <summary>
    /// Provides access to locked files and directories in NTFS volumes
    /// </summary>
    public class RawNtfsAccessor : IDisposable
    {
        private readonly RawDiskReader _diskReader;
        private readonly LinkResolver _linkResolver;
        private readonly NtfsFileReader _fileReader;
        private readonly NtfsDirectoryReader _directoryReader;
        private readonly char _driveLetter;
        private readonly RawNtfsOptions _options;
        private bool _disposed;

        /// <summary>
        /// Initializes a new instance of the RawNtfsAccessor class
        /// </summary>
        /// <param name="driveLetter">Drive letter (e.g., 'C')</param>
        /// <param name="options">NTFS access options</param>
        public RawNtfsAccessor(char driveLetter, RawNtfsOptions options = null)
        {
            _driveLetter = char.ToUpper(driveLetter);
            _options = options ?? new RawNtfsOptions();
            _diskReader = new RawDiskReader(_driveLetter);
            _linkResolver = new LinkResolver(_diskReader);
            _fileReader = new NtfsFileReader(_diskReader, _linkResolver);
            _directoryReader = new NtfsDirectoryReader(_diskReader, _linkResolver);
            _disposed = false;
        }

        /// <summary>
        /// Gets the drive letter
        /// </summary>
        public char DriveLetter => _driveLetter;

        /// <summary>
        /// Gets the NTFS access options
        /// </summary>
        public RawNtfsOptions Options => _options;

        #region File Operations

        /// <summary>
        /// Checks if a file exists
        /// </summary>
        /// <param name="filePath">Path to the file</param>
        /// <returns>True if the file exists; otherwise, false</returns>
        public bool FileExists(string filePath)
        {
            if (string.IsNullOrEmpty(filePath))
                throw new ArgumentException("File path cannot be null or empty", nameof(filePath));

            ThrowIfDisposed();

            try
            {
                return _fileReader.FileExists(filePath);
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
        /// <param name="filePath">Path to the file</param>
        /// <param name="resolveLinks">Whether to resolve links</param>
        /// <returns>File information</returns>
        public NtfsFileInfo GetFileInfo(string filePath, bool resolveLinks = true)
        {
            if (string.IsNullOrEmpty(filePath))
                throw new ArgumentException("File path cannot be null or empty", nameof(filePath));

            ThrowIfDisposed();

            try
            {
                return _fileReader.GetFileInfo(filePath, resolveLinks, _options);
            }
            catch (Exception ex)
            {
                throw new NtfsAccessException($"Error getting file info for {filePath}: {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Opens a read-only stream to a file
        /// </summary>
        /// <param name="filePath">Path to the file</param>
        /// <returns>Stream for reading the file</returns>
        public Stream OpenFile(string filePath)
        {
            if (string.IsNullOrEmpty(filePath))
                throw new ArgumentException("File path cannot be null or empty", nameof(filePath));

            ThrowIfDisposed();

            try
            {
                return _fileReader.OpenFile(filePath, _options);
            }
            catch (Exception ex)
            {
                throw new NtfsAccessException($"Error opening file {filePath}: {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Copies a file to a destination
        /// </summary>
        /// <param name="sourcePath">Source file path</param>
        /// <param name="destinationPath">Destination file path</param>
        /// <param name="overwrite">Whether to overwrite the destination file if it exists</param>
        public void CopyFile(string sourcePath, string destinationPath, bool overwrite = false)
        {
            if (string.IsNullOrEmpty(sourcePath))
                throw new ArgumentException("Source path cannot be null or empty", nameof(sourcePath));
                
            if (string.IsNullOrEmpty(destinationPath))
                throw new ArgumentException("Destination path cannot be null or empty", nameof(destinationPath));

            ThrowIfDisposed();

            try
            {
                _fileReader.CopyFile(sourcePath, destinationPath, overwrite, _options);
            }
            catch (Exception ex)
            {
                throw new NtfsAccessException($"Error copying file from {sourcePath} to {destinationPath}: {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Checks if a file has an alternate data stream
        /// </summary>
        /// <param name="filePath">Path to the file</param>
        /// <param name="streamName">Name of the alternate data stream</param>
        /// <returns>True if the alternate data stream exists; otherwise, false</returns>
        public bool HasAlternateDataStream(string filePath, string streamName)
        {
            if (string.IsNullOrEmpty(filePath))
                throw new ArgumentException("File path cannot be null or empty", nameof(filePath));
                
            if (string.IsNullOrEmpty(streamName))
                throw new ArgumentException("Stream name cannot be null or empty", nameof(streamName));

            ThrowIfDisposed();

            try
            {
                var fileInfo = GetFileInfo(filePath, false);
                return fileInfo.AlternateDataStreams.Contains(streamName);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error checking alternate data stream: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Gets all alternate data stream names for a file
        /// </summary>
        /// <param name="filePath">Path to the file</param>
        /// <returns>List of alternate data stream names</returns>
        public IEnumerable<string> GetAlternateDataStreamNames(string filePath)
        {
            if (string.IsNullOrEmpty(filePath))
                throw new ArgumentException("File path cannot be null or empty", nameof(filePath));

            ThrowIfDisposed();

            try
            {
                var fileInfo = GetFileInfo(filePath, false);
                return fileInfo.AlternateDataStreams;
            }
            catch (Exception ex)
            {
                throw new NtfsAccessException($"Error getting alternate data stream names for {filePath}: {ex.Message}", ex);
            }
        }

        #endregion

        #region Directory Operations

        /// <summary>
        /// Checks if a directory exists
        /// </summary>
        /// <param name="directoryPath">Path to the directory</param>
        /// <returns>True if the directory exists; otherwise, false</returns>
        public bool DirectoryExists(string directoryPath)
        {
            if (string.IsNullOrEmpty(directoryPath))
                throw new ArgumentException("Directory path cannot be null or empty", nameof(directoryPath));

            ThrowIfDisposed();

            try
            {
                return _directoryReader.DirectoryExists(directoryPath);
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
        /// <param name="directoryPath">Path to the directory</param>
        /// <param name="resolveLinks">Whether to resolve links</param>
        /// <returns>Directory information</returns>
        public NtfsDirectoryInfo GetDirectoryInfo(string directoryPath, bool resolveLinks = true)
        {
            if (string.IsNullOrEmpty(directoryPath))
                throw new ArgumentException("Directory path cannot be null or empty", nameof(directoryPath));

            ThrowIfDisposed();

            try
            {
                return _directoryReader.GetDirectoryInfo(directoryPath, resolveLinks, _options);
            }
            catch (Exception ex)
            {
                throw new NtfsAccessException($"Error getting directory info for {directoryPath}: {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Gets all files in a directory
        /// </summary>
        /// <param name="directoryPath">Path to the directory</param>
        /// <param name="searchPattern">Search pattern</param>
        /// <param name="searchOption">Whether to include subdirectories</param>
        /// <param name="resolveLinks">Whether to resolve links</param>
        /// <returns>List of file information objects</returns>
        public IEnumerable<NtfsFileInfo> GetFiles(
            string directoryPath, 
            string searchPattern = "*", 
            SearchOption searchOption = SearchOption.TopDirectoryOnly, 
            bool resolveLinks = true)
        {
            if (string.IsNullOrEmpty(directoryPath))
                throw new ArgumentException("Directory path cannot be null or empty", nameof(directoryPath));

            ThrowIfDisposed();

            try
            {
                return _directoryReader.GetFiles(directoryPath, searchPattern, searchOption, resolveLinks, _options);
            }
            catch (Exception ex)
            {
                throw new NtfsAccessException($"Error getting files in directory {directoryPath}: {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Gets all directories in a directory
        /// </summary>
        /// <param name="directoryPath">Path to the directory</param>
        /// <param name="searchPattern">Search pattern</param>
        /// <param name="searchOption">Whether to include subdirectories</param>
        /// <param name="resolveLinks">Whether to resolve links</param>
        /// <returns>List of directory information objects</returns>
        public IEnumerable<NtfsDirectoryInfo> GetDirectories(
            string directoryPath, 
            string searchPattern = "*", 
            SearchOption searchOption = SearchOption.TopDirectoryOnly, 
            bool resolveLinks = true)
        {
            if (string.IsNullOrEmpty(directoryPath))
                throw new ArgumentException("Directory path cannot be null or empty", nameof(directoryPath));

            ThrowIfDisposed();

            try
            {
                return _directoryReader.GetDirectories(directoryPath, searchPattern, searchOption, resolveLinks, _options);
            }
            catch (Exception ex)
            {
                throw new NtfsAccessException($"Error getting directories in directory {directoryPath}: {ex.Message}", ex);
            }
        }

        #endregion

        #region Link Operations

        /// <summary>
        /// Resolves a link target to its final destination
        /// </summary>
        /// <param name="linkPath">Path to the link</param>
        /// <returns>Path to the final target</returns>
        public string ResolveLinkTarget(string linkPath)
        {
            if (string.IsNullOrEmpty(linkPath))
                throw new ArgumentException("Link path cannot be null or empty", nameof(linkPath));

            ThrowIfDisposed();

            try
            {
                return _linkResolver.ResolveLinkTarget(linkPath, _options);
            }
            catch (Exception ex)
            {
                throw new NtfsAccessException($"Error resolving link target for {linkPath}: {ex.Message}", ex);
            }
        }

        #endregion

        #region IDisposable Implementation

        /// <summary>
        /// Disposes resources used by the RawNtfsAccessor
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Disposes resources used by the RawNtfsAccessor
        /// </summary>
        /// <param name="disposing">Whether managed resources should be disposed</param>
        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    _diskReader?.Dispose();
                }

                _disposed = true;
            }
        }

        /// <summary>
        /// Throws an ObjectDisposedException if the accessor has been disposed
        /// </summary>
        private void ThrowIfDisposed()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(RawNtfsAccessor));
        }

        #endregion
    }
}
