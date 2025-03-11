using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using DiscUtils.Ntfs;
using RawNtfsAccess.Exceptions;

namespace RawNtfsAccess.IO
{
    /// <summary>
    /// Handles operations related to alternate data streams in NTFS files
    /// </summary>
    internal class AlternateDataStreamHandler
    {
        private readonly RawDiskReader _diskReader;

        /// <summary>
        /// Initializes a new instance of the AlternateDataStreamHandler class
        /// </summary>
        /// <param name="diskReader">The raw disk reader</param>
        public AlternateDataStreamHandler(RawDiskReader diskReader)
        {
            _diskReader = diskReader ?? throw new ArgumentNullException(nameof(diskReader));
        }

        /// <summary>
        /// Gets all alternate data stream names for a file
        /// </summary>
        /// <param name="filePath">Path to the file</param>
        /// <returns>List of alternate data stream names</returns>
        public IEnumerable<string> GetAlternateDataStreamNames(string filePath)
        {
            if (string.IsNullOrEmpty(filePath))
                throw new ArgumentNullException(nameof(filePath));

            try
            {
                string normalizedPath = NormalizePath(filePath);
                
                // Make sure the file exists before trying to get its streams
                if (!_diskReader.NtfsFileSystem.FileExists(normalizedPath))
                {
                    throw new FileNotFoundException($"File not found: {filePath}");
                }
                
                // Use the built-in DiscUtils method to get alternate data streams
                // In DiscUtils.Ntfs 0.16, the method is GetAlternateDataStreams
                var streams = _diskReader.NtfsFileSystem.GetAlternateDataStreams(normalizedPath);

                return streams ?? Enumerable.Empty<string>();
            }
            catch (Exception ex)
            {
                throw new AlternateDataStreamException(
                    filePath, 
                    string.Empty, 
                    $"Failed to get alternate data stream names: {ex.Message}", 
                    ex);
            }
        }

        /// <summary>
        /// Checks if a file has a specific alternate data stream
        /// </summary>
        /// <param name="filePath">Path to the file</param>
        /// <param name="streamName">Name of the alternate data stream</param>
        /// <returns>True if the alternate data stream exists; otherwise, false</returns>
        public bool HasAlternateDataStream(string filePath, string streamName)
        {
            if (string.IsNullOrEmpty(filePath))
                throw new ArgumentNullException(nameof(filePath));
                
            if (string.IsNullOrEmpty(streamName))
                throw new ArgumentException("Stream name cannot be null or empty", nameof(streamName));

            try
            {
                // Get all stream names and check if the requested one exists
                Console.WriteLine(filePath);
                var streamNames = GetAlternateDataStreamNames(filePath);
                return streamNames.Contains(streamName, StringComparer.OrdinalIgnoreCase);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error checking alternate data stream {streamName}: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Opens a stream to read from an alternate data stream
        /// </summary>
        /// <param name="filePath">Path to the file</param>
        /// <param name="streamName">Name of the alternate data stream</param>
        /// <param name="isSparse"></param>
        /// <param name="ntfsFileSystem"></param>
        /// <returns>Stream for reading the alternate data stream</returns>
        public Stream OpenAlternateDataStream(string filePath, string streamName, bool isSparse,
            NtfsFileSystem ntfsFileSystem)
        {
            if (string.IsNullOrEmpty(filePath))
                throw new ArgumentNullException(nameof(filePath));
                
            if (string.IsNullOrEmpty(streamName))
                throw new ArgumentException("Stream name cannot be null or empty", nameof(streamName));

            try
            {
                string normalizedPath = NormalizePath(filePath);

                // Make sure the main file exists
                if (!_diskReader.NtfsFileSystem.FileExists(normalizedPath))
                {
                    throw new FileNotFoundException($"File not found: {filePath}");
                }
                string adsPathnew = $"{normalizedPath}:{streamName}";
                if (isSparse)
                {
                    return new SparseFileStream(ntfsFileSystem, adsPathnew);
                }
                else
                {
                    return _diskReader.NtfsFileSystem.OpenFile(adsPathnew, FileMode.Open, FileAccess.Read);
                }

                // Check if the stream exists
                // TODO - Skipping this - we will just try to open the stream directly
                /*if (!HasAlternateDataStream(filePath, streamName))
                {
                    throw new AlternateDataStreamException(
                        filePath, 
                        streamName, 
                        $"Alternate data stream '{streamName}' not found");
                }*/
                
                try
                {
                    // Open the file first to get a handle on it
                    using (var fileStream = _diskReader.NtfsFileSystem.OpenFile(normalizedPath, FileMode.Open, FileAccess.Read))
                    {
                        Console.WriteLine(normalizedPath);
                        // Now try to open the specific stream
                        // The correct approach depends on DiscUtils.Ntfs version, but one of these should work:
                        
                        // Method 1: Use specific file path format (most common approach)
                        string adsPath = $"{normalizedPath}:{streamName}";
                        try
                        {
                            return _diskReader.NtfsFileSystem.OpenFile(adsPath, FileMode.Open, FileAccess.Read);
                        }
                        catch
                        {
                            // If that fails, try alternative methods
                            // Method 2: Some versions may need a different path format
                            if (normalizedPath.StartsWith("\\"))
                            {
                                // Try without the leading slash if it has one
                                string altPath = normalizedPath.Substring(1);
                                string altAdsPath = $"{altPath}:{streamName}";
                                return _diskReader.NtfsFileSystem.OpenFile(altAdsPath, FileMode.Open, FileAccess.Read);
                            }
                            
                            // If we get here, both methods failed
                            throw new AlternateDataStreamException(
                                filePath,
                                streamName,
                                "Could not open alternate data stream using available methods");
                        }
                    }
                }
                catch (AlternateDataStreamException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    throw new AlternateDataStreamException(
                        filePath,
                        streamName,
                        $"Failed to open alternate data stream: {ex.Message}",
                        ex);
                }
            }
            catch (FileNotFoundException)
            {
                throw;
            }
            catch (AlternateDataStreamException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new AlternateDataStreamException(
                    filePath, 
                    streamName, 
                    $"Failed to open alternate data stream: {ex.Message}", 
                    ex);
            }
        }

        /// <summary>
        /// Gets the length of an alternate data stream
        /// </summary>
        /// <param name="filePath">Path to the file</param>
        /// <param name="streamName">Name of the alternate data stream</param>
        /// <returns>Length of the alternate data stream in bytes</returns>
        public long GetAlternateDataStreamLength(string filePath, string streamName)
        {
            if (string.IsNullOrEmpty(filePath))
                throw new ArgumentNullException(nameof(filePath));
                
            if (string.IsNullOrEmpty(streamName))
                throw new ArgumentException("Stream name cannot be null or empty", nameof(streamName));

            try
            {
                // Check if the stream exists
                if (!HasAlternateDataStream(filePath, streamName))
                {
                    throw new AlternateDataStreamException(
                        filePath, 
                        streamName, 
                        $"Alternate data stream '{streamName}' not found");
                }
                
                // Open the stream and get its length
                using (var stream = OpenAlternateDataStream(filePath, streamName, false, null))
                {
                    return stream.Length;
                }
            }
            catch (AlternateDataStreamException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new AlternateDataStreamException(
                    filePath, 
                    streamName, 
                    $"Failed to get alternate data stream length: {ex.Message}", 
                    ex);
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
            
            // Handle paths with stream names separately
            string streamName = null;
            if (path.Contains(':'))
            {
                var parts = path.Split(new[] { ':' }, 2);
                path = parts[0];
                streamName = parts[1];
            }
            
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
            
            // Add back the stream name if it was present
            if (!string.IsNullOrEmpty(streamName))
            {
                path = $"{path}:{streamName}";
            }
            
            return path;
        }
    }
}