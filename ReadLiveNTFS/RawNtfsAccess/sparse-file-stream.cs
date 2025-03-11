using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using DiscUtils.Ntfs;
using DiscUtils.Streams;

namespace RawNtfsAccess.IO
{
    /// <summary>
    /// Provides a stream for efficiently reading sparse NTFS files by only reading non-sparse regions
    /// </summary>
    internal class SparseFileStream : Stream
    {
        private readonly NtfsFileSystem _ntfsFileSystem;
        private readonly string _filePath;
        private readonly long _length;
        private readonly Stream _baseStream;
        private readonly int _bufferSize;
        private readonly List<Tuple<long, long>> _dataRegions;
        private long _position;
        private bool _disposed;

        // Current data region being read
        private int _currentRegionIndex = -1;
        private long _currentRegionStart = 0;
        private long _currentRegionEnd = 0;

        /// <summary>
        /// Initializes a new instance of the SparseFileStream class
        /// </summary>
        /// <param name="ntfsFileSystem">The NTFS file system</param>
        /// <param name="filePath">Path to the file</param>
        /// <param name="bufferSize">Buffer size in bytes</param>
        public SparseFileStream(NtfsFileSystem ntfsFileSystem, string filePath, int bufferSize = 4096)
        {
            _ntfsFileSystem = ntfsFileSystem ?? throw new ArgumentNullException(nameof(ntfsFileSystem));
            _filePath = filePath ?? throw new ArgumentNullException(nameof(filePath));
            _bufferSize = bufferSize;

            if (!_ntfsFileSystem.FileExists(_filePath))
                throw new FileNotFoundException($"File not found: {_filePath}");

            var fileInfo = _ntfsFileSystem.GetFileInfo(_filePath);
            _length = fileInfo.Length;
            _position = 0;
            
            // Open the base stream
            _baseStream = _ntfsFileSystem.OpenFile(_filePath, FileMode.Open, FileAccess.Read);
            _disposed = false;
            
            // Initialize the data regions (non-sparse regions)
            _dataRegions = GetDataRegions();
            
            // Sort regions by start position
            _dataRegions = _dataRegions.OrderBy(r => r.Item1).ToList();
            
            // Set initial region index
            UpdateCurrentRegion();
        }

        /// <summary>
        /// Gets a value indicating whether the stream can be read
        /// </summary>
        public override bool CanRead => true;

        /// <summary>
        /// Gets a value indicating whether the stream can be written
        /// </summary>
        public override bool CanWrite => false;

        /// <summary>
        /// Gets a value indicating whether the stream can seek
        /// </summary>
        public override bool CanSeek => true;

        /// <summary>
        /// Gets the length of the stream
        /// </summary>
        public override long Length => _length;

        /// <summary>
        /// Gets or sets the position in the stream
        /// </summary>
        public override long Position
        {
            get => _position;
            set
            {
                if (value < 0)
                    throw new ArgumentOutOfRangeException(nameof(value), "Position cannot be negative");

                if (value != _position)
                {
                    _position = value;
                    UpdateCurrentRegion();
                }
            }
        }

        /// <summary>
        /// Updates the current region index based on the current position
        /// </summary>
        private void UpdateCurrentRegion()
        {
            // Reset current region
            _currentRegionIndex = -1;
            _currentRegionStart = 0;
            _currentRegionEnd = 0;
            
            // Find the region containing the current position
            for (int i = 0; i < _dataRegions.Count; i++)
            {
                var region = _dataRegions[i];
                long start = region.Item1;
                long end = start + region.Item2;
                
                if (_position >= start && _position < end)
                {
                    _currentRegionIndex = i;
                    _currentRegionStart = start;
                    _currentRegionEnd = end;
                    break;
                }
                
                // If we've passed the current position, there's no region containing it
                if (start > _position)
                    break;
            }
        }

        /// <summary>
        /// Gets the non-sparse data regions in the file
        /// </summary>
        /// <returns>List of tuples containing start position and length of data regions</returns>
        private List<Tuple<long, long>> GetDataRegions()
        {
            try
            {
                var regions = new List<Tuple<long, long>>();
                
                // Try to use DiscUtils to get data runs if it's available
                /*var dataRuns = TryGetDataRuns();
                
                if (dataRuns != null && dataRuns.Count > 0)
                {
                    return dataRuns;
                }*/
                
                // Fallback to a simple approach by analyzing the file
                return AnalyzeFileForDataRegions();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error getting data regions: {ex.Message}");
                
                // If all else fails, treat the entire file as one data region
                return new List<Tuple<long, long>> { Tuple.Create(0L, _length) };
            }
        }

        /// <summary>
        /// Tries to get data runs directly from NTFS metadata using DiscUtils
        /// </summary>
        /// <returns>List of data regions if successful; otherwise, null</returns>
        /// joeavanzato note: The AI has completely hallucinated this section so I removed it from functionality above
        private List<Tuple<long, long>> TryGetDataRuns()
        {
            try
            {
                // We need to access DiscUtils.Ntfs internals to get the data runs
                // First, get the NtfsFileSystem type for reflection
                Type ntfsFileSystemType = _ntfsFileSystem.GetType();
                
                // Get the internal GetFile method or similar method to access the NtfsFile object
                MethodInfo getFileMethod = ntfsFileSystemType.GetMethod("GetFile", 
                    BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public);
                
                if (getFileMethod == null)
                {
                    // Try alternative method names
                    getFileMethod = ntfsFileSystemType.GetMethod("OpenFile", 
                        BindingFlags.Instance | BindingFlags.NonPublic);
                    
                    if (getFileMethod == null)
                    {
                        // Get any method that might return a File object and has a string parameter
                        var methods = ntfsFileSystemType.GetMethods(BindingFlags.Instance | 
                            BindingFlags.NonPublic | BindingFlags.Public)
                            .Where(m => m.GetParameters().Length == 1 && 
                                m.GetParameters()[0].ParameterType == typeof(string))
                            .ToArray();
                        
                        foreach (var method in methods)
                        {
                            if (method.Name.Contains("File") || method.Name.Contains("Stream"))
                            {
                                getFileMethod = method;
                                break;
                            }
                        }
                        
                        if (getFileMethod == null)
                            return null; // Can't find appropriate method
                    }
                }
                
                // Invoke the method to get the internal file object
                object ntfsFile = getFileMethod.Invoke(_ntfsFileSystem, new object[] { _filePath });
                if (ntfsFile == null)
                    return null;
                
                // Now try to access the data attribute of the file
                Type fileType = ntfsFile.GetType();
                
                // DiscUtils.Ntfs uses an attribute system, look for methods to get data attribute
                MethodInfo getAttributeMethod = fileType.GetMethod("GetAttribute", 
                    BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
                
                if (getAttributeMethod == null)
                {
                    // Try to find any method that might give us the data attribute
                    var attrMethods = fileType.GetMethods(BindingFlags.Instance | 
                        BindingFlags.NonPublic | BindingFlags.Public)
                        .Where(m => m.Name.Contains("Attribute") || m.Name.Contains("Stream") || 
                            m.Name.Contains("Data"))
                        .ToArray();
                    
                    foreach (var method in attrMethods)
                    {
                        if (method.GetParameters().Length <= 2) // Accept methods with 0-2 parameters
                        {
                            getAttributeMethod = method;
                            break;
                        }
                    }
                    
                    if (getAttributeMethod == null)
                        return null;
                }
                
                // Find AttributeType enum for DATA
                Type attributeTypeType = null;
                object dataAttributeType = null;
                
                // Try to find the AttributeType enum
                var assemblies = AppDomain.CurrentDomain.GetAssemblies();
                foreach (var assembly in assemblies)
                {
                    var types = assembly.GetTypes();
                    foreach (var type in types)
                    {
                        if (type.IsEnum && type.Name == "AttributeType")
                        {
                            attributeTypeType = type;
                            break;
                        }
                    }
                    if (attributeTypeType != null)
                        break;
                }
                
                if (attributeTypeType != null)
                {
                    // Get the Data value from the AttributeType enum
                    foreach (var value in Enum.GetValues(attributeTypeType))
                    {
                        if (value.ToString() == "Data")
                        {
                            dataAttributeType = value;
                            break;
                        }
                    }
                }
                
                // Invoke the method to get the data attribute
                object dataAttribute = null;
                if (dataAttributeType != null)
                {
                    // Try with the AttributeType.Data and null for the name parameter
                    try
                    {
                        if (getAttributeMethod.GetParameters().Length == 2)
                        {
                            dataAttribute = getAttributeMethod.Invoke(ntfsFile, 
                                new object[] { dataAttributeType, null });
                        }
                        else if (getAttributeMethod.GetParameters().Length == 1)
                        {
                            dataAttribute = getAttributeMethod.Invoke(ntfsFile, 
                                new object[] { dataAttributeType });
                        }
                    }
                    catch
                    {
                        // Fallback to trying other approaches
                    }
                }
                
                // If we still don't have the data attribute, try other methods
                if (dataAttribute == null)
                {
                    // Try to find a property or method that gives us the data attribute
                    var dataProps = fileType.GetProperties(BindingFlags.Instance | 
                        BindingFlags.NonPublic | BindingFlags.Public)
                        .Where(p => p.Name.Contains("Data") || p.Name.Contains("Content") || 
                            p.Name.Contains("Stream"))
                        .ToArray();
                    
                    foreach (var prop in dataProps)
                    {
                        try
                        {
                            var value = prop.GetValue(ntfsFile);
                            if (value != null)
                            {
                                dataAttribute = value;
                                break;
                            }
                        }
                        catch
                        {
                            // Continue to next property
                        }
                    }
                }
                
                if (dataAttribute == null)
                    return null;
                
                // Now, try to get the data runs or extents from the data attribute
                Type dataAttrType = dataAttribute.GetType();
                object nonResidentData = null;
                
                // Look for a method to get non-resident content
                MethodInfo getNonResidentMethod = dataAttrType.GetMethod("GetNonResidentContent", 
                    BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
                
                if (getNonResidentMethod != null)
                {
                    try
                    {
                        nonResidentData = getNonResidentMethod.Invoke(dataAttribute, null);
                    }
                    catch
                    {
                        // Fallback to trying other approaches
                    }
                }
                
                // If that didn't work, look for a property that might give non-resident data
                if (nonResidentData == null)
                {
                    var contentProps = dataAttrType.GetProperties(BindingFlags.Instance | 
                        BindingFlags.NonPublic | BindingFlags.Public)
                        .Where(p => p.Name.Contains("Content") || p.Name.Contains("Data") || 
                            p.Name.Contains("Clusters") || p.Name.Contains("Extent"))
                        .ToArray();
                    
                    foreach (var prop in contentProps)
                    {
                        try
                        {
                            var value = prop.GetValue(dataAttribute);
                            if (value != null)
                            {
                                nonResidentData = value;
                                break;
                            }
                        }
                        catch
                        {
                            // Continue to next property
                        }
                    }
                }
                
                if (nonResidentData == null)
                    return null;
                
                // Now, try to get the extents from the non-resident data
                Type nonResidentType = nonResidentData.GetType();
                
                // Look for an Extents property
                PropertyInfo extentsProperty = nonResidentType.GetProperty("Extents", 
                    BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
                
                if (extentsProperty == null)
                {
                    // Try other properties that might contain extents information
                    var extentProps = nonResidentType.GetProperties(BindingFlags.Instance | 
                        BindingFlags.NonPublic | BindingFlags.Public)
                        .Where(p => p.Name.Contains("Extent") || p.Name.Contains("Cluster") || 
                            p.Name.Contains("Allocation"))
                        .ToArray();
                    
                    foreach (var prop in extentProps)
                    {
                        try
                        {
                            var value = prop.GetValue(nonResidentData);
                            if (value != null)
                            {
                                extentsProperty = prop;
                                break;
                            }
                        }
                        catch
                        {
                            // Continue to next property
                        }
                    }
                }
                
                if (extentsProperty == null)
                    return null;
                
                // Get the extents
                object extentsObj = extentsProperty.GetValue(nonResidentData);
                if (extentsObj == null)
                    return null;
                
                // Check if extents is enumerable
                if (!(extentsObj is System.Collections.IEnumerable enumerable))
                    return null;
                
                // Convert extents to data regions
                var dataRegions = new List<Tuple<long, long>>();
                foreach (var extent in enumerable)
                {
                    // Try to get the start and length of each extent
                    Type extentType = extent.GetType();
                    PropertyInfo startProp = extentType.GetProperty("Start") ?? 
                        extentType.GetProperty("Offset") ?? 
                        extentType.GetProperty("FirstCluster");
                    
                    PropertyInfo lengthProp = extentType.GetProperty("Length") ?? 
                        extentType.GetProperty("Size") ?? 
                        extentType.GetProperty("ClusterCount");
                    
                    if (startProp != null && lengthProp != null)
                    {
                        long start = Convert.ToInt64(startProp.GetValue(extent));
                        long length = Convert.ToInt64(lengthProp.GetValue(extent));
                        
                        // Convert from clusters to bytes if needed
                        // In NTFS, a cluster is typically 4KB, but can vary
                        // We need to check if we're dealing with clusters or bytes
                        if (start < _length / 10 && length < _length / 10)
                        {
                            // Most likely these are clusters, not bytes
                            // Try to find the bytes per cluster value
                            int bytesPerCluster = GetBytesPerCluster();
                            start *= bytesPerCluster;
                            length *= bytesPerCluster;
                        }
                        
                        // Add to our list, but only if the extent has valid length
                        if (length > 0)
                        {
                            dataRegions.Add(Tuple.Create(start, length));
                        }
                    }
                }
                
                // Sort the regions by start position
                return dataRegions.OrderBy(r => r.Item1).ToList();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error getting data runs: {ex.Message}");
                return null;
            }
        }

        /// <summary>
        /// Gets the bytes per cluster value from the NTFS file system
        /// </summary>
        /// <returns>Bytes per cluster value</returns>
        private int GetBytesPerCluster()
        {
            try
            {
                // Try to get bytes per cluster through reflection
                Type ntfsFileSystemType = _ntfsFileSystem.GetType();
                PropertyInfo clusterSizeProp = ntfsFileSystemType.GetProperty("ClusterSize") ?? 
                    ntfsFileSystemType.GetProperty("BytesPerCluster") ?? 
                    ntfsFileSystemType.GetProperty("SectorsPerCluster");
                
                if (clusterSizeProp != null)
                {
                    return Convert.ToInt32(clusterSizeProp.GetValue(_ntfsFileSystem));
                }
                
                // Fallback to standard NTFS defaults
                return 4096; // 4KB is a common cluster size
            }
            catch
            {
                return 4096; // Default fallback
            }
        }

        /// <summary>
        /// Analyzes the file to identify data regions by scanning the file content
        /// </summary>
        /// <returns>List of data regions</returns>
        private List<Tuple<long, long>> AnalyzeFileForDataRegions()
        {
            // This is a simplified approach that scans the file to identify non-zero regions
            // In a sparse file, regions of all zeros are not allocated on disk
            
            var regions = new List<Tuple<long, long>>();
            long startPos = 0;
            long currentPos = 0;
            bool inDataRegion = false;
            
            // Set a reasonable chunk size for scanning
            const int chunkSize = 65536; // 64 KB
            byte[] buffer = new byte[chunkSize];
            
            // Reset the base stream position
            _baseStream.Position = 0;
            
            while (currentPos < _length)
            {
                // Read a chunk
                int bytesRead = _baseStream.Read(buffer, 0, (int)Math.Min(chunkSize, _length - currentPos));
                
                if (bytesRead == 0)
                    break;
                
                // Check for non-zero data in this chunk
                bool hasNonZeroData = HasNonZeroData(buffer, bytesRead);
                
                if (hasNonZeroData && !inDataRegion)
                {
                    // Start of a new data region
                    startPos = currentPos;
                    inDataRegion = true;
                }
                else if (!hasNonZeroData && inDataRegion)
                {
                    // End of a data region
                    regions.Add(Tuple.Create(startPos, currentPos - startPos));
                    inDataRegion = false;
                }
                
                currentPos += bytesRead;
            }
            
            // If we ended in a data region, add it
            if (inDataRegion)
            {
                regions.Add(Tuple.Create(startPos, currentPos - startPos));
            }
            
            // If no regions were found (all zeros), return an empty list
            // This is the correct behavior for a completely sparse file
            
            // Reset the base stream position
            _baseStream.Position = 0;
            
            return regions;
        }

        /// <summary>
        /// Checks if a buffer contains any non-zero data
        /// </summary>
        /// <param name="buffer">Buffer to check</param>
        /// <param name="length">Length of data to check</param>
        /// <returns>True if non-zero data is found; otherwise, false</returns>
        private bool HasNonZeroData(byte[] buffer, int length)
        {
            for (int i = 0; i < length; i++)
            {
                if (buffer[i] != 0)
                {
                    return true;
                }
            }
            
            return false;
        }

        /// <summary>
        /// Reads data from the stream, skipping sparse regions
        /// </summary>
        /// <param name="buffer">Buffer to read into</param>
        /// <param name="offset">Offset in the buffer to start at</param>
        /// <param name="count">Number of bytes to read</param>
        /// <returns>Number of bytes read</returns>
        public override int Read(byte[] buffer, int offset, int count)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(SparseFileStream));

            if (buffer == null)
                throw new ArgumentNullException(nameof(buffer));

            if (offset < 0)
                throw new ArgumentOutOfRangeException(nameof(offset), "Offset cannot be negative");

            if (count < 0)
                throw new ArgumentOutOfRangeException(nameof(count), "Count cannot be negative");

            if (offset + count > buffer.Length)
                throw new ArgumentException("Buffer too small for the requested read operation");

            if (_position >= _length)
                return 0; // End of stream

            // Make sure we don't read beyond the end of the file
            if (_position + count > _length)
                count = (int)(_length - _position);

            // If we're not in a data region, find the next one
            if (_currentRegionIndex == -1)
            {
                // Find the next data region after our current position
                for (int i = 0; i < _dataRegions.Count; i++)
                {
                    var region = _dataRegions[i];
                    long start = region.Item1;
                    
                    if (start > _position)
                    {
                        // We're in a sparse region - skip to the next data region
                        _position = start;
                        _currentRegionIndex = i;
                        _currentRegionStart = start;
                        _currentRegionEnd = start + region.Item2;
                        break;
                    }
                }
                
                // If we didn't find a region, we're in a sparse region until EOF
                if (_currentRegionIndex == -1)
                {
                    // Skip to EOF
                    _position = _length;
                    return 0; // No data to read
                }
            }
            
            // We're in a data region - read from it
            long currentRegionRemaining = _currentRegionEnd - _position;
            int toRead = (int)Math.Min(count, currentRegionRemaining);
            
            if (toRead <= 0)
            {
                // We've reached the end of the current region
                // Move to the next region or sparse area
                _currentRegionIndex++;
                
                if (_currentRegionIndex < _dataRegions.Count)
                {
                    // Move to the next data region
                    var nextRegion = _dataRegions[_currentRegionIndex];
                    _position = nextRegion.Item1;
                    _currentRegionStart = nextRegion.Item1;
                    _currentRegionEnd = _currentRegionStart + nextRegion.Item2;
                    
                    // Recursive call to continue reading
                    return Read(buffer, offset, count);
                }
                else
                {
                    // No more data regions
                    _currentRegionIndex = -1;
                    _position = _length; // Skip to EOF
                    return 0;
                }
            }
            
            // Position the base stream
            _baseStream.Position = _position;
            
            // Read from the base stream
            int bytesRead = _baseStream.Read(buffer, offset, toRead);
            
            // Update our position
            _position += bytesRead;
            
            // If we've reached the end of this region, set up for the next read
            if (_position >= _currentRegionEnd)
            {
                _currentRegionIndex++;
                
                if (_currentRegionIndex >= _dataRegions.Count)
                {
                    _currentRegionIndex = -1; // No more regions
                }
                else
                {
                    var nextRegion = _dataRegions[_currentRegionIndex];
                    _currentRegionStart = nextRegion.Item1;
                    _currentRegionEnd = _currentRegionStart + nextRegion.Item2;
                }
            }
            
            return bytesRead;
        }

        /// <summary>
        /// Writes data to the stream
        /// </summary>
        /// <param name="buffer">Buffer to write from</param>
        /// <param name="offset">Offset in the buffer to start at</param>
        /// <param name="count">Number of bytes to write</param>
        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException("This stream does not support writing");
        }

        /// <summary>
        /// Sets the position in the stream
        /// </summary>
        /// <param name="offset">Offset from the origin</param>
        /// <param name="origin">Origin to seek from</param>
        /// <returns>New position in the stream</returns>
        public override long Seek(long offset, SeekOrigin origin)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(SparseFileStream));

            long newPosition;

            switch (origin)
            {
                case SeekOrigin.Begin:
                    newPosition = offset;
                    break;
                case SeekOrigin.Current:
                    newPosition = _position + offset;
                    break;
                case SeekOrigin.End:
                    newPosition = _length + offset;
                    break;
                default:
                    throw new ArgumentException("Invalid seek origin", nameof(origin));
            }

            if (newPosition < 0)
                throw new IOException("Cannot seek before the beginning of the stream");

            Position = newPosition; // This will update the current region
            return _position;
        }

        /// <summary>
        /// Clears all buffers for this stream and causes any buffered data to be written
        /// </summary>
        public override void Flush()
        {
            // No-op since stream is read-only
        }

        /// <summary>
        /// Sets the length of the stream
        /// </summary>
        /// <param name="value">New length of the stream</param>
        public override void SetLength(long value)
        {
            throw new NotSupportedException("Cannot set length of sparse file stream");
        }

        /// <summary>
        /// Disposes resources used by the stream
        /// </summary>
        /// <param name="disposing">Whether managed resources should be disposed</param>
        protected override void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    _baseStream?.Dispose();
                }

                _disposed = true;
            }

            base.Dispose(disposing);
        }
    }
}