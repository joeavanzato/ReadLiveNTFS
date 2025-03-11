using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;
using DiscUtils.Ntfs;
using DiscUtils.Streams;
using RawDiskLib;
using RawNtfsAccess.Exceptions;

namespace RawNtfsAccess.IO
{
    /// <summary>
    /// Provides low-level access to raw NTFS disks
    /// </summary>
    internal class RawDiskReader : IDisposable
    {
        private readonly RawDisk _rawDisk;
        private readonly NtfsFileSystem _ntfsFileSystem;
        private readonly string _driveLetter;
        private bool _disposed = false;

        /// <summary>
        /// Initializes a new instance of the RawDiskReader class
        /// </summary>
        /// <param name="driveLetter">Drive letter (e.g., 'C')</param>
        public RawDiskReader(char driveLetter)
        {
            _driveLetter = driveLetter.ToString().ToUpper();
            
            try
            {
                // Initialize raw disk access
                _rawDisk = new RawDisk(driveLetter);
                
                // Create a stream to access the disk
                var diskStream = new RawDiskStream(_rawDisk);
                
                // Initialize NTFS file system from the disk stream
                _ntfsFileSystem = new NtfsFileSystem(diskStream);
                
                // Verify that the file system is valid
                if (!IsValidNtfsFileSystem(_ntfsFileSystem))
                {
                    throw new InvalidNtfsVolumeException(
                        $"{_driveLetter}:", 
                        $"The volume {_driveLetter}: does not appear to be a valid NTFS volume.");
                }
            }
            catch (InvalidNtfsVolumeException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new InvalidNtfsVolumeException(
                    $"{_driveLetter}:", 
                    $"Failed to access NTFS volume {_driveLetter}:. {ex.Message}",
                    ex);
            }
        }

        /// <summary>
        /// Gets the NTFS file system
        /// </summary>
        public NtfsFileSystem NtfsFileSystem => _ntfsFileSystem;

        /// <summary>
        /// Gets the underlying raw disk
        /// </summary>
        public RawDisk RawDisk => _rawDisk;

        /// <summary>
        /// Gets the drive letter
        /// </summary>
        public string DriveLetter => _driveLetter;

        /// <summary>
        /// Checks if the NTFS file system is valid
        /// </summary>
        /// <param name="fileSystem">The NTFS file system to check</param>
        /// <returns>True if the file system is valid; otherwise, false</returns>
        private bool IsValidNtfsFileSystem(NtfsFileSystem fileSystem)
        {
            // Check if the file system is valid
            try
            {
                // Try to access the root directory
                fileSystem.GetDirectoryInfo("\\");
                return true;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Disposes resources used by the RawDiskReader
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Disposes resources used by the RawDiskReader
        /// </summary>
        /// <param name="disposing">Whether managed resources should be disposed</param>
        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    _ntfsFileSystem?.Dispose();
                    _rawDisk?.Dispose();
                }

                _disposed = true;
            }
        }
    }

    /// <summary>
    /// Provides a stream interface over the raw disk
    /// </summary>
    internal class RawDiskStream : SparseStream
    {
        private readonly RawDisk _rawDisk;
        private long _position;
        private bool _disposed = false;

        /// <summary>
        /// Initializes a new instance of the RawDiskStream class
        /// </summary>
        /// <param name="rawDisk">The raw disk to stream from</param>
        public RawDiskStream(RawDisk rawDisk)
        {
            _rawDisk = rawDisk ?? throw new ArgumentNullException(nameof(rawDisk));
            _position = 0;
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
        public override long Length => _rawDisk.DiskInfo.DiskSize;

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

                _position = value;
            }
        }

        /// <summary>
        /// Gets the parts of the stream that are mapped
        /// </summary>
        public override IEnumerable<StreamExtent> Extents => new StreamExtent[] { new StreamExtent(0, Length) };

        /// <summary>
        /// Reads data from the stream
        /// </summary>
        /// <param name="buffer">Buffer to read into</param>
        /// <param name="offset">Offset in the buffer to start at</param>
        /// <param name="count">Number of bytes to read</param>
        /// <returns>Number of bytes read</returns>
        public override int Read(byte[] buffer, int offset, int count)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(RawDiskStream));

            if (buffer == null)
                throw new ArgumentNullException(nameof(buffer));

            if (offset < 0)
                throw new ArgumentOutOfRangeException(nameof(offset), "Offset cannot be negative");

            if (count < 0)
                throw new ArgumentOutOfRangeException(nameof(count), "Count cannot be negative");

            if (offset + count > buffer.Length)
                throw new ArgumentException("Buffer too small for the requested read operation");

            if (_position >= Length)
                return 0;

            // Adjust count to not read beyond the end of the stream
            if (_position + count > Length)
                count = (int)(Length - _position);

            try
            {
                int bytesRead = 0;
                int sectorSize = _rawDisk.DiskInfo.BytesPerSector;
                long sectorNum = _position / sectorSize;
                int sectorOffset = (int)(_position % sectorSize);

                // If not aligned with sector boundaries, handle partial sector read at the beginning
                if (sectorOffset != 0)
                {
                    byte[] sectorData = _rawDisk.ReadSectors(sectorNum, 1);

                    int bytesToCopy = Math.Min(sectorSize - sectorOffset, count);
                    Array.Copy(sectorData, sectorOffset, buffer, offset, bytesToCopy);

                    bytesRead += bytesToCopy;
                    _position += bytesToCopy;
                    offset += bytesToCopy;
                    count -= bytesToCopy;
                    sectorNum++;
                }

                // Read full sectors in chunks
                while (count >= sectorSize)
                {
                    int sectorsToRead = Math.Min(count / sectorSize, 128); // Read up to 128 sectors at once
                    
                    // Use ReadSectors to read multiple sectors at once
                    byte[] sectorData = _rawDisk.ReadSectors(sectorNum, sectorsToRead);
                    Array.Copy(sectorData, 0, buffer, offset, sectorsToRead * sectorSize);

                    int bytesToCopy = sectorsToRead * sectorSize;
                    bytesRead += bytesToCopy;
                    _position += bytesToCopy;
                    offset += bytesToCopy;
                    count -= bytesToCopy;
                    sectorNum += sectorsToRead;
                }

                // If there are remaining bytes to read (less than a sector)
                if (count > 0)
                {
                    byte[] sectorData = _rawDisk.ReadSectors(sectorNum, 1);

                    Array.Copy(sectorData, 0, buffer, offset, count);

                    bytesRead += count;
                    _position += count;
                }

                return bytesRead;
            }
            catch (Exception ex)
            {
                throw new IOException($"Error reading from raw disk: {ex.Message}", ex);
            }
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
                throw new ObjectDisposedException(nameof(RawDiskStream));

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
                    newPosition = Length + offset;
                    break;
                default:
                    throw new ArgumentException("Invalid seek origin", nameof(origin));
            }

            if (newPosition < 0)
                throw new IOException("Cannot seek before the beginning of the stream");

            _position = newPosition;
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
            throw new NotSupportedException("Cannot set length of raw disk stream");
        }

        /// <summary>
        /// Disposes resources used by the stream
        /// </summary>
        /// <param name="disposing">Whether managed resources should be disposed</param>
        protected override void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                _disposed = true;
            }

            base.Dispose(disposing);
        }
    }
}