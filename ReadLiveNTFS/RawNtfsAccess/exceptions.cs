using System;

namespace RawNtfsAccess.Exceptions
{
    /// <summary>
    /// Base exception for all NTFS access operations
    /// </summary>
    public class NtfsAccessException : Exception
    {
        public NtfsAccessException(string message) : base(message) { }
        public NtfsAccessException(string message, Exception innerException) 
            : base(message, innerException) { }
    }

    /// <summary>
    /// Exception thrown when attempting to access a non-NTFS volume or a corrupt NTFS volume
    /// </summary>
    public class InvalidNtfsVolumeException : NtfsAccessException
    {
        public string DrivePath { get; }

        public InvalidNtfsVolumeException(string drivePath, string message) 
            : base(message)
        {
            DrivePath = drivePath;
        }

        public InvalidNtfsVolumeException(string drivePath, string message, Exception innerException) 
            : base(message, innerException)
        {
            DrivePath = drivePath;
        }
    }

    /// <summary>
    /// Exception thrown when a circular link reference is detected
    /// </summary>
    public class LinkRecursionException : NtfsAccessException
    {
        public string LinkPath { get; }
        public int RecursionDepth { get; }

        public LinkRecursionException(string linkPath, int recursionDepth, string message) 
            : base(message)
        {
            LinkPath = linkPath;
            RecursionDepth = recursionDepth;
        }
    }

    /// <summary>
    /// Exception thrown when a specific file attribute operation fails
    /// </summary>
    public class FileAttributeException : NtfsAccessException
    {
        public string FilePath { get; }
        public string AttributeName { get; }

        public FileAttributeException(string filePath, string attributeName, string message) 
            : base(message)
        {
            FilePath = filePath;
            AttributeName = attributeName;
        }

        public FileAttributeException(string filePath, string attributeName, string message, Exception innerException) 
            : base(message, innerException)
        {
            FilePath = filePath;
            AttributeName = attributeName;
        }
    }
    
    /// <summary>
    /// Exception thrown when an alternate data stream operation fails
    /// </summary>
    public class AlternateDataStreamException : NtfsAccessException
    {
        public string FilePath { get; }
        public string StreamName { get; }

        public AlternateDataStreamException(string filePath, string streamName, string message) 
            : base(message)
        {
            FilePath = filePath;
            StreamName = streamName;
        }

        public AlternateDataStreamException(string filePath, string streamName, string message, Exception innerException) 
            : base(message, innerException)
        {
            FilePath = filePath;
            StreamName = streamName;
        }
    }
}
