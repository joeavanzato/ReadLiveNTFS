using System;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using Microsoft.Win32.SafeHandles;
using RawNtfsAccess;
using RawNtfsAccess.Models;
using RawNtfsAccess.Exceptions;

namespace RawNtfsAccessSample
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                // Configure options
                var options = new RawNtfsOptions
                {
                    BufferSize = 8 * 1024 * 1024, // 8MB buffer
                    MaxLinkDepth = 10,
                    FollowRelativeLinks = true,
                    FollowAbsoluteLinks = false
                };

                // Get system drive letter (usually C:)
                char systemDrive = Path.GetPathRoot(Environment.SystemDirectory)[0];
                Console.WriteLine($"Using system drive: {systemDrive}:");

                using (var ntfsAccessor = new RawNtfsAccessor(systemDrive, options))
                {
                    // Example 1: Copy a locked system file
                    Console.WriteLine("\nExample 1: Copy a locked system file (SOFTWARE)");
                    CopyLockedSystemFile(ntfsAccessor);
                    Console.WriteLine("####");

                    // Example 2: List contents of a restricted directory
                    Console.WriteLine("\nExample 2: List contents of a restricted directory");
                    ListRestrictedDirectory(ntfsAccessor);
                    Console.WriteLine("####");

                    // Example 3: Access alternate data streams
                    Console.WriteLine("\nExample 3: Access alternate data streams");
                    AccessAlternateDataStreams(ntfsAccessor);
                    Console.WriteLine("####");

                    // Example 4: Handle symbolic links and junctions
                    Console.WriteLine("\nExample 4: Handle symbolic links and junctions");
                    HandleSymbolicLinks(ntfsAccessor);
                    Console.WriteLine("####");

                    // Example 5: Copy $UsnJrnl:$J
                    Console.WriteLine("\nExample 5: $UsnJrnl:$J Extraction");
                    CopyUSNJournalJ(ntfsAccessor);
                    Console.WriteLine("####");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
                if (ex.InnerException != null)
                {
                    Console.WriteLine($"Inner error: {ex.InnerException.Message}");
                }
                Console.WriteLine(ex.StackTrace);
            }
            
        }

        static void CopyLockedSystemFile(RawNtfsAccessor accessor)
        {
            try
            {
                // Windows registry file (usually locked while system is running)
                string sourceFile = $"{accessor.DriveLetter}:\\Windows\\System32\\config\\SOFTWARE";
                string destinationFile = $"{accessor.DriveLetter}:\\Temp\\SOFTWARE";

                // Create destination directory if it doesn't exist
                Directory.CreateDirectory(Path.GetDirectoryName(destinationFile));

                Console.WriteLine($"Copying locked file: {sourceFile}");
                Console.WriteLine($"To: {destinationFile}");

                // Get file info (including size) before copying
                var fileInfo = accessor.GetFileInfo(sourceFile);
                Console.WriteLine($"File size: {FormatFileSize(fileInfo.Size)}");
                Console.WriteLine($"Created: {fileInfo.CreationTime}");
                Console.WriteLine($"Last modified: {fileInfo.LastWriteTime}");
                
                // Check if the file is sparse or compressed
                if (fileInfo.IsSparse)
                    Console.WriteLine("File is sparse");
                if (fileInfo.IsCompressed)
                    Console.WriteLine("File is compressed");

                // Get alternate data streams (if any)
                var adsNames = accessor.GetAlternateDataStreamNames(sourceFile);
                foreach (var adsName in adsNames)
                {
                    Console.WriteLine($"Found ADS: {adsName}");
                }

                // Perform the copy
                var startTime = DateTime.Now;
                accessor.CopyFile(sourceFile, destinationFile, true);
                var elapsedTime = DateTime.Now - startTime;

                Console.WriteLine($"File copied successfully in {elapsedTime.TotalSeconds:0.##} seconds");
                Console.WriteLine($"Destination file exists: {File.Exists(destinationFile)}");
                Console.WriteLine($"Destination file size: {FormatFileSize(new FileInfo(destinationFile).Length)}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to copy locked file: {ex.Message}");
            }
        }

        static void ListRestrictedDirectory(RawNtfsAccessor accessor)
        {
            try
            {
                // Windows directory that often has restricted permissions
                string restrictedDir = $"{accessor.DriveLetter}:\\Windows\\System32\\config";
                Console.WriteLine($"Listing contents of restricted directory: {restrictedDir}");

                // Get directory info
                var dirInfo = accessor.GetDirectoryInfo(restrictedDir);
                Console.WriteLine($"Directory created: {dirInfo.CreationTime}");
                Console.WriteLine($"Directory last modified: {dirInfo.LastWriteTime}");

                // Get files in the directory
                Console.WriteLine("\nFiles:");
                int fileCount = 0;
                long totalSize = 0;

                foreach (var file in accessor.GetFiles(restrictedDir))
                {
                    fileCount++;
                    totalSize += file.Size;
                    Console.WriteLine($"  {file.FullPath} ({FormatFileSize(file.Size)})");
                    
                    // Only show first 10 files to avoid flooding the console
                    if (fileCount >= 10)
                    {
                        Console.WriteLine("  ... (more files)");
                        break;
                    }
                }

                // Get subdirectories
                Console.WriteLine("\nSubdirectories:");
                int dirCount = 0;
                foreach (var subdir in accessor.GetDirectories(restrictedDir))
                {
                    dirCount++;
                    Console.WriteLine($"  {subdir.FullPath}");
                    
                    // Only show first 10 directories to avoid flooding the console
                    if (dirCount >= 10)
                    {
                        Console.WriteLine("  ... (more directories)");
                        break;
                    }
                }

                Console.WriteLine($"\nTotal: {fileCount} files, {dirCount} directories");
                Console.WriteLine($"Total file size: {FormatFileSize(totalSize)}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to list restricted directory: {ex.Message}");
            }
        }

        static void AccessAlternateDataStreams(RawNtfsAccessor accessor)
        {
            try
            {
                // Create a test file with alternate data streams
                string testDir = $"{accessor.DriveLetter}:\\Temp\\AdsTest";
                Directory.CreateDirectory(testDir);

                string testFile = Path.Combine(testDir, "test.txt");
                string testFileADS = testFile+":streamtest";
                File.WriteAllText(testFile, "This is the main content of the file.");
                SafeFileHandle handle = CreateFile(
                    testFileADS,
                    GENERIC_WRITE,
                    FILE_SHARE_NONE,
                    IntPtr.Zero,
                    CREATE_ALWAYS,
                    FILE_ATTRIBUTE_NORMAL,
                    IntPtr.Zero);
                if (handle.IsInvalid)
                {
                    Console.WriteLine("Failed to open ADS. Error code: " + Marshal.GetLastWin32Error());
                    return;
                }
                using (var fs = new FileStream(handle, FileAccess.Write))
                using (var writer = new StreamWriter(fs))
                {
                    writer.Write("Hello from the alternate data stream!");
                }
                Console.WriteLine($"Created test file with ADS: {testFile}");

                // Use raw accessor to read the alternate data streams
                var adsNames = accessor.GetAlternateDataStreamNames(testFile);
                Console.WriteLine($"Found {adsNames.Count()} alternate data streams:");

                foreach (var adsName in adsNames)
                {
                    Console.WriteLine($"  ADS: {adsName}");
                    
                    // Read the content of the ADS
                    using (var stream = accessor.OpenFile($"{testFile}:{adsName}"))
                    using (var reader = new StreamReader(stream))
                    {
                        string content = reader.ReadToEnd();
                        Console.WriteLine($"  Content: {content}");
                    }
                }

                // Copy the file with all its alternate data streams
                string copiedFile = Path.Combine(testDir, "test_copy.txt");
                Console.WriteLine($"\nCopying file with ADS to: {copiedFile}");
                accessor.CopyFile(testFile, copiedFile, true);

                // Verify that the ADS were copied
                var copiedAdsNames = accessor.GetAlternateDataStreamNames(copiedFile);
                Console.WriteLine($"Copied file has {copiedAdsNames.Count()} alternate data streams:");

                foreach (var adsName in copiedAdsNames)
                {
                    Console.WriteLine($"  ADS: {adsName}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to access alternate data streams: {ex.Message}");
                Console.WriteLine(ex.StackTrace);
            }
        }

        static void CopyUSNJournalJ(RawNtfsAccessor accessor)
        {
            // This works, just slow right now because we are doing naive parsing - TODO implement faster large-sparse parsing - skip right to data runs
            try
            {
                // Windows registry file (usually locked while system is running)
                string sourceFile = $"{accessor.DriveLetter}:\\$Extend\\$UsnJrnl:$J";
                string destinationFile = $"{accessor.DriveLetter}:\\Temp\\$J";

                // Create destination directory if it doesn't exist
                Directory.CreateDirectory(Path.GetDirectoryName(destinationFile));
                
                // Create destination file if it doesn't exist
                //File.Create(destinationFile);

                Console.WriteLine($"Copying locked file: {sourceFile}");
                Console.WriteLine($"To: {destinationFile}");

                // Get file info (including size) before copying
                var fileInfo = accessor.GetFileInfo(sourceFile);
                Console.WriteLine($"File size: {FormatFileSize(fileInfo.Size)}");
                Console.WriteLine($"Created: {fileInfo.CreationTime}");
                Console.WriteLine($"Last modified: {fileInfo.LastWriteTime}");
                
                // Check if the file is sparse or compressed
                if (fileInfo.IsSparse)
                    Console.WriteLine("File is sparse");
                if (fileInfo.IsCompressed)
                    Console.WriteLine("File is compressed");
                
                // Perform the copy
                var startTime = DateTime.Now;
                accessor.CopyFile(sourceFile, destinationFile, true);
                var elapsedTime = DateTime.Now - startTime;

                Console.WriteLine($"File copied successfully in {elapsedTime.TotalSeconds:0.##} seconds");
                Console.WriteLine($"Destination file exists: {File.Exists(destinationFile)}");
                Console.WriteLine($"Destination file size: {FormatFileSize(new FileInfo(destinationFile).Length)}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to copy locked file: {ex.Message}");
                Console.WriteLine(ex.StackTrace);
            }

        }

        static void HandleSymbolicLinks(RawNtfsAccessor accessor)
        {
            try
            {
                // Windows has several built-in symbolic links
                string documentsAndSettings = $"{accessor.DriveLetter}:\\Documents and Settings";
                
                // Check if it exists
                if (!accessor.DirectoryExists(documentsAndSettings))
                {
                    Console.WriteLine($"{documentsAndSettings} does not exist or is not accessible.");
                    return;
                }

                Console.WriteLine($"Analyzing symbolic link: {documentsAndSettings}");

                // Get info about the link
                var dirInfo = accessor.GetDirectoryInfo(documentsAndSettings);
                if (dirInfo.IsReparsePoint)
                {
                    Console.WriteLine("This is a reparse point (symbolic link or junction).");
                    Console.WriteLine($"Link target: {dirInfo.LinkTarget}");

                    // Resolve the link
                    string resolvedPath = accessor.ResolveLinkTarget(documentsAndSettings);
                    Console.WriteLine($"Resolved target: {resolvedPath}");

                    // List some files in the resolved directory
                    Console.WriteLine("\nFiles in the target directory:");
                    try
                    {
                        int count = 0;
                        foreach (var file in accessor.GetFiles(resolvedPath))
                        {
                            count++;
                            Console.WriteLine($"  {file.FullPath}");
                            
                            // Only show first 5 files
                            if (count >= 5)
                            {
                                Console.WriteLine("  ... (more files)");
                                break;
                            }
                        }

                        if (count == 0)
                        {
                            Console.WriteLine("  (No files found)");
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"  Error listing files: {ex.Message}");
                    }
                    Console.WriteLine("\nDirs in the target directory:");
                    try
                    {
                        int count = 0;
                        foreach (var file in accessor.GetDirectories(resolvedPath))
                        {
                            count++;
                            Console.WriteLine($"  {file.FullPath}");
                            
                            // Only show first 5 files
                            if (count >= 5)
                            {
                                Console.WriteLine("  ... (more dirs)");
                                break;
                            }
                        }

                        if (count == 0)
                        {
                            Console.WriteLine("  (No dirs found)");
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"  Error listing files: {ex.Message}");
                    }
                }
                else
                {
                    Console.WriteLine("This is not a reparse point.");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to handle symbolic links: {ex.Message}");
                Console.WriteLine(ex.StackTrace);
            }
        }

        static string FormatFileSize(long bytes)
        {
            string[] suffixes = { "B", "KB", "MB", "GB", "TB", "PB" };
            int suffixIndex = 0;
            double size = bytes;

            while (size >= 1024 && suffixIndex < suffixes.Length - 1)
            {
                size /= 1024;
                suffixIndex++;
            }

            return $"{size:0.##} {suffixes[suffixIndex]}";
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
    }
    
}