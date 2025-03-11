using System;
using System.IO;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using RawNtfsAccess;
using RawNtfsAccess.Models;
using RawNtfsAccess.Exceptions;

namespace RawNtfsAccess.Tests
{
    [TestClass]
    public class IntegrationTests
    {
        private char _systemDrive;
        private string _testDir;
        private string _testFile;
        private string _testAdsFile;

        [TestInitialize]
        public void Initialize()
        {
            // Get system drive
            _systemDrive = Path.GetPathRoot(Environment.SystemDirectory)[0];
            
            // Create test directory and files
            _testDir = Path.Combine(Path.GetTempPath(), "RawNtfsAccess_Tests");
            _testFile = Path.Combine(_testDir, "test.txt");
            _testAdsFile = Path.Combine(_testDir, "adsTest.txt");
            
            Directory.CreateDirectory(_testDir);
            
            // Create test file
            File.WriteAllText(_testFile, "This is test content");
            
            // Create file with ADS
            File.WriteAllText(_testAdsFile, "Main content");
            File.WriteAllText($"{_testAdsFile}:ads1", "ADS1 content");
            File.WriteAllText($"{_testAdsFile}:ads2", "ADS2 content");
        }

        [TestCleanup]
        public void Cleanup()
        {
            try
            {
                if (Directory.Exists(_testDir))
                {
                    Directory.Delete(_testDir, true);
                }
            }
            catch
            {
                // Ignore cleanup errors
            }
        }

        [TestMethod]
        public void TestFileOperations()
        {
            using (var accessor = new RawNtfsAccessor(_systemDrive))
            {
                // Test file exists
                Assert.IsTrue(accessor.FileExists(_testFile));
                
                // Test get file info
                var fileInfo = accessor.GetFileInfo(_testFile);
                Assert.IsNotNull(fileInfo);
                Assert.AreEqual(_testFile, fileInfo.FullPath);
                Assert.AreEqual(19, fileInfo.Size); // "This is test content" = 19 bytes
                
                // Test open file
                using (var stream = accessor.OpenFile(_testFile))
                using (var reader = new StreamReader(stream))
                {
                    string content = reader.ReadToEnd();
                    Assert.AreEqual("This is test content", content);
                }
                
                // Test copy file
                string destFile = Path.Combine(_testDir, "test_copy.txt");
                accessor.CopyFile(_testFile, destFile);
                Assert.IsTrue(File.Exists(destFile));
                Assert.AreEqual("This is test content", File.ReadAllText(destFile));
            }
        }

        [TestMethod]
        public void TestDirectoryOperations()
        {
            using (var accessor = new RawNtfsAccessor(_systemDrive))
            {
                // Test directory exists
                Assert.IsTrue(accessor.DirectoryExists(_testDir));
                
                // Test get directory info
                var dirInfo = accessor.GetDirectoryInfo(_testDir);
                Assert.IsNotNull(dirInfo);
                Assert.AreEqual(_testDir, dirInfo.FullPath);
                
                // Test get files
                var files = accessor.GetFiles(_testDir).ToList();
                Assert.IsTrue(files.Count >= 2); // Should have at least test.txt and adsTest.txt
                Assert.IsTrue(files.Any(f => f.FullPath.EndsWith("test.txt")));
                Assert.IsTrue(files.Any(f => f.FullPath.EndsWith("adsTest.txt")));
                
                // Create subdirectory for testing
                string subDir = Path.Combine(_testDir, "subdir");
                Directory.CreateDirectory(subDir);
                File.WriteAllText(Path.Combine(subDir, "subfile.txt"), "Subdir test");
                
                // Test get directories
                var dirs = accessor.GetDirectories(_testDir).ToList();
                Assert.AreEqual(1, dirs.Count);
                Assert.AreEqual(Path.Combine(_testDir, "subdir"), dirs[0].FullPath);
                
                // Test get files recursively
                var allFiles = accessor.GetFiles(_testDir, "*", SearchOption.AllDirectories).ToList();
                Assert.IsTrue(allFiles.Count >= 3); // test.txt, adsTest.txt, and subdir/subfile.txt
                Assert.IsTrue(allFiles.Any(f => f.FullPath.EndsWith("subfile.txt")));
            }
        }

        [TestMethod]
        public void TestAlternateDataStreams()
        {
            using (var accessor = new RawNtfsAccessor(_systemDrive))
            {
                // Test get ADS names
                var adsNames = accessor.GetAlternateDataStreamNames(_testAdsFile).ToList();
                Assert.AreEqual(2, adsNames.Count);
                Assert.IsTrue(adsNames.Contains("ads1"));
                Assert.IsTrue(adsNames.Contains("ads2"));
                
                // Test has ADS
                Assert.IsTrue(accessor.HasAlternateDataStream(_testAdsFile, "ads1"));
                Assert.IsTrue(accessor.HasAlternateDataStream(_testAdsFile, "ads2"));
                Assert.IsFalse(accessor.HasAlternateDataStream(_testAdsFile, "nonexistent"));
                
                // Test read from ADS
                using (var stream = accessor.OpenFile($"{_testAdsFile}:ads1"))
                using (var reader = new StreamReader(stream))
                {
                    string content = reader.ReadToEnd();
                    Assert.AreEqual("ADS1 content", content);
                }
                
                // Test copy file with ADS
                string destFile = Path.Combine(_testDir, "adsTest_copy.txt");
                accessor.CopyFile(_testAdsFile, destFile);
                Assert.IsTrue(File.Exists(destFile));
                
                // Verify ADS were copied
                var destAdsNames = accessor.GetAlternateDataStreamNames(destFile).ToList();
                Assert.AreEqual(2, destAdsNames.Count);
                Assert.IsTrue(destAdsNames.Contains("ads1"));
                Assert.IsTrue(destAdsNames.Contains("ads2"));
                
                // Verify ADS content
                using (var stream = accessor.OpenFile($"{destFile}:ads1"))
                using (var reader = new StreamReader(stream))
                {
                    string content = reader.ReadToEnd();
                    Assert.AreEqual("ADS1 content", content);
                }
            }
        }

        [TestMethod]
        public void TestLockedFileAccess()
        {
            // Test accessing a file that's typically locked while Windows is running
            string registryFile = $"{_systemDrive}:\\Windows\\System32\\config\\SOFTWARE";
            
            using (var accessor = new RawNtfsAccessor(_systemDrive))
            {
                // Check if file exists
                if (!accessor.FileExists(registryFile))
                {
                    Assert.Inconclusive("Registry file not found. Test skipped.");
                    return;
                }
                
                try
                {
                    // Try to get file info
                    var fileInfo = accessor.GetFileInfo(registryFile);
                    Assert.IsNotNull(fileInfo);
                    Assert.IsTrue(fileInfo.Size > 0);
                    
                    // Try to copy the locked file
                    string destFile = Path.Combine(_testDir, "SOFTWARE_copy");
                    accessor.CopyFile(registryFile, destFile);
                    
                    // Verify the copy
                    Assert.IsTrue(File.Exists(destFile));
                    Assert.AreEqual(fileInfo.Size, new FileInfo(destFile).Length);
                }
                catch (Exception ex)
                {
                    Assert.Fail($"Failed to access locked file: {ex.Message}");
                }
            }
        }

        [TestMethod]
        public void TestLinkResolution()
        {
            // Windows has several built-in symbolic links to test with
            string documentsAndSettings = $"{_systemDrive}:\\Documents and Settings";
            
            using (var accessor = new RawNtfsAccessor(_systemDrive))
            {
                // Skip test if the link doesn't exist
                if (!accessor.DirectoryExists(documentsAndSettings))
                {
                    Assert.Inconclusive("Documents and Settings link not found. Test skipped.");
                    return;
                }
                
                // Get info about the link
                var dirInfo = accessor.GetDirectoryInfo(documentsAndSettings);
                Assert.IsTrue(dirInfo.IsReparsePoint);
                Assert.IsNotNull(dirInfo.LinkTarget);
                
                // Resolve the link
                string resolvedPath = accessor.ResolveLinkTarget(documentsAndSettings);
                Assert.IsFalse(string.IsNullOrEmpty(resolvedPath));
                Assert.AreNotEqual(documentsAndSettings, resolvedPath);
                
                // Verify the resolved path exists
                Assert.IsTrue(accessor.DirectoryExists(resolvedPath));
            }
        }
    }
}
