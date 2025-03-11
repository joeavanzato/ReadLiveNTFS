using System;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using RawNtfsAccess;
using RawNtfsAccess.Models;
using RawNtfsAccess.Exceptions;
using System.Collections.Generic;
using System.Security.Principal;

namespace LiveCopy
{
    class Program
    {
        static void Main(string[] args)
        {
            var ntfsoptions = new RawNtfsOptions
            {
                BufferSize = 8 * 1024 * 1024, // 8MB buffer
                MaxLinkDepth = 10,
                FollowRelativeLinks = true,
                FollowAbsoluteLinks = true,
            };

            if (!IsAdministrator())
            {
                Console.WriteLine("Must run as administrator!");
                return;
            }
            
            var arguments = ParseArguments(args);
                
            // Validate required arguments
            if (!arguments.ContainsKey("src") || !arguments.ContainsKey("dest"))
            {
                Console.WriteLine("Error: Source and destination paths are required.");
                ShowUsage();
                return;
            }

            string sourcePath = arguments["src"];
            string destinationPath = arguments["dest"];
            
            Console.WriteLine($"Source: {sourcePath}");
            Console.WriteLine($"Destination: {destinationPath}");
            Directory.CreateDirectory(Path.GetDirectoryName(destinationPath));
            
            char systemDrive = Path.GetPathRoot(Environment.SystemDirectory)[0];
            Console.WriteLine($"Using system drive: {systemDrive}:");

            using (var ntfsAccessor = new RawNtfsAccessor(systemDrive, ntfsoptions))
            {
               if (ntfsAccessor.FileExists(sourcePath))
                {
                    // We are copying a file to the destination
                    var destFile = Path.GetFileName(sourcePath);
                    destFile = Path.Combine(destinationPath, destFile);
                    Console.WriteLine("Copying: {0} -> {1}", sourcePath, destFile);
                    ntfsAccessor.CopyFile(sourcePath, destFile, true);
                } else if (ntfsAccessor.DirectoryExists(sourcePath))
                {
                    // Find all files in the base dir and copy
                    var files = ntfsAccessor.GetFiles(sourcePath);
                    foreach (var f in files)
                    {
                        var destFile = Path.GetFileName(f.FullPath);
                        destFile = Path.Combine(destinationPath, destFile);
                        Console.WriteLine("Copying: {0} -> {1}", f.FullPath, destFile);
                        ntfsAccessor.CopyFile(f.FullPath, destFile, true);
                    }
                }

            }
        }

        static Dictionary<string, string> ParseArguments(string[] args)
        {
            var arguments = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            
            for (int i = 0; i < args.Length; i++)
            {
                string arg = args[i];
                
                // Check for named argument format: /name:value or --name=value
                if ((arg.StartsWith("/") || arg.StartsWith("--")) && (arg.Contains(":") || arg.Contains("=")))
                {
                    string[] parts = arg.StartsWith("/") 
                        ? arg.Substring(1).Split(new char[] { ':' }, 2) 
                        : arg.Substring(2).Split(new char[] { '=' }, 2);
                    
                    if (parts.Length == 2)
                    {
                        arguments[parts[0]] = parts[1];
                        continue;
                    }
                }
                
                // Check for flag format: /flag or --flag
                if (arg.StartsWith("/") || arg.StartsWith("--"))
                {
                    string flagName = arg.StartsWith("/") ? arg.Substring(1) : arg.Substring(2);
                    arguments[flagName] = "true";
                    continue;
                }
            }
            
            return arguments;
        }

        static void ShowUsage()
        {
            Console.WriteLine("LiveCopy - Copies files and directories using raw NTFS access");
            Console.WriteLine("github.com/joeavanzato/ReadLiveNTFS");
            Console.WriteLine("Usage:");
            Console.WriteLine("  LiveCopy.exe /src:<source_path> /dest:<destination_path>");
            Console.WriteLine();
            Console.WriteLine("Parameters:");
            Console.WriteLine("  /src:<path>    - Source file or directory path");
            Console.WriteLine("  /dest:<path>   - Destination path");
            Console.WriteLine();
            Console.WriteLine("Examples:");
            Console.WriteLine("  LiveCopy.exe /src:C:\\Windows\\System32\\config\\SAM /dest:C:\\temp");
            Console.WriteLine("  LiveCopy.exe /src:C:\\Windows\\System32\\config /dest:C:\\temp");
            Console.WriteLine();
        }

        public static bool IsAdministrator()
        {
            using (WindowsIdentity identity = WindowsIdentity.GetCurrent())
            {
                WindowsPrincipal principal = new WindowsPrincipal(identity);
                return principal.IsInRole(WindowsBuiltInRole.Administrator);
            }
        }
    }
}
