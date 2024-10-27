using Azure.AI.Vision.ImageAnalysis;
using Serilog;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SQLite;
using System.IO;

namespace AILargeScaleImageToJson
{
    /// <summary>
    /// Converts a directory of JPG files to JSON files using Azure Image AI service.
    /// Works on large volume of files by using a SQLite database as a work queue.
    /// </summary>
    internal class Program
    {
        private static ImageAnalysisClient _client;

        static void Main(string[] args)
        {
            // Setup Serilog
            Log.Logger = new LoggerConfiguration()
                .WriteTo.File("log.txt", rollingInterval: RollingInterval.Day)
                .CreateLogger();

            try
            {
                // Read configuration
                string imageFilesDirectory = ConfigurationManager.AppSettings["ImageFilesDirectory"];
                string jsonFilesDirectory = ConfigurationManager.AppSettings["JsonFilesDirectory"];
                string databaseFilePath = ConfigurationManager.AppSettings["DatabaseFilePath"];
                string azureImageAIEndPoint = ConfigurationManager.AppSettings["AzureImageAIEndPoint"];
                string azureImageAISubscriptionKey = ConfigurationManager.AppSettings["AzureImageAISubscriptionKey"];

                _client = new ImageAnalysisClient(
                    new Uri(azureImageAIEndPoint),
                    new Azure.AzureKeyCredential(azureImageAISubscriptionKey)
                );

                // Verify directories
                VerifyDirectory(imageFilesDirectory);
                VerifyDirectory(jsonFilesDirectory);

                // Verify database and table
                VerifyDatabaseAndTable(databaseFilePath);

                // Populate WorkQueue
                PopulateWorkQueue(databaseFilePath, imageFilesDirectory);

                // Process WorkQueue
                ProcessWorkQueue(databaseFilePath, jsonFilesDirectory);

                Log.Information("JPG to JSON Conversions Completed");
            }
            catch (Exception ex)
            {
                Log.Error(ex, "An error occurred during application startup.");
            }

            Console.WriteLine("Press any key to exit.");
            Console.ReadKey();
        }

        static void VerifyDirectory(string path)
        {
            if (!Directory.Exists(path))
            {
                Directory.CreateDirectory(path);
                Log.Information($"Directory created: {path}");
            }
            else
            {
                Log.Information($"Directory exists: {path}");
            }
        }

        static void VerifyDatabaseAndTable(string databaseFilePath)
        {
            // Ensure the directory for the database file exists
            string databaseDirectory = Path.GetDirectoryName(databaseFilePath);

            if (!Directory.Exists(databaseDirectory))
            {
                Directory.CreateDirectory(databaseDirectory);
                Log.Information($"Directory created: {databaseDirectory}");
            }

            // Open a connection to the database, which will create the file if it doesn't exist
            using (var connection = new SQLiteConnection($"Data Source={databaseFilePath};"))
            {
                connection.Open();
                Log.Information($"Database verified: {databaseFilePath}");

                string createTableQuery = @"
            CREATE TABLE IF NOT EXISTS WorkQueue (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                EcardImageFileName TEXT NOT NULL,
                Status INTEGER NOT NULL,
                Comments TEXT
            )";
                using (var command = new SQLiteCommand(createTableQuery, connection))
                {
                    command.ExecuteNonQuery();
                    Log.Information("Verified that the WorkQueue table exists.");
                }
            }
        }


        static void ProcessWorkQueue(string databaseFilePath, string jsonFilesDirectory)
        {
            using (var connection = new SQLiteConnection($"Data Source={databaseFilePath};Version=3;"))
            {
                connection.Open();

                string selectQuery = "SELECT Id, EcardImageFileName FROM WorkQueue WHERE Status = 0";
                using (var command = new SQLiteCommand(selectQuery, connection))
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        int id = reader.GetInt32(0);
                        string imageFileName = reader.GetString(1);

                        // Process the image file (placeholder for actual AI processing)
                        string jsonContent = ProcessImageFile(imageFileName);

                        // Save JSON content to file
                        string jsonFileName = Path.Combine(jsonFilesDirectory, Path.GetFileNameWithoutExtension(imageFileName) + ".json");
                        File.WriteAllText(jsonFileName, jsonContent);

                        // Update WorkQueue status
                        string updateQuery = "UPDATE WorkQueue SET Status = 1 WHERE Id = @Id";
                        using (var updateCommand = new SQLiteCommand(updateQuery, connection))
                        {
                            updateCommand.Parameters.AddWithValue("@Id", id);
                            updateCommand.ExecuteNonQuery();
                        }

                        Log.Information($"Processed image file: {imageFileName}");
                    }
                }
            }
        }

        private static String[] GetFileNamesInFolder(string folderPath, string[] arrayOfFileTypes)
        {
            if (folderPath == null || arrayOfFileTypes == null)
                return new string[] { };

            if (!Directory.Exists(folderPath))
            {
                Console.WriteLine("Invalid image directory. Value is: " + folderPath);
                return new string[] { };
            }

            if (arrayOfFileTypes.Length == 0)
                return new string[] { };

            List<String> filesFound = new List<String>();
            foreach (var fileType in arrayOfFileTypes)
            {
                filesFound.AddRange(Directory.GetFiles(folderPath, String.Format("*.{0}", fileType)));
            }

            return filesFound.ToArray();
        }

        static void PopulateWorkQueue(string databaseFilePath, string imageFilesDirectory)
        {
            using (var connection = new SQLiteConnection($"Data Source={databaseFilePath};Version=3;"))
            {
                connection.Open();

                // Check if WorkQueue table is empty
                string checkTableQuery = "SELECT COUNT(*) FROM WorkQueue";
                using (var command = new SQLiteCommand(checkTableQuery, connection))
                {
                    long count = (long)command.ExecuteScalar();
                    if (count == 0)
                    {
                        var jpgFiles = GetFileNamesInFolder(imageFilesDirectory, new String[] { "jpg", "jpeg" });

                        foreach (var file in jpgFiles)
                        {
                            string insertQuery = "INSERT INTO WorkQueue (EcardImageFileName, Status) VALUES (@EcardImageFileName, 0)";
                            using (var insertCommand = new SQLiteCommand(insertQuery, connection))
                            {
                                insertCommand.Parameters.AddWithValue("@EcardImageFileName", file);
                                insertCommand.ExecuteNonQuery();
                            }
                        }
                        Log.Information("Populated WorkQueue table with image files.");
                    }
                }
            }
        }

        static string ProcessImageFile(string imageFileName)
        {
            // Placeholder for actual AI processing
            // This function should call the Amazon AI service and return the JSON content
            return "{ \"example\": \"json\" }";
        }


    }
}
