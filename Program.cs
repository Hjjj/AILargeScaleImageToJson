using Azure;
using Azure.AI.Vision.ImageAnalysis;
using Newtonsoft.Json;
using Serilog;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SQLite;
using System.IO;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace AILargeScaleImageToJson
{
    /// <summary>
    /// Converts a directory of JPG files to JSON files using Azure Image AI service.
    /// Works on large volume of files by using a SQLite database as a work queue.
    /// </summary>
    internal class Program
    {
        private static ImageAnalysisClient _client;

        static async Task Main(string[] args)
        {
            // Setup Serilog
            Log.Logger = new LoggerConfiguration()
                .WriteTo.File("log.txt", rollingInterval: RollingInterval.Day)
                .CreateLogger();

            LogAndPrint("BEGIN BATCH");

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
                string fullPathDbFile = VerifyDatabaseAndTable(databaseFilePath);

                // Populate WorkQueue
                PopulateWorkQueue(fullPathDbFile, imageFilesDirectory);

                // Process WorkQueue
                await ProcessWorkQueue(fullPathDbFile, jsonFilesDirectory);

                Log.Information("Entire Work Queue of JPG to JSON Conversions Completed");
            }
            catch (Exception ex)
            {
                Log.Error(ex, $"Caught in Main(). {ex.Message} {ex.InnerException}");
                Console.WriteLine($"Caught in Main(). {ex.Message} {ex.InnerException}");
            }

            Console.WriteLine("Press any key to exit.");
            Console.ReadKey();
        }

        static void VerifyDirectory(string path)
        {
            if (!Directory.Exists(path))
            {
                LogAndPrint($"Dir Not Exist. Creating. {path}");
                Directory.CreateDirectory(path);
            }
        }

        static string VerifyDatabaseAndTable(string databaseFilePath)
        {
            string workingDirectory = AppDomain.CurrentDomain.BaseDirectory;
            string fullDatabaseFilePath = workingDirectory + databaseFilePath.Substring(1);
            string databaseDirectory = Path.GetDirectoryName(fullDatabaseFilePath);

            if (!Directory.Exists(databaseDirectory))
            {
                Directory.CreateDirectory(databaseDirectory);
                LogAndPrint($"DB Dir not exist. Creating {databaseDirectory}");
            }

            // Open a connection to the database, which will create the file if it doesn't exist
            using (var connection = new SQLiteConnection($"Data Source={fullDatabaseFilePath};"))
            {
                connection.Open();
                LogAndPrint($"Database verified at: {fullDatabaseFilePath}");

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
                    LogAndPrint($"DB Table 'WorkQueue' verified");
                }
            }

            return fullDatabaseFilePath;
        }

        static void LogAndPrint(string info)
        {
            Console.WriteLine(info);
            Log.Information(info);
        }

        static async Task ProcessWorkQueue(string databaseFilePath, string jsonFilesDirectory)
        {
            using (var connection = new SQLiteConnection($"Data Source={databaseFilePath};Version=3;"))
            {
                connection.Open();

                string selectQuery = "SELECT Id, EcardImageFileName FROM WorkQueue WHERE Status = 0";
                using (var command = new SQLiteCommand(selectQuery, connection))
                using (var reader = command.ExecuteReader())
                {
                    int i = 0;
                    LogAndPrint("BEGIN LOOP Processing the WorkQueue");
                    while (reader.Read())
                    {
                        i++;
                        int id = reader.GetInt32(0);
                        string imageFileName = reader.GetString(1);

                        // Process the image file with Azure Image AI service
                        LogAndPrint($"{i} Processing @ Azure Cognitive Services Image Analysis{imageFileName}");
                        var aiResult = await ProcessImageFile(imageFileName);

                        if (aiResult?.Value?.Read is null)
                        {
                            //log bad ai result
                            LogAndPrint($"{i} Skipping Bad Result from AI Service-{imageFileName}");

                            // Update WorkQueue status
                            using (var updateCommand = new SQLiteCommand("UPDATE WorkQueue SET Status = -1 WHERE Id = @Id", connection))
                            {
                                updateCommand.Parameters.AddWithValue("@Id", id);
                                updateCommand.ExecuteNonQuery();
                            }

                            continue;
                        }

                        string jsonResult = JsonConvert.SerializeObject(aiResult, Newtonsoft.Json.Formatting.Indented);
                        
                        if (string.IsNullOrEmpty(jsonResult))
                        {
                            LogAndPrint($"{i} Skipping Bad Json conversion for {imageFileName}");
                            continue;
                        }

                        // Save JSON content to file
                        string jsonFileName = Path.Combine(jsonFilesDirectory, Path.GetFileNameWithoutExtension(imageFileName) + ".json");
                        File.WriteAllText(jsonFileName, jsonResult);

                        // Update WorkQueue status
                        string updateQuery = "UPDATE WorkQueue SET Status = 1 WHERE Id = @Id";
                        using (var updateCommand = new SQLiteCommand(updateQuery, connection))
                        {
                            updateCommand.Parameters.AddWithValue("@Id", id);
                            updateCommand.ExecuteNonQuery();
                        }

                        LogAndPrint($"{i} - Success saving JSON:{jsonFileName}");
                    }
                    LogAndPrint($"END LOOP - Procesed {i} records in the WorkQueue.");
                }
            }
        }

        private static String[] GetFileNamesInFolder(string folderPath, string[] arrayOfFileTypes)
        {
            LogAndPrint($"Getting images in folder {folderPath}");

            if (folderPath == null || arrayOfFileTypes == null)
            {
                LogAndPrint($"Error: folderPath was blank");
                return new string[] { };
            }
                

            if (!Directory.Exists(folderPath))
            {
                LogAndPrint($"Error: folderPath was bad: {folderPath}");
                return new string[] { };
            }

            if (arrayOfFileTypes.Length == 0)
                return new string[] { };

            List<String> filesFound = new List<String>();
            foreach (var fileType in arrayOfFileTypes)
            {
                filesFound.AddRange(Directory.GetFiles(folderPath, String.Format("*.{0}", fileType)));
            }

            LogAndPrint($"Image count found in folder:{filesFound.Count}");
            return filesFound.ToArray();
        }

        /// <summary>
        /// If the workqueue has no records to process, go to the image directory and see if there are 
        /// any images in there to be processed. if there are, add them to the workqueue table
        /// </summary>
        /// <param name="databaseFilePath"></param>
        /// <param name="imageFilesDirectory"></param>
        static void PopulateWorkQueue(string databaseFilePath, string imageFilesDirectory)
        {
            using (var connection = new SQLiteConnection($"Data Source={databaseFilePath};Version=3;"))
            {
                connection.Open();

                // Check if WorkQueue table is empty
                string checkTableQuery = "SELECT COUNT(*) FROM WorkQueue WHERE Status=0";

                using (var command = new SQLiteCommand(checkTableQuery, connection))
                {
                    long count = (long)command.ExecuteScalar();
                    LogAndPrint($"There are {count} images ready in the Work Queue");

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
                        LogAndPrint($"{jpgFiles.Length} images from image folder are now queued up in the work queue.");
                    }
                }
            }
        }

        private static async Task<Response<ImageAnalysisResult>> ProcessImageFile(string imagePath)
        {
            try
            {
                using (var imageStream = System.IO.File.OpenRead(imagePath))
                {
                    return await _client.AnalyzeAsync(BinaryData.FromStream(imageStream), VisualFeatures.Read);
                }
            }
            catch(RequestFailedException ex)
            {
                // Log the error details
                Log.Error(ex, $"Azure AI Request failed for image {imagePath}. Status: {ex.Status}, Message: {ex.Message}");
                Console.WriteLine($"Azure AI Request failed for image {imagePath}. Status: {ex.Status}, Message: {ex.Message}");

                // Handle specific status codes if needed
                if (ex.Status == 403) // Forbidden
                {
                    LogAndPrint("AI Account possibly exhaused funding or bandwidth.");
                    Console.WriteLine("Press [S]kip this eCard, or [E]xit.");

                    while (true)
                    {
                        var keyInfo = Console.ReadKey();

                        if (keyInfo.Key == ConsoleKey.S)
                        {
                            return await Task.FromResult<Response<ImageAnalysisResult>>(null);
                        }
                        else if (keyInfo.Key == ConsoleKey.E)
                        {
                            //does exiting here screw up the db connections? yes
                            Environment.Exit(0);
                        }
                    }
                }

                //some exception we don't know about happened, just return null back to the processing loop
                //so what we can continue processing the next image in the work queue
                return await Task.FromResult<Response<ImageAnalysisResult>>(null);

            }

        }

    }
}
