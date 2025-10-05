using Microsoft.Extensions.Logging;
using DataTransfer.Web.Models;

namespace DataTransfer.Web.Services;

/// <summary>
/// Service for discovering and managing Parquet files in the storage directory
/// </summary>
public class ParquetFileService
{
    private readonly ILogger<ParquetFileService> _logger;
    private readonly string _parquetDirectory;

    public ParquetFileService(ILogger<ParquetFileService> logger, string parquetDirectory = "./parquet-files")
    {
        _logger = logger;
        _parquetDirectory = parquetDirectory;
    }

    /// <summary>
    /// Gets list of all Parquet files in the storage directory (recursive)
    /// </summary>
    public List<ParquetFileInfo> GetAvailableParquetFiles()
    {
        var files = new List<ParquetFileInfo>();

        try
        {
            if (!Directory.Exists(_parquetDirectory))
            {
                _logger.LogWarning("Parquet directory does not exist: {Directory}", _parquetDirectory);
                return files;
            }

            // Get all files - both with .parquet extension and without (for backwards compatibility)
            var allFiles = Directory.GetFiles(_parquetDirectory, "*", SearchOption.AllDirectories)
                .Where(f =>
                {
                    var fileName = Path.GetFileName(f);
                    var extension = Path.GetExtension(f).ToLowerInvariant();

                    // Include:
                    // 1. Files with .parquet extension
                    // 2. Files without any extension (backwards compatibility - e.g., "orders", "customer")
                    // 3. Exclude common non-parquet file types
                    if (extension == ".parquet")
                        return true;

                    if (string.IsNullOrEmpty(extension))
                        return true; // No extension - might be parquet

                    // Exclude known non-parquet extensions
                    var excludedExtensions = new[] { ".txt", ".csv", ".json", ".xml", ".log", ".md" };
                    return !excludedExtensions.Contains(extension);
                })
                .Where(f => !Path.GetFileName(f).StartsWith(".")); // Exclude hidden files

            foreach (var file in allFiles)
            {
                var fileInfo = new FileInfo(file);
                var relativePath = Path.GetRelativePath(_parquetDirectory, file);

                files.Add(new ParquetFileInfo
                {
                    FileName = fileInfo.Name,
                    RelativePath = relativePath,
                    SizeBytes = fileInfo.Length,
                    LastModified = fileInfo.LastWriteTime
                });
            }

            // Sort by most recently modified first
            files = files.OrderByDescending(f => f.LastModified).ToList();

            _logger.LogInformation("Found {Count} Parquet files in {Directory}", files.Count, _parquetDirectory);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error listing Parquet files from {Directory}", _parquetDirectory);
        }

        return files;
    }

    /// <summary>
    /// Gets full path to a Parquet file given its relative path
    /// </summary>
    public string GetFullPath(string relativePath)
    {
        return Path.Combine(_parquetDirectory, relativePath);
    }

    /// <summary>
    /// Gets preview data for a Parquet file (schema + 10 sample rows)
    /// TODO: Implement in next GREEN phase
    /// </summary>
    public async Task<DataPreview> GetParquetPreviewAsync(string relativePath)
    {
        // Stub implementation - will be completed in next GREEN phase
        await Task.CompletedTask;
        return new DataPreview();
    }
}
