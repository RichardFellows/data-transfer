using Microsoft.Extensions.Logging;
using DataTransfer.Web.Models;
using Parquet;
using Parquet.Data;

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
    /// </summary>
    public async Task<DataPreview> GetParquetPreviewAsync(string relativePath)
    {
        var preview = new DataPreview();
        var fullPath = GetFullPath(relativePath);

        if (!File.Exists(fullPath))
        {
            _logger.LogWarning("Parquet file not found: {Path}", fullPath);
            return preview;
        }

        // Local function to convert Parquet field type to display string
        string GetParquetDataType(dynamic field)
        {
            var clrType = field.ClrType;
            var typeName = clrType.Name;

            // Handle nullable types
            if (clrType.IsGenericType && clrType.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                typeName = Nullable.GetUnderlyingType(clrType)?.Name ?? typeName;
            }

            // Simplify type names for display
            return typeName switch
            {
                "Int32" => "INT",
                "Int64" => "BIGINT",
                "String" => "STRING",
                "Boolean" => "BOOLEAN",
                "Decimal" => "DECIMAL",
                "Double" => "DOUBLE",
                "Single" => "FLOAT",
                "DateTime" => "DATETIME",
                "DateTimeOffset" => "DATETIMEOFFSET",
                "Byte[]" => "BINARY",
                _ => typeName.ToUpper()
            };
        }

        try
        {
            await using var fileStream = File.OpenRead(fullPath);
            using var parquetReader = await ParquetReader.CreateAsync(fileStream);

            var schema = parquetReader.Schema;
            var dataFields = schema.GetDataFields();

            // Get column information
            foreach (var field in dataFields)
            {
                preview.Columns.Add(new ColumnInfo
                {
                    Name = field.Name,
                    DataType = GetParquetDataType(field),
                    IsNullable = field.IsNullable
                });
            }

            // Read first row group only (for preview)
            if (parquetReader.RowGroupCount > 0)
            {
                using var rowGroupReader = parquetReader.OpenRowGroupReader(0);
                var rowCount = Math.Min((int)rowGroupReader.RowCount, 10); // Max 10 rows

                // Read all columns
                var columnData = new Dictionary<string, Array>();
                foreach (var field in dataFields)
                {
                    var column = await rowGroupReader.ReadColumnAsync(field);
                    columnData[field.Name] = column.Data;
                }

                // Convert to rows
                for (int i = 0; i < rowCount; i++)
                {
                    var row = new Dictionary<string, object?>();
                    foreach (var field in dataFields)
                    {
                        var array = columnData[field.Name];
                        var value = array.GetValue(i);
                        row[field.Name] = value;
                    }
                    preview.Rows.Add(row);
                }

                // Get total row count (sum of all row groups)
                preview.TotalRowCount = 0;
                for (int i = 0; i < parquetReader.RowGroupCount; i++)
                {
                    using var rg = parquetReader.OpenRowGroupReader(i);
                    preview.TotalRowCount += rg.RowCount;
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error reading Parquet file preview: {Path}", fullPath);
        }

        return preview;
    }
}
