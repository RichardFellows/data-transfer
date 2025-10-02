using System.Text.Json;
using DataTransfer.Core.Interfaces;
using DataTransfer.Core.Models;
using Parquet;
using Parquet.Data;

namespace DataTransfer.Parquet;

/// <summary>
/// Extracts data from Parquet files to JSON stream
/// </summary>
public class ParquetExtractor : IParquetExtractor
{
    private readonly string _basePath;

    public ParquetExtractor(string basePath)
    {
        if (string.IsNullOrWhiteSpace(basePath))
        {
            throw new ArgumentException("Base path cannot be empty", nameof(basePath));
        }

        _basePath = basePath;
    }

    public async Task<ExtractionResult> ExtractFromParquetAsync(
        string parquetPath,
        Stream outputStream,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(parquetPath))
        {
            throw new ArgumentException("Parquet path cannot be empty", nameof(parquetPath));
        }

        ArgumentNullException.ThrowIfNull(outputStream);

        var fullPath = Path.IsPathRooted(parquetPath)
            ? parquetPath
            : Path.Combine(_basePath, parquetPath);

        if (!File.Exists(fullPath))
        {
            throw new FileNotFoundException($"Parquet file not found: {fullPath}", fullPath);
        }

        cancellationToken.ThrowIfCancellationRequested();

        var startTime = DateTime.UtcNow;

        try
        {

            // Check if file is empty (created for 0 rows)
            var fileInfo = new FileInfo(fullPath);
            if (fileInfo.Length == 0)
            {
                // Write empty JSON array
                await JsonSerializer.SerializeAsync(
                    outputStream,
                    new List<Dictionary<string, object?>>(),
                    cancellationToken: cancellationToken);

                return new ExtractionResult
                {
                    RowsExtracted = 0,
                    StartTime = startTime,
                    EndTime = DateTime.UtcNow,
                    Success = true
                };
            }

            // Read Parquet file
            await using var fileStream = File.OpenRead(fullPath);
            using var parquetReader = await ParquetReader.CreateAsync(fileStream, cancellationToken: cancellationToken);

            var jsonRows = new List<Dictionary<string, object?>>();

            // Read all row groups
            for (int groupIndex = 0; groupIndex < parquetReader.RowGroupCount; groupIndex++)
            {
                using var rowGroupReader = parquetReader.OpenRowGroupReader(groupIndex);
                var rowCount = (int)rowGroupReader.RowCount;
                var schema = parquetReader.Schema;
                var dataFields = schema.GetDataFields();

                // Read all columns
                var columnData = new Dictionary<string, Array>();
                foreach (var field in dataFields)
                {
                    var column = await rowGroupReader.ReadColumnAsync(field, cancellationToken);
                    columnData[field.Name] = column.Data;
                }

                // Convert to JSON rows
                for (int i = 0; i < rowCount; i++)
                {
                    var row = new Dictionary<string, object?>();
                    foreach (var field in dataFields)
                    {
                        var data = columnData[field.Name];
                        row[field.Name] = data.GetValue(i);
                    }
                    jsonRows.Add(row);
                }
            }

            // Write JSON array to output stream
            await JsonSerializer.SerializeAsync(outputStream, jsonRows, cancellationToken: cancellationToken);

            return new ExtractionResult
            {
                RowsExtracted = jsonRows.Count,
                StartTime = startTime,
                EndTime = DateTime.UtcNow,
                Success = true
            };
        }
        catch (Exception ex)
        {
            return new ExtractionResult
            {
                RowsExtracted = 0,
                StartTime = startTime,
                EndTime = DateTime.UtcNow,
                Success = false,
                ErrorMessage = ex.Message
            };
        }
    }
}
