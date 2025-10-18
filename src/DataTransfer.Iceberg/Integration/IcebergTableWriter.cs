using DataTransfer.Core.Models.Iceberg;
using DataTransfer.Iceberg.Catalog;
using DataTransfer.Iceberg.Metadata;
using DataTransfer.Iceberg.Models;
using DataTransfer.Iceberg.Writers;
using Microsoft.Extensions.Logging;

namespace DataTransfer.Iceberg.Integration;

/// <summary>
/// End-to-end orchestrator for writing Iceberg tables
/// Coordinates: schema → Parquet data → manifest → manifest list → metadata → commit
/// </summary>
public class IcebergTableWriter
{
    private readonly FilesystemCatalog _catalog;
    private readonly ILogger<IcebergTableWriter> _logger;

    public IcebergTableWriter(FilesystemCatalog catalog, ILogger<IcebergTableWriter> logger)
    {
        _catalog = catalog;
        _logger = logger;
    }

    /// <summary>
    /// Writes a complete Iceberg table from schema and data (streaming version)
    /// </summary>
    /// <param name="tableName">Name of the table to create</param>
    /// <param name="schema">Iceberg schema with field-ids</param>
    /// <param name="data">Async stream of records as dictionaries (field name → value)</param>
    /// <param name="maxRecordsPerFile">Maximum records per Parquet file (default: 1,000,000)</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Result containing success status and metadata</returns>
    public async Task<IcebergWriteResult> WriteTableAsync(
        string tableName,
        IcebergSchema schema,
        IAsyncEnumerable<Dictionary<string, object>> data,
        int maxRecordsPerFile = 1_000_000,
        CancellationToken cancellationToken = default)
    {
        try
        {
            _logger.LogInformation("Starting streaming Iceberg table write for {Table}",
                tableName);

            // Validate schema
            if (schema.Fields == null || schema.Fields.Count == 0)
            {
                return new IcebergWriteResult
                {
                    Success = false,
                    ErrorMessage = "Schema must contain at least one field"
                };
            }

            // 1. Initialize table structure
            var tablePath = _catalog.InitializeTable(tableName);
            _logger.LogDebug("Initialized table structure at {TablePath}", tablePath);

            // 2. Generate snapshot ID (timestamp-based)
            var snapshotId = GenerateSnapshotId();

            // 3. Write Parquet data files (streaming)
            var dataFiles = await WriteDataFilesStreamingAsync(
                tablePath,
                schema,
                data,
                maxRecordsPerFile,
                cancellationToken);

            _logger.LogDebug("Wrote {FileCount} data files", dataFiles.Count);

            // Guard against empty data
            if (dataFiles.Count == 0)
            {
                _logger.LogError("Cannot write empty table {Table} - no data provided", tableName);
                return new IcebergWriteResult
                {
                    Success = false,
                    ErrorMessage = "Cannot create table with no data. Ensure source contains at least one row."
                };
            }

            // 4. Generate manifest file
            var manifestPath = GenerateManifest(tablePath, dataFiles, snapshotId);
            _logger.LogDebug("Generated manifest at {ManifestPath}", manifestPath);

            // 5. Generate manifest list
            var manifestListPath = GenerateManifestList(tablePath, manifestPath, dataFiles.Count);
            _logger.LogDebug("Generated manifest list at {ManifestListPath}", manifestListPath);

            // 6. Generate table metadata
            var metadata = GenerateTableMetadata(schema, tablePath, manifestListPath, snapshotId);

            // 7. Atomic commit
            var success = await _catalog.CommitAsync(tableName, metadata, cancellationToken);

            if (!success)
            {
                return new IcebergWriteResult
                {
                    Success = false,
                    ErrorMessage = $"Failed to commit Iceberg table {tableName}"
                };
            }

            var totalRecords = dataFiles.Sum(df => df.RecordCount);

            _logger.LogInformation(
                "Successfully created Iceberg table {Table} with {FileCount} files and {RecordCount} records",
                tableName, dataFiles.Count, totalRecords);

            return new IcebergWriteResult
            {
                Success = true,
                SnapshotId = snapshotId,
                TablePath = tablePath,
                DataFileCount = dataFiles.Count,
                RecordCount = totalRecords
            };
        }
        catch (OperationCanceledException)
        {
            _logger.LogWarning("Write operation cancelled for table {Table}", tableName);
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to write Iceberg table {Table}", tableName);
            return new IcebergWriteResult
            {
                Success = false,
                ErrorMessage = $"Failed to write table: {ex.Message}"
            };
        }
    }

    /// <summary>
    /// Writes a complete Iceberg table from schema and data (list version - for backward compatibility)
    /// </summary>
    /// <param name="tableName">Name of the table to create</param>
    /// <param name="schema">Iceberg schema with field-ids</param>
    /// <param name="data">List of records as dictionaries (field name → value)</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Result containing success status and metadata</returns>
    public async Task<IcebergWriteResult> WriteTableAsync(
        string tableName,
        IcebergSchema schema,
        List<Dictionary<string, object>> data,
        CancellationToken cancellationToken = default)
    {
        // Delegate to streaming version
        return await WriteTableAsync(
            tableName,
            schema,
            ToAsyncEnumerable(data),
            maxRecordsPerFile: 1_000_000,
            cancellationToken);
    }

    /// <summary>
    /// Writes data to Parquet files with Iceberg-compliant schema (streaming version)
    /// Splits large datasets into multiple files based on maxRecordsPerFile
    /// </summary>
    private async Task<List<DataFileMetadata>> WriteDataFilesStreamingAsync(
        string tablePath,
        IcebergSchema schema,
        IAsyncEnumerable<Dictionary<string, object>> data,
        int maxRecordsPerFile,
        CancellationToken cancellationToken)
    {
        var dataFiles = new List<DataFileMetadata>();
        var dataDir = Path.Combine(tablePath, "data");

        IcebergParquetWriter? currentWriter = null;
        string? currentFilePath = null;
        long currentFileRecordCount = 0;
        long totalRecordCount = 0;

        try
        {
            await foreach (var record in data.WithCancellation(cancellationToken))
            {
                // Start new file if needed
                if (currentWriter == null || currentFileRecordCount >= maxRecordsPerFile)
                {
                    // Close previous file
                    if (currentWriter != null)
                    {
                        currentWriter.Close();

                        var fileInfo = new FileInfo(currentFilePath!);
                        dataFiles.Add(new DataFileMetadata
                        {
                            FilePath = $"data/{Path.GetFileName(currentFilePath)}",
                            FileSizeInBytes = fileInfo.Length,
                            RecordCount = currentFileRecordCount
                        });

                        _logger.LogDebug("Wrote Parquet file {FileName} with {RecordCount} records ({FileSize} bytes)",
                            Path.GetFileName(currentFilePath), currentFileRecordCount, fileInfo.Length);
                    }

                    // Start new file
                    currentFilePath = Path.Combine(dataDir, $"data-{Guid.NewGuid()}.parquet");
                    currentWriter = new IcebergParquetWriter(currentFilePath, schema);
                    currentFileRecordCount = 0;
                }

                // Convert dictionary to typed values array matching schema order
                var values = new object[schema.Fields.Count];
                for (int i = 0; i < schema.Fields.Count; i++)
                {
                    var fieldName = schema.Fields[i].Name;
                    values[i] = record.ContainsKey(fieldName) ? record[fieldName] : null!;
                }

                currentWriter.WriteRow(values);
                currentFileRecordCount++;
                totalRecordCount++;
            }

            // Close final file
            if (currentWriter != null && currentFileRecordCount > 0)
            {
                currentWriter.Close();

                var fileInfo = new FileInfo(currentFilePath!);
                dataFiles.Add(new DataFileMetadata
                {
                    FilePath = $"data/{Path.GetFileName(currentFilePath)}",
                    FileSizeInBytes = fileInfo.Length,
                    RecordCount = currentFileRecordCount
                });

                _logger.LogDebug("Wrote final Parquet file {FileName} with {RecordCount} records ({FileSize} bytes)",
                    Path.GetFileName(currentFilePath), currentFileRecordCount, fileInfo.Length);
            }

            _logger.LogInformation("Streaming write complete: {TotalRecords} records across {FileCount} files",
                totalRecordCount, dataFiles.Count);

            return dataFiles;
        }
        finally
        {
            currentWriter?.Close();
        }
    }

    /// <summary>
    /// Writes data to Parquet files with Iceberg-compliant schema (legacy list-based version)
    /// </summary>
    private async Task<List<DataFileMetadata>> WriteDataFilesAsync(
        string tablePath,
        IcebergSchema schema,
        List<Dictionary<string, object>> data,
        CancellationToken cancellationToken)
    {
        var dataFiles = new List<DataFileMetadata>();
        var dataDir = Path.Combine(tablePath, "data");
        var dataFilePath = Path.Combine(dataDir, $"data-{Guid.NewGuid()}.parquet");

        // Write single data file for now (future: support partitioning/batching)
        using var writer = new IcebergParquetWriter(dataFilePath, schema);

        long recordCount = 0;
        foreach (var record in data)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // Convert dictionary to typed values array matching schema order
            var values = new object[schema.Fields.Count];
            for (int i = 0; i < schema.Fields.Count; i++)
            {
                var fieldName = schema.Fields[i].Name;
                values[i] = record.ContainsKey(fieldName) ? record[fieldName] : null!;
            }

            writer.WriteRow(values);
            recordCount++;
        }

        writer.Close();

        var fileInfo = new FileInfo(dataFilePath);
        dataFiles.Add(new DataFileMetadata
        {
            FilePath = $"data/{Path.GetFileName(dataFilePath)}",  // Relative path
            FileSizeInBytes = fileInfo.Length,
            RecordCount = recordCount
        });

        return await Task.FromResult(dataFiles);
    }

    /// <summary>
    /// Helper to convert List to IAsyncEnumerable
    /// </summary>
    private async IAsyncEnumerable<Dictionary<string, object>> ToAsyncEnumerable(
        List<Dictionary<string, object>> data)
    {
        foreach (var item in data)
        {
            yield return item;
        }
        await Task.CompletedTask;
    }

    /// <summary>
    /// Generates manifest file listing data files
    /// </summary>
    private string GenerateManifest(
        string tablePath,
        List<DataFileMetadata> dataFiles,
        long snapshotId)
    {
        var generator = new ManifestFileGenerator();
        var manifestFileName = $"manifest-{Guid.NewGuid()}.avro";
        var manifestPath = Path.Combine(tablePath, "metadata", manifestFileName);

        generator.WriteManifest(dataFiles, manifestPath, snapshotId);

        return $"metadata/{manifestFileName}";  // Return relative path
    }

    /// <summary>
    /// Generates manifest list indexing all manifests
    /// </summary>
    private string GenerateManifestList(
        string tablePath,
        string manifestRelativePath,
        int addedFilesCount)
    {
        var generator = new ManifestListGenerator();
        var manifestListFileName = $"snap-{Guid.NewGuid()}.avro";
        var manifestListPath = Path.Combine(tablePath, "metadata", manifestListFileName);

        // Get manifest file size
        var manifestFullPath = Path.Combine(tablePath, manifestRelativePath);
        var manifestSize = new FileInfo(manifestFullPath).Length;

        generator.WriteManifestList(
            manifestPath: manifestRelativePath,
            outputPath: manifestListPath,
            manifestSizeInBytes: manifestSize,
            addedFilesCount: addedFilesCount);

        return $"metadata/{manifestListFileName}";  // Return relative path
    }

    /// <summary>
    /// Generates root table metadata
    /// </summary>
    private IcebergTableMetadata GenerateTableMetadata(
        IcebergSchema schema,
        string tablePath,
        string manifestListRelativePath,
        long snapshotId)
    {
        var generator = new TableMetadataGenerator();
        return generator.CreateInitialMetadata(
            schema: schema,
            tableLocation: tablePath,
            manifestListPath: manifestListRelativePath,
            snapshotId: snapshotId);
    }

    /// <summary>
    /// Generates a unique snapshot ID based on current timestamp
    /// </summary>
    private long GenerateSnapshotId()
    {
        return DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    }
}
