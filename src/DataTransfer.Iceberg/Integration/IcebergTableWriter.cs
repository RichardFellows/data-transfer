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
    /// Writes a complete Iceberg table from schema and data
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
        try
        {
            _logger.LogInformation("Starting Iceberg table write for {Table} with {RecordCount} records",
                tableName, data.Count);

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

            // Guard against empty data - empty tables should not be created
            if (data.Count == 0)
            {
                _logger.LogError("Cannot write empty table {Table} - no data provided", tableName);
                return new IcebergWriteResult
                {
                    Success = false,
                    ErrorMessage = "Cannot create table with no data. Ensure source contains at least one row."
                };
            }

            // 2. Generate snapshot ID (timestamp-based)
            var snapshotId = GenerateSnapshotId();

            // 3. Write Parquet data files
            var dataFiles = await WriteDataFilesAsync(tablePath, schema, data, cancellationToken);
            _logger.LogDebug("Wrote {FileCount} data files", dataFiles.Count);

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
    /// Writes data to Parquet files with Iceberg-compliant schema
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
