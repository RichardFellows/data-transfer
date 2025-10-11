using DataTransfer.Iceberg.Catalog;
using DataTransfer.Iceberg.Metadata;
using DataTransfer.Iceberg.Models;
using DataTransfer.Iceberg.Writers;
using Microsoft.Extensions.Logging;

namespace DataTransfer.Iceberg.Integration;

/// <summary>
/// Appends new data to existing Iceberg tables (creates new snapshots)
/// </summary>
public class IcebergAppender
{
    private readonly FilesystemCatalog _catalog;
    private readonly ILogger<IcebergAppender> _logger;

    public IcebergAppender(FilesystemCatalog catalog, ILogger<IcebergAppender> logger)
    {
        _catalog = catalog;
        _logger = logger;
    }

    /// <summary>
    /// Appends new data to an existing Iceberg table
    /// </summary>
    /// <param name="tableName">Name of the table to append to</param>
    /// <param name="newData">List of records to append</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Result containing success status and new snapshot ID</returns>
    public async Task<AppendResult> AppendAsync(
        string tableName,
        List<Dictionary<string, object>> newData,
        CancellationToken cancellationToken = default)
    {
        try
        {
            _logger.LogInformation(
                "Starting append to Iceberg table {Table} with {RecordCount} records",
                tableName,
                newData.Count);

            // 1. Load existing table metadata
            var existingMetadata = _catalog.LoadTable(tableName);
            if (existingMetadata == null)
            {
                throw new InvalidOperationException($"Table {tableName} does not exist");
            }

            // Handle empty data case
            if (newData.Count == 0)
            {
                _logger.LogWarning("No data to append for table {Table}", tableName);
                return new AppendResult
                {
                    Success = true,
                    NewSnapshotId = 0,
                    RowsAppended = 0,
                    DataFileCount = 0
                };
            }

            // 2. Get schema from existing metadata
            var schema = existingMetadata.Schemas.FirstOrDefault(s => s.SchemaId == existingMetadata.CurrentSchemaId)
                ?? existingMetadata.Schemas[0];

            // 3. Generate new snapshot ID
            var newSnapshotId = GenerateSnapshotId();

            // 4. Write new Parquet data files
            var tablePath = _catalog.GetTablePath(tableName);
            var dataFiles = await WriteDataFilesAsync(tablePath, schema, newData, cancellationToken);
            _logger.LogDebug("Wrote {FileCount} new data files", dataFiles.Count);

            // 5. Generate new manifest
            var manifestPath = GenerateManifest(tablePath, dataFiles, newSnapshotId);
            _logger.LogDebug("Generated manifest at {ManifestPath}", manifestPath);

            // 6. Generate new manifest list
            var manifestListPath = GenerateManifestList(tablePath, manifestPath, dataFiles.Count);
            _logger.LogDebug("Generated manifest list at {ManifestListPath}", manifestListPath);

            // 7. Update metadata with new snapshot (preserving old ones)
            var metadataGenerator = new TableMetadataGenerator();
            var updatedMetadata = metadataGenerator.UpdateMetadataWithNewSnapshot(
                existingMetadata,
                newSnapshotId,
                manifestListPath);

            // 8. Commit as new version (v{N+1}.metadata.json)
            var success = await _catalog.CommitAsync(tableName, updatedMetadata, cancellationToken);

            if (!success)
            {
                return new AppendResult
                {
                    Success = false,
                    ErrorMessage = $"Failed to commit append to Iceberg table {tableName}"
                };
            }

            var totalRecords = dataFiles.Sum(df => df.RecordCount);

            _logger.LogInformation(
                "Successfully appended to Iceberg table {Table} with {FileCount} files and {RecordCount} records. New snapshot: {SnapshotId}",
                tableName, dataFiles.Count, totalRecords, newSnapshotId);

            return new AppendResult
            {
                Success = true,
                NewSnapshotId = newSnapshotId,
                RowsAppended = (int)totalRecords,
                DataFileCount = dataFiles.Count
            };
        }
        catch (OperationCanceledException)
        {
            _logger.LogWarning("Append operation cancelled for table {Table}", tableName);
            throw;
        }
        catch (InvalidOperationException)
        {
            // Re-throw validation errors (e.g., table doesn't exist)
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to append to Iceberg table {Table}", tableName);
            return new AppendResult
            {
                Success = false,
                ErrorMessage = $"Failed to append: {ex.Message}"
            };
        }
    }

    /// <summary>
    /// Writes data to Parquet files with Iceberg-compliant schema
    /// </summary>
    private async Task<List<DataFileMetadata>> WriteDataFilesAsync(
        string tablePath,
        DataTransfer.Core.Models.Iceberg.IcebergSchema schema,
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
    /// Generates a unique snapshot ID based on current timestamp
    /// </summary>
    private long GenerateSnapshotId()
    {
        return DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    }
}

