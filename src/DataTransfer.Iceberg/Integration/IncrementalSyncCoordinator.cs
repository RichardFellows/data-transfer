using DataTransfer.Core.Models.Iceberg;
using DataTransfer.Iceberg.Catalog;
using DataTransfer.Iceberg.ChangeDetection;
using DataTransfer.Iceberg.Mapping;
using DataTransfer.Iceberg.MergeStrategies;
using DataTransfer.Iceberg.Models;
using DataTransfer.Iceberg.Readers;
using DataTransfer.Iceberg.Watermarks;
using DataTransfer.Iceberg.Writers;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace DataTransfer.Iceberg.Integration;

/// <summary>
/// Coordinates end-to-end incremental synchronization workflow
/// </summary>
public class IncrementalSyncCoordinator
{
    private readonly IChangeDetectionStrategy _changeDetection;
    private readonly IcebergAppender _appender;
    private readonly IcebergReader _reader;
    private readonly SqlServerImporter _importer;
    private readonly IWatermarkStore _watermarkStore;
    private readonly ILogger<IncrementalSyncCoordinator> _logger;

    public IncrementalSyncCoordinator(
        IChangeDetectionStrategy changeDetection,
        IcebergAppender appender,
        IcebergReader reader,
        SqlServerImporter importer,
        IWatermarkStore watermarkStore,
        ILogger<IncrementalSyncCoordinator> logger)
    {
        _changeDetection = changeDetection ?? throw new ArgumentNullException(nameof(changeDetection));
        _appender = appender ?? throw new ArgumentNullException(nameof(appender));
        _reader = reader ?? throw new ArgumentNullException(nameof(reader));
        _importer = importer ?? throw new ArgumentNullException(nameof(importer));
        _watermarkStore = watermarkStore ?? throw new ArgumentNullException(nameof(watermarkStore));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <summary>
    /// Executes incremental synchronization workflow
    /// </summary>
    /// <param name="sourceConnection">Source database connection string</param>
    /// <param name="sourceTable">Source table name</param>
    /// <param name="icebergTable">Iceberg table name</param>
    /// <param name="targetConnection">Target database connection string</param>
    /// <param name="targetTable">Target table name</param>
    /// <param name="options">Sync options</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Sync result</returns>
    public async Task<SyncResult> SyncAsync(
        string sourceConnection,
        string sourceTable,
        string icebergTable,
        string targetConnection,
        string targetTable,
        SyncOptions options,
        CancellationToken cancellationToken = default)
    {
        var startTime = DateTime.UtcNow;
        _logger.LogInformation("Starting incremental sync for {Table}", icebergTable);

        try
        {
            // 1. Get last watermark
            var lastWatermark = await _watermarkStore.GetWatermarkAsync(icebergTable);
            _logger.LogInformation("Last watermark: {Watermark}", lastWatermark?.LastSyncTimestamp);

            // 2. Extract changes from source
            await using var sourceConn = new SqlConnection(sourceConnection);
            await sourceConn.OpenAsync(cancellationToken);

            var query = await _changeDetection.BuildIncrementalQueryAsync(sourceTable, lastWatermark, sourceConn);
            var changes = await ExtractChanges(sourceConn, query, cancellationToken);

            if (changes.Count == 0)
            {
                _logger.LogInformation("No changes detected");
                return new SyncResult
                {
                    Success = true,
                    RowsExtracted = 0,
                    Duration = DateTime.UtcNow - startTime
                };
            }

            _logger.LogInformation("Extracted {Count} changed rows", changes.Count);

            // 3. Append to Iceberg (or create new table if first sync)
            AppendResult appendResult;
            if (lastWatermark == null)
            {
                // First sync - create new table
                _logger.LogInformation("First sync - creating initial Iceberg table");
                appendResult = await CreateInitialTable(icebergTable, changes, options.WarehousePath, cancellationToken);
            }
            else
            {
                // Incremental sync - append
                appendResult = await _appender.AppendAsync(icebergTable, changes, cancellationToken);
            }

            _logger.LogInformation("Appended to Iceberg, snapshot: {Snapshot}", appendResult.NewSnapshotId);

            // 4. Read from Iceberg
            var data = _reader.ReadTableAsync(icebergTable, cancellationToken);

            // 5. Import to target
            var mergeStrategy = CreateMergeStrategy(options);
            var importResult = await _importer.ImportAsync(data, targetConnection, targetTable, mergeStrategy, cancellationToken);

            _logger.LogInformation("Imported {Count} rows to target", importResult.RowsImported);

            // 6. Update watermark
            var newWatermark = new Watermark
            {
                TableName = icebergTable,
                LastSyncTimestamp = DateTime.UtcNow,
                LastIcebergSnapshot = appendResult.NewSnapshotId,
                RowCount = changes.Count,
                CreatedAt = DateTime.UtcNow
            };
            await _watermarkStore.SetWatermarkAsync(icebergTable, newWatermark);

            return new SyncResult
            {
                Success = true,
                RowsExtracted = changes.Count,
                RowsAppended = appendResult.RowsAppended,
                RowsImported = importResult.RowsImported,
                NewSnapshotId = appendResult.NewSnapshotId,
                NewWatermark = newWatermark,
                Duration = DateTime.UtcNow - startTime
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Sync failed");
            return new SyncResult
            {
                Success = false,
                ErrorMessage = ex.Message,
                Duration = DateTime.UtcNow - startTime
            };
        }
    }

    private async Task<List<Dictionary<string, object>>> ExtractChanges(
        SqlConnection connection,
        IncrementalQuery query,
        CancellationToken cancellationToken)
    {
        var results = new List<Dictionary<string, object>>();

        await using var command = new SqlCommand(query.Sql, connection);
        if (query.Parameters != null && query.Parameters.Count > 0)
        {
            foreach (var param in query.Parameters)
            {
                command.Parameters.AddWithValue(param.Key, param.Value);
            }
        }

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            var row = new Dictionary<string, object>();
            for (int i = 0; i < reader.FieldCount; i++)
            {
                var value = reader.IsDBNull(i) ? null! : reader.GetValue(i);
                row[reader.GetName(i)] = value!;
            }
            results.Add(row);
        }

        return results;
    }

    private async Task<AppendResult> CreateInitialTable(
        string tableName,
        List<Dictionary<string, object>> data,
        string warehousePath,
        CancellationToken cancellationToken)
    {
        // Create catalog and table writer for initial table creation
        var catalog = new FilesystemCatalog(warehousePath, NullLogger<FilesystemCatalog>.Instance);
        var writer = new IcebergTableWriter(catalog, NullLogger<IcebergTableWriter>.Instance);

        // Infer schema from data
        var schema = InferSchemaFromData(data);

        // Write initial table
        var writeResult = await writer.WriteTableAsync(tableName, schema, data, cancellationToken);

        if (!writeResult.Success)
        {
            throw new InvalidOperationException($"Failed to create initial table: {writeResult.ErrorMessage}");
        }

        return new AppendResult
        {
            Success = true,
            NewSnapshotId = writeResult.SnapshotId,
            RowsAppended = (int)writeResult.RecordCount,
            DataFileCount = writeResult.DataFileCount
        };
    }

    private IMergeStrategy CreateMergeStrategy(SyncOptions options)
    {
        return new UpsertMergeStrategy(options.PrimaryKeyColumn);
    }

    private IcebergSchema InferSchemaFromData(List<Dictionary<string, object>> data)
    {
        if (data.Count == 0)
        {
            throw new InvalidOperationException("Cannot infer schema from empty data");
        }

        var firstRow = data[0];
        var fields = new List<IcebergField>();
        int fieldId = 1;

        foreach (var kvp in firstRow)
        {
            var icebergType = InferIcebergType(kvp.Value);
            fields.Add(new IcebergField
            {
                Id = fieldId++,
                Name = kvp.Key,
                Required = false,
                Type = icebergType
            });
        }

        return new IcebergSchema
        {
            SchemaId = 0,
            Type = "struct",
            Fields = fields
        };
    }

    private string InferIcebergType(object? value)
    {
        if (value == null) return "string";

        return value switch
        {
            int => "int",
            long => "long",
            float => "float",
            double => "double",
            decimal => "double",  // Map decimal to double for Parquet compatibility
            bool => "boolean",
            DateTime => "timestamp",
            DateTimeOffset => "timestamptz",
            byte[] => "binary",
            _ => "string"
        };
    }
}
