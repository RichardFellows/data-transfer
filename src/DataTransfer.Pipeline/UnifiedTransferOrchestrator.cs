using DataTransfer.Core.Interfaces;
using DataTransfer.Core.Models;
using DataTransfer.Iceberg.Catalog;
using DataTransfer.Iceberg.Integration;
using DataTransfer.Iceberg.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace DataTransfer.Pipeline;

/// <summary>
/// Orchestrates all types of data transfers: SQL→Parquet, Parquet→SQL, SQL→SQL, SQL→Iceberg, Iceberg→SQL
/// </summary>
public class UnifiedTransferOrchestrator
{
    private readonly ITableExtractor _sqlExtractor;
    private readonly IParquetExtractor _parquetExtractor;
    private readonly IDataLoader _sqlLoader;
    private readonly IParquetWriter _parquetWriter;
    private readonly SqlServerToIcebergExporter _icebergExporter;
    private readonly IncrementalSyncCoordinator _incrementalSync;
    private readonly FilesystemCatalog _icebergCatalog;
    private readonly ILogger<UnifiedTransferOrchestrator> _logger;
    private readonly string _icebergWarehousePath;

    public UnifiedTransferOrchestrator(
        ITableExtractor sqlExtractor,
        IParquetExtractor parquetExtractor,
        IDataLoader sqlLoader,
        IParquetWriter parquetWriter,
        SqlServerToIcebergExporter icebergExporter,
        IncrementalSyncCoordinator incrementalSync,
        FilesystemCatalog icebergCatalog,
        IConfiguration configuration,
        ILogger<UnifiedTransferOrchestrator> logger)
    {
        _sqlExtractor = sqlExtractor ?? throw new ArgumentNullException(nameof(sqlExtractor));
        _parquetExtractor = parquetExtractor ?? throw new ArgumentNullException(nameof(parquetExtractor));
        _sqlLoader = sqlLoader ?? throw new ArgumentNullException(nameof(sqlLoader));
        _parquetWriter = parquetWriter ?? throw new ArgumentNullException(nameof(parquetWriter));
        _icebergExporter = icebergExporter ?? throw new ArgumentNullException(nameof(icebergExporter));
        _incrementalSync = incrementalSync ?? throw new ArgumentNullException(nameof(incrementalSync));
        _icebergCatalog = icebergCatalog ?? throw new ArgumentNullException(nameof(icebergCatalog));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        ArgumentNullException.ThrowIfNull(configuration);
        _icebergWarehousePath = configuration["Iceberg:WarehousePath"] ?? "./iceberg-warehouse";
    }

    /// <summary>
    /// Executes a transfer based on the configuration
    /// </summary>
    public async Task<TransferResult> ExecuteTransferAsync(
        TransferConfiguration config,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(config);

        var result = new TransferResult { StartTime = DateTime.UtcNow };

        try
        {
            _logger.LogInformation("Starting {TransferType} transfer", config.TransferType);

            switch (config.TransferType)
            {
                case TransferType.SqlToParquet:
                    await TransferSqlToParquetAsync(config, result, cancellationToken);
                    break;

                case TransferType.ParquetToSql:
                    await TransferParquetToSqlAsync(config, result, cancellationToken);
                    break;

                case TransferType.SqlToSql:
                    throw new NotImplementedException(
                        "SQL→SQL transfer should use the existing DataTransferOrchestrator");

                case TransferType.SqlToIceberg:
                    await TransferSqlToIcebergAsync(config, result, cancellationToken);
                    break;

                case TransferType.IcebergToSql:
                    await TransferIcebergToSqlAsync(config, result, cancellationToken);
                    break;

                case TransferType.SqlToIcebergIncremental:
                    await TransferSqlToIcebergIncrementalAsync(config, result, cancellationToken);
                    break;

                default:
                    throw new NotSupportedException($"Transfer type {config.TransferType} is not supported");
            }

            result.Success = true;
            result.EndTime = DateTime.UtcNow;
            _logger.LogInformation("Transfer completed successfully in {Duration}ms",
                result.Duration.TotalMilliseconds);

            return result;
        }
        catch (Exception ex)
        {
            result.Success = false;
            result.EndTime = DateTime.UtcNow;
            result.ErrorMessage = ex.Message;
            _logger.LogError(ex, "Transfer failed: {Error}", ex.Message);
            throw;
        }
    }

    private async Task TransferSqlToParquetAsync(
        TransferConfiguration config,
        TransferResult result,
        CancellationToken cancellationToken)
    {
        _logger.LogInformation("Extracting from SQL Server to Parquet");

        using var dataStream = new MemoryStream();

        // Extract from SQL
        var tableConfig = new TableConfiguration
        {
            Source = config.Source.Table!,
            Partitioning = config.Partitioning ?? new PartitioningConfiguration
            {
                Type = PartitionType.Static
            }
        };

        var extractResult = await _sqlExtractor.ExtractAsync(
            tableConfig,
            config.Source.ConnectionString!,
            dataStream,
            cancellationToken);

        result.RowsExtracted = extractResult.RowsExtracted;
        _logger.LogInformation("Extracted {RowCount} rows from SQL Server", extractResult.RowsExtracted);

        // Write to Parquet
        dataStream.Position = 0;
        var rowsWritten = await _parquetWriter.WriteToParquetAsync(
            dataStream,
            config.Destination.ParquetPath!,
            DateTime.UtcNow,
            cancellationToken);

        result.RowsLoaded = rowsWritten;
        result.ParquetFilePath = config.Destination.ParquetPath;
        _logger.LogInformation("Wrote {RowCount} rows to Parquet file: {FilePath}",
            rowsWritten, config.Destination.ParquetPath);
    }

    private async Task TransferParquetToSqlAsync(
        TransferConfiguration config,
        TransferResult result,
        CancellationToken cancellationToken)
    {
        _logger.LogInformation("Loading from Parquet to SQL Server");

        using var dataStream = new MemoryStream();

        // Extract from Parquet
        var extractResult = await _parquetExtractor.ExtractFromParquetAsync(
            config.Source.ParquetPath!,
            dataStream,
            cancellationToken);

        result.RowsExtracted = extractResult.RowsExtracted;
        result.ParquetFilePath = config.Source.ParquetPath;
        _logger.LogInformation("Extracted {RowCount} rows from Parquet file: {FilePath}",
            extractResult.RowsExtracted, config.Source.ParquetPath);

        // Load to SQL
        dataStream.Position = 0;
        var tableConfig = new TableConfiguration
        {
            Destination = config.Destination.Table!
        };

        var loadResult = await _sqlLoader.LoadAsync(
            tableConfig,
            config.Destination.ConnectionString!,
            dataStream,
            cancellationToken);

        result.RowsLoaded = loadResult.RowsLoaded;
        _logger.LogInformation("Loaded {RowCount} rows to SQL Server", loadResult.RowsLoaded);
    }

    private async Task TransferSqlToIcebergAsync(
        TransferConfiguration config,
        TransferResult result,
        CancellationToken cancellationToken)
    {
        _logger.LogInformation("Exporting from SQL Server to Iceberg table");

        var icebergTableName = config.Destination.IcebergTable!.TableName;
        var sourceTable = $"{config.Source.Table!.Schema}.{config.Source.Table!.Table}";

        // Export using SqlServerToIcebergExporter
        var exportResult = await _icebergExporter.ExportTableAsync(
            config.Source.ConnectionString!,
            sourceTable,
            icebergTableName,
            query: null,
            cancellationToken);

        if (!exportResult.Success)
        {
            throw new InvalidOperationException($"Iceberg export failed: {exportResult.ErrorMessage}");
        }

        result.RowsExtracted = exportResult.RecordCount;
        result.RowsLoaded = exportResult.RecordCount;
        result.ParquetFilePath = exportResult.TablePath;

        _logger.LogInformation(
            "Exported {RowCount} rows to Iceberg table {TableName} (Snapshot: {SnapshotId})",
            exportResult.RecordCount,
            icebergTableName,
            exportResult.SnapshotId);
    }

    private async Task TransferIcebergToSqlAsync(
        TransferConfiguration config,
        TransferResult result,
        CancellationToken cancellationToken)
    {
        _logger.LogInformation("Importing from Iceberg table to SQL Server");

        var icebergTableName = config.Source.IcebergTable!.TableName;
        var destinationTable = config.Destination.Table!;

        // Read from Iceberg using IcebergReader
        var reader = new Iceberg.Readers.IcebergReader(
            _icebergCatalog,
            _logger as ILogger<Iceberg.Readers.IcebergReader> ??
                Microsoft.Extensions.Logging.Abstractions.NullLogger<Iceberg.Readers.IcebergReader>.Instance);

        var dataStream = reader.ReadTableAsync(icebergTableName, cancellationToken);

        _logger.LogInformation("Reading data from Iceberg table {TableName}", icebergTableName);

        // Import to SQL Server using SqlServerImporter with simple insert strategy
        var importer = new SqlServerImporter(
            _logger as ILogger<SqlServerImporter> ??
                Microsoft.Extensions.Logging.Abstractions.NullLogger<SqlServerImporter>.Instance);

        // Use upsert strategy for import (default primary key: "Id")
        var mergeStrategy = new Iceberg.MergeStrategies.UpsertMergeStrategy("Id");

        var importResult = await importer.ImportAsync(
            dataStream,
            config.Destination.ConnectionString!,
            $"{destinationTable.Schema}.{destinationTable.Table}",
            mergeStrategy,
            cancellationToken);

        if (!importResult.Success)
        {
            throw new InvalidOperationException($"SQL Server import failed: {importResult.ErrorMessage}");
        }

        result.RowsExtracted = importResult.RowsImported;
        result.RowsLoaded = importResult.RowsImported;
        result.ParquetFilePath = Path.Combine(_icebergWarehousePath, icebergTableName);

        _logger.LogInformation(
            "Imported {RowCount} rows to SQL Server table {Schema}.{Table}",
            importResult.RowsImported,
            destinationTable.Schema,
            destinationTable.Table);
    }

    private async Task TransferSqlToIcebergIncrementalAsync(
        TransferConfiguration config,
        TransferResult result,
        CancellationToken cancellationToken)
    {
        _logger.LogInformation("Starting incremental sync: SQL Server → Iceberg → SQL Server");

        var icebergTableName = config.Destination.IcebergTable!.TableName;
        var sourceTable = config.Source.Table!;
        var destinationTable = config.Destination.Table!;
        var incrementalOptions = config.Destination.IcebergTable.IncrementalSync!;

        // Build sync options from configuration
        var syncOptions = new SyncOptions
        {
            PrimaryKeyColumn = incrementalOptions.PrimaryKeyColumn,
            WatermarkColumn = incrementalOptions.WatermarkColumn,
            MergeStrategy = incrementalOptions.MergeStrategy,
            WarehousePath = _icebergWarehousePath,
            WatermarkDirectory = Path.Combine(_icebergWarehousePath, ".watermarks")
        };

        // Execute incremental sync using IncrementalSyncCoordinator
        var syncResult = await _incrementalSync.SyncAsync(
            sourceConnection: config.Source.ConnectionString!,
            sourceTable: $"{sourceTable.Schema}.{sourceTable.Table}",
            icebergTable: icebergTableName,
            targetConnection: config.Destination.ConnectionString!,
            targetTable: $"{destinationTable.Schema}.{destinationTable.Table}",
            options: syncOptions,
            cancellationToken: cancellationToken);

        if (!syncResult.Success)
        {
            throw new InvalidOperationException($"Incremental sync failed: {syncResult.ErrorMessage}");
        }

        result.RowsExtracted = syncResult.RowsExtracted;
        result.RowsLoaded = syncResult.RowsImported;
        result.ParquetFilePath = Path.Combine(_icebergWarehousePath, icebergTableName);

        _logger.LogInformation(
            "Incremental sync completed: Extracted {Extracted} rows, Imported {Imported} rows (Watermark: {Watermark})",
            syncResult.RowsExtracted,
            syncResult.RowsImported,
            syncResult.NewWatermark);
    }
}
