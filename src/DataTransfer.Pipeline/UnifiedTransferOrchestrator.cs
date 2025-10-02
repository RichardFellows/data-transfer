using DataTransfer.Core.Interfaces;
using DataTransfer.Core.Models;
using Microsoft.Extensions.Logging;

namespace DataTransfer.Pipeline;

/// <summary>
/// Orchestrates all types of data transfers: SQL→Parquet, Parquet→SQL, SQL→SQL
/// </summary>
public class UnifiedTransferOrchestrator
{
    private readonly ITableExtractor _sqlExtractor;
    private readonly IParquetExtractor _parquetExtractor;
    private readonly IDataLoader _sqlLoader;
    private readonly IParquetWriter _parquetWriter;
    private readonly ILogger<UnifiedTransferOrchestrator> _logger;

    public UnifiedTransferOrchestrator(
        ITableExtractor sqlExtractor,
        IParquetExtractor parquetExtractor,
        IDataLoader sqlLoader,
        IParquetWriter parquetWriter,
        ILogger<UnifiedTransferOrchestrator> logger)
    {
        _sqlExtractor = sqlExtractor ?? throw new ArgumentNullException(nameof(sqlExtractor));
        _parquetExtractor = parquetExtractor ?? throw new ArgumentNullException(nameof(parquetExtractor));
        _sqlLoader = sqlLoader ?? throw new ArgumentNullException(nameof(sqlLoader));
        _parquetWriter = parquetWriter ?? throw new ArgumentNullException(nameof(parquetWriter));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
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
}
