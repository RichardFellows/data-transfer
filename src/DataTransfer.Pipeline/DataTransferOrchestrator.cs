using DataTransfer.Core.Interfaces;
using DataTransfer.Core.Models;
using DataTransfer.Core.Strategies;
using Microsoft.Extensions.Logging;

namespace DataTransfer.Pipeline;

public class DataTransferOrchestrator
{
    private readonly ITableExtractor _extractor;
    private readonly IParquetStorage _storage;
    private readonly IDataLoader _loader;
    private readonly ILogger<DataTransferOrchestrator> _logger;

    public DataTransferOrchestrator(
        ITableExtractor extractor,
        IParquetStorage storage,
        IDataLoader loader,
        ILogger<DataTransferOrchestrator> logger)
    {
        _extractor = extractor ?? throw new ArgumentNullException(nameof(extractor));
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        _loader = loader ?? throw new ArgumentNullException(nameof(loader));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task<TransferResult> TransferTableAsync(
        TableConfiguration tableConfig,
        string sourceConnectionString,
        string destinationConnectionString,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(tableConfig);

        if (string.IsNullOrWhiteSpace(sourceConnectionString))
        {
            throw new ArgumentException("Source connection string cannot be empty", nameof(sourceConnectionString));
        }

        if (string.IsNullOrWhiteSpace(destinationConnectionString))
        {
            throw new ArgumentException("Destination connection string cannot be empty", nameof(destinationConnectionString));
        }

        var result = new TransferResult
        {
            StartTime = DateTime.UtcNow
        };

        try
        {
            _logger.LogInformation("Starting transfer for table {Table}", tableConfig.Source.FullyQualifiedName);

            // Get partition strategy
            var partitionStrategy = PartitionStrategyFactory.Create(tableConfig.Partitioning);
            var partitionDate = DateTime.UtcNow;

            // Generate Parquet file path
            var fileName = $"{tableConfig.Destination.Schema}_{tableConfig.Destination.Table}_{partitionDate:yyyyMMddHHmmss}.parquet";

            // Step 1: Extract from source
            _logger.LogInformation("Extracting data from source table {Table}", tableConfig.Source.FullyQualifiedName);
            using var extractStream = new MemoryStream();
            var extractionResult = await _extractor.ExtractAsync(
                tableConfig,
                sourceConnectionString,
                extractStream,
                cancellationToken);

            result.RowsExtracted = extractionResult.RowsExtracted;
            _logger.LogInformation("Extracted {RowCount} rows from {Table}", extractionResult.RowsExtracted, tableConfig.Source.FullyQualifiedName);

            // Step 2: Write to Parquet
            extractStream.Position = 0;
            _logger.LogInformation("Writing data to Parquet file {FileName}", fileName);
            await _storage.WriteAsync(extractStream, fileName, partitionDate, cancellationToken);
            _logger.LogInformation("Successfully wrote data to Parquet file");

            // Calculate full Parquet path for reading
            var partitionPath = $"year={partitionDate.Year:D4}/month={partitionDate.Month:D2}/day={partitionDate.Day:D2}";
            var fullParquetPath = $"{partitionPath}/{fileName}";
            result.ParquetFilePath = fullParquetPath;

            // Step 3: Read from Parquet
            _logger.LogInformation("Reading data from Parquet file {FilePath}", fullParquetPath);
            using var parquetStream = await _storage.ReadAsync(fullParquetPath, cancellationToken);

            // Step 4: Load to destination
            _logger.LogInformation("Loading data to destination table {Table}", tableConfig.Destination.FullyQualifiedName);
            var loadResult = await _loader.LoadAsync(
                tableConfig,
                destinationConnectionString,
                parquetStream,
                cancellationToken);

            result.RowsLoaded = loadResult.RowsLoaded;
            _logger.LogInformation("Loaded {RowCount} rows to {Table}", loadResult.RowsLoaded, tableConfig.Destination.FullyQualifiedName);

            result.Success = true;
            result.EndTime = DateTime.UtcNow;

            _logger.LogInformation("Transfer completed successfully in {Duration}ms", result.Duration.TotalMilliseconds);

            return result;
        }
        catch (Exception ex)
        {
            result.Success = false;
            result.EndTime = DateTime.UtcNow;
            result.ErrorMessage = ex.Message;

            _logger.LogError(ex, "Transfer failed for table {Table}: {Error}", tableConfig.Source.FullyQualifiedName, ex.Message);

            throw;
        }
    }
}
