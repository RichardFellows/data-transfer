using DataTransfer.Core.Models;
using DataTransfer.Pipeline;
using DataTransfer.Web.Models;
using Microsoft.Extensions.Logging;

namespace DataTransfer.Web.Services;

/// <summary>
/// Executes transfers and records them in history
/// </summary>
public class TransferExecutionService
{
    private readonly UnifiedTransferOrchestrator _orchestrator;
    private readonly TransferHistoryService _history;
    private readonly ILogger<TransferExecutionService> _logger;

    public TransferExecutionService(
        UnifiedTransferOrchestrator orchestrator,
        TransferHistoryService history,
        ILogger<TransferExecutionService> logger)
    {
        _orchestrator = orchestrator ?? throw new ArgumentNullException(nameof(orchestrator));
        _history = history ?? throw new ArgumentNullException(nameof(history));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <summary>
    /// Executes a transfer and adds it to history
    /// </summary>
    public async Task<TransferResult> ExecuteAsync(
        TransferConfiguration config,
        string transferId,
        CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Starting transfer {TransferId} of type {TransferType}",
            transferId, config.TransferType);

        var result = await _orchestrator.ExecuteTransferAsync(config, cancellationToken);

        // Create history entry
        var historyEntry = new TransferHistoryEntry
        {
            Id = transferId,
            TransferType = config.TransferType,
            StartTime = result.StartTime,
            EndTime = result.EndTime,
            RowsTransferred = result.RowsLoaded,
            Success = result.Success,
            ErrorMessage = result.ErrorMessage,
            SourceInfo = GetSourceInfo(config),
            DestinationInfo = GetDestinationInfo(config)
        };

        await _history.AddAsync(historyEntry);

        _logger.LogInformation("Transfer {TransferId} completed with status: {Success}",
            transferId, result.Success);

        return result;
    }

    private static string GetSourceInfo(TransferConfiguration config)
    {
        return config.Source.Type switch
        {
            SourceType.SqlServer => $"SQL: {config.Source.Table?.FullyQualifiedName ?? "Unknown"}",
            SourceType.Parquet => $"Parquet: {config.Source.ParquetPath ?? "Unknown"}",
            _ => "Unknown"
        };
    }

    private static string GetDestinationInfo(TransferConfiguration config)
    {
        return config.Destination.Type switch
        {
            DestinationType.SqlServer => $"SQL: {config.Destination.Table?.FullyQualifiedName ?? "Unknown"}",
            DestinationType.Parquet => $"Parquet: {config.Destination.ParquetPath ?? "Unknown"}",
            _ => "Unknown"
        };
    }
}
