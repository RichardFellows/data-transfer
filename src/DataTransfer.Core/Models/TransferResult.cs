namespace DataTransfer.Core.Models;

public class TransferResult
{
    public bool Success { get; set; }
    public long RowsExtracted { get; set; }
    public long RowsLoaded { get; set; }
    public DateTime StartTime { get; set; }
    public DateTime EndTime { get; set; }
    public TimeSpan Duration => EndTime - StartTime;
    public string? ErrorMessage { get; set; }
    public string? ParquetFilePath { get; set; }

    /// <summary>
    /// Actual row count from source (verified after extraction)
    /// </summary>
    public long? SourceRowCount { get; set; }

    /// <summary>
    /// Actual row count in destination (verified after load)
    /// </summary>
    public long? DestinationRowCount { get; set; }

    /// <summary>
    /// Whether source and destination counts match
    /// </summary>
    public bool CountsMatch => SourceRowCount.HasValue &&
                               DestinationRowCount.HasValue &&
                               SourceRowCount.Value == DestinationRowCount.Value;

    /// <summary>
    /// Difference between destination and source counts (DestinationRowCount - SourceRowCount)
    /// Returns null if either count is not available
    /// </summary>
    public long? CountDifference => SourceRowCount.HasValue && DestinationRowCount.HasValue
        ? DestinationRowCount.Value - SourceRowCount.Value
        : null;

    /// <summary>
    /// Validation message for row count verification results
    /// </summary>
    public string? ValidationMessage { get; set; }
}
