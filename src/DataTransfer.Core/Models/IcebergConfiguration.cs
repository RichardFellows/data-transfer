namespace DataTransfer.Core.Models;

/// <summary>
/// Configuration for Iceberg table transfers
/// </summary>
public class IcebergTransferConfiguration
{
    /// <summary>
    /// Iceberg table name
    /// </summary>
    public string TableName { get; set; } = string.Empty;

    /// <summary>
    /// Optional incremental sync configuration
    /// </summary>
    public IncrementalSyncOptions? IncrementalSync { get; set; }
}

/// <summary>
/// Configuration for incremental synchronization with watermark tracking
/// </summary>
public class IncrementalSyncOptions
{
    /// <summary>
    /// Primary key column(s) for merge operations (required for upsert strategy)
    /// </summary>
    public required string PrimaryKeyColumn { get; set; }

    /// <summary>
    /// Watermark column for change detection (e.g., LastModifiedDate, UpdatedTimestamp)
    /// </summary>
    public required string WatermarkColumn { get; set; }

    /// <summary>
    /// Merge strategy: "upsert" (update existing + insert new) or "append" (insert only)
    /// </summary>
    public string MergeStrategy { get; set; } = "upsert";

    /// <summary>
    /// Type of watermark column: "timestamp" or "integer"
    /// </summary>
    public string WatermarkType { get; set; } = "timestamp";
}
