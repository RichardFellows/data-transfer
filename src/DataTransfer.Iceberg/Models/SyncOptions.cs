namespace DataTransfer.Iceberg.Models;

/// <summary>
/// Options for incremental synchronization
/// </summary>
public class SyncOptions
{
    /// <summary>
    /// Primary key column for merge operations
    /// </summary>
    public string PrimaryKeyColumn { get; set; } = "Id";

    /// <summary>
    /// Watermark column for change detection (e.g., ModifiedDate, UpdatedAt)
    /// </summary>
    public string WatermarkColumn { get; set; } = "ModifiedDate";

    /// <summary>
    /// Merge strategy: "upsert" (insert or update) or "append" (insert only)
    /// </summary>
    public string MergeStrategy { get; set; } = "upsert";

    /// <summary>
    /// Warehouse path for Iceberg storage
    /// </summary>
    public required string WarehousePath { get; set; }

    /// <summary>
    /// Watermark storage directory
    /// </summary>
    public required string WatermarkDirectory { get; set; }
}
