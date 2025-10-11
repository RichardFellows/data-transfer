namespace DataTransfer.Iceberg.Models;

/// <summary>
/// Tracks sync state between incremental sync runs
/// </summary>
public class Watermark
{
    /// <summary>
    /// Name of the table being synchronized
    /// </summary>
    public string TableName { get; set; } = string.Empty;

    /// <summary>
    /// Last sync timestamp for timestamp-based change detection
    /// </summary>
    public DateTime? LastSyncTimestamp { get; set; }

    /// <summary>
    /// Last sync ID for ID-based change detection
    /// </summary>
    public long? LastSyncId { get; set; }

    /// <summary>
    /// Last Iceberg snapshot ID that was synchronized
    /// </summary>
    public long? LastIcebergSnapshot { get; set; }

    /// <summary>
    /// Number of rows synchronized in last sync
    /// </summary>
    public int RowCount { get; set; }

    /// <summary>
    /// When this watermark was created/updated
    /// </summary>
    public DateTime CreatedAt { get; set; }
}
