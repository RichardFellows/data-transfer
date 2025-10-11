namespace DataTransfer.Iceberg.Models;

/// <summary>
/// Result of an incremental synchronization operation
/// </summary>
public class SyncResult
{
    /// <summary>
    /// Whether the sync succeeded
    /// </summary>
    public bool Success { get; set; }

    /// <summary>
    /// Number of rows extracted from source database
    /// </summary>
    public int RowsExtracted { get; set; }

    /// <summary>
    /// Number of rows appended to Iceberg table
    /// </summary>
    public int RowsAppended { get; set; }

    /// <summary>
    /// Number of rows imported to target database
    /// </summary>
    public int RowsImported { get; set; }

    /// <summary>
    /// New Iceberg snapshot ID created
    /// </summary>
    public long NewSnapshotId { get; set; }

    /// <summary>
    /// Updated watermark after sync
    /// </summary>
    public Watermark? NewWatermark { get; set; }

    /// <summary>
    /// Duration of the sync operation
    /// </summary>
    public TimeSpan Duration { get; set; }

    /// <summary>
    /// Error message if sync failed
    /// </summary>
    public string? ErrorMessage { get; set; }
}
