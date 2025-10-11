namespace DataTransfer.Iceberg.Models;

/// <summary>
/// Result of appending data to an Iceberg table
/// </summary>
public class AppendResult
{
    /// <summary>
    /// Whether the append operation succeeded
    /// </summary>
    public bool Success { get; set; }

    /// <summary>
    /// ID of the newly created snapshot
    /// </summary>
    public long NewSnapshotId { get; set; }

    /// <summary>
    /// Number of rows appended
    /// </summary>
    public int RowsAppended { get; set; }

    /// <summary>
    /// Number of data files created for the append
    /// </summary>
    public int DataFileCount { get; set; }

    /// <summary>
    /// Error message if the operation failed
    /// </summary>
    public string? ErrorMessage { get; set; }
}
