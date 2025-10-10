namespace DataTransfer.Iceberg.Models;

/// <summary>
/// Result of an Iceberg table write operation
/// </summary>
public class IcebergWriteResult
{
    /// <summary>
    /// Whether the write operation succeeded
    /// </summary>
    public bool Success { get; set; }

    /// <summary>
    /// Error message if the operation failed
    /// </summary>
    public string ErrorMessage { get; set; } = string.Empty;

    /// <summary>
    /// Unique snapshot ID for this write
    /// </summary>
    public long SnapshotId { get; set; }

    /// <summary>
    /// Full path to the table directory
    /// </summary>
    public string TablePath { get; set; } = string.Empty;

    /// <summary>
    /// Number of data files written
    /// </summary>
    public int DataFileCount { get; set; }

    /// <summary>
    /// Total number of records written
    /// </summary>
    public long RecordCount { get; set; }
}
