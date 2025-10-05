namespace DataTransfer.Iceberg.Models;

/// <summary>
/// Metadata about a written Parquet data file
/// Used for generating Iceberg manifest entries
/// </summary>
public class DataFileMetadata
{
    /// <summary>
    /// Full path to the data file
    /// </summary>
    public string FilePath { get; set; } = string.Empty;

    /// <summary>
    /// File size in bytes
    /// </summary>
    public long FileSizeInBytes { get; set; }

    /// <summary>
    /// Total number of records in the file
    /// </summary>
    public long RecordCount { get; set; }

    /// <summary>
    /// Column-level statistics (min/max values, null counts)
    /// Key: column name, Value: statistics object
    /// </summary>
    public Dictionary<string, object> ColumnStatistics { get; set; } = new();

    /// <summary>
    /// Partition values for this file (if partitioned)
    /// </summary>
    public Dictionary<string, string> PartitionValues { get; set; } = new();
}
