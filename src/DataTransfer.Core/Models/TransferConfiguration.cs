namespace DataTransfer.Core.Models;

/// <summary>
/// Configuration for a data transfer operation
/// </summary>
public class TransferConfiguration
{
    /// <summary>
    /// Type of transfer operation
    /// </summary>
    public TransferType TransferType { get; set; }

    /// <summary>
    /// Source configuration
    /// </summary>
    public SourceConfiguration Source { get; set; } = new();

    /// <summary>
    /// Destination configuration
    /// </summary>
    public DestinationConfiguration Destination { get; set; } = new();

    /// <summary>
    /// Optional partitioning configuration
    /// </summary>
    public PartitioningConfiguration? Partitioning { get; set; }
}

/// <summary>
/// Configuration for the data source
/// </summary>
public class SourceConfiguration
{
    /// <summary>
    /// Type of source
    /// </summary>
    public SourceType Type { get; set; }

    // For SQL Server sources
    /// <summary>
    /// Connection string for SQL Server source
    /// </summary>
    public string? ConnectionString { get; set; }

    /// <summary>
    /// Table identifier for SQL Server source
    /// </summary>
    public TableIdentifier? Table { get; set; }

    // For Parquet sources
    /// <summary>
    /// Path to Parquet file source
    /// </summary>
    public string? ParquetPath { get; set; }

    // For Iceberg sources
    /// <summary>
    /// Iceberg table configuration for source
    /// </summary>
    public IcebergTransferConfiguration? IcebergTable { get; set; }
}

/// <summary>
/// Configuration for the data destination
/// </summary>
public class DestinationConfiguration
{
    /// <summary>
    /// Type of destination
    /// </summary>
    public DestinationType Type { get; set; }

    // For SQL Server destinations
    /// <summary>
    /// Connection string for SQL Server destination
    /// </summary>
    public string? ConnectionString { get; set; }

    /// <summary>
    /// Table identifier for SQL Server destination
    /// </summary>
    public TableIdentifier? Table { get; set; }

    // For Parquet destinations
    /// <summary>
    /// Path to Parquet file destination
    /// </summary>
    public string? ParquetPath { get; set; }

    /// <summary>
    /// Compression algorithm for Parquet files (default: Snappy)
    /// </summary>
    public string? Compression { get; set; } = "Snappy";

    // For Iceberg destinations
    /// <summary>
    /// Iceberg table configuration for destination
    /// </summary>
    public IcebergTransferConfiguration? IcebergTable { get; set; }
}
