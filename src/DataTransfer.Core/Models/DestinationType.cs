namespace DataTransfer.Core.Models;

/// <summary>
/// Defines the destination type for a data transfer
/// </summary>
public enum DestinationType
{
    /// <summary>
    /// SQL Server database
    /// </summary>
    SqlServer,

    /// <summary>
    /// Parquet file
    /// </summary>
    Parquet,

    /// <summary>
    /// Apache Iceberg table (Parquet-backed with metadata)
    /// </summary>
    Iceberg
}
