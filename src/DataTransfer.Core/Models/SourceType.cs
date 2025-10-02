namespace DataTransfer.Core.Models;

/// <summary>
/// Defines the source type for a data transfer
/// </summary>
public enum SourceType
{
    /// <summary>
    /// SQL Server database
    /// </summary>
    SqlServer,

    /// <summary>
    /// Parquet file
    /// </summary>
    Parquet
}
