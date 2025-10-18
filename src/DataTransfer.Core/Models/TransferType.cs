namespace DataTransfer.Core.Models;

/// <summary>
/// Defines the type of data transfer operation
/// </summary>
public enum TransferType
{
    /// <summary>
    /// SQL Server to SQL Server (via Parquet intermediate storage)
    /// </summary>
    SqlToSql,

    /// <summary>
    /// SQL Server to Parquet (export only)
    /// </summary>
    SqlToParquet,

    /// <summary>
    /// Parquet to SQL Server (import only)
    /// </summary>
    ParquetToSql,

    /// <summary>
    /// SQL Server to Iceberg (export to Iceberg table format)
    /// </summary>
    SqlToIceberg,

    /// <summary>
    /// Iceberg to SQL Server (import from Iceberg table format)
    /// </summary>
    IcebergToSql,

    /// <summary>
    /// SQL Server to Iceberg with incremental sync (delta loads with watermark tracking)
    /// </summary>
    SqlToIcebergIncremental
}
