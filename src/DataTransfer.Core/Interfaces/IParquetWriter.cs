namespace DataTransfer.Core.Interfaces;

/// <summary>
/// Writes data directly to Parquet files (simplified wrapper)
/// </summary>
public interface IParquetWriter
{
    /// <summary>
    /// Write JSON stream directly to Parquet file
    /// </summary>
    /// <param name="dataStream">JSON data stream</param>
    /// <param name="outputPath">Output file path</param>
    /// <param name="partitionDate">Optional partition date (null = no partitioning)</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Number of rows written</returns>
    Task<int> WriteToParquetAsync(
        Stream dataStream,
        string outputPath,
        DateTime? partitionDate = null,
        CancellationToken cancellationToken = default);
}
