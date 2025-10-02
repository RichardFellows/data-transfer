using DataTransfer.Core.Models;

namespace DataTransfer.Core.Interfaces;

/// <summary>
/// Extracts data from Parquet files to JSON stream
/// </summary>
public interface IParquetExtractor
{
    /// <summary>
    /// Extract data from a Parquet file to JSON format
    /// </summary>
    /// <param name="parquetPath">Relative or absolute path to Parquet file</param>
    /// <param name="outputStream">Stream to write JSON data</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Extraction result with row count</returns>
    Task<ExtractionResult> ExtractFromParquetAsync(
        string parquetPath,
        Stream outputStream,
        CancellationToken cancellationToken = default);
}
