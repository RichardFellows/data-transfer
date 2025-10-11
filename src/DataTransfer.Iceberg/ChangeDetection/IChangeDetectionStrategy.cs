using DataTransfer.Iceberg.Models;
using Microsoft.Data.SqlClient;

namespace DataTransfer.Iceberg.ChangeDetection;

/// <summary>
/// Strategy for detecting changes in source database
/// </summary>
public interface IChangeDetectionStrategy
{
    /// <summary>
    /// Builds an incremental query based on watermark
    /// </summary>
    /// <param name="tableName">Source table name</param>
    /// <param name="lastWatermark">Last watermark (null for initial sync)</param>
    /// <param name="connection">SQL connection for metadata queries</param>
    /// <returns>Query to extract changed rows</returns>
    Task<IncrementalQuery> BuildIncrementalQueryAsync(
        string tableName,
        Watermark? lastWatermark,
        SqlConnection connection);
}
