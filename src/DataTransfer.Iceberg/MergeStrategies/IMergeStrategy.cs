using DataTransfer.Iceberg.Models;
using Microsoft.Data.SqlClient;

namespace DataTransfer.Iceberg.MergeStrategies;

/// <summary>
/// Strategy for merging data from temp table into target table
/// </summary>
public interface IMergeStrategy
{
    /// <summary>
    /// Merges data from temp table into target table
    /// </summary>
    /// <param name="connection">Open SQL connection</param>
    /// <param name="targetTable">Target table name</param>
    /// <param name="tempTable">Temporary staging table name</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Merge result with inserted/updated counts</returns>
    Task<MergeResult> MergeAsync(
        SqlConnection connection,
        string targetTable,
        string tempTable,
        CancellationToken cancellationToken = default);
}
