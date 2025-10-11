using DataTransfer.Iceberg.Models;

namespace DataTransfer.Iceberg.Watermarks;

/// <summary>
/// Stores and retrieves watermarks for incremental sync
/// </summary>
public interface IWatermarkStore
{
    /// <summary>
    /// Gets the last watermark for a table
    /// </summary>
    /// <param name="tableName">Table name</param>
    /// <returns>Watermark or null if never synchronized</returns>
    Task<Watermark?> GetWatermarkAsync(string tableName);

    /// <summary>
    /// Stores a watermark for a table
    /// </summary>
    /// <param name="tableName">Table name</param>
    /// <param name="watermark">Watermark to store</param>
    Task SetWatermarkAsync(string tableName, Watermark watermark);
}
