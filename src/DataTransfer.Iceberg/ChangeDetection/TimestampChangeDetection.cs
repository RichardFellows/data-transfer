using DataTransfer.Iceberg.Models;
using Microsoft.Data.SqlClient;

namespace DataTransfer.Iceberg.ChangeDetection;

/// <summary>
/// Detects changes using timestamp-based watermark
/// </summary>
public class TimestampChangeDetection : IChangeDetectionStrategy
{
    private readonly string _watermarkColumn;

    public TimestampChangeDetection(string watermarkColumn)
    {
        _watermarkColumn = watermarkColumn ?? throw new ArgumentNullException(nameof(watermarkColumn));
    }

    public Task<IncrementalQuery> BuildIncrementalQueryAsync(
        string tableName,
        Watermark? lastWatermark,
        SqlConnection connection)
    {
        string query;
        Dictionary<string, object> parameters;

        if (lastWatermark == null || !lastWatermark.LastSyncTimestamp.HasValue)
        {
            // First sync - get all rows
            query = $"SELECT * FROM {tableName}";
            parameters = new Dictionary<string, object>();
        }
        else
        {
            // Incremental - get rows modified after watermark
            query = $"SELECT * FROM {tableName} WHERE {_watermarkColumn} > @WatermarkValue";
            parameters = new Dictionary<string, object>
            {
                ["@WatermarkValue"] = lastWatermark.LastSyncTimestamp.Value
            };
        }

        return Task.FromResult(new IncrementalQuery
        {
            Sql = query,
            Parameters = parameters
        });
    }
}
