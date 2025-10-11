namespace DataTransfer.Iceberg.Models;

/// <summary>
/// Represents an incremental query for extracting changed data
/// </summary>
public class IncrementalQuery
{
    /// <summary>
    /// SQL query to execute
    /// </summary>
    public string Sql { get; set; } = string.Empty;

    /// <summary>
    /// Parameters for the query
    /// </summary>
    public Dictionary<string, object> Parameters { get; set; } = new();
}
