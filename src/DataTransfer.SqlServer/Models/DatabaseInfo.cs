namespace DataTransfer.SqlServer.Models;

/// <summary>
/// Represents metadata about a SQL Server database
/// </summary>
public class DatabaseInfo
{
    /// <summary>
    /// Database name
    /// </summary>
    public required string DatabaseName { get; init; }

    /// <summary>
    /// SQL Server version
    /// </summary>
    public required string ServerVersion { get; init; }

    /// <summary>
    /// List of tables in the database
    /// </summary>
    public required List<TableInfo> Tables { get; init; }

    /// <summary>
    /// Total number of tables
    /// </summary>
    public int TotalTables => Tables.Count;

    /// <summary>
    /// Total row count across all tables
    /// </summary>
    public long TotalRows => Tables.Sum(t => t.RowCount);

    /// <summary>
    /// Get tables filtered by schema
    /// </summary>
    public List<TableInfo> GetTablesBySchema(string schema)
    {
        return Tables.Where(t => t.Schema.Equals(schema, StringComparison.OrdinalIgnoreCase)).ToList();
    }

    /// <summary>
    /// Find a specific table
    /// </summary>
    public TableInfo? FindTable(string schema, string tableName)
    {
        return Tables.FirstOrDefault(t =>
            t.Schema.Equals(schema, StringComparison.OrdinalIgnoreCase) &&
            t.TableName.Equals(tableName, StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>
    /// Get table name suggestions based on partial input
    /// </summary>
    public List<string> GetTableSuggestions(string partialName, int maxResults = 5)
    {
        return Tables
            .Where(t => t.TableName.Contains(partialName, StringComparison.OrdinalIgnoreCase))
            .OrderBy(t => t.TableName.Length) // Prefer shorter matches
            .ThenBy(t => t.TableName)
            .Take(maxResults)
            .Select(t => t.FullName)
            .ToList();
    }
}
