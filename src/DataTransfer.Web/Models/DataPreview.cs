namespace DataTransfer.Web.Models;

/// <summary>
/// Represents a preview of table/file data including schema and sample rows
/// </summary>
public class DataPreview
{
    /// <summary>
    /// Column definitions (name + data type)
    /// </summary>
    public List<ColumnInfo> Columns { get; set; } = new();

    /// <summary>
    /// Sample data rows (max 10)
    /// Each row is a dictionary: ColumnName => Value
    /// </summary>
    public List<Dictionary<string, object?>> Rows { get; set; } = new();

    /// <summary>
    /// Total row count (if available, otherwise null)
    /// </summary>
    public long? TotalRowCount { get; set; }
}

/// <summary>
/// Represents a column definition with name and data type
/// </summary>
public class ColumnInfo
{
    /// <summary>
    /// Column name
    /// </summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>
    /// Data type (e.g., "INT", "NVARCHAR(100)", "VARCHAR", etc.)
    /// </summary>
    public string DataType { get; set; } = string.Empty;

    /// <summary>
    /// Whether the column allows NULL values
    /// </summary>
    public bool IsNullable { get; set; }
}
