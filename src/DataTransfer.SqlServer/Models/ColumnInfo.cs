namespace DataTransfer.SqlServer.Models;

/// <summary>
/// Represents metadata about a SQL Server table column
/// </summary>
public class ColumnInfo
{
    /// <summary>
    /// Column name
    /// </summary>
    public required string ColumnName { get; init; }

    /// <summary>
    /// SQL Server data type (e.g., int, varchar, datetime2)
    /// </summary>
    public required string DataType { get; init; }

    /// <summary>
    /// Whether the column allows NULL values
    /// </summary>
    public bool IsNullable { get; init; }

    /// <summary>
    /// Maximum length for character columns (-1 for MAX)
    /// </summary>
    public int MaxLength { get; init; }

    /// <summary>
    /// Numeric precision for decimal/numeric types
    /// </summary>
    public int Precision { get; init; }

    /// <summary>
    /// Numeric scale for decimal/numeric types
    /// </summary>
    public int Scale { get; init; }

    /// <summary>
    /// Get partition strategy suggestion for this column
    /// </summary>
    /// <returns>Partition suggestion if applicable, null otherwise</returns>
    public PartitionSuggestion? GetPartitionSuggestion()
    {
        var dataTypeLower = DataType.ToLowerInvariant();

        // Suggest date partitioning for date/time columns
        if (IsDateTimeType(dataTypeLower))
        {
            return new PartitionSuggestion
            {
                PartitionType = "date",
                ColumnName = ColumnName,
                Reason = $"Column '{ColumnName}' is a DATE/DATETIME type, suitable for date-based partitioning"
            };
        }

        // Suggest int_date partitioning for integer columns with date-like names
        if (dataTypeLower == "int" && IsDateLikeName(ColumnName))
        {
            return new PartitionSuggestion
            {
                PartitionType = "int_date",
                ColumnName = ColumnName,
                Reason = $"Column '{ColumnName}' is an integer with date-like name (likely YYYYMMDD format)"
            };
        }

        return null;
    }

    private static bool IsDateTimeType(string dataType)
    {
        return dataType switch
        {
            "date" => true,
            "datetime" => true,
            "datetime2" => true,
            "smalldatetime" => true,
            "datetimeoffset" => true,
            _ => false
        };
    }

    private static bool IsDateLikeName(string columnName)
    {
        var nameLower = columnName.ToLowerInvariant();
        return nameLower.Contains("date") ||
               nameLower.Contains("day") ||
               nameLower.EndsWith("dt") ||
               nameLower.EndsWith("key") && nameLower.Contains("date");
    }
}
