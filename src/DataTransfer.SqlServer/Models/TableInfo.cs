namespace DataTransfer.SqlServer.Models;

/// <summary>
/// Represents metadata about a SQL Server table
/// </summary>
public class TableInfo
{
    /// <summary>
    /// Schema name (e.g., dbo)
    /// </summary>
    public required string Schema { get; init; }

    /// <summary>
    /// Table name
    /// </summary>
    public required string TableName { get; init; }

    /// <summary>
    /// Approximate row count
    /// </summary>
    public long RowCount { get; init; }

    /// <summary>
    /// List of columns in the table
    /// </summary>
    public required List<ColumnInfo> Columns { get; init; }

    /// <summary>
    /// Full table name (Schema.TableName)
    /// </summary>
    public string FullName => $"{Schema}.{TableName}";

    /// <summary>
    /// Get the best partition strategy suggestion for this table
    /// </summary>
    public PartitionSuggestion? GetBestPartitionSuggestion()
    {
        // Check for SCD2 pattern (EffectiveDate + ExpirationDate columns)
        var effectiveCol = Columns.FirstOrDefault(c =>
            c.ColumnName.Equals("EffectiveDate", StringComparison.OrdinalIgnoreCase) ||
            c.ColumnName.Equals("ValidFrom", StringComparison.OrdinalIgnoreCase) ||
            c.ColumnName.Equals("StartDate", StringComparison.OrdinalIgnoreCase));

        var expirationCol = Columns.FirstOrDefault(c =>
            c.ColumnName.Equals("ExpirationDate", StringComparison.OrdinalIgnoreCase) ||
            c.ColumnName.Equals("ValidTo", StringComparison.OrdinalIgnoreCase) ||
            c.ColumnName.Equals("EndDate", StringComparison.OrdinalIgnoreCase));

        if (effectiveCol != null && expirationCol != null)
        {
            return new PartitionSuggestion
            {
                PartitionType = "scd2",
                EffectiveDateColumn = effectiveCol.ColumnName,
                ExpirationDateColumn = expirationCol.ColumnName,
                Reason = $"Table has SCD2 pattern with '{effectiveCol.ColumnName}' and '{expirationCol.ColumnName}' columns",
                Confidence = 0.9
            };
        }

        // For small tables (< 10,000 rows), suggest static partitioning
        if (RowCount < 10000)
        {
            return new PartitionSuggestion
            {
                PartitionType = "static",
                Reason = $"Table is small ({RowCount:N0} rows), static partitioning is sufficient",
                Confidence = 0.85
            };
        }

        // Look for date columns for date-based partitioning
        var dateColumns = Columns
            .Select(c => c.GetPartitionSuggestion())
            .Where(s => s != null)
            .ToList();

        if (dateColumns.Count > 0)
        {
            // Prefer columns with "date" in the name, or the first date column
            var bestDateCol = dateColumns
                .OrderByDescending(s => s!.ColumnName!.Contains("Date", StringComparison.OrdinalIgnoreCase))
                .ThenBy(s => s!.ColumnName)
                .First();

            return new PartitionSuggestion
            {
                PartitionType = bestDateCol!.PartitionType,
                ColumnName = bestDateCol.ColumnName,
                Reason = $"Table has {RowCount:N0} rows and date column '{bestDateCol.ColumnName}' for time-based partitioning",
                Confidence = 0.8
            };
        }

        // Default to static if no other pattern detected
        return new PartitionSuggestion
        {
            PartitionType = "static",
            Reason = "No clear partitioning pattern detected, using static partitioning",
            Confidence = 0.6
        };
    }
}
