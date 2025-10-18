namespace DataTransfer.SqlServer.Models;

/// <summary>
/// Represents a suggested partition strategy for a table or column
/// </summary>
public class PartitionSuggestion
{
    /// <summary>
    /// Suggested partition type (date, int_date, scd2, static)
    /// </summary>
    public required string PartitionType { get; init; }

    /// <summary>
    /// Primary partition column name
    /// </summary>
    public string? ColumnName { get; init; }

    /// <summary>
    /// Effective date column for SCD2 tables
    /// </summary>
    public string? EffectiveDateColumn { get; init; }

    /// <summary>
    /// Expiration date column for SCD2 tables
    /// </summary>
    public string? ExpirationDateColumn { get; init; }

    /// <summary>
    /// Explanation of why this strategy was suggested
    /// </summary>
    public required string Reason { get; init; }

    /// <summary>
    /// Confidence level (0.0 to 1.0)
    /// </summary>
    public double Confidence { get; init; } = 0.8;

    /// <summary>
    /// Generate sample configuration JSON for this suggestion
    /// </summary>
    public string ToConfigurationJson()
    {
        return PartitionType switch
        {
            "static" => """
            {
              "type": "static"
            }
            """,
            "date" => $$"""
            {
              "type": "date",
              "column": "{{ColumnName}}"
            }
            """,
            "int_date" => $$"""
            {
              "type": "int_date",
              "column": "{{ColumnName}}"
            }
            """,
            "scd2" => $$"""
            {
              "type": "scd2",
              "scdEffectiveDateColumn": "{{EffectiveDateColumn}}",
              "scdExpirationDateColumn": "{{ExpirationDateColumn}}"
            }
            """,
            _ => "{}"
        };
    }
}
