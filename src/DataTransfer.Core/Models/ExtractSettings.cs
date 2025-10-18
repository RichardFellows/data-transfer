namespace DataTransfer.Core.Models;

public class ExtractSettings
{
    public int BatchSize { get; set; }
    public DateRange DateRange { get; set; } = new DateRange();

    /// <summary>
    /// Optional WHERE clause to filter extracted data (without the WHERE keyword)
    /// Example: "Status = 'Active' AND CreatedDate > '2024-01-01'"
    /// </summary>
    public string? WhereClause { get; set; }

    /// <summary>
    /// Optional row limit for extraction (TOP N in SQL Server)
    /// When specified, only the first N rows will be extracted
    /// </summary>
    public int? RowLimit { get; set; }
}
