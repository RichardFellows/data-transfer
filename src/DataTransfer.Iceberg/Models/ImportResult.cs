namespace DataTransfer.Iceberg.Models;

/// <summary>
/// Result of importing data from Iceberg to SQL Server
/// </summary>
public class ImportResult
{
    /// <summary>
    /// Whether the import succeeded
    /// </summary>
    public bool Success { get; set; }

    /// <summary>
    /// Total number of rows imported from Iceberg
    /// </summary>
    public int RowsImported { get; set; }

    /// <summary>
    /// Number of rows inserted into target table
    /// </summary>
    public int RowsInserted { get; set; }

    /// <summary>
    /// Number of rows updated in target table
    /// </summary>
    public int RowsUpdated { get; set; }

    /// <summary>
    /// Error message if import failed
    /// </summary>
    public string? ErrorMessage { get; set; }
}
