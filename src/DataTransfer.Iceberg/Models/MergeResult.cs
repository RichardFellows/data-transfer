namespace DataTransfer.Iceberg.Models;

/// <summary>
/// Result of a merge operation
/// </summary>
public class MergeResult
{
    /// <summary>
    /// Number of rows inserted
    /// </summary>
    public int Inserted { get; set; }

    /// <summary>
    /// Number of rows updated
    /// </summary>
    public int Updated { get; set; }

    /// <summary>
    /// Total rows affected (inserted + updated)
    /// </summary>
    public int TotalAffected => Inserted + Updated;
}
