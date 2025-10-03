namespace DataTransfer.Web.Models;

/// <summary>
/// Represents metadata about a database table or view
/// </summary>
public class TableInfo
{
    /// <summary>
    /// The name of the table or view
    /// </summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>
    /// The type of object (TABLE or VIEW)
    /// </summary>
    public string Type { get; set; } = string.Empty;

    /// <summary>
    /// The full display name including type
    /// </summary>
    public string FullName => $"{Name} ({Type})";
}
