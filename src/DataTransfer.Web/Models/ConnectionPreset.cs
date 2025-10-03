namespace DataTransfer.Web.Models;

/// <summary>
/// Represents a named connection string preset for easier database selection
/// </summary>
public class ConnectionPreset
{
    /// <summary>
    /// The display name of the connection preset
    /// </summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>
    /// The connection string for this preset
    /// </summary>
    public string ConnectionString { get; set; } = string.Empty;
}
