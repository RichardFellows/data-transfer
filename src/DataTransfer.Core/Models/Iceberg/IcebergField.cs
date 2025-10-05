using System.Text.Json.Serialization;

namespace DataTransfer.Core.Models.Iceberg;

/// <summary>
/// Iceberg field with stable field-id for schema evolution
/// </summary>
public class IcebergField
{
    /// <summary>
    /// Field ID - CRITICAL: Enables schema evolution in Iceberg
    /// Field IDs are stable identifiers that persist across schema changes
    /// </summary>
    [JsonPropertyName("id")]
    public int Id { get; set; }

    /// <summary>
    /// Field name
    /// </summary>
    [JsonPropertyName("name")]
    public string Name { get; set; } = string.Empty;

    /// <summary>
    /// Whether the field is required (non-nullable)
    /// </summary>
    [JsonPropertyName("required")]
    public bool Required { get; set; }

    /// <summary>
    /// Field type - string for primitives (e.g., "int", "string"),
    /// object for complex types (e.g., decimal with precision/scale)
    /// </summary>
    [JsonPropertyName("type")]
    public object Type { get; set; } = string.Empty;
}
