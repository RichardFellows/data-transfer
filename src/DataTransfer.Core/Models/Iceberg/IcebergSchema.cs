using System.Text.Json.Serialization;

namespace DataTransfer.Core.Models.Iceberg;

/// <summary>
/// Represents an Iceberg table schema with mandatory field IDs
/// </summary>
public class IcebergSchema
{
    /// <summary>
    /// Schema type - always "struct" for table schemas
    /// </summary>
    [JsonPropertyName("type")]
    public string Type { get; set; } = "struct";

    /// <summary>
    /// Schema identifier for schema evolution
    /// </summary>
    [JsonPropertyName("schema-id")]
    public int SchemaId { get; set; }

    /// <summary>
    /// List of fields in the schema
    /// </summary>
    [JsonPropertyName("fields")]
    public List<IcebergField> Fields { get; set; } = new();
}
