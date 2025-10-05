using System.Text.Json.Serialization;

namespace DataTransfer.Core.Models.Iceberg;

/// <summary>
/// Root table metadata - the single source of truth for an Iceberg table
/// This is the v{N}.metadata.json file stored in the table's metadata directory
/// </summary>
public class IcebergTableMetadata
{
    /// <summary>
    /// Iceberg format version (1 or 2)
    /// </summary>
    [JsonPropertyName("format-version")]
    public int FormatVersion { get; set; } = 2;

    /// <summary>
    /// Unique identifier for the table
    /// </summary>
    [JsonPropertyName("table-uuid")]
    public string TableUuid { get; set; } = string.Empty;

    /// <summary>
    /// Base location of the table (warehouse path)
    /// </summary>
    [JsonPropertyName("location")]
    public string Location { get; set; } = string.Empty;

    /// <summary>
    /// Last updated timestamp in milliseconds since epoch
    /// </summary>
    [JsonPropertyName("last-updated-ms")]
    public long LastUpdatedMs { get; set; }

    /// <summary>
    /// Highest assigned column/field ID
    /// </summary>
    [JsonPropertyName("last-column-id")]
    public int LastColumnId { get; set; }

    /// <summary>
    /// List of all schemas (current and historical)
    /// </summary>
    [JsonPropertyName("schemas")]
    public List<IcebergSchema> Schemas { get; set; } = new();

    /// <summary>
    /// ID of the current schema
    /// </summary>
    [JsonPropertyName("current-schema-id")]
    public int CurrentSchemaId { get; set; }

    /// <summary>
    /// List of partition specifications
    /// </summary>
    [JsonPropertyName("partition-specs")]
    public List<object> PartitionSpecs { get; set; } = new();

    /// <summary>
    /// ID of the default partition spec
    /// </summary>
    [JsonPropertyName("default-spec-id")]
    public int DefaultSpecId { get; set; } = 0;

    /// <summary>
    /// Highest assigned partition field ID
    /// </summary>
    [JsonPropertyName("last-partition-id")]
    public int LastPartitionId { get; set; } = 0;

    /// <summary>
    /// List of snapshots (table states at points in time)
    /// </summary>
    [JsonPropertyName("snapshots")]
    public List<IcebergSnapshot> Snapshots { get; set; } = new();

    /// <summary>
    /// ID of the current snapshot (nullable - null for empty tables)
    /// </summary>
    [JsonPropertyName("current-snapshot-id")]
    public long? CurrentSnapshotId { get; set; }
}
