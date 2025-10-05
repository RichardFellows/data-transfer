using System.Text.Json.Serialization;

namespace DataTransfer.Core.Models.Iceberg;

/// <summary>
/// Snapshot representing table state at a point in time
/// </summary>
public class IcebergSnapshot
{
    /// <summary>
    /// Unique snapshot identifier
    /// </summary>
    [JsonPropertyName("snapshot-id")]
    public long SnapshotId { get; set; }

    /// <summary>
    /// Snapshot timestamp in milliseconds since epoch
    /// </summary>
    [JsonPropertyName("timestamp-ms")]
    public long TimestampMs { get; set; }

    /// <summary>
    /// Relative path to the manifest list file for this snapshot
    /// </summary>
    [JsonPropertyName("manifest-list")]
    public string ManifestList { get; set; } = string.Empty;
}
