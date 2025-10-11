using System.Text.Json;
using DataTransfer.Core.Models.Iceberg;

namespace DataTransfer.Iceberg.Metadata;

/// <summary>
/// Generates the root Iceberg table metadata JSON file (v{N}.metadata.json)
/// This is the single source of truth for an Iceberg table
/// </summary>
public class TableMetadataGenerator
{
    /// <summary>
    /// Creates initial table metadata for a new Iceberg table
    /// </summary>
    /// <param name="schema">Iceberg schema with fields and field-ids</param>
    /// <param name="tableLocation">Base location/warehouse path for the table</param>
    /// <param name="manifestListPath">Relative path to the manifest list file</param>
    /// <param name="snapshotId">Unique snapshot identifier</param>
    /// <returns>Initial table metadata ready for serialization</returns>
    public IcebergTableMetadata CreateInitialMetadata(
        IcebergSchema schema,
        string tableLocation,
        string manifestListPath,
        long snapshotId)
    {
        var currentTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        return new IcebergTableMetadata
        {
            FormatVersion = 2,
            TableUuid = Guid.NewGuid().ToString(),
            Location = tableLocation,
            LastUpdatedMs = currentTimestamp,
            LastColumnId = schema.Fields.Any() ? schema.Fields.Max(f => f.Id) : 0,
            Schemas = new List<IcebergSchema> { schema },
            CurrentSchemaId = schema.SchemaId,
            PartitionSpecs = new List<object>(),
            DefaultSpecId = 0,
            LastPartitionId = 0,
            Snapshots = new List<IcebergSnapshot>
            {
                new IcebergSnapshot
                {
                    SnapshotId = snapshotId,
                    TimestampMs = currentTimestamp,
                    ManifestList = manifestListPath
                }
            },
            CurrentSnapshotId = snapshotId
        };
    }

    /// <summary>
    /// Updates existing table metadata with a new snapshot
    /// Preserves all existing snapshots and adds the new one
    /// </summary>
    /// <param name="existingMetadata">Current table metadata</param>
    /// <param name="newSnapshotId">ID for the new snapshot</param>
    /// <param name="manifestListPath">Relative path to the new manifest list</param>
    /// <returns>Updated table metadata with new snapshot</returns>
    public IcebergTableMetadata UpdateMetadataWithNewSnapshot(
        IcebergTableMetadata existingMetadata,
        long newSnapshotId,
        string manifestListPath)
    {
        var currentTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        // Create new snapshot
        var newSnapshot = new IcebergSnapshot
        {
            SnapshotId = newSnapshotId,
            TimestampMs = currentTimestamp,
            ManifestList = manifestListPath
        };

        // Clone existing metadata and add new snapshot
        return new IcebergTableMetadata
        {
            FormatVersion = existingMetadata.FormatVersion,
            TableUuid = existingMetadata.TableUuid,
            Location = existingMetadata.Location,
            LastUpdatedMs = currentTimestamp,
            LastColumnId = existingMetadata.LastColumnId,
            Schemas = existingMetadata.Schemas,
            CurrentSchemaId = existingMetadata.CurrentSchemaId,
            PartitionSpecs = existingMetadata.PartitionSpecs,
            DefaultSpecId = existingMetadata.DefaultSpecId,
            LastPartitionId = existingMetadata.LastPartitionId,
            Snapshots = existingMetadata.Snapshots.Concat(new[] { newSnapshot }).ToList(),
            CurrentSnapshotId = newSnapshotId
        };
    }

    /// <summary>
    /// Writes table metadata to a JSON file with Iceberg-compliant formatting
    /// </summary>
    /// <param name="metadata">Table metadata to write</param>
    /// <param name="outputPath">Path where metadata JSON file will be written</param>
    /// <returns>Path to the written metadata file</returns>
    public string WriteMetadata(IcebergTableMetadata metadata, string outputPath)
    {
        // Validate output directory exists
        var directory = Path.GetDirectoryName(outputPath);
        if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
        {
            throw new DirectoryNotFoundException($"Output directory does not exist: {directory}");
        }

        // Configure JSON serialization for Iceberg compliance
        var options = new JsonSerializerOptions
        {
            WriteIndented = true  // Human-readable formatting
            // Note: Include null values per Iceberg spec
        };

        // Serialize and write
        var json = JsonSerializer.Serialize(metadata, options);
        File.WriteAllText(outputPath, json);

        return outputPath;
    }
}
