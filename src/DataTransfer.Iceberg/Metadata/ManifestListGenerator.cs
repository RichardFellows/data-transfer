using Avro;
using Avro.File;
using Avro.Generic;
using Avro.IO;

namespace DataTransfer.Iceberg.Metadata;

/// <summary>
/// Generates Iceberg manifest list files (Avro format) indexing all manifests for a snapshot
/// Manifest lists are referenced by snapshots and contain manifest-level metadata
/// </summary>
public class ManifestListGenerator
{
    // Iceberg manifest list schema with field-ids (per Iceberg spec)
    private const string ManifestListSchemaJson = @"{
        ""type"": ""record"",
        ""name"": ""manifest_file"",
        ""fields"": [
            {
                ""name"": ""manifest_path"",
                ""type"": ""string"",
                ""field-id"": 500
            },
            {
                ""name"": ""manifest_length"",
                ""type"": ""long"",
                ""field-id"": 501
            },
            {
                ""name"": ""partition_spec_id"",
                ""type"": ""int"",
                ""field-id"": 502
            },
            {
                ""name"": ""added_files_count"",
                ""type"": [""null"", ""int""],
                ""default"": null,
                ""field-id"": 512
            },
            {
                ""name"": ""existing_files_count"",
                ""type"": [""null"", ""int""],
                ""default"": null,
                ""field-id"": 513
            },
            {
                ""name"": ""deleted_files_count"",
                ""type"": [""null"", ""int""],
                ""default"": null,
                ""field-id"": 514
            }
        ]
    }";

    /// <summary>
    /// Writes a manifest list file with a single manifest entry
    /// </summary>
    /// <param name="manifestPath">Relative path to the manifest file</param>
    /// <param name="outputPath">Path where manifest list .avro file will be written</param>
    /// <param name="manifestSizeInBytes">Size of the manifest file in bytes</param>
    /// <param name="addedFilesCount">Number of files added in this manifest</param>
    /// <returns>Path to the written manifest list file</returns>
    public string WriteManifestList(
        string manifestPath,
        string outputPath,
        long manifestSizeInBytes,
        int addedFilesCount)
    {
        // Validate output directory exists
        var directory = Path.GetDirectoryName(outputPath);
        if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
        {
            throw new DirectoryNotFoundException($"Output directory does not exist: {directory}");
        }

        // Parse manifest list schema (Apache.Avro 1.11.3+ preserves field-id attributes)
        var schema = Avro.Schema.Parse(ManifestListSchemaJson) as RecordSchema;

        if (schema == null)
        {
            throw new InvalidOperationException("Failed to parse manifest list schema");
        }

        // Create Avro file writer
        using var writer = new AvroDataFileWriter(outputPath, schema);

        // Write manifest entry
        var entry = CreateManifestListEntry(schema, manifestPath, manifestSizeInBytes, addedFilesCount);
        writer.WriteRecord(entry);

        return outputPath;
    }

    /// <summary>
    /// Writes a manifest list file with multiple manifest entries
    /// </summary>
    /// <param name="outputPath">Path where manifest list .avro file will be written</param>
    /// <param name="manifests">List of manifest metadata (path, size, added file count)</param>
    /// <returns>Path to the written manifest list file</returns>
    public string WriteManifestList(
        string outputPath,
        List<(string Path, long Size, int AddedCount)> manifests)
    {
        // Validate output directory exists
        var directory = Path.GetDirectoryName(outputPath);
        if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
        {
            throw new DirectoryNotFoundException($"Output directory does not exist: {directory}");
        }

        // Parse manifest list schema (Apache.Avro 1.11.3+ preserves field-id attributes)
        var schema = Avro.Schema.Parse(ManifestListSchemaJson) as RecordSchema;

        if (schema == null)
        {
            throw new InvalidOperationException("Failed to parse manifest list schema");
        }

        // Create Avro file writer
        using var writer = new AvroDataFileWriter(outputPath, schema);

        // Write each manifest entry
        foreach (var (path, size, addedCount) in manifests)
        {
            var entry = CreateManifestListEntry(schema, path, size, addedCount);
            writer.WriteRecord(entry);
        }

        return outputPath;
    }

    /// <summary>
    /// Creates a manifest list entry record for a manifest file
    /// </summary>
    private GenericRecord CreateManifestListEntry(
        RecordSchema schema,
        string manifestPath,
        long manifestSizeInBytes,
        int addedFilesCount)
    {
        var entry = new GenericRecord(schema);

        // Manifest file metadata
        entry.Add("manifest_path", manifestPath);
        entry.Add("manifest_length", manifestSizeInBytes);
        entry.Add("partition_spec_id", 0);  // Default spec for now

        // File counts (tracking changes)
        entry.Add("added_files_count", addedFilesCount);
        entry.Add("existing_files_count", 0);  // For initial snapshots
        entry.Add("deleted_files_count", 0);   // For initial snapshots

        return entry;
    }

    /// <summary>
    /// Helper class for writing Avro manifest list files with proper resource management
    /// </summary>
    private class AvroDataFileWriter : IDisposable
    {
        private readonly IFileWriter<GenericRecord> _writer;
        private readonly FileStream _fileStream;

        public AvroDataFileWriter(string path, RecordSchema schema)
        {
            // Create datum writer for GenericRecord
            var datumWriter = new GenericDatumWriter<GenericRecord>(schema);

            // Create file stream for Avro output
            _fileStream = new FileStream(path, FileMode.Create, FileAccess.Write);

            // Create Avro file writer (preserves field-id attributes from schema)
            var codec = Codec.CreateCodec(Codec.Type.Null);
            _writer = DataFileWriter<GenericRecord>.OpenWriter(datumWriter, _fileStream, codec);
        }

        public void WriteRecord(GenericRecord record)
        {
            _writer.Append(record);
        }

        public void Dispose()
        {
            _writer?.Dispose();
            _fileStream?.Dispose();
        }
    }
}
