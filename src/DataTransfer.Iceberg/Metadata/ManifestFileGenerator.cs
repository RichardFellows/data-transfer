using Avro;
using Avro.File;
using Avro.Generic;
using Avro.IO;
using DataTransfer.Iceberg.Models;

namespace DataTransfer.Iceberg.Metadata;

/// <summary>
/// Generates Iceberg manifest files (Avro format) listing data files
/// Manifest files are referenced by manifest lists and contain file-level metadata
/// </summary>
public class ManifestFileGenerator
{
    // Iceberg manifest entry schema with field-ids
    private const string ManifestEntrySchemaJson = @"{
        ""type"": ""record"",
        ""name"": ""manifest_entry"",
        ""fields"": [
            {
                ""name"": ""status"",
                ""type"": ""int"",
                ""field-id"": 0
            },
            {
                ""name"": ""snapshot_id"",
                ""type"": [""null"", ""long""],
                ""default"": null,
                ""field-id"": 1
            },
            {
                ""name"": ""data_file"",
                ""type"": {
                    ""type"": ""record"",
                    ""name"": ""data_file"",
                    ""fields"": [
                        {
                            ""name"": ""file_path"",
                            ""type"": ""string"",
                            ""field-id"": 100
                        },
                        {
                            ""name"": ""file_format"",
                            ""type"": ""string"",
                            ""field-id"": 101
                        },
                        {
                            ""name"": ""partition"",
                            ""type"": [""null"", {""type"": ""map"", ""values"": ""string""}],
                            ""default"": null,
                            ""field-id"": 102
                        },
                        {
                            ""name"": ""record_count"",
                            ""type"": ""long"",
                            ""field-id"": 103
                        },
                        {
                            ""name"": ""file_size_in_bytes"",
                            ""type"": ""long"",
                            ""field-id"": 104
                        }
                    ]
                },
                ""field-id"": 2
            }
        ]
    }";

    /// <summary>
    /// Writes an Iceberg manifest file containing data file entries
    /// </summary>
    /// <param name="dataFiles">List of data files to include in manifest</param>
    /// <param name="outputPath">Path where manifest .avro file will be written</param>
    /// <param name="snapshotId">Snapshot ID for these data files</param>
    /// <returns>Path to the written manifest file</returns>
    public string WriteManifest(List<DataFileMetadata> dataFiles, string outputPath, long snapshotId)
    {
        // Validate output directory exists
        var directory = Path.GetDirectoryName(outputPath);
        if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
        {
            throw new DirectoryNotFoundException($"Output directory does not exist: {directory}");
        }

        // Parse manifest entry schema (Apache.Avro 1.11.3+ preserves field-id attributes)
        var schema = Avro.Schema.Parse(ManifestEntrySchemaJson) as RecordSchema;

        if (schema == null)
        {
            throw new InvalidOperationException("Failed to parse manifest entry schema");
        }

        // Create Avro file writer
        using var writer = new AvroDataFileWriter(outputPath, schema);

        // Write each data file as a manifest entry
        foreach (var dataFile in dataFiles)
        {
            var entry = CreateManifestEntry(schema, dataFile, snapshotId);
            writer.WriteRecord(entry);
        }

        // Note: Don't call Close() explicitly - Dispose() will handle it
        return outputPath;
    }

    /// <summary>
    /// Creates a manifest entry record for a data file
    /// </summary>
    private GenericRecord CreateManifestEntry(RecordSchema schema, DataFileMetadata dataFile, long snapshotId)
    {
        var entry = new GenericRecord(schema);

        // Status: 1 = ADDED (per Iceberg spec)
        entry.Add("status", 1);

        // Snapshot ID
        entry.Add("snapshot_id", snapshotId);

        // Data file nested record
        var dataFileSchema = (RecordSchema)((schema.Fields.First(f => f.Name == "data_file")).Schema);
        var dataFileRecord = new GenericRecord(dataFileSchema);

        dataFileRecord.Add("file_path", dataFile.FilePath);
        dataFileRecord.Add("file_format", "PARQUET");
        dataFileRecord.Add("record_count", dataFile.RecordCount);
        dataFileRecord.Add("file_size_in_bytes", dataFile.FileSizeInBytes);

        // Partition values (if any)
        // Note: Avro maps require Dictionary<string, object> not Dictionary<string, string>
        if (dataFile.PartitionValues != null && dataFile.PartitionValues.Any())
        {
            var avroMap = dataFile.PartitionValues.ToDictionary(kvp => kvp.Key, kvp => (object)kvp.Value);
            dataFileRecord.Add("partition", avroMap);
        }
        else
        {
            dataFileRecord.Add("partition", null);
        }

        entry.Add("data_file", dataFileRecord);

        return entry;
    }

    /// <summary>
    /// Helper class for writing Avro manifest files with proper resource management
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
