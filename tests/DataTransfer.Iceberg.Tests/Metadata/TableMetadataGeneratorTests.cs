using System.Text.Json;
using DataTransfer.Core.Models.Iceberg;
using DataTransfer.Iceberg.Metadata;
using Xunit;

namespace DataTransfer.Iceberg.Tests.Metadata;

public class TableMetadataGeneratorTests : IDisposable
{
    private readonly string _tempDirectory;
    private readonly List<string> _filesToCleanup = new();

    public TableMetadataGeneratorTests()
    {
        _tempDirectory = Path.Combine(Path.GetTempPath(), $"iceberg-metadata-tests-{Guid.NewGuid()}");
        Directory.CreateDirectory(_tempDirectory);
    }

    [Fact]
    public void Should_Create_Initial_Metadata_With_Required_Fields()
    {
        // Arrange
        var generator = new TableMetadataGenerator();
        var schema = new IcebergSchema
        {
            SchemaId = 0,
            Type = "struct",
            Fields = new List<IcebergField>
            {
                new IcebergField { Id = 1, Name = "id", Required = true, Type = "long" },
                new IcebergField { Id = 2, Name = "name", Required = false, Type = "string" }
            }
        };

        // Act
        var metadata = generator.CreateInitialMetadata(
            schema: schema,
            tableLocation: "/warehouse/my_table",
            manifestListPath: "metadata/snap-123.avro",
            snapshotId: 123456789L);

        // Assert
        Assert.Equal(2, metadata.FormatVersion);
        Assert.NotEmpty(metadata.TableUuid);
        Assert.Equal("/warehouse/my_table", metadata.Location);
        Assert.True(metadata.LastUpdatedMs > 0);
        Assert.Equal(2, metadata.LastColumnId);  // Highest field ID
        Assert.Single(metadata.Schemas);
        Assert.Equal(0, metadata.CurrentSchemaId);
        Assert.Empty(metadata.PartitionSpecs);
        Assert.Equal(0, metadata.DefaultSpecId);
        Assert.Equal(0, metadata.LastPartitionId);
        Assert.Single(metadata.Snapshots);
        Assert.Equal(123456789L, metadata.CurrentSnapshotId);
    }

    [Fact]
    public void Should_Include_Schema_In_Metadata()
    {
        // Arrange
        var generator = new TableMetadataGenerator();
        var schema = new IcebergSchema
        {
            SchemaId = 0,
            Type = "struct",
            Fields = new List<IcebergField>
            {
                new IcebergField { Id = 1, Name = "order_id", Required = true, Type = "int" },
                new IcebergField { Id = 2, Name = "customer_name", Required = false, Type = "string" },
                new IcebergField { Id = 3, Name = "order_date", Required = true, Type = "date" }
            }
        };

        // Act
        var metadata = generator.CreateInitialMetadata(
            schema: schema,
            tableLocation: "/warehouse/orders",
            manifestListPath: "metadata/snap-001.avro",
            snapshotId: 1L);

        // Assert
        Assert.Single(metadata.Schemas);
        Assert.Equal(schema, metadata.Schemas[0]);
        Assert.Equal(3, metadata.LastColumnId);  // Max field ID
    }

    [Fact]
    public void Should_Create_Snapshot_With_Manifest_List()
    {
        // Arrange
        var generator = new TableMetadataGenerator();
        var schema = new IcebergSchema
        {
            SchemaId = 0,
            Fields = new List<IcebergField>
            {
                new IcebergField { Id = 1, Name = "id", Required = true, Type = "long" }
            }
        };

        var snapshotId = 999888777L;
        var manifestListPath = "metadata/snap-999888777.avro";

        // Act
        var metadata = generator.CreateInitialMetadata(
            schema: schema,
            tableLocation: "/warehouse/test",
            manifestListPath: manifestListPath,
            snapshotId: snapshotId);

        // Assert
        Assert.Single(metadata.Snapshots);
        var snapshot = metadata.Snapshots[0];
        Assert.Equal(snapshotId, snapshot.SnapshotId);
        Assert.Equal(manifestListPath, snapshot.ManifestList);
        Assert.True(snapshot.TimestampMs > 0);
    }

    [Fact]
    public void Should_Write_Metadata_To_Json_File()
    {
        // Arrange
        var generator = new TableMetadataGenerator();
        var metadata = new IcebergTableMetadata
        {
            FormatVersion = 2,
            TableUuid = "12345678-1234-1234-1234-123456789012",
            Location = "/warehouse/test_table",
            LastUpdatedMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            LastColumnId = 5,
            Schemas = new List<IcebergSchema>
            {
                new IcebergSchema
                {
                    SchemaId = 0,
                    Type = "struct",
                    Fields = new List<IcebergField>
                    {
                        new IcebergField { Id = 1, Name = "id", Required = true, Type = "int" }
                    }
                }
            },
            CurrentSchemaId = 0,
            PartitionSpecs = new List<object>(),
            DefaultSpecId = 0,
            LastPartitionId = 0,
            Snapshots = new List<IcebergSnapshot>
            {
                new IcebergSnapshot
                {
                    SnapshotId = 1L,
                    TimestampMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    ManifestList = "metadata/snap-1.avro"
                }
            },
            CurrentSnapshotId = 1L
        };

        var outputPath = Path.Combine(_tempDirectory, "v1.metadata.json");
        _filesToCleanup.Add(outputPath);

        // Act
        generator.WriteMetadata(metadata, outputPath);

        // Assert
        Assert.True(File.Exists(outputPath));
    }

    [Fact]
    public void Should_Serialize_With_Hyphenated_Property_Names()
    {
        // Arrange
        var generator = new TableMetadataGenerator();
        var metadata = new IcebergTableMetadata
        {
            FormatVersion = 2,
            TableUuid = "test-uuid",
            Location = "/warehouse/test",
            LastUpdatedMs = 1234567890000L,
            LastColumnId = 1,
            Schemas = new List<IcebergSchema>
            {
                new IcebergSchema
                {
                    SchemaId = 0,
                    Type = "struct",
                    Fields = new List<IcebergField>
                    {
                        new IcebergField { Id = 1, Name = "id", Required = true, Type = "int" }
                    }
                }
            },
            CurrentSchemaId = 0,
            PartitionSpecs = new List<object>(),
            DefaultSpecId = 0,
            LastPartitionId = 0,
            Snapshots = new List<IcebergSnapshot>(),
            CurrentSnapshotId = null
        };

        var outputPath = Path.Combine(_tempDirectory, "hyphenated.metadata.json");
        _filesToCleanup.Add(outputPath);

        // Act
        generator.WriteMetadata(metadata, outputPath);

        // Assert - Read and verify JSON property names
        var json = File.ReadAllText(outputPath);
        Assert.Contains("\"format-version\"", json);
        Assert.Contains("\"table-uuid\"", json);
        Assert.Contains("\"last-updated-ms\"", json);
        Assert.Contains("\"last-column-id\"", json);
        Assert.Contains("\"current-schema-id\"", json);
        Assert.Contains("\"partition-specs\"", json);
        Assert.Contains("\"default-spec-id\"", json);
        Assert.Contains("\"last-partition-id\"", json);
        Assert.Contains("\"current-snapshot-id\"", json);
        Assert.Contains("\"schema-id\"", json);  // In nested schema
    }

    [Fact]
    public void Should_Format_Json_With_Indentation()
    {
        // Arrange
        var generator = new TableMetadataGenerator();
        var metadata = new IcebergTableMetadata
        {
            FormatVersion = 2,
            TableUuid = "test-uuid",
            Location = "/warehouse/test",
            LastUpdatedMs = 1234567890000L,
            LastColumnId = 1,
            Schemas = new List<IcebergSchema>
            {
                new IcebergSchema { SchemaId = 0, Type = "struct", Fields = new List<IcebergField>() }
            },
            CurrentSchemaId = 0
        };

        var outputPath = Path.Combine(_tempDirectory, "formatted.metadata.json");
        _filesToCleanup.Add(outputPath);

        // Act
        generator.WriteMetadata(metadata, outputPath);

        // Assert - Verify indentation exists
        var json = File.ReadAllText(outputPath);
        var lines = json.Split('\n');
        Assert.True(lines.Length > 5);  // Multi-line formatted
        Assert.Contains(lines, l => l.StartsWith("  ") || l.StartsWith("\t"));  // Has indentation
    }

    [Fact]
    public void Should_Generate_Unique_Table_Uuid()
    {
        // Arrange
        var generator = new TableMetadataGenerator();
        var schema = new IcebergSchema
        {
            SchemaId = 0,
            Fields = new List<IcebergField>
            {
                new IcebergField { Id = 1, Name = "id", Required = true, Type = "int" }
            }
        };

        // Act
        var metadata1 = generator.CreateInitialMetadata(schema, "/warehouse/table1", "metadata/snap-1.avro", 1L);
        var metadata2 = generator.CreateInitialMetadata(schema, "/warehouse/table2", "metadata/snap-2.avro", 2L);

        // Assert
        Assert.NotEqual(metadata1.TableUuid, metadata2.TableUuid);
        Assert.NotEmpty(metadata1.TableUuid);
        Assert.NotEmpty(metadata2.TableUuid);
    }

    [Fact]
    public void Should_Set_Current_Snapshot_Id_To_Initial_Snapshot()
    {
        // Arrange
        var generator = new TableMetadataGenerator();
        var schema = new IcebergSchema
        {
            SchemaId = 0,
            Fields = new List<IcebergField>
            {
                new IcebergField { Id = 1, Name = "id", Required = true, Type = "int" }
            }
        };

        var snapshotId = 555L;

        // Act
        var metadata = generator.CreateInitialMetadata(schema, "/warehouse/test", "metadata/snap-555.avro", snapshotId);

        // Assert
        Assert.Equal(snapshotId, metadata.CurrentSnapshotId);
        Assert.Single(metadata.Snapshots);
        Assert.Equal(snapshotId, metadata.Snapshots[0].SnapshotId);
    }

    [Fact]
    public void Should_Throw_When_Output_Directory_Does_Not_Exist()
    {
        // Arrange
        var generator = new TableMetadataGenerator();
        var metadata = new IcebergTableMetadata();
        var invalidPath = "/invalid/path/that/does/not/exist/metadata.json";

        // Act & Assert
        var exception = Assert.Throws<DirectoryNotFoundException>(() =>
        {
            generator.WriteMetadata(metadata, invalidPath);
        });

        Assert.NotNull(exception);
    }

    [Fact]
    public void Should_Return_File_Path_On_Successful_Write()
    {
        // Arrange
        var generator = new TableMetadataGenerator();
        var metadata = new IcebergTableMetadata
        {
            FormatVersion = 2,
            TableUuid = "test-uuid",
            Location = "/warehouse/test",
            Schemas = new List<IcebergSchema>(),
            CurrentSchemaId = 0
        };

        var outputPath = Path.Combine(_tempDirectory, "return-test.metadata.json");
        _filesToCleanup.Add(outputPath);

        // Act
        var result = generator.WriteMetadata(metadata, outputPath);

        // Assert
        Assert.Equal(outputPath, result);
    }

    [Fact]
    public void Should_Deserialize_Written_Metadata_Correctly()
    {
        // Arrange
        var generator = new TableMetadataGenerator();
        var originalMetadata = new IcebergTableMetadata
        {
            FormatVersion = 2,
            TableUuid = "roundtrip-test-uuid",
            Location = "/warehouse/roundtrip",
            LastUpdatedMs = 1234567890000L,
            LastColumnId = 3,
            Schemas = new List<IcebergSchema>
            {
                new IcebergSchema
                {
                    SchemaId = 0,
                    Type = "struct",
                    Fields = new List<IcebergField>
                    {
                        new IcebergField { Id = 1, Name = "col1", Required = true, Type = "int" },
                        new IcebergField { Id = 2, Name = "col2", Required = false, Type = "string" },
                        new IcebergField { Id = 3, Name = "col3", Required = true, Type = "date" }
                    }
                }
            },
            CurrentSchemaId = 0,
            PartitionSpecs = new List<object>(),
            DefaultSpecId = 0,
            LastPartitionId = 0,
            Snapshots = new List<IcebergSnapshot>
            {
                new IcebergSnapshot
                {
                    SnapshotId = 999L,
                    TimestampMs = 1234567890000L,
                    ManifestList = "metadata/snap-999.avro"
                }
            },
            CurrentSnapshotId = 999L
        };

        var outputPath = Path.Combine(_tempDirectory, "roundtrip.metadata.json");
        _filesToCleanup.Add(outputPath);

        // Act
        generator.WriteMetadata(originalMetadata, outputPath);

        // Read back
        var json = File.ReadAllText(outputPath);
        var options = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
        var deserializedMetadata = JsonSerializer.Deserialize<IcebergTableMetadata>(json, options);

        // Assert
        Assert.NotNull(deserializedMetadata);
        Assert.Equal(originalMetadata.FormatVersion, deserializedMetadata.FormatVersion);
        Assert.Equal(originalMetadata.TableUuid, deserializedMetadata.TableUuid);
        Assert.Equal(originalMetadata.Location, deserializedMetadata.Location);
        Assert.Equal(originalMetadata.LastColumnId, deserializedMetadata.LastColumnId);
        Assert.Equal(originalMetadata.Schemas.Count, deserializedMetadata.Schemas.Count);
        Assert.Equal(originalMetadata.Schemas[0].Fields.Count, deserializedMetadata.Schemas[0].Fields.Count);
        Assert.Equal(originalMetadata.CurrentSnapshotId, deserializedMetadata.CurrentSnapshotId);
    }

    public void Dispose()
    {
        foreach (var file in _filesToCleanup)
        {
            if (File.Exists(file))
            {
                try { File.Delete(file); } catch { /* Ignore */ }
            }
        }

        if (Directory.Exists(_tempDirectory))
        {
            try { Directory.Delete(_tempDirectory, recursive: true); } catch { /* Ignore */ }
        }
    }
}
