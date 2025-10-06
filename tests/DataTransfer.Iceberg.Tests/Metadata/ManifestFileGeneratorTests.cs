using Avro.File;
using Avro.Generic;
using DataTransfer.Iceberg.Metadata;
using DataTransfer.Iceberg.Models;
using Xunit;
using System.Linq;

namespace DataTransfer.Iceberg.Tests.Metadata;

public class ManifestFileGeneratorTests : IDisposable
{
    private readonly string _tempDirectory;
    private readonly List<string> _filesToCleanup = new();

    public ManifestFileGeneratorTests()
    {
        _tempDirectory = Path.Combine(Path.GetTempPath(), $"iceberg-manifest-tests-{Guid.NewGuid()}");
        Directory.CreateDirectory(_tempDirectory);
    }

    [Fact]
    public void Should_Create_Valid_Avro_Manifest_File()
    {
        // Arrange
        var generator = new ManifestFileGenerator();
        var dataFiles = new List<DataFileMetadata>
        {
            new DataFileMetadata
            {
                FilePath = "data/file-001.parquet",
                FileSizeInBytes = 1024,
                RecordCount = 100
            }
        };

        var outputPath = Path.Combine(_tempDirectory, "manifest-test.avro");
        _filesToCleanup.Add(outputPath);

        // Act
        var manifestPath = generator.WriteManifest(dataFiles, outputPath, snapshotId: 12345);

        // Assert
        Assert.Equal(outputPath, manifestPath);
        Assert.True(File.Exists(manifestPath));
    }

    [Fact]
    public void Should_Write_Manifest_Entry_With_Required_Fields()
    {
        // Arrange
        var generator = new ManifestFileGenerator();
        var dataFiles = new List<DataFileMetadata>
        {
            new DataFileMetadata
            {
                FilePath = "data/year=2025/month=01/data-001.parquet",
                FileSizeInBytes = 2048,
                RecordCount = 250
            }
        };

        var outputPath = Path.Combine(_tempDirectory, "manifest-entry.avro");
        _filesToCleanup.Add(outputPath);

        // Act
        generator.WriteManifest(dataFiles, outputPath, snapshotId: 99999);

        // Assert - Read back and verify
        using var reader = DataFileReader<GenericRecord>.OpenReader(outputPath);

        GenericRecord? record = null;
        while (reader.HasNext())
        {
            record = reader.Next();
            break;  // Get first record
        }

        Assert.NotNull(record);
        Assert.Equal(1, record["status"]);  // 1 = ADDED
        Assert.Equal(99999L, record["snapshot_id"]);

        var dataFileRecord = (GenericRecord)record["data_file"];
        Assert.NotNull(dataFileRecord);
        Assert.Equal("data/year=2025/month=01/data-001.parquet", dataFileRecord["file_path"]);
        Assert.Equal("PARQUET", dataFileRecord["file_format"]);
        Assert.Equal(2048L, dataFileRecord["file_size_in_bytes"]);
        Assert.Equal(250L, dataFileRecord["record_count"]);
    }

    [Fact]
    public void Should_Preserve_Field_Ids_In_Manifest_Schema()
    {
        // Arrange
        var generator = new ManifestFileGenerator();
        var dataFiles = new List<DataFileMetadata>
        {
            new DataFileMetadata
            {
                FilePath = "data/test.parquet",
                FileSizeInBytes = 512,
                RecordCount = 10
            }
        };

        var outputPath = Path.Combine(_tempDirectory, "manifest-field-ids.avro");
        _filesToCleanup.Add(outputPath);

        // Act
        generator.WriteManifest(dataFiles, outputPath, snapshotId: 1);

        // Assert - Read schema and verify field-ids are present
        using var reader = DataFileReader<GenericRecord>.OpenReader(outputPath);
        var schema = reader.GetSchema();
        var schemaJson = schema.ToString();

        // Verify Iceberg-required field-ids are present in schema
        Assert.Contains("\"field-id\"", schemaJson);
        Assert.Contains("\"field-id\":0", schemaJson);  // status
        Assert.Contains("\"field-id\":1", schemaJson);  // snapshot_id
        Assert.Contains("\"field-id\":2", schemaJson);  // data_file
    }

    [Fact]
    public void Should_Handle_Multiple_Data_Files()
    {
        // Arrange
        var generator = new ManifestFileGenerator();
        var dataFiles = new List<DataFileMetadata>
        {
            new DataFileMetadata { FilePath = "data/file-001.parquet", FileSizeInBytes = 1024, RecordCount = 100 },
            new DataFileMetadata { FilePath = "data/file-002.parquet", FileSizeInBytes = 2048, RecordCount = 200 },
            new DataFileMetadata { FilePath = "data/file-003.parquet", FileSizeInBytes = 4096, RecordCount = 400 }
        };

        var outputPath = Path.Combine(_tempDirectory, "manifest-multiple.avro");
        _filesToCleanup.Add(outputPath);

        // Act
        generator.WriteManifest(dataFiles, outputPath, snapshotId: 5000);

        // Assert - Verify all entries written
        using var reader = DataFileReader<GenericRecord>.OpenReader(outputPath);
        var records = new List<GenericRecord>();

        while (reader.HasNext())
        {
            records.Add(reader.Next());
        }

        Assert.Equal(3, records.Count);

        var dataFile1 = (GenericRecord)records[0]["data_file"];
        Assert.Equal("data/file-001.parquet", dataFile1["file_path"]);
        Assert.Equal(100L, dataFile1["record_count"]);

        var dataFile2 = (GenericRecord)records[1]["data_file"];
        Assert.Equal("data/file-002.parquet", dataFile2["file_path"]);
        Assert.Equal(200L, dataFile2["record_count"]);

        var dataFile3 = (GenericRecord)records[2]["data_file"];
        Assert.Equal("data/file-003.parquet", dataFile3["file_path"]);
        Assert.Equal(400L, dataFile3["record_count"]);
    }

    [Fact]
    public void Should_Handle_Partition_Values()
    {
        // Arrange
        var generator = new ManifestFileGenerator();
        var dataFiles = new List<DataFileMetadata>
        {
            new DataFileMetadata
            {
                FilePath = "data/year=2025/month=01/data.parquet",
                FileSizeInBytes = 1024,
                RecordCount = 100,
                PartitionValues = new Dictionary<string, string>
                {
                    { "year", "2025" },
                    { "month", "01" }
                }
            }
        };

        var outputPath = Path.Combine(_tempDirectory, "manifest-partitions.avro");
        _filesToCleanup.Add(outputPath);

        // Act
        generator.WriteManifest(dataFiles, outputPath, snapshotId: 1);

        // Assert
        using var reader = DataFileReader<GenericRecord>.OpenReader(outputPath);

        GenericRecord? record = null;
        while (reader.HasNext())
        {
            record = reader.Next();
            break;  // Get first record
        }

        Assert.NotNull(record);
        var dataFileRecord = (GenericRecord)record["data_file"];
        var partition = dataFileRecord["partition"];

        Assert.NotNull(partition);
        // Partition should be a map/dictionary
        if (partition is IDictionary<string, object> partitionMap)
        {
            Assert.Equal("2025", partitionMap["year"]);
            Assert.Equal("01", partitionMap["month"]);
        }
    }

    [Fact]
    public void Should_Set_Status_To_Added_For_New_Files()
    {
        // Arrange
        var generator = new ManifestFileGenerator();
        var dataFiles = new List<DataFileMetadata>
        {
            new DataFileMetadata
            {
                FilePath = "data/new-file.parquet",
                FileSizeInBytes = 512,
                RecordCount = 50
            }
        };

        var outputPath = Path.Combine(_tempDirectory, "manifest-status.avro");
        _filesToCleanup.Add(outputPath);

        // Act
        generator.WriteManifest(dataFiles, outputPath, snapshotId: 1);

        // Assert
        using var reader = DataFileReader<GenericRecord>.OpenReader(outputPath);

        GenericRecord? record = null;
        while (reader.HasNext())
        {
            record = reader.Next();
            break;  // Get first record
        }

        Assert.NotNull(record);
        // Status 1 = ADDED (per Iceberg spec)
        Assert.Equal(1, record["status"]);
    }

    [Fact]
    public void Should_Throw_When_Output_Path_Is_Invalid()
    {
        // Arrange
        var generator = new ManifestFileGenerator();
        var dataFiles = new List<DataFileMetadata>
        {
            new DataFileMetadata { FilePath = "data/test.parquet", FileSizeInBytes = 1024, RecordCount = 100 }
        };

        var invalidPath = "/invalid/path/that/does/not/exist/manifest.avro";

        // Act & Assert
        Assert.Throws<DirectoryNotFoundException>(() =>
            generator.WriteManifest(dataFiles, invalidPath, snapshotId: 1));
    }

    [Fact]
    public void Should_Return_Manifest_Path_On_Success()
    {
        // Arrange
        var generator = new ManifestFileGenerator();
        var dataFiles = new List<DataFileMetadata>
        {
            new DataFileMetadata { FilePath = "data/test.parquet", FileSizeInBytes = 1024, RecordCount = 100 }
        };

        var outputPath = Path.Combine(_tempDirectory, "manifest-return.avro");
        _filesToCleanup.Add(outputPath);

        // Act
        var result = generator.WriteManifest(dataFiles, outputPath, snapshotId: 1);

        // Assert
        Assert.Equal(outputPath, result);
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
