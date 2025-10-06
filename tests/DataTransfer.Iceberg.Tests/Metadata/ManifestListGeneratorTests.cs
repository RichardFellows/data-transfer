using Avro.File;
using Avro.Generic;
using DataTransfer.Iceberg.Metadata;
using Xunit;

namespace DataTransfer.Iceberg.Tests.Metadata;

public class ManifestListGeneratorTests : IDisposable
{
    private readonly string _tempDirectory;
    private readonly List<string> _filesToCleanup = new();

    public ManifestListGeneratorTests()
    {
        _tempDirectory = Path.Combine(Path.GetTempPath(), $"iceberg-manifest-list-tests-{Guid.NewGuid()}");
        Directory.CreateDirectory(_tempDirectory);
    }

    [Fact]
    public void Should_Create_Valid_Avro_Manifest_List_File()
    {
        // Arrange
        var generator = new ManifestListGenerator();
        var manifestPath = "metadata/manifest-abc.avro";
        var outputPath = Path.Combine(_tempDirectory, "manifest-list-test.avro");
        _filesToCleanup.Add(outputPath);

        // Create a dummy manifest file for size calculation
        var dummyManifestPath = Path.Combine(_tempDirectory, "manifest-abc.avro");
        File.WriteAllText(dummyManifestPath, "dummy content for size test");
        _filesToCleanup.Add(dummyManifestPath);

        // Act
        var manifestListPath = generator.WriteManifestList(
            manifestPath: manifestPath,
            outputPath: outputPath,
            manifestSizeInBytes: new FileInfo(dummyManifestPath).Length,
            addedFilesCount: 5);

        // Assert
        Assert.Equal(outputPath, manifestListPath);
        Assert.True(File.Exists(manifestListPath));
    }

    [Fact]
    public void Should_Write_Manifest_List_Entry_With_Required_Fields()
    {
        // Arrange
        var generator = new ManifestListGenerator();
        var manifestPath = "metadata/snap-12345/manifest-001.avro";
        var manifestSize = 2048L;
        var outputPath = Path.Combine(_tempDirectory, "manifest-list-entry.avro");
        _filesToCleanup.Add(outputPath);

        // Act
        generator.WriteManifestList(
            manifestPath: manifestPath,
            outputPath: outputPath,
            manifestSizeInBytes: manifestSize,
            addedFilesCount: 10);

        // Assert - Read back and verify
        using var reader = DataFileReader<GenericRecord>.OpenReader(outputPath);

        GenericRecord? record = null;
        while (reader.HasNext())
        {
            record = reader.Next();
            break;  // Get first record
        }

        Assert.NotNull(record);
        Assert.Equal(manifestPath, record["manifest_path"]);
        Assert.Equal(manifestSize, record["manifest_length"]);
        Assert.Equal(0, record["partition_spec_id"]);
        Assert.Equal(10, record["added_files_count"]);
    }

    [Fact]
    public void Should_Preserve_Field_Ids_In_Manifest_List_Schema()
    {
        // Arrange
        var generator = new ManifestListGenerator();
        var manifestPath = "metadata/test.avro";
        var outputPath = Path.Combine(_tempDirectory, "manifest-list-field-ids.avro");
        _filesToCleanup.Add(outputPath);

        // Act
        generator.WriteManifestList(
            manifestPath: manifestPath,
            outputPath: outputPath,
            manifestSizeInBytes: 1024,
            addedFilesCount: 1);

        // Assert - Read schema and verify field-ids are present
        using var reader = DataFileReader<GenericRecord>.OpenReader(outputPath);
        var schema = reader.GetSchema();
        var schemaJson = schema.ToString();

        // Verify Iceberg-required field-ids are present in schema
        Assert.Contains("\"field-id\"", schemaJson);
        Assert.Contains("\"field-id\":500", schemaJson);  // manifest_path
        Assert.Contains("\"field-id\":501", schemaJson);  // manifest_length
        Assert.Contains("\"field-id\":502", schemaJson);  // partition_spec_id
        Assert.Contains("\"field-id\":512", schemaJson);  // added_files_count
    }

    [Fact]
    public void Should_Handle_Multiple_Manifests()
    {
        // Arrange
        var generator = new ManifestListGenerator();
        var outputPath = Path.Combine(_tempDirectory, "manifest-list-multiple.avro");
        _filesToCleanup.Add(outputPath);

        var manifests = new List<(string Path, long Size, int AddedCount)>
        {
            ("metadata/manifest-001.avro", 1024, 5),
            ("metadata/manifest-002.avro", 2048, 10),
            ("metadata/manifest-003.avro", 4096, 15)
        };

        // Act
        generator.WriteManifestList(outputPath, manifests);

        // Assert - Verify all entries written
        using var reader = DataFileReader<GenericRecord>.OpenReader(outputPath);
        var records = new List<GenericRecord>();

        while (reader.HasNext())
        {
            records.Add(reader.Next());
        }

        Assert.Equal(3, records.Count);

        Assert.Equal("metadata/manifest-001.avro", records[0]["manifest_path"]);
        Assert.Equal(1024L, records[0]["manifest_length"]);
        Assert.Equal(5, records[0]["added_files_count"]);

        Assert.Equal("metadata/manifest-002.avro", records[1]["manifest_path"]);
        Assert.Equal(2048L, records[1]["manifest_length"]);
        Assert.Equal(10, records[1]["added_files_count"]);

        Assert.Equal("metadata/manifest-003.avro", records[2]["manifest_path"]);
        Assert.Equal(4096L, records[2]["manifest_length"]);
        Assert.Equal(15, records[2]["added_files_count"]);
    }

    [Fact]
    public void Should_Set_Default_Values_For_Optional_Fields()
    {
        // Arrange
        var generator = new ManifestListGenerator();
        var outputPath = Path.Combine(_tempDirectory, "manifest-list-defaults.avro");
        _filesToCleanup.Add(outputPath);

        // Act
        generator.WriteManifestList(
            manifestPath: "metadata/test.avro",
            outputPath: outputPath,
            manifestSizeInBytes: 512,
            addedFilesCount: 3);

        // Assert
        using var reader = DataFileReader<GenericRecord>.OpenReader(outputPath);

        GenericRecord? record = null;
        while (reader.HasNext())
        {
            record = reader.Next();
            break;
        }

        Assert.NotNull(record);
        Assert.Equal(3, record["added_files_count"]);
        Assert.Equal(0, record["existing_files_count"]);
        Assert.Equal(0, record["deleted_files_count"]);
    }

    [Fact]
    public void Should_Throw_When_Output_Path_Is_Invalid()
    {
        // Arrange
        var generator = new ManifestListGenerator();
        var invalidPath = "/invalid/path/that/does/not/exist/manifest-list.avro";

        // Act & Assert
        var exception = Assert.Throws<DirectoryNotFoundException>(() =>
        {
            generator.WriteManifestList(
                manifestPath: "metadata/test.avro",
                outputPath: invalidPath,
                manifestSizeInBytes: 1024,
                addedFilesCount: 1);
        });

        Assert.NotNull(exception);
    }

    [Fact]
    public void Should_Return_Manifest_List_Path_On_Success()
    {
        // Arrange
        var generator = new ManifestListGenerator();
        var outputPath = Path.Combine(_tempDirectory, "manifest-list-return.avro");
        _filesToCleanup.Add(outputPath);

        // Act
        var result = generator.WriteManifestList(
            manifestPath: "metadata/test.avro",
            outputPath: outputPath,
            manifestSizeInBytes: 1024,
            addedFilesCount: 1);

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
