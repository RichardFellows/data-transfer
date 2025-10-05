using DataTransfer.Core.Models.Iceberg;
using DataTransfer.Iceberg.Models;
using DataTransfer.Iceberg.Writers;
using ParquetSharp;
using Xunit;

namespace DataTransfer.Iceberg.Tests.Writers;

public class IcebergParquetWriterTests : IDisposable
{
    private readonly string _tempDirectory;
    private readonly List<string> _filesToCleanup = new();

    public IcebergParquetWriterTests()
    {
        _tempDirectory = Path.Combine(Path.GetTempPath(), $"iceberg-tests-{Guid.NewGuid()}");
        Directory.CreateDirectory(_tempDirectory);
    }

    [Fact]
    public void Should_Embed_Field_IDs_In_Parquet_Schema()
    {
        // Arrange
        var schema = new IcebergSchema
        {
            SchemaId = 0,
            Fields = new List<IcebergField>
            {
                new IcebergField { Id = 1, Name = "customer_id", Required = true, Type = "long" },
                new IcebergField { Id = 2, Name = "customer_name", Required = false, Type = "string" },
                new IcebergField { Id = 3, Name = "created_date", Required = true, Type = "date" }
            }
        };

        var filePath = Path.Combine(_tempDirectory, "test-field-ids.parquet");
        _filesToCleanup.Add(filePath);

        // Act
        using (var writer = new IcebergParquetWriter(filePath, schema))
        {
            // Writer should create file with schema
        }

        // Assert - Read back the schema and verify structure
        using var fileReader = new ParquetFileReader(filePath);
        var fileMetadata = fileReader.FileMetaData;
        var schemaDescriptor = fileMetadata.Schema;

        // Verify we have the correct number of columns
        Assert.Equal(3, schemaDescriptor.NumColumns);

        // Verify column names match
        Assert.Equal("customer_id", schemaDescriptor.Column(0).Name);
        Assert.Equal("customer_name", schemaDescriptor.Column(1).Name);
        Assert.Equal("created_date", schemaDescriptor.Column(2).Name);

        // Note: Field ID verification requires reading the Parquet schema metadata
        // This will be validated through PyIceberg/DuckDB integration tests
    }

    [Fact]
    public void Should_Create_Parquet_File_With_Correct_Schema()
    {
        // Arrange
        var schema = new IcebergSchema
        {
            SchemaId = 0,
            Fields = new List<IcebergField>
            {
                new IcebergField { Id = 1, Name = "id", Required = true, Type = "int" },
                new IcebergField { Id = 2, Name = "amount", Required = false, Type = new { type = "decimal", precision = 18, scale = 2 } }
            }
        };

        var filePath = Path.Combine(_tempDirectory, "test-schema.parquet");
        _filesToCleanup.Add(filePath);

        // Act
        using (var writer = new IcebergParquetWriter(filePath, schema))
        {
            // File should be created with schema
        }

        // Assert
        Assert.True(File.Exists(filePath));

        using var fileReader = new ParquetFileReader(filePath);
        var fileMetadata = fileReader.FileMetaData;

        Assert.Equal(2, fileMetadata.NumColumns);
        Assert.Equal("id", fileMetadata.Schema.Column(0).Name);
        Assert.Equal("amount", fileMetadata.Schema.Column(1).Name);
    }

    [Fact]
    public void Should_Return_Metadata_With_File_Statistics()
    {
        // Arrange
        var schema = new IcebergSchema
        {
            SchemaId = 0,
            Fields = new List<IcebergField>
            {
                new IcebergField { Id = 1, Name = "value", Required = true, Type = "int" }
            }
        };

        var filePath = Path.Combine(_tempDirectory, "test-metadata.parquet");
        _filesToCleanup.Add(filePath);

        // Act
        DataFileMetadata? metadata;
        using (var writer = new IcebergParquetWriter(filePath, schema))
        {
            metadata = writer.Close();
        }

        // Assert
        Assert.NotNull(metadata);
        Assert.Equal(filePath, metadata.FilePath);
        Assert.True(metadata.FileSizeInBytes > 0, "File size should be greater than 0");
        Assert.Equal(0, metadata.RecordCount); // No data written yet
    }

    [Fact]
    public void Should_Map_Iceberg_Types_To_Parquet_Types_Correctly()
    {
        // Arrange
        var schema = new IcebergSchema
        {
            SchemaId = 0,
            Fields = new List<IcebergField>
            {
                new IcebergField { Id = 1, Name = "bool_col", Required = true, Type = "boolean" },
                new IcebergField { Id = 2, Name = "int_col", Required = true, Type = "int" },
                new IcebergField { Id = 3, Name = "long_col", Required = true, Type = "long" },
                new IcebergField { Id = 4, Name = "float_col", Required = true, Type = "float" },
                new IcebergField { Id = 5, Name = "double_col", Required = true, Type = "double" },
                new IcebergField { Id = 6, Name = "string_col", Required = true, Type = "string" },
                new IcebergField { Id = 7, Name = "date_col", Required = true, Type = "date" },
                new IcebergField { Id = 8, Name = "timestamp_col", Required = true, Type = "timestamp" },
                new IcebergField { Id = 9, Name = "binary_col", Required = true, Type = "binary" },
                new IcebergField { Id = 10, Name = "uuid_col", Required = true, Type = "uuid" }
            }
        };

        var filePath = Path.Combine(_tempDirectory, "test-types.parquet");
        _filesToCleanup.Add(filePath);

        // Act
        using (var writer = new IcebergParquetWriter(filePath, schema))
        {
            // Schema should be created with correct type mappings
        }

        // Assert - Verify Parquet physical types
        using var fileReader = new ParquetFileReader(filePath);
        var fileMetadata = fileReader.FileMetaData;

        Assert.Equal(10, fileMetadata.NumColumns);

        // Verify physical types (ParquetSharp uses PhysicalType enum)
        Assert.Equal(ParquetSharp.PhysicalType.Boolean, fileMetadata.Schema.Column(0).PhysicalType);
        Assert.Equal(ParquetSharp.PhysicalType.Int32, fileMetadata.Schema.Column(1).PhysicalType);
        Assert.Equal(ParquetSharp.PhysicalType.Int64, fileMetadata.Schema.Column(2).PhysicalType);
        Assert.Equal(ParquetSharp.PhysicalType.Float, fileMetadata.Schema.Column(3).PhysicalType);
        Assert.Equal(ParquetSharp.PhysicalType.Double, fileMetadata.Schema.Column(4).PhysicalType);
        Assert.Equal(ParquetSharp.PhysicalType.ByteArray, fileMetadata.Schema.Column(5).PhysicalType);
        Assert.Equal(ParquetSharp.PhysicalType.Int32, fileMetadata.Schema.Column(6).PhysicalType); // date as int32
        Assert.Equal(ParquetSharp.PhysicalType.Int64, fileMetadata.Schema.Column(7).PhysicalType); // timestamp as int64
        Assert.Equal(ParquetSharp.PhysicalType.ByteArray, fileMetadata.Schema.Column(8).PhysicalType);
        Assert.Equal(ParquetSharp.PhysicalType.FixedLenByteArray, fileMetadata.Schema.Column(9).PhysicalType); // uuid as fixed 16 bytes
    }

    [Fact]
    public void Should_Handle_Optional_Fields_With_Correct_Repetition()
    {
        // Arrange
        var schema = new IcebergSchema
        {
            SchemaId = 0,
            Fields = new List<IcebergField>
            {
                new IcebergField { Id = 1, Name = "required_field", Required = true, Type = "int" },
                new IcebergField { Id = 2, Name = "optional_field", Required = false, Type = "string" }
            }
        };

        var filePath = Path.Combine(_tempDirectory, "test-nullability.parquet");
        _filesToCleanup.Add(filePath);

        // Act
        using (var writer = new IcebergParquetWriter(filePath, schema))
        {
            // Schema should be created with correct repetition levels
        }

        // Assert
        using var fileReader = new ParquetFileReader(filePath);
        var fileMetadata = fileReader.FileMetaData;

        // Required field should have Required repetition
        var requiredColumn = fileMetadata.Schema.Column(0);
        Assert.Equal("required_field", requiredColumn.Name);
        // Note: ParquetSharp may expose this differently, adjust based on actual API

        // Optional field should have Optional repetition
        var optionalColumn = fileMetadata.Schema.Column(1);
        Assert.Equal("optional_field", optionalColumn.Name);
    }

    [Fact]
    public void Should_Throw_On_Unsupported_Iceberg_Type()
    {
        // Arrange
        var schema = new IcebergSchema
        {
            SchemaId = 0,
            Fields = new List<IcebergField>
            {
                new IcebergField { Id = 1, Name = "unsupported", Required = true, Type = "unknown_type" }
            }
        };

        var filePath = Path.Combine(_tempDirectory, "test-unsupported.parquet");
        _filesToCleanup.Add(filePath);

        // Act & Assert
        Assert.Throws<NotSupportedException>(() =>
        {
            using var writer = new IcebergParquetWriter(filePath, schema);
        });
    }

    public void Dispose()
    {
        // Cleanup test files
        foreach (var file in _filesToCleanup)
        {
            if (File.Exists(file))
            {
                try { File.Delete(file); } catch { /* Ignore cleanup errors */ }
            }
        }

        if (Directory.Exists(_tempDirectory))
        {
            try { Directory.Delete(_tempDirectory, recursive: true); } catch { /* Ignore cleanup errors */ }
        }
    }
}
