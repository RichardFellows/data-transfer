using DataTransfer.Core.Models.Iceberg;
using DataTransfer.Iceberg.Catalog;
using DataTransfer.Iceberg.Integration;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace DataTransfer.Iceberg.Tests.Integration;

/// <summary>
/// Integration tests for end-to-end Iceberg table writing
/// </summary>
public class IcebergTableWriterTests : IDisposable
{
    private readonly string _tempWarehouse;
    private readonly FilesystemCatalog _catalog;

    public IcebergTableWriterTests()
    {
        _tempWarehouse = Path.Combine(Path.GetTempPath(), $"iceberg-writer-tests-{Guid.NewGuid()}");
        _catalog = new FilesystemCatalog(_tempWarehouse, NullLogger<FilesystemCatalog>.Instance);
    }

    [Fact]
    public async Task Should_Create_Complete_Iceberg_Table_From_Schema_And_Data()
    {
        // Arrange
        var writer = new IcebergTableWriter(_catalog, NullLogger<IcebergTableWriter>.Instance);

        var schema = new IcebergSchema
        {
            SchemaId = 0,
            Type = "struct",
            Fields = new List<IcebergField>
            {
                new IcebergField { Id = 1, Name = "id", Required = true, Type = "int" },
                new IcebergField { Id = 2, Name = "name", Required = false, Type = "string" },
                new IcebergField { Id = 3, Name = "created_at", Required = true, Type = "timestamp" }
            }
        };

        var data = new List<Dictionary<string, object>>
        {
            new() { ["id"] = 1, ["name"] = "Alice", ["created_at"] = DateTime.UtcNow },
            new() { ["id"] = 2, ["name"] = "Bob", ["created_at"] = DateTime.UtcNow },
            new() { ["id"] = 3, ["name"] = "Charlie", ["created_at"] = DateTime.UtcNow }
        };

        // Act
        var result = await writer.WriteTableAsync("test_table", schema, data);

        // Assert
        Assert.True(result.Success, $"Write failed: {result.ErrorMessage}");
        Assert.True(_catalog.TableExists("test_table"));
    }

    [Fact]
    public async Task Should_Create_All_Required_Files_And_Directories()
    {
        // Arrange
        var writer = new IcebergTableWriter(_catalog, NullLogger<IcebergTableWriter>.Instance);
        var schema = CreateSimpleSchema();
        var data = new List<Dictionary<string, object>>
        {
            new() { ["id"] = 1, ["value"] = "test" }
        };

        // Act
        await writer.WriteTableAsync("structure_test", schema, data);

        // Assert
        var tablePath = _catalog.GetTablePath("structure_test");

        // Check directories
        Assert.True(Directory.Exists(Path.Combine(tablePath, "metadata")));
        Assert.True(Directory.Exists(Path.Combine(tablePath, "data")));

        // Check metadata files
        var metadataDir = Path.Combine(tablePath, "metadata");
        Assert.True(File.Exists(Path.Combine(metadataDir, "v1.metadata.json")));
        Assert.True(File.Exists(Path.Combine(metadataDir, "version-hint.txt")));

        // Check manifest and manifest list exist
        var manifestFiles = Directory.GetFiles(metadataDir, "manifest-*.avro");
        var manifestListFiles = Directory.GetFiles(metadataDir, "snap-*.avro");
        Assert.NotEmpty(manifestFiles);
        Assert.NotEmpty(manifestListFiles);

        // Check data file exists
        var dataFiles = Directory.GetFiles(Path.Combine(tablePath, "data"), "*.parquet");
        Assert.Single(dataFiles);
    }

    [Fact]
    public async Task Should_Return_Result_With_Snapshot_Id()
    {
        // Arrange
        var writer = new IcebergTableWriter(_catalog, NullLogger<IcebergTableWriter>.Instance);
        var schema = CreateSimpleSchema();
        var data = new List<Dictionary<string, object>> { new() { ["id"] = 1, ["value"] = "test" } };

        // Act
        var result = await writer.WriteTableAsync("snapshot_test", schema, data);

        // Assert
        Assert.True(result.Success);
        Assert.True(result.SnapshotId > 0);
        Assert.NotEmpty(result.TablePath);
        Assert.Equal(1, result.DataFileCount);
        Assert.True(result.RecordCount > 0);
    }

    [Fact]
    public async Task Should_Write_Data_File_With_Correct_Record_Count()
    {
        // Arrange
        var writer = new IcebergTableWriter(_catalog, NullLogger<IcebergTableWriter>.Instance);
        var schema = CreateSimpleSchema();
        var data = new List<Dictionary<string, object>>();

        for (int i = 1; i <= 100; i++)
        {
            data.Add(new Dictionary<string, object> { ["id"] = i, ["value"] = $"row_{i}" });
        }

        // Act
        var result = await writer.WriteTableAsync("count_test", schema, data);

        // Assert
        Assert.Equal(100, result.RecordCount);
    }

    [Fact]
    public async Task Should_Create_Loadable_Table_Metadata()
    {
        // Arrange
        var writer = new IcebergTableWriter(_catalog, NullLogger<IcebergTableWriter>.Instance);
        var schema = new IcebergSchema
        {
            SchemaId = 0,
            Type = "struct",
            Fields = new List<IcebergField>
            {
                new IcebergField { Id = 1, Name = "order_id", Required = true, Type = "long" },
                new IcebergField { Id = 2, Name = "customer_name", Required = false, Type = "string" },
                new IcebergField { Id = 3, Name = "amount", Required = true, Type = "double" }
            }
        };

        var data = new List<Dictionary<string, object>>
        {
            new() { ["order_id"] = 1001L, ["customer_name"] = "John Doe", ["amount"] = 99.99 },
            new() { ["order_id"] = 1002L, ["customer_name"] = "Jane Smith", ["amount"] = 149.50 }
        };

        // Act
        await writer.WriteTableAsync("loadable_test", schema, data);

        // Assert - Load table and verify
        var metadata = _catalog.LoadTable("loadable_test");
        Assert.NotNull(metadata);
        Assert.Equal(2, metadata.FormatVersion);
        Assert.NotEmpty(metadata.TableUuid);
        Assert.Single(metadata.Schemas);
        Assert.Equal(3, metadata.Schemas[0].Fields.Count);
        Assert.Single(metadata.Snapshots);
        Assert.NotNull(metadata.CurrentSnapshotId);
    }

    [Fact]
    public async Task Should_Support_Multiple_Tables_In_Same_Warehouse()
    {
        // Arrange
        var writer = new IcebergTableWriter(_catalog, NullLogger<IcebergTableWriter>.Instance);
        var schema1 = CreateSimpleSchema();
        var schema2 = CreateSimpleSchema();
        var data = new List<Dictionary<string, object>> { new() { ["id"] = 1, ["value"] = "test" } };

        // Act
        await writer.WriteTableAsync("table_a", schema1, data);
        await writer.WriteTableAsync("table_b", schema2, data);

        // Assert
        Assert.True(_catalog.TableExists("table_a"));
        Assert.True(_catalog.TableExists("table_b"));

        var metadataA = _catalog.LoadTable("table_a");
        var metadataB = _catalog.LoadTable("table_b");

        Assert.NotNull(metadataA);
        Assert.NotNull(metadataB);
        Assert.NotEqual(metadataA.TableUuid, metadataB.TableUuid);
    }

    [Fact]
    public async Task Should_Handle_Empty_Data_Gracefully()
    {
        // Arrange
        var writer = new IcebergTableWriter(_catalog, NullLogger<IcebergTableWriter>.Instance);
        var schema = CreateSimpleSchema();
        var emptyData = new List<Dictionary<string, object>>();

        // Act
        var result = await writer.WriteTableAsync("empty_table", schema, emptyData);

        // Assert
        Assert.True(result.Success);
        Assert.Equal(0, result.RecordCount);
        Assert.Equal(0, result.DataFileCount);
    }

    [Fact]
    public async Task Should_Return_Error_Result_On_Invalid_Schema()
    {
        // Arrange
        var writer = new IcebergTableWriter(_catalog, NullLogger<IcebergTableWriter>.Instance);
        var invalidSchema = new IcebergSchema
        {
            SchemaId = 0,
            Type = "struct",
            Fields = new List<IcebergField>() // Empty fields
        };
        var data = new List<Dictionary<string, object>> { new() { ["id"] = 1 } };

        // Act
        var result = await writer.WriteTableAsync("invalid_schema", invalidSchema, data);

        // Assert
        Assert.False(result.Success);
        Assert.NotEmpty(result.ErrorMessage);
    }

    [Fact]
    public async Task Should_Support_Cancellation_Token()
    {
        // Arrange
        var writer = new IcebergTableWriter(_catalog, NullLogger<IcebergTableWriter>.Instance);
        var schema = CreateSimpleSchema();
        var data = new List<Dictionary<string, object>> { new() { ["id"] = 1, ["value"] = "test" } };
        var cts = new CancellationTokenSource();
        cts.Cancel();

        // Act & Assert
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await writer.WriteTableAsync("cancel_test", schema, data, cts.Token);
        });
    }

    [Fact]
    public async Task Should_Generate_Unique_Snapshot_Ids_For_Different_Tables()
    {
        // Arrange
        var writer = new IcebergTableWriter(_catalog, NullLogger<IcebergTableWriter>.Instance);
        var schema = CreateSimpleSchema();
        var data = new List<Dictionary<string, object>> { new() { ["id"] = 1, ["value"] = "test" } };

        // Act
        var result1 = await writer.WriteTableAsync("snapshot_1", schema, data);
        await Task.Delay(10); // Ensure different timestamps
        var result2 = await writer.WriteTableAsync("snapshot_2", schema, data);

        // Assert
        Assert.NotEqual(result1.SnapshotId, result2.SnapshotId);
    }

    [Fact]
    public async Task Should_Preserve_Field_Ids_In_Written_Parquet_Files()
    {
        // Arrange
        var writer = new IcebergTableWriter(_catalog, NullLogger<IcebergTableWriter>.Instance);
        var schema = new IcebergSchema
        {
            SchemaId = 0,
            Type = "struct",
            Fields = new List<IcebergField>
            {
                new IcebergField { Id = 10, Name = "custom_id", Required = true, Type = "int" },
                new IcebergField { Id = 20, Name = "custom_name", Required = false, Type = "string" }
            }
        };
        var data = new List<Dictionary<string, object>>
        {
            new() { ["custom_id"] = 1, ["custom_name"] = "test" }
        };

        // Act
        var result = await writer.WriteTableAsync("field_id_test", schema, data);

        // Assert
        Assert.True(result.Success);

        // Verify metadata has correct field IDs
        var metadata = _catalog.LoadTable("field_id_test");
        Assert.NotNull(metadata);
        Assert.Equal(10, metadata.Schemas[0].Fields[0].Id);
        Assert.Equal(20, metadata.Schemas[0].Fields[1].Id);
    }

    // Helper method
    private IcebergSchema CreateSimpleSchema()
    {
        return new IcebergSchema
        {
            SchemaId = 0,
            Type = "struct",
            Fields = new List<IcebergField>
            {
                new IcebergField { Id = 1, Name = "id", Required = true, Type = "int" },
                new IcebergField { Id = 2, Name = "value", Required = false, Type = "string" }
            }
        };
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempWarehouse))
        {
            try
            {
                Directory.Delete(_tempWarehouse, recursive: true);
            }
            catch
            {
                // Ignore cleanup errors
            }
        }
    }
}
