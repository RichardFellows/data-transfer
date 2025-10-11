using DataTransfer.Core.Models.Iceberg;
using DataTransfer.Iceberg.Catalog;
using DataTransfer.Iceberg.Integration;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace DataTransfer.Iceberg.Tests.Integration;

/// <summary>
/// Integration tests for appending data to existing Iceberg tables
/// Tests incremental data append functionality (creating new snapshots)
/// </summary>
public class IcebergAppenderTests : IDisposable
{
    private readonly string _tempWarehouse;
    private readonly FilesystemCatalog _catalog;
    private readonly IcebergTableWriter _writer;

    public IcebergAppenderTests()
    {
        _tempWarehouse = Path.Combine(Path.GetTempPath(), $"iceberg-appender-tests-{Guid.NewGuid()}");
        _catalog = new FilesystemCatalog(_tempWarehouse, NullLogger<FilesystemCatalog>.Instance);
        _writer = new IcebergTableWriter(_catalog, NullLogger<IcebergTableWriter>.Instance);
    }

    [Fact]
    public async Task Should_Append_Data_To_Existing_Table()
    {
        // Arrange
        var appender = new IcebergAppender(_catalog, NullLogger<IcebergAppender>.Instance);

        // Create initial table with 5 rows
        var schema = CreateSimpleSchema();
        var initialData = CreateSampleData(5);
        await _writer.WriteTableAsync("test_table", schema, initialData);

        // Append 3 more rows
        var appendData = CreateSampleData(3);

        // Act
        var result = await appender.AppendAsync("test_table", appendData);

        // Assert
        Assert.True(result.Success, $"Append failed: {result.ErrorMessage}");
        Assert.Equal(3, result.RowsAppended);
        Assert.True(result.NewSnapshotId > 0);
    }

    [Fact]
    public async Task Should_Create_New_Snapshot_With_Incremented_Version()
    {
        // Arrange
        var appender = new IcebergAppender(_catalog, NullLogger<IcebergAppender>.Instance);
        var schema = CreateSimpleSchema();

        // Create initial table (v1.metadata.json)
        await _writer.WriteTableAsync("version_test", schema, CreateSampleData(5));

        // Act - Append data (should create v2.metadata.json)
        await appender.AppendAsync("version_test", CreateSampleData(3));

        // Assert
        var tablePath = _catalog.GetTablePath("version_test");
        var metadataDir = Path.Combine(tablePath, "metadata");

        Assert.True(File.Exists(Path.Combine(metadataDir, "v1.metadata.json")), "v1 metadata should exist");
        Assert.True(File.Exists(Path.Combine(metadataDir, "v2.metadata.json")), "v2 metadata should exist");

        var versionHint = File.ReadAllText(Path.Combine(metadataDir, "version-hint.txt"));
        Assert.Equal("2", versionHint.Trim());
    }

    [Fact]
    public async Task Should_Preserve_Previous_Snapshots()
    {
        // Arrange
        var appender = new IcebergAppender(_catalog, NullLogger<IcebergAppender>.Instance);
        var schema = CreateSimpleSchema();

        // Create initial table
        var initialResult = await _writer.WriteTableAsync("snapshot_test", schema, CreateSampleData(5));
        var firstSnapshotId = initialResult.SnapshotId;

        // Act - Append data
        var appendResult = await appender.AppendAsync("snapshot_test", CreateSampleData(3));

        // Assert
        var metadata = _catalog.LoadTable("snapshot_test");
        Assert.NotNull(metadata);
        Assert.Equal(2, metadata.Snapshots.Count);

        // Verify both snapshots exist
        Assert.Contains(metadata.Snapshots, s => s.SnapshotId == firstSnapshotId);
        Assert.Contains(metadata.Snapshots, s => s.SnapshotId == appendResult.NewSnapshotId);
    }

    [Fact]
    public async Task Should_Update_Current_Snapshot_Id()
    {
        // Arrange
        var appender = new IcebergAppender(_catalog, NullLogger<IcebergAppender>.Instance);
        var schema = CreateSimpleSchema();

        // Create initial table
        await _writer.WriteTableAsync("current_snapshot_test", schema, CreateSampleData(5));

        // Act - Append data
        var appendResult = await appender.AppendAsync("current_snapshot_test", CreateSampleData(3));

        // Assert
        var metadata = _catalog.LoadTable("current_snapshot_test");
        Assert.NotNull(metadata);
        Assert.Equal(appendResult.NewSnapshotId, metadata.CurrentSnapshotId);
    }

    [Fact]
    public async Task Should_Increment_Last_Sequence_Number()
    {
        // Arrange
        var appender = new IcebergAppender(_catalog, NullLogger<IcebergAppender>.Instance);
        var schema = CreateSimpleSchema();

        // Create initial table
        await _writer.WriteTableAsync("sequence_test", schema, CreateSampleData(5));

        // Act - Append data twice
        await appender.AppendAsync("sequence_test", CreateSampleData(2));
        await appender.AppendAsync("sequence_test", CreateSampleData(2));

        // Assert
        var metadata = _catalog.LoadTable("sequence_test");
        Assert.NotNull(metadata);
        Assert.Equal(3, metadata.Snapshots.Count);

        // Verify snapshot IDs are unique
        var snapshotIds = metadata.Snapshots.Select(s => s.SnapshotId).ToList();
        Assert.Equal(snapshotIds.Count, snapshotIds.Distinct().Count());
    }

    [Fact]
    public async Task Should_Handle_Empty_Append()
    {
        // Arrange
        var appender = new IcebergAppender(_catalog, NullLogger<IcebergAppender>.Instance);
        var schema = CreateSimpleSchema();

        // Create initial table
        await _writer.WriteTableAsync("empty_append_test", schema, CreateSampleData(5));

        // Act - Append empty data
        var result = await appender.AppendAsync("empty_append_test", new List<Dictionary<string, object>>());

        // Assert
        Assert.True(result.Success);
        Assert.Equal(0, result.RowsAppended);
    }

    [Fact]
    public async Task Should_Fail_If_Table_Does_Not_Exist()
    {
        // Arrange
        var appender = new IcebergAppender(_catalog, NullLogger<IcebergAppender>.Instance);

        // Act & Assert
        await Assert.ThrowsAnyAsync<InvalidOperationException>(async () =>
        {
            await appender.AppendAsync("nonexistent_table", CreateSampleData(3));
        });
    }

    [Fact]
    public async Task Should_Create_New_Data_Files_For_Appended_Data()
    {
        // Arrange
        var appender = new IcebergAppender(_catalog, NullLogger<IcebergAppender>.Instance);
        var schema = CreateSimpleSchema();

        // Create initial table
        await _writer.WriteTableAsync("data_files_test", schema, CreateSampleData(5));

        var tablePath = _catalog.GetTablePath("data_files_test");
        var dataDir = Path.Combine(tablePath, "data");
        var initialFileCount = Directory.GetFiles(dataDir, "*.parquet").Length;

        // Act - Append data
        await appender.AppendAsync("data_files_test", CreateSampleData(3));

        // Assert
        var finalFileCount = Directory.GetFiles(dataDir, "*.parquet").Length;
        Assert.True(finalFileCount > initialFileCount, "Should have created additional data files");
    }

    [Fact]
    public async Task Should_Support_Cancellation_Token()
    {
        // Arrange
        var appender = new IcebergAppender(_catalog, NullLogger<IcebergAppender>.Instance);
        var schema = CreateSimpleSchema();

        await _writer.WriteTableAsync("cancel_test", schema, CreateSampleData(5));

        var cts = new CancellationTokenSource();
        cts.Cancel();

        // Act & Assert
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await appender.AppendAsync("cancel_test", CreateSampleData(100), cts.Token);
        });
    }

    [Fact]
    public async Task Should_Return_Correct_Data_File_Count_In_Result()
    {
        // Arrange
        var appender = new IcebergAppender(_catalog, NullLogger<IcebergAppender>.Instance);
        var schema = CreateSimpleSchema();

        await _writer.WriteTableAsync("file_count_test", schema, CreateSampleData(5));

        // Act
        var result = await appender.AppendAsync("file_count_test", CreateSampleData(10));

        // Assert
        Assert.True(result.DataFileCount > 0);
    }

    [Fact]
    public async Task Should_Maintain_Schema_Consistency_Across_Appends()
    {
        // Arrange
        var appender = new IcebergAppender(_catalog, NullLogger<IcebergAppender>.Instance);
        var schema = CreateSimpleSchema();

        // Create initial table
        await _writer.WriteTableAsync("schema_test", schema, CreateSampleData(5));

        // Act - Append data
        await appender.AppendAsync("schema_test", CreateSampleData(3));

        // Assert
        var metadata = _catalog.LoadTable("schema_test");
        Assert.NotNull(metadata);

        // Schema should not change
        Assert.Single(metadata.Schemas);
        Assert.Equal(schema.Fields.Count, metadata.Schemas[0].Fields.Count);
    }

    // Helper methods
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

    private List<Dictionary<string, object>> CreateSampleData(int count)
    {
        var data = new List<Dictionary<string, object>>();
        for (int i = 1; i <= count; i++)
        {
            data.Add(new Dictionary<string, object>
            {
                ["id"] = i,
                ["value"] = $"row_{i}"
            });
        }
        return data;
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
