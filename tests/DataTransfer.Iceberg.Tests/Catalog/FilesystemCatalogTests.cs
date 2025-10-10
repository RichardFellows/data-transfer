using DataTransfer.Core.Models.Iceberg;
using DataTransfer.Iceberg.Catalog;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace DataTransfer.Iceberg.Tests.Catalog;

public class FilesystemCatalogTests : IDisposable
{
    private readonly string _tempWarehouse;
    private readonly FilesystemCatalog _catalog;

    public FilesystemCatalogTests()
    {
        _tempWarehouse = Path.Combine(Path.GetTempPath(), $"iceberg-catalog-tests-{Guid.NewGuid()}");
        _catalog = new FilesystemCatalog(_tempWarehouse, NullLogger<FilesystemCatalog>.Instance);
    }

    [Fact]
    public void Should_Create_Warehouse_Directory_Structure()
    {
        // Act
        var tablePath = _catalog.InitializeTable("orders");

        // Assert
        Assert.True(Directory.Exists(tablePath));
        Assert.True(Directory.Exists(Path.Combine(tablePath, "metadata")));
        Assert.True(Directory.Exists(Path.Combine(tablePath, "data")));
    }

    [Fact]
    public void Should_Return_Table_Path_On_Initialization()
    {
        // Act
        var tablePath = _catalog.InitializeTable("customers");

        // Assert
        Assert.Equal(Path.Combine(_tempWarehouse, "customers"), tablePath);
    }

    [Fact]
    public void Should_Create_Nested_Warehouse_Directories()
    {
        // Act
        var tablePath = _catalog.InitializeTable("sales_db.transactions");

        // Assert
        Assert.True(Directory.Exists(tablePath));
        Assert.Contains("sales_db.transactions", tablePath);
    }

    [Fact]
    public async Task Should_Commit_Initial_Snapshot_As_Version_1()
    {
        // Arrange
        _catalog.InitializeTable("test_table");

        var metadata = CreateTestMetadata();

        // Act
        var success = await _catalog.CommitAsync("test_table", metadata);

        // Assert
        Assert.True(success);

        var metadataPath = Path.Combine(_tempWarehouse, "test_table", "metadata");
        Assert.True(File.Exists(Path.Combine(metadataPath, "v1.metadata.json")));
        Assert.True(File.Exists(Path.Combine(metadataPath, "version-hint.txt")));
    }

    [Fact]
    public async Task Should_Write_Version_Hint_File_On_Commit()
    {
        // Arrange
        _catalog.InitializeTable("hint_test");
        var metadata = CreateTestMetadata();

        // Act
        await _catalog.CommitAsync("hint_test", metadata);

        // Assert
        var hintPath = Path.Combine(_tempWarehouse, "hint_test", "metadata", "version-hint.txt");
        Assert.True(File.Exists(hintPath));

        var version = File.ReadAllText(hintPath);
        Assert.Equal("1", version);
    }

    [Fact]
    public async Task Should_Increment_Version_On_Subsequent_Commits()
    {
        // Arrange
        _catalog.InitializeTable("multi_version");
        var metadata1 = CreateTestMetadata();
        var metadata2 = CreateTestMetadata();
        var metadata3 = CreateTestMetadata();

        // Act
        await _catalog.CommitAsync("multi_version", metadata1);
        await _catalog.CommitAsync("multi_version", metadata2);
        await _catalog.CommitAsync("multi_version", metadata3);

        // Assert
        var metadataPath = Path.Combine(_tempWarehouse, "multi_version", "metadata");
        Assert.True(File.Exists(Path.Combine(metadataPath, "v1.metadata.json")));
        Assert.True(File.Exists(Path.Combine(metadataPath, "v2.metadata.json")));
        Assert.True(File.Exists(Path.Combine(metadataPath, "v3.metadata.json")));

        var hintPath = Path.Combine(metadataPath, "version-hint.txt");
        var currentVersion = File.ReadAllText(hintPath);
        Assert.Equal("3", currentVersion);
    }

    [Fact]
    public async Task Should_Load_Current_Table_Metadata()
    {
        // Arrange
        _catalog.InitializeTable("load_test");
        var originalMetadata = new IcebergTableMetadata
        {
            FormatVersion = 2,
            TableUuid = "test-uuid-12345",
            Location = "/warehouse/load_test",
            LastColumnId = 5,
            Schemas = new List<IcebergSchema>
            {
                new IcebergSchema
                {
                    SchemaId = 0,
                    Type = "struct",
                    Fields = new List<IcebergField>
                    {
                        new IcebergField { Id = 1, Name = "id", Required = true, Type = "int" },
                        new IcebergField { Id = 2, Name = "name", Required = false, Type = "string" }
                    }
                }
            },
            CurrentSchemaId = 0
        };

        await _catalog.CommitAsync("load_test", originalMetadata);

        // Act
        var loadedMetadata = _catalog.LoadTable("load_test");

        // Assert
        Assert.NotNull(loadedMetadata);
        Assert.Equal(originalMetadata.TableUuid, loadedMetadata.TableUuid);
        Assert.Equal(originalMetadata.Location, loadedMetadata.Location);
        Assert.Equal(originalMetadata.LastColumnId, loadedMetadata.LastColumnId);
        Assert.Equal(2, loadedMetadata.Schemas[0].Fields.Count);
    }

    [Fact]
    public void Should_Return_Null_When_Loading_NonExistent_Table()
    {
        // Act
        var metadata = _catalog.LoadTable("nonexistent_table");

        // Assert
        Assert.Null(metadata);
    }

    [Fact]
    public async Task Should_Load_Latest_Version_After_Multiple_Commits()
    {
        // Arrange
        _catalog.InitializeTable("version_test");

        var metadata1 = CreateTestMetadata();
        metadata1.TableUuid = "uuid-v1";

        var metadata2 = CreateTestMetadata();
        metadata2.TableUuid = "uuid-v2";

        var metadata3 = CreateTestMetadata();
        metadata3.TableUuid = "uuid-v3";

        // Act
        await _catalog.CommitAsync("version_test", metadata1);
        await _catalog.CommitAsync("version_test", metadata2);
        await _catalog.CommitAsync("version_test", metadata3);

        var loadedMetadata = _catalog.LoadTable("version_test");

        // Assert
        Assert.NotNull(loadedMetadata);
        Assert.Equal("uuid-v3", loadedMetadata.TableUuid);  // Should load latest
    }

    [Fact]
    public async Task Should_Preserve_All_Historical_Metadata_Versions()
    {
        // Arrange
        _catalog.InitializeTable("history_test");
        var metadata1 = CreateTestMetadata();
        var metadata2 = CreateTestMetadata();

        // Act
        await _catalog.CommitAsync("history_test", metadata1);
        await _catalog.CommitAsync("history_test", metadata2);

        // Assert - All versions should exist
        var metadataPath = Path.Combine(_tempWarehouse, "history_test", "metadata");
        Assert.True(File.Exists(Path.Combine(metadataPath, "v1.metadata.json")));
        Assert.True(File.Exists(Path.Combine(metadataPath, "v2.metadata.json")));

        // Both files should have content
        var v1Content = File.ReadAllText(Path.Combine(metadataPath, "v1.metadata.json"));
        var v2Content = File.ReadAllText(Path.Combine(metadataPath, "v2.metadata.json"));
        Assert.NotEmpty(v1Content);
        Assert.NotEmpty(v2Content);
    }

    [Fact]
    public async Task Should_Handle_Concurrent_Commit_Attempts()
    {
        // Arrange
        _catalog.InitializeTable("concurrent_test");

        var metadata1 = CreateTestMetadata();
        var metadata2 = CreateTestMetadata();

        // Act - Simulate concurrent commits
        var task1 = _catalog.CommitAsync("concurrent_test", metadata1);
        var task2 = _catalog.CommitAsync("concurrent_test", metadata2);

        var results = await Task.WhenAll(task1, task2);

        // Assert - Both should succeed (version-hint provides atomicity)
        Assert.All(results, success => Assert.True(success));

        // Should have 2 versions
        var metadataPath = Path.Combine(_tempWarehouse, "concurrent_test", "metadata");
        Assert.True(File.Exists(Path.Combine(metadataPath, "v1.metadata.json")));
        Assert.True(File.Exists(Path.Combine(metadataPath, "v2.metadata.json")));
    }

    [Fact]
    public async Task Should_Return_False_On_Commit_Failure()
    {
        // Arrange - Initialize table but make metadata directory read-only
        _catalog.InitializeTable("fail_test");
        var metadataPath = Path.Combine(_tempWarehouse, "fail_test", "metadata");

        // Make directory unusable
        var tempFile = Path.Combine(metadataPath, "v1.metadata.json");
        File.WriteAllText(tempFile, "test");
        File.SetAttributes(tempFile, FileAttributes.ReadOnly);

        var metadata = CreateTestMetadata();

        // Act
        var success = await _catalog.CommitAsync("fail_test", metadata);

        // Assert
        Assert.False(success);

        // Cleanup
        File.SetAttributes(tempFile, FileAttributes.Normal);
    }

    [Fact]
    public async Task Should_Support_Cancellation_Token()
    {
        // Arrange
        _catalog.InitializeTable("cancel_test");
        var metadata = CreateTestMetadata();
        var cts = new CancellationTokenSource();
        cts.Cancel();

        // Act & Assert - TaskCanceledException is a subclass of OperationCanceledException
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await _catalog.CommitAsync("cancel_test", metadata, cts.Token);
        });
    }

    [Fact]
    public void Should_Get_Table_Path()
    {
        // Act
        var tablePath = _catalog.GetTablePath("my_table");

        // Assert
        Assert.Equal(Path.Combine(_tempWarehouse, "my_table"), tablePath);
    }

    [Fact]
    public async Task Should_Check_Table_Exists()
    {
        // Arrange
        _catalog.InitializeTable("existing_table");
        var metadata = CreateTestMetadata();
        await _catalog.CommitAsync("existing_table", metadata);

        // Act & Assert
        Assert.True(_catalog.TableExists("existing_table"));
        Assert.False(_catalog.TableExists("nonexistent_table"));
    }

    // Helper method
    private IcebergTableMetadata CreateTestMetadata()
    {
        return new IcebergTableMetadata
        {
            FormatVersion = 2,
            TableUuid = Guid.NewGuid().ToString(),
            Location = "/warehouse/test",
            LastUpdatedMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
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
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempWarehouse))
        {
            try
            {
                // Remove read-only attributes from test files
                foreach (var file in Directory.GetFiles(_tempWarehouse, "*", SearchOption.AllDirectories))
                {
                    File.SetAttributes(file, FileAttributes.Normal);
                }
                Directory.Delete(_tempWarehouse, recursive: true);
            }
            catch
            {
                // Ignore cleanup errors
            }
        }
    }
}
