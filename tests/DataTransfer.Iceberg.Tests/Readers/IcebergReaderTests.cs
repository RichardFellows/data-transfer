using DataTransfer.Core.Models.Iceberg;
using DataTransfer.Iceberg.Catalog;
using DataTransfer.Iceberg.Integration;
using DataTransfer.Iceberg.Readers;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace DataTransfer.Iceberg.Tests.Readers;

/// <summary>
/// Integration tests for reading data from Iceberg tables
/// </summary>
public class IcebergReaderTests : IDisposable
{
    private readonly string _tempWarehouse;
    private readonly FilesystemCatalog _catalog;
    private readonly IcebergTableWriter _writer;
    private readonly IcebergReader _reader;

    public IcebergReaderTests()
    {
        _tempWarehouse = Path.Combine(Path.GetTempPath(), $"iceberg-reader-tests-{Guid.NewGuid()}");
        _catalog = new FilesystemCatalog(_tempWarehouse, NullLogger<FilesystemCatalog>.Instance);
        _writer = new IcebergTableWriter(_catalog, NullLogger<IcebergTableWriter>.Instance);
        _reader = new IcebergReader(_catalog, NullLogger<IcebergReader>.Instance);
    }

    [Fact]
    public async Task Should_Read_All_Rows_From_Table()
    {
        // Arrange
        var schema = CreateSimpleSchema();
        var data = CreateSampleData(10);
        await _writer.WriteTableAsync("test_table", schema, data);

        // Act
        var rows = new List<Dictionary<string, object>>();
        await foreach (var row in _reader.ReadTableAsync("test_table"))
        {
            rows.Add(row);
        }

        // Assert
        Assert.Equal(10, rows.Count);
        Assert.Equal(1, rows[0]["id"]);
        Assert.Equal("row_1", rows[0]["value"]);
    }

    [Fact]
    public async Task Should_Read_Specific_Snapshot()
    {
        // Arrange
        var schema = CreateSimpleSchema();
        var appender = new IcebergAppender(_catalog, NullLogger<IcebergAppender>.Instance);

        // Create initial table with 5 rows
        var initialResult = await _writer.WriteTableAsync("snapshot_test", schema, CreateSampleData(5));

        // Append 3 more rows (creates snapshot 2)
        await appender.AppendAsync("snapshot_test", CreateSampleData(3, startId: 6));

        // Act - Read first snapshot (should have only 5 rows)
        var rows = new List<Dictionary<string, object>>();
        await foreach (var row in _reader.ReadSnapshotAsync("snapshot_test", initialResult.SnapshotId))
        {
            rows.Add(row);
        }

        // Assert
        Assert.Equal(5, rows.Count);
    }

    [Fact]
    public async Task Should_Handle_Multiple_Data_Files()
    {
        // Arrange
        var schema = CreateSimpleSchema();
        var appender = new IcebergAppender(_catalog, NullLogger<IcebergAppender>.Instance);

        // Create table and append multiple times (creates multiple data files)
        await _writer.WriteTableAsync("multi_file_test", schema, CreateSampleData(5));
        await appender.AppendAsync("multi_file_test", CreateSampleData(5, startId: 6));
        await appender.AppendAsync("multi_file_test", CreateSampleData(5, startId: 11));

        // Act - Read all data
        var rows = new List<Dictionary<string, object>>();
        await foreach (var row in _reader.ReadTableAsync("multi_file_test"))
        {
            rows.Add(row);
        }

        // Assert
        Assert.Equal(15, rows.Count);
    }

    [Fact]
    public async Task Should_Reconstruct_Rows_Correctly()
    {
        // Arrange
        var schema = new IcebergSchema
        {
            SchemaId = 0,
            Type = "struct",
            Fields = new List<IcebergField>
            {
                new IcebergField { Id = 1, Name = "id", Required = true, Type = "int" },
                new IcebergField { Id = 2, Name = "name", Required = false, Type = "string" },
                new IcebergField { Id = 3, Name = "amount", Required = true, Type = "double" }
            }
        };

        var data = new List<Dictionary<string, object>>
        {
            new() { ["id"] = 1, ["name"] = "Alice", ["amount"] = 99.99 },
            new() { ["id"] = 2, ["name"] = "Bob", ["amount"] = 149.50 }
        };

        await _writer.WriteTableAsync("complex_test", schema, data);

        // Act
        var rows = new List<Dictionary<string, object>>();
        await foreach (var row in _reader.ReadTableAsync("complex_test"))
        {
            rows.Add(row);
        }

        // Assert
        Assert.Equal(2, rows.Count);
        Assert.Equal(1, rows[0]["id"]);
        Assert.Equal("Alice", rows[0]["name"]);
        Assert.Equal(99.99, (double)rows[0]["amount"], precision: 2);
    }

    [Fact]
    public async Task Should_Handle_Nullable_Fields()
    {
        // Arrange
        var schema = CreateSimpleSchema(); // "value" field is nullable
        var data = new List<Dictionary<string, object>>
        {
            new() { ["id"] = 1, ["value"] = "test" },
            new() { ["id"] = 2, ["value"] = null! }
        };

        await _writer.WriteTableAsync("nullable_test", schema, data);

        // Act
        var rows = new List<Dictionary<string, object>>();
        await foreach (var row in _reader.ReadTableAsync("nullable_test"))
        {
            rows.Add(row);
        }

        // Assert
        Assert.Equal(2, rows.Count);
        Assert.Equal("test", rows[0]["value"]);
        Assert.Null(rows[1]["value"]);
    }

    [Fact]
    public async Task Should_Support_Cancellation()
    {
        // Arrange
        var schema = CreateSimpleSchema();
        await _writer.WriteTableAsync("cancel_test", schema, CreateSampleData(100));

        var cts = new CancellationTokenSource();
        cts.Cancel();

        // Act & Assert
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await foreach (var row in _reader.ReadTableAsync("cancel_test", cts.Token))
            {
                // Should throw before reading
            }
        });
    }

    [Fact]
    public async Task Should_Fail_For_Nonexistent_Table()
    {
        // Act & Assert
        await Assert.ThrowsAnyAsync<InvalidOperationException>(async () =>
        {
            await foreach (var row in _reader.ReadTableAsync("nonexistent_table"))
            {
                // Should throw
            }
        });
    }

    [Fact]
    public async Task Should_Return_Empty_For_Empty_Table()
    {
        // Arrange
        var schema = CreateSimpleSchema();
        await _writer.WriteTableAsync("empty_table", schema, new List<Dictionary<string, object>>());

        // Act
        var rows = new List<Dictionary<string, object>>();
        await foreach (var row in _reader.ReadTableAsync("empty_table"))
        {
            rows.Add(row);
        }

        // Assert
        Assert.Empty(rows);
    }

    [Fact]
    public async Task Should_Preserve_Field_Order()
    {
        // Arrange
        var schema = new IcebergSchema
        {
            SchemaId = 0,
            Type = "struct",
            Fields = new List<IcebergField>
            {
                new IcebergField { Id = 1, Name = "col_a", Required = true, Type = "int" },
                new IcebergField { Id = 2, Name = "col_b", Required = true, Type = "string" },
                new IcebergField { Id = 3, Name = "col_c", Required = true, Type = "double" }
            }
        };

        var data = new List<Dictionary<string, object>>
        {
            new() { ["col_a"] = 1, ["col_b"] = "test", ["col_c"] = 3.14 }
        };

        await _writer.WriteTableAsync("order_test", schema, data);

        // Act
        var rows = new List<Dictionary<string, object>>();
        await foreach (var row in _reader.ReadTableAsync("order_test"))
        {
            rows.Add(row);
        }

        // Assert
        var keys = rows[0].Keys.ToList();
        Assert.Equal("col_a", keys[0]);
        Assert.Equal("col_b", keys[1]);
        Assert.Equal("col_c", keys[2]);
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

    private List<Dictionary<string, object>> CreateSampleData(int count, int startId = 1)
    {
        var data = new List<Dictionary<string, object>>();
        for (int i = 0; i < count; i++)
        {
            var id = startId + i;
            data.Add(new Dictionary<string, object>
            {
                ["id"] = id,
                ["value"] = $"row_{id}"
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
