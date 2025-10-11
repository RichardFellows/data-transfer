using System.Data;
using DataTransfer.Iceberg.Integration;
using DataTransfer.Iceberg.MergeStrategies;
using DataTransfer.Iceberg.Models;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace DataTransfer.Iceberg.Tests.Integration;

public class SqlServerImporterTests : IDisposable
{
    private readonly string _connectionString;
    private readonly string _testTable = "ImportTest";

    public SqlServerImporterTests()
    {
        // Use Docker SQL Server (sqlserver-iceberg-demo container)
        _connectionString = "Server=localhost;Database=IcebergTests;User Id=sa;Password=IcebergDemo@2024;TrustServerCertificate=true;";
        EnsureDatabase();
    }

    private void EnsureDatabase()
    {
        var builder = new SqlConnectionStringBuilder(_connectionString);
        var database = builder.InitialCatalog;
        builder.InitialCatalog = "master";

        using var connection = new SqlConnection(builder.ConnectionString);
        connection.Open();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = $@"
            IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '{database}')
            BEGIN
                CREATE DATABASE [{database}]
            END";
        cmd.ExecuteNonQuery();
    }

    [Fact]
    public async Task Should_Import_Data_To_Target_Table()
    {
        // Arrange
        await CreateTestTable();

        var data = new List<Dictionary<string, object>>
        {
            new() { ["Id"] = 1, ["Name"] = "Alice", ["Age"] = 30 },
            new() { ["Id"] = 2, ["Name"] = "Bob", ["Age"] = 25 },
            new() { ["Id"] = 3, ["Name"] = "Charlie", ["Age"] = 35 }
        };

        var importer = new SqlServerImporter(NullLogger<SqlServerImporter>.Instance);
        var mergeStrategy = new UpsertMergeStrategy("Id");

        // Act
        var result = await importer.ImportAsync(
            ToAsyncEnumerable(data),
            _connectionString,
            _testTable,
            mergeStrategy);

        // Assert
        Assert.True(result.Success, $"Import failed: {result.ErrorMessage}");
        Assert.Equal(3, result.RowsImported);

        var rowCount = await GetRowCount();
        Assert.Equal(3, rowCount);
    }

    [Fact]
    public async Task Should_Use_Bulk_Copy_For_Performance()
    {
        // Arrange
        await CreateTestTable();

        var largeDataset = Enumerable.Range(1, 10000)
            .Select(i => new Dictionary<string, object>
            {
                ["Id"] = i,
                ["Name"] = $"User{i}",
                ["Age"] = 20 + (i % 50)
            })
            .ToList();

        var importer = new SqlServerImporter(NullLogger<SqlServerImporter>.Instance);
        var mergeStrategy = new UpsertMergeStrategy("Id");

        // Act
        var startTime = DateTime.UtcNow;
        var result = await importer.ImportAsync(
            ToAsyncEnumerable(largeDataset),
            _connectionString,
            _testTable,
            mergeStrategy);
        var duration = DateTime.UtcNow - startTime;

        // Assert
        Assert.True(result.Success);
        Assert.Equal(10000, result.RowsImported);
        Assert.True(duration.TotalSeconds < 5, $"Import took {duration.TotalSeconds}s (expected < 5s)");
    }

    [Fact]
    public async Task Should_Execute_Upsert_Strategy()
    {
        // Arrange
        await CreateTestTable();

        // Initial data
        var initialData = new List<Dictionary<string, object>>
        {
            new() { ["Id"] = 1, ["Name"] = "Alice", ["Age"] = 30 },
            new() { ["Id"] = 2, ["Name"] = "Bob", ["Age"] = 25 }
        };

        var importer = new SqlServerImporter(NullLogger<SqlServerImporter>.Instance);
        var mergeStrategy = new UpsertMergeStrategy("Id");

        await importer.ImportAsync(
            ToAsyncEnumerable(initialData),
            _connectionString,
            _testTable,
            mergeStrategy);

        // New data with update and insert
        var newData = new List<Dictionary<string, object>>
        {
            new() { ["Id"] = 1, ["Name"] = "Alice Updated", ["Age"] = 31 },  // Update
            new() { ["Id"] = 3, ["Name"] = "Charlie", ["Age"] = 35 }          // Insert
        };

        // Act
        var result = await importer.ImportAsync(
            ToAsyncEnumerable(newData),
            _connectionString,
            _testTable,
            mergeStrategy);

        // Assert
        Assert.True(result.Success);
        Assert.Equal(2, result.RowsImported);

        var totalRows = await GetRowCount();
        Assert.Equal(3, totalRows);

        var updatedName = await GetNameById(1);
        Assert.Equal("Alice Updated", updatedName);
    }

    [Fact]
    public async Task Should_Handle_Large_Datasets()
    {
        // Arrange
        await CreateTestTable();

        var largeDataset = Enumerable.Range(1, 50000)
            .Select(i => new Dictionary<string, object>
            {
                ["Id"] = i,
                ["Name"] = $"User{i}",
                ["Age"] = 20 + (i % 50)
            })
            .ToList();

        var importer = new SqlServerImporter(NullLogger<SqlServerImporter>.Instance);
        var mergeStrategy = new UpsertMergeStrategy("Id");

        // Act
        var result = await importer.ImportAsync(
            ToAsyncEnumerable(largeDataset),
            _connectionString,
            _testTable,
            mergeStrategy);

        // Assert
        Assert.True(result.Success);
        Assert.Equal(50000, result.RowsImported);

        var rowCount = await GetRowCount();
        Assert.Equal(50000, rowCount);
    }

    [Fact]
    public async Task Should_Handle_Empty_Data()
    {
        // Arrange
        await CreateTestTable();

        var emptyData = new List<Dictionary<string, object>>();

        var importer = new SqlServerImporter(NullLogger<SqlServerImporter>.Instance);
        var mergeStrategy = new UpsertMergeStrategy("Id");

        // Act
        var result = await importer.ImportAsync(
            ToAsyncEnumerable(emptyData),
            _connectionString,
            _testTable,
            mergeStrategy);

        // Assert
        Assert.True(result.Success);
        Assert.Equal(0, result.RowsImported);
    }

    [Fact]
    public async Task Should_Handle_Nullable_Fields()
    {
        // Arrange
        await CreateTestTable();

        var data = new List<Dictionary<string, object>>
        {
            new() { ["Id"] = 1, ["Name"] = "Alice", ["Age"] = DBNull.Value },
            new() { ["Id"] = 2, ["Name"] = DBNull.Value, ["Age"] = 25 }
        };

        var importer = new SqlServerImporter(NullLogger<SqlServerImporter>.Instance);
        var mergeStrategy = new UpsertMergeStrategy("Id");

        // Act
        var result = await importer.ImportAsync(
            ToAsyncEnumerable(data),
            _connectionString,
            _testTable,
            mergeStrategy);

        // Assert
        Assert.True(result.Success);
        Assert.Equal(2, result.RowsImported);
    }

    [Fact]
    public async Task Should_Return_Error_For_Nonexistent_Table()
    {
        // Arrange
        var data = new List<Dictionary<string, object>>
        {
            new() { ["Id"] = 1, ["Name"] = "Alice", ["Age"] = 30 }
        };

        var importer = new SqlServerImporter(NullLogger<SqlServerImporter>.Instance);
        var mergeStrategy = new UpsertMergeStrategy("Id");

        // Act
        var result = await importer.ImportAsync(
            ToAsyncEnumerable(data),
            _connectionString,
            "NonexistentTable",
            mergeStrategy);

        // Assert
        Assert.False(result.Success);
        Assert.NotNull(result.ErrorMessage);
    }

    // Helper methods

    private async Task CreateTestTable()
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $@"
            IF OBJECT_ID('{_testTable}', 'U') IS NOT NULL
                DROP TABLE {_testTable};

            CREATE TABLE {_testTable} (
                Id INT PRIMARY KEY,
                Name NVARCHAR(100),
                Age INT
            )";
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task<int> GetRowCount()
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM {_testTable}";
        return (int)await cmd.ExecuteScalarAsync()!;
    }

    private async Task<string?> GetNameById(int id)
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $"SELECT Name FROM {_testTable} WHERE Id = @Id";
        cmd.Parameters.AddWithValue("@Id", id);
        return await cmd.ExecuteScalarAsync() as string;
    }

    private static async IAsyncEnumerable<Dictionary<string, object>> ToAsyncEnumerable(
        List<Dictionary<string, object>> data)
    {
        foreach (var item in data)
        {
            await Task.Yield();
            yield return item;
        }
    }

    public void Dispose()
    {
        // Cleanup test table
        try
        {
            using var connection = new SqlConnection(_connectionString);
            connection.Open();
            using var cmd = connection.CreateCommand();
            cmd.CommandText = $"IF OBJECT_ID('{_testTable}', 'U') IS NOT NULL DROP TABLE {_testTable}";
            cmd.ExecuteNonQuery();
        }
        catch
        {
            // Ignore cleanup errors
        }
    }
}
