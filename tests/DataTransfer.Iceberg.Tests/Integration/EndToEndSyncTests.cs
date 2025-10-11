using DataTransfer.Iceberg.Catalog;
using DataTransfer.Iceberg.ChangeDetection;
using DataTransfer.Iceberg.Integration;
using DataTransfer.Iceberg.Models;
using DataTransfer.Iceberg.Readers;
using DataTransfer.Iceberg.Watermarks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace DataTransfer.Iceberg.Tests.Integration;

/// <summary>
/// End-to-end integration tests for complete incremental sync workflow
/// spanning multiple databases and multiple sync cycles
/// </summary>
public class EndToEndSyncTests : IDisposable
{
    private readonly string _connectionString;
    private readonly string _sourceDatabase;
    private readonly string _targetDatabase;
    private readonly string _warehousePath;
    private readonly string _watermarkPath;

    public EndToEndSyncTests()
    {
        _connectionString = "Server=localhost;User Id=sa;Password=IcebergDemo@2024;TrustServerCertificate=true;";
        _sourceDatabase = $"E2ESourceDb_{Guid.NewGuid():N}";
        _targetDatabase = $"E2ETargetDb_{Guid.NewGuid():N}";
        _warehousePath = Path.Combine(Path.GetTempPath(), "e2e-warehouse", Guid.NewGuid().ToString());
        _watermarkPath = Path.Combine(Path.GetTempPath(), "e2e-watermarks", Guid.NewGuid().ToString());

        Directory.CreateDirectory(_warehousePath);
        Directory.CreateDirectory(_watermarkPath);

        EnsureDatabases();
    }

    private void EnsureDatabases()
    {
        using var connection = new SqlConnection(_connectionString);
        connection.Open();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = $@"
            IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '{_sourceDatabase}')
                CREATE DATABASE [{_sourceDatabase}];
            IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '{_targetDatabase}')
                CREATE DATABASE [{_targetDatabase}];";
        cmd.ExecuteNonQuery();
    }

    [Fact]
    public async Task Should_Sync_Complete_Workflow_Across_Multiple_Cycles()
    {
        // Arrange
        await CreateSourceTable("Products");
        await CreateTargetTable("Products");

        var coordinator = CreateCoordinator();
        var options = new SyncOptions
        {
            PrimaryKeyColumn = "ProductId",
            WatermarkColumn = "ModifiedDate",
            WarehousePath = _warehousePath,
            WatermarkDirectory = _watermarkPath
        };

        // Cycle 1: Initial sync with 500 rows
        await InsertProducts(1, 500);
        var result1 = await coordinator.SyncAsync(
            GetConnectionString(_sourceDatabase),
            "Products",
            "products_sync",
            GetConnectionString(_targetDatabase),
            "Products",
            options);

        Assert.True(result1.Success, $"Cycle 1 failed: {result1.ErrorMessage}");
        Assert.Equal(500, result1.RowsExtracted);
        Assert.Equal(500, await GetTargetRowCount("Products"));

        // Cycle 2: Add 200 new rows
        await Task.Delay(100);
        await InsertProducts(501, 700);
        var result2 = await coordinator.SyncAsync(
            GetConnectionString(_sourceDatabase),
            "Products",
            "products_sync",
            GetConnectionString(_targetDatabase),
            "Products",
            options);

        Assert.True(result2.Success);
        Assert.Equal(200, result2.RowsExtracted);
        Assert.Equal(700, await GetTargetRowCount("Products"));

        // Cycle 3: Update 50 rows
        await Task.Delay(100);
        await UpdateProducts(1, 50, "Updated");
        var result3 = await coordinator.SyncAsync(
            GetConnectionString(_sourceDatabase),
            "Products",
            "products_sync",
            GetConnectionString(_targetDatabase),
            "Products",
            options);

        Assert.True(result3.Success);
        Assert.Equal(50, result3.RowsExtracted);
        Assert.Equal(700, await GetTargetRowCount("Products")); // Still 700 (updates, not inserts)

        // Verify updated data
        var updatedCount = await GetTargetRowCountWhere("Products", "Name LIKE '%Updated%'");
        Assert.Equal(50, updatedCount);

        // Cycle 4: No changes
        await Task.Delay(100);
        var result4 = await coordinator.SyncAsync(
            GetConnectionString(_sourceDatabase),
            "Products",
            "products_sync",
            GetConnectionString(_targetDatabase),
            "Products",
            options);

        Assert.True(result4.Success);
        Assert.Equal(0, result4.RowsExtracted);

        // Final verification
        var sourceCount = await GetSourceRowCount("Products");
        var targetCount = await GetTargetRowCount("Products");
        Assert.Equal(sourceCount, targetCount);
    }

    [Fact]
    public async Task Should_Handle_Large_Dataset_Sync()
    {
        // Arrange
        await CreateSourceTable("LargeOrders", "OrderId");
        await CreateTargetTable("LargeOrders", "OrderId");

        var coordinator = CreateCoordinator();
        var options = new SyncOptions
        {
            PrimaryKeyColumn = "OrderId",
            WatermarkColumn = "ModifiedDate",
            WarehousePath = _warehousePath,
            WatermarkDirectory = _watermarkPath
        };

        // Insert 10,000 orders
        await InsertLargeDataset(10000);

        // Act
        var startTime = DateTime.UtcNow;
        var result = await coordinator.SyncAsync(
            GetConnectionString(_sourceDatabase),
            "LargeOrders",
            "large_orders_sync",
            GetConnectionString(_targetDatabase),
            "LargeOrders",
            options);
        var duration = DateTime.UtcNow - startTime;

        // Assert
        Assert.True(result.Success, $"Sync failed: {result.ErrorMessage}");
        Assert.Equal(10000, result.RowsExtracted);
        Assert.Equal(10000, await GetTargetRowCount("LargeOrders"));
        Assert.True(duration.TotalSeconds < 60, $"Sync took {duration.TotalSeconds}s (expected < 60s)");
    }

    [Fact]
    public async Task Should_Preserve_Data_Accuracy_Across_Sync()
    {
        // Arrange
        await CreateSourceTable("Transactions");
        await CreateTargetTable("Transactions");

        await InsertTransactions();

        var coordinator = CreateCoordinator();
        var options = new SyncOptions
        {
            PrimaryKeyColumn = "ProductId",
            WatermarkColumn = "ModifiedDate",
            WarehousePath = _warehousePath,
            WatermarkDirectory = _watermarkPath
        };

        // Act
        var result = await coordinator.SyncAsync(
            GetConnectionString(_sourceDatabase),
            "Transactions",
            "transactions_sync",
            GetConnectionString(_targetDatabase),
            "Transactions",
            options);

        // Assert
        Assert.True(result.Success);

        // Verify specific data accuracy
        var sourceData = await GetTransactionData();
        var targetData = await GetTargetTransactionData();

        Assert.Equal(sourceData.Count, targetData.Count);

        for (int i = 0; i < sourceData.Count; i++)
        {
            Assert.Equal(sourceData[i].Id, targetData[i].Id);
            Assert.Equal(sourceData[i].Amount, targetData[i].Amount);
            Assert.Equal(sourceData[i].Description, targetData[i].Description);
        }
    }

    [Fact]
    public async Task Should_Handle_Multiple_Tables_Independently()
    {
        // Arrange
        await CreateSourceTable("TableA");
        await CreateTargetTable("TableA");
        await CreateSourceTable("TableB");
        await CreateTargetTable("TableB");

        await InsertProducts(1, 100, "TableA");
        await InsertProducts(1, 50, "TableB");

        var coordinator = CreateCoordinator();

        // Act - Sync TableA
        var optionsA = new SyncOptions
        {
            PrimaryKeyColumn = "ProductId",
            WatermarkColumn = "ModifiedDate",
            WarehousePath = _warehousePath,
            WatermarkDirectory = _watermarkPath
        };

        var resultA = await coordinator.SyncAsync(
            GetConnectionString(_sourceDatabase),
            "TableA",
            "table_a_sync",
            GetConnectionString(_targetDatabase),
            "TableA",
            optionsA);

        // Act - Sync TableB
        var optionsB = new SyncOptions
        {
            PrimaryKeyColumn = "ProductId",
            WatermarkColumn = "ModifiedDate",
            WarehousePath = _warehousePath,
            WatermarkDirectory = _watermarkPath
        };

        var resultB = await coordinator.SyncAsync(
            GetConnectionString(_sourceDatabase),
            "TableB",
            "table_b_sync",
            GetConnectionString(_targetDatabase),
            "TableB",
            optionsB);

        // Assert
        Assert.True(resultA.Success);
        Assert.True(resultB.Success);
        Assert.Equal(100, await GetTargetRowCount("TableA"));
        Assert.Equal(50, await GetTargetRowCount("TableB"));

        // Verify watermarks are independent
        var watermarkStore = new FileWatermarkStore(_watermarkPath);
        var watermarkA = await watermarkStore.GetWatermarkAsync("table_a_sync");
        var watermarkB = await watermarkStore.GetWatermarkAsync("table_b_sync");

        Assert.NotNull(watermarkA);
        Assert.NotNull(watermarkB);
        Assert.NotEqual(watermarkA.LastIcebergSnapshot, watermarkB.LastIcebergSnapshot);
    }

    // Helper methods

    private IncrementalSyncCoordinator CreateCoordinator()
    {
        var catalog = new FilesystemCatalog(_warehousePath, NullLogger<FilesystemCatalog>.Instance);
        var changeDetection = new TimestampChangeDetection("ModifiedDate");
        var appender = new IcebergAppender(catalog, NullLogger<IcebergAppender>.Instance);
        var reader = new IcebergReader(catalog, NullLogger<IcebergReader>.Instance);
        var importer = new SqlServerImporter(NullLogger<SqlServerImporter>.Instance);
        var watermarkStore = new FileWatermarkStore(_watermarkPath);

        return new IncrementalSyncCoordinator(
            changeDetection,
            appender,
            reader,
            importer,
            watermarkStore,
            NullLogger<IncrementalSyncCoordinator>.Instance);
    }

    private string GetConnectionString(string database)
    {
        return $"Server=localhost;Database={database};User Id=sa;Password=IcebergDemo@2024;TrustServerCertificate=true;";
    }

    private async Task CreateSourceTable(string tableName, string primaryKeyColumn = "ProductId")
    {
        await using var connection = new SqlConnection(GetConnectionString(_sourceDatabase));
        await connection.OpenAsync();

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $@"
            IF OBJECT_ID('{tableName}', 'U') IS NOT NULL DROP TABLE {tableName};
            CREATE TABLE {tableName} (
                {primaryKeyColumn} INT PRIMARY KEY,
                Name NVARCHAR(200),
                Price DECIMAL(18,2),
                ModifiedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE()
            )";
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task CreateTargetTable(string tableName, string primaryKeyColumn = "ProductId")
    {
        await using var connection = new SqlConnection(GetConnectionString(_targetDatabase));
        await connection.OpenAsync();

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $@"
            IF OBJECT_ID('{tableName}', 'U') IS NOT NULL DROP TABLE {tableName};
            CREATE TABLE {tableName} (
                {primaryKeyColumn} INT PRIMARY KEY,
                Name NVARCHAR(200),
                Price DECIMAL(18,2),
                ModifiedDate DATETIME2
            )";
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task InsertProducts(int startId, int endId, string tableName = "Products")
    {
        await using var connection = new SqlConnection(GetConnectionString(_sourceDatabase));
        await connection.OpenAsync();

        for (int i = startId; i <= endId; i++)
        {
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = $@"
                INSERT INTO {tableName} (ProductId, Name, Price, ModifiedDate)
                VALUES (@Id, @Name, @Price, GETUTCDATE())";
            cmd.Parameters.AddWithValue("@Id", i);
            cmd.Parameters.AddWithValue("@Name", $"Product{i}");
            cmd.Parameters.AddWithValue("@Price", i * 10.99m);
            await cmd.ExecuteNonQueryAsync();
        }
    }

    private async Task UpdateProducts(int startId, int endId, string nameSuffix)
    {
        await using var connection = new SqlConnection(GetConnectionString(_sourceDatabase));
        await connection.OpenAsync();

        for (int i = startId; i <= endId; i++)
        {
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
                UPDATE Products
                SET Name = @Name, ModifiedDate = GETUTCDATE()
                WHERE ProductId = @Id";
            cmd.Parameters.AddWithValue("@Id", i);
            cmd.Parameters.AddWithValue("@Name", $"Product{i}_{nameSuffix}");
            await cmd.ExecuteNonQueryAsync();
        }
    }

    private async Task InsertLargeDataset(int count)
    {
        await using var connection = new SqlConnection(GetConnectionString(_sourceDatabase));
        await connection.OpenAsync();

        // Use batch inserts for performance
        for (int batch = 0; batch < count / 100; batch++)
        {
            var values = new List<string>();
            for (int i = 0; i < 100; i++)
            {
                int id = batch * 100 + i + 1;
                values.Add($"({id}, 'Order{id}', {id * 25.50m}, GETUTCDATE())");
            }

            await using var cmd = connection.CreateCommand();
            cmd.CommandText = $@"
                INSERT INTO LargeOrders (OrderId, Name, Price, ModifiedDate)
                VALUES {string.Join(", ", values)}";
            await cmd.ExecuteNonQueryAsync();
        }
    }

    private async Task InsertTransactions()
    {
        await using var connection = new SqlConnection(GetConnectionString(_sourceDatabase));
        await connection.OpenAsync();

        var transactions = new[]
        {
            (1, 100.50m, "Payment from customer A"),
            (2, 250.00m, "Refund to customer B"),
            (3, 75.25m, "Payment from customer C")
        };

        foreach (var (id, amount, desc) in transactions)
        {
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
                INSERT INTO Transactions (ProductId, Name, Price, ModifiedDate)
                VALUES (@Id, @Desc, @Amount, GETUTCDATE())";
            cmd.Parameters.AddWithValue("@Id", id);
            cmd.Parameters.AddWithValue("@Desc", desc);
            cmd.Parameters.AddWithValue("@Amount", amount);
            await cmd.ExecuteNonQueryAsync();
        }
    }

    private async Task<int> GetSourceRowCount(string tableName)
    {
        await using var connection = new SqlConnection(GetConnectionString(_sourceDatabase));
        await connection.OpenAsync();

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM {tableName}";
        return (int)await cmd.ExecuteScalarAsync()!;
    }

    private async Task<int> GetTargetRowCount(string tableName)
    {
        await using var connection = new SqlConnection(GetConnectionString(_targetDatabase));
        await connection.OpenAsync();

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM {tableName}";
        return (int)await cmd.ExecuteScalarAsync()!;
    }

    private async Task<int> GetTargetRowCountWhere(string tableName, string whereClause)
    {
        await using var connection = new SqlConnection(GetConnectionString(_targetDatabase));
        await connection.OpenAsync();

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM {tableName} WHERE {whereClause}";
        return (int)await cmd.ExecuteScalarAsync()!;
    }

    private async Task<List<(int Id, decimal Amount, string Description)>> GetTransactionData()
    {
        await using var connection = new SqlConnection(GetConnectionString(_sourceDatabase));
        await connection.OpenAsync();

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT ProductId, Price, Name FROM Transactions ORDER BY ProductId";
        await using var reader = await cmd.ExecuteReaderAsync();

        var results = new List<(int, decimal, string)>();
        while (await reader.ReadAsync())
        {
            results.Add((reader.GetInt32(0), reader.GetDecimal(1), reader.GetString(2)));
        }
        return results;
    }

    private async Task<List<(int Id, decimal Amount, string Description)>> GetTargetTransactionData()
    {
        await using var connection = new SqlConnection(GetConnectionString(_targetDatabase));
        await connection.OpenAsync();

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT ProductId, Price, Name FROM Transactions ORDER BY ProductId";
        await using var reader = await cmd.ExecuteReaderAsync();

        var results = new List<(int, decimal, string)>();
        while (await reader.ReadAsync())
        {
            results.Add((reader.GetInt32(0), reader.GetDecimal(1), reader.GetString(2)));
        }
        return results;
    }

    public void Dispose()
    {
        // Cleanup
        try
        {
            using var connection = new SqlConnection(_connectionString);
            connection.Open();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = $@"
                IF EXISTS (SELECT * FROM sys.databases WHERE name = '{_sourceDatabase}')
                    DROP DATABASE [{_sourceDatabase}];
                IF EXISTS (SELECT * FROM sys.databases WHERE name = '{_targetDatabase}')
                    DROP DATABASE [{_targetDatabase}];";
            cmd.ExecuteNonQuery();

            if (Directory.Exists(_warehousePath))
                Directory.Delete(_warehousePath, true);
            if (Directory.Exists(_watermarkPath))
                Directory.Delete(_watermarkPath, true);
        }
        catch
        {
            // Ignore cleanup errors
        }
    }
}
