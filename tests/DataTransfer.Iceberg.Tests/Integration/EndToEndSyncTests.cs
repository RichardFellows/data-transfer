using DataTransfer.Iceberg.Catalog;
using DataTransfer.Iceberg.ChangeDetection;
using DataTransfer.Iceberg.Integration;
using DataTransfer.Iceberg.Models;
using DataTransfer.Iceberg.Readers;
using DataTransfer.Iceberg.Watermarks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using Xunit.Abstractions;

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
    private readonly ITestOutputHelper _output;
    private readonly ILoggerFactory _loggerFactory;

    public EndToEndSyncTests(ITestOutputHelper output)
    {
        _output = output;
        _connectionString = "Server=localhost;User Id=sa;Password=IcebergDemo@2024;TrustServerCertificate=true;";
        _sourceDatabase = $"E2ESourceDb_{Guid.NewGuid():N}";
        _targetDatabase = $"E2ETargetDb_{Guid.NewGuid():N}";
        _warehousePath = Path.Combine(Path.GetTempPath(), "e2e-warehouse", Guid.NewGuid().ToString());
        _watermarkPath = Path.Combine(Path.GetTempPath(), "e2e-watermarks", Guid.NewGuid().ToString());

        // Create logger factory for debugging
        _loggerFactory = LoggerFactory.Create(builder =>
        {
            builder.AddConsole();
            builder.SetMinimumLevel(LogLevel.Debug);
        });

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

        // Log watermark before update
        var watermarkStore = new FileWatermarkStore(_watermarkPath);
        var watermarkBeforeUpdate = await watermarkStore.GetWatermarkAsync("products_sync");
        Console.WriteLine($"[CYCLE 3] === UPDATE CYCLE STARTING ===");
        Console.WriteLine($"[CYCLE 3] Watermark before update: {watermarkBeforeUpdate?.LastSyncTimestamp:O}");
        Console.WriteLine($"[CYCLE 3] Current UTC time: {DateTime.UtcNow:O}");

        // Sleep to ensure timestamp separation
        await Task.Delay(1000);

        var timeBeforeUpdate = DateTime.UtcNow;
        Console.WriteLine($"[CYCLE 3] Time before update: {timeBeforeUpdate:O}");

        await UpdateProducts(1, 50, "Updated");

        var timeAfterUpdate = DateTime.UtcNow;
        Console.WriteLine($"[CYCLE 3] Time after update: {timeAfterUpdate:O}");
        Console.WriteLine($"[CYCLE 3] Update duration: {(timeAfterUpdate - timeBeforeUpdate).TotalMilliseconds}ms");

        // Verify updates in source
        var sourceUpdatedCount = await GetSourceRowCountWhere("Products", "Name LIKE '%Updated%'");
        Console.WriteLine($"[CYCLE 3] Source has {sourceUpdatedCount} rows with 'Updated' suffix");

        // Check ModifiedDate of updated rows
        var updatedModifiedDates = await GetModifiedDatesForUpdatedRows();
        Console.WriteLine($"[CYCLE 3] Updated rows have ModifiedDate range: {updatedModifiedDates.Min():O} to {updatedModifiedDates.Max():O}");
        Console.WriteLine($"[CYCLE 3] Watermark LastSyncTimestamp: {watermarkBeforeUpdate?.LastSyncTimestamp:O}");
        Console.WriteLine($"[CYCLE 3] Updated rows are AFTER watermark: {updatedModifiedDates.All(d => d > watermarkBeforeUpdate?.LastSyncTimestamp)}");

        // Test the incremental query that will be generated
        var changeDetection = new TimestampChangeDetection("ModifiedDate");
        await using var testConn = new SqlConnection(GetConnectionString(_sourceDatabase));
        await testConn.OpenAsync();
        var incrementalQuery = await changeDetection.BuildIncrementalQueryAsync("Products", watermarkBeforeUpdate, testConn);
        Console.WriteLine($"[CYCLE 3] Incremental query SQL: {incrementalQuery.Sql}");
        if (incrementalQuery.Parameters != null && incrementalQuery.Parameters.Count > 0)
        {
            foreach (var param in incrementalQuery.Parameters)
            {
                Console.WriteLine($"[CYCLE 3] Parameter {param.Key} = {param.Value}");
            }
        }

        var result3 = await coordinator.SyncAsync(
            GetConnectionString(_sourceDatabase),
            "Products",
            "products_sync",
            GetConnectionString(_targetDatabase),
            "Products",
            options);

        Console.WriteLine($"[CYCLE 3] Sync result - Success: {result3.Success}, RowsExtracted: {result3.RowsExtracted}, RowsImported: {result3.RowsImported}");

        // Check what's actually in Iceberg
        var icebergData = await ReadIcebergTable("products_sync");
        var icebergUpdatedCount = icebergData.Count(row => row.ContainsKey("Name") && row["Name"]?.ToString()?.Contains("Updated") == true);
        Console.WriteLine($"[CYCLE 3] Iceberg has {icebergData.Count} total rows, {icebergUpdatedCount} with 'Updated' suffix");

        // Sample Iceberg data
        var icebergSample = icebergData.Take(5).ToList();
        Console.WriteLine($"[CYCLE 3] Iceberg sample data (first 5 rows):");
        foreach (var row in icebergSample)
        {
            var id = row.ContainsKey("ProductId") ? row["ProductId"] : "?";
            var name = row.ContainsKey("Name") ? row["Name"] : "?";
            Console.WriteLine($"  ProductId={id}, Name={name}");
        }

        Assert.True(result3.Success, $"Cycle 3 failed: {result3.ErrorMessage}");
        Assert.Equal(50, result3.RowsExtracted);
        Assert.Equal(700, await GetTargetRowCount("Products")); // Still 700 (updates, not inserts)

        // Verify updated data in target
        var updatedCount = await GetTargetRowCountWhere("Products", "Name LIKE '%Updated%'");
        Console.WriteLine($"[CYCLE 3] Target has {updatedCount} rows with 'Updated' suffix (expected 50)");

        // Sample some target data to see what was actually written
        var targetSample = await GetTargetSampleData("Products", 5);
        Console.WriteLine($"[CYCLE 3] Target sample data (first 5 rows):");
        foreach (var row in targetSample)
        {
            Console.WriteLine($"  ProductId={row.Id}, Name={row.Name}");
        }

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
        var catalog = new FilesystemCatalog(_warehousePath, _loggerFactory.CreateLogger<FilesystemCatalog>());
        var changeDetection = new TimestampChangeDetection("ModifiedDate");
        var appender = new IcebergAppender(catalog, _loggerFactory.CreateLogger<IcebergAppender>());
        var reader = new IcebergReader(catalog, _loggerFactory.CreateLogger<IcebergReader>());
        var importer = new SqlServerImporter(_loggerFactory.CreateLogger<SqlServerImporter>());
        var watermarkStore = new FileWatermarkStore(_watermarkPath);

        return new IncrementalSyncCoordinator(
            changeDetection,
            appender,
            reader,
            importer,
            watermarkStore,
            _loggerFactory.CreateLogger<IncrementalSyncCoordinator>());
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

    private async Task<int> GetSourceRowCountWhere(string tableName, string whereClause)
    {
        await using var connection = new SqlConnection(GetConnectionString(_sourceDatabase));
        await connection.OpenAsync();

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM {tableName} WHERE {whereClause}";
        return (int)await cmd.ExecuteScalarAsync()!;
    }

    private async Task<List<DateTime>> GetModifiedDatesForUpdatedRows()
    {
        await using var connection = new SqlConnection(GetConnectionString(_sourceDatabase));
        await connection.OpenAsync();

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT ModifiedDate FROM Products WHERE Name LIKE '%Updated%' ORDER BY ModifiedDate";
        await using var reader = await cmd.ExecuteReaderAsync();

        var dates = new List<DateTime>();
        while (await reader.ReadAsync())
        {
            dates.Add(reader.GetDateTime(0));
        }
        return dates;
    }

    private async Task<List<(int Id, string Name)>> GetTargetSampleData(string tableName, int limit)
    {
        await using var connection = new SqlConnection(GetConnectionString(_targetDatabase));
        await connection.OpenAsync();

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $"SELECT TOP({limit}) ProductId, Name FROM {tableName} ORDER BY ProductId";
        await using var reader = await cmd.ExecuteReaderAsync();

        var results = new List<(int, string)>();
        while (await reader.ReadAsync())
        {
            results.Add((reader.GetInt32(0), reader.GetString(1)));
        }
        return results;
    }

    private async Task<List<Dictionary<string, object>>> ReadIcebergTable(string tableName)
    {
        var catalog = new FilesystemCatalog(_warehousePath, NullLogger<FilesystemCatalog>.Instance);
        var reader = new IcebergReader(catalog, NullLogger<IcebergReader>.Instance);

        var rows = new List<Dictionary<string, object>>();
        await foreach (var row in reader.ReadTableAsync(tableName))
        {
            rows.Add(row);
        }

        return rows;
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

            _loggerFactory?.Dispose();
        }
        catch
        {
            // Ignore cleanup errors
        }
    }
}
