using DataTransfer.Iceberg.Catalog;
using DataTransfer.Iceberg.ChangeDetection;
using DataTransfer.Iceberg.Integration;
using DataTransfer.Iceberg.Models;
using DataTransfer.Iceberg.Watermarks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Xunit;
using Xunit.Abstractions;

namespace DataTransfer.Iceberg.Tests.Integration;

/// <summary>
/// Comprehensive demonstration tests for Iceberg multi-cycle incremental sync.
/// These tests showcase the complete incremental sync workflow with watermark tracking,
/// snapshot management, and data consistency across multiple sync cycles.
/// </summary>
public class MultiCycleSyncDemoTests : IDisposable
{
    private readonly string _connectionString;
    private readonly string _sourceDatabase;
    private readonly string _targetDatabase;
    private readonly string _warehousePath;
    private readonly string _watermarkPath;
    private readonly ITestOutputHelper _output;
    private readonly ILoggerFactory _loggerFactory;

    public MultiCycleSyncDemoTests(ITestOutputHelper output)
    {
        _output = output;
        _connectionString = "Server=localhost;User Id=sa;Password=IcebergDemo@2024;TrustServerCertificate=true;";
        _sourceDatabase = $"MultiCycleSourceDb_{Guid.NewGuid():N}";
        _targetDatabase = $"MultiCycleTargetDb_{Guid.NewGuid():N}";
        _warehousePath = Path.Combine(Path.GetTempPath(), "multicycle-warehouse", Guid.NewGuid().ToString());
        _watermarkPath = Path.Combine(Path.GetTempPath(), "multicycle-watermarks", Guid.NewGuid().ToString());

        _loggerFactory = LoggerFactory.Create(builder =>
        {
            builder.AddConsole();
            builder.SetMinimumLevel(LogLevel.Information);
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
    public async Task Should_Demonstrate_Five_Cycle_Incremental_Sync_With_Watermark_Progression()
    {
        // This test demonstrates a production-like scenario:
        // - Daily batch job running incremental sync
        // - Watermark tracks last processed timestamp
        // - Each cycle only processes new/changed records
        // - Snapshots preserve history

        // Arrange
        await CreateOrdersTable();
        await CreateTargetTable("Orders");

        var coordinator = CreateCoordinator();
        var options = new SyncOptions
        {
            PrimaryKeyColumn = "OrderId",
            WatermarkColumn = "ModifiedTimestamp",
            WarehousePath = _warehousePath,
            WatermarkDirectory = _watermarkPath
        };

        // Track watermark progression
        var watermarks = new List<DateTime>();
        var rowCounts = new List<int>();

        _output.WriteLine("=== Multi-Cycle Incremental Sync Demonstration ===");
        _output.WriteLine($"Warehouse Path: {_warehousePath}");
        _output.WriteLine($"Watermark Path: {_watermarkPath}");
        _output.WriteLine("");

        // Cycle 1: Initial load - 1000 orders (Day 1)
        _output.WriteLine("--- Cycle 1: Initial Load (Day 1) ---");
        var baseDate = new DateTime(2024, 11, 1, 0, 0, 0);
        await InsertOrders(1, 1000, baseDate);

        var result1 = await coordinator.SyncAsync(
            GetConnectionString(_sourceDatabase),
            "Orders",
            "orders_incremental",
            GetConnectionString(_targetDatabase),
            "Orders",
            options);

        Assert.True(result1.Success, $"Cycle 1 failed: {result1.ErrorMessage}");
        Assert.Equal(1000, result1.RowsExtracted);

        var targetCount1 = await GetTargetRowCount("Orders");
        Assert.Equal(1000, targetCount1);
        watermarks.Add(result1.LastWatermark!.Value);
        rowCounts.Add(targetCount1);

        _output.WriteLine($"  Rows Extracted: {result1.RowsExtracted}");
        _output.WriteLine($"  Watermark: {result1.LastWatermark:yyyy-MM-dd HH:mm:ss}");
        _output.WriteLine($"  Target Total: {targetCount1}");
        _output.WriteLine("");

        // Cycle 2: Day 2 batch - 500 new orders
        _output.WriteLine("--- Cycle 2: Daily Batch (Day 2) ---");
        await Task.Delay(50); // Simulate time passing
        var day2Date = baseDate.AddDays(1);
        await InsertOrders(1001, 1500, day2Date);

        var result2 = await coordinator.SyncAsync(
            GetConnectionString(_sourceDatabase),
            "Orders",
            "orders_incremental",
            GetConnectionString(_targetDatabase),
            "Orders",
            options);

        Assert.True(result2.Success);
        Assert.Equal(500, result2.RowsExtracted); // Only new records

        var targetCount2 = await GetTargetRowCount("Orders");
        Assert.Equal(1500, targetCount2);
        watermarks.Add(result2.LastWatermark!.Value);
        rowCounts.Add(targetCount2);

        _output.WriteLine($"  Rows Extracted: {result2.RowsExtracted}");
        _output.WriteLine($"  Watermark: {result2.LastWatermark:yyyy-MM-dd HH:mm:ss}");
        _output.WriteLine($"  Target Total: {targetCount2}");
        _output.WriteLine($"  Incremental: +{result2.RowsExtracted}");
        _output.WriteLine("");

        // Cycle 3: Day 3 batch - 300 new orders
        _output.WriteLine("--- Cycle 3: Daily Batch (Day 3) ---");
        await Task.Delay(50);
        var day3Date = baseDate.AddDays(2);
        await InsertOrders(1501, 1800, day3Date);

        var result3 = await coordinator.SyncAsync(
            GetConnectionString(_sourceDatabase),
            "Orders",
            "orders_incremental",
            GetConnectionString(_targetDatabase),
            "Orders",
            options);

        Assert.True(result3.Success);
        Assert.Equal(300, result3.RowsExtracted);

        var targetCount3 = await GetTargetRowCount("Orders");
        Assert.Equal(1800, targetCount3);
        watermarks.Add(result3.LastWatermark!.Value);
        rowCounts.Add(targetCount3);

        _output.WriteLine($"  Rows Extracted: {result3.RowsExtracted}");
        _output.WriteLine($"  Watermark: {result3.LastWatermark:yyyy-MM-dd HH:mm:ss}");
        _output.WriteLine($"  Target Total: {targetCount3}");
        _output.WriteLine($"  Incremental: +{result3.RowsExtracted}");
        _output.WriteLine("");

        // Cycle 4: Day 4 batch - 200 new orders
        _output.WriteLine("--- Cycle 4: Daily Batch (Day 4) ---");
        await Task.Delay(50);
        var day4Date = baseDate.AddDays(3);
        await InsertOrders(1801, 2000, day4Date);

        var result4 = await coordinator.SyncAsync(
            GetConnectionString(_sourceDatabase),
            "Orders",
            "orders_incremental",
            GetConnectionString(_targetDatabase),
            "Orders",
            options);

        Assert.True(result4.Success);
        Assert.Equal(200, result4.RowsExtracted);

        var targetCount4 = await GetTargetRowCount("Orders");
        Assert.Equal(2000, targetCount4);
        watermarks.Add(result4.LastWatermark!.Value);
        rowCounts.Add(targetCount4);

        _output.WriteLine($"  Rows Extracted: {result4.RowsExtracted}");
        _output.WriteLine($"  Watermark: {result4.LastWatermark:yyyy-MM-dd HH:mm:ss}");
        _output.WriteLine($"  Target Total: {targetCount4}");
        _output.WriteLine($"  Incremental: +{result4.RowsExtracted}");
        _output.WriteLine("");

        // Cycle 5: Day 5 batch - 100 new orders
        _output.WriteLine("--- Cycle 5: Daily Batch (Day 5) ---");
        await Task.Delay(50);
        var day5Date = baseDate.AddDays(4);
        await InsertOrders(2001, 2100, day5Date);

        var result5 = await coordinator.SyncAsync(
            GetConnectionString(_sourceDatabase),
            "Orders",
            "orders_incremental",
            GetConnectionString(_targetDatabase),
            "Orders",
            options);

        Assert.True(result5.Success);
        Assert.Equal(100, result5.RowsExtracted);

        var targetCount5 = await GetTargetRowCount("Orders");
        Assert.Equal(2100, targetCount5);
        watermarks.Add(result5.LastWatermark!.Value);
        rowCounts.Add(targetCount5);

        _output.WriteLine($"  Rows Extracted: {result5.RowsExtracted}");
        _output.WriteLine($"  Watermark: {result5.LastWatermark:yyyy-MM-dd HH:mm:ss}");
        _output.WriteLine($"  Target Total: {targetCount5}");
        _output.WriteLine($"  Incremental: +{result5.RowsExtracted}");
        _output.WriteLine("");

        // Verify watermark progression (should be strictly increasing)
        _output.WriteLine("=== Watermark Progression ===");
        for (int i = 0; i < watermarks.Count; i++)
        {
            _output.WriteLine($"Cycle {i + 1}: {watermarks[i]:yyyy-MM-dd HH:mm:ss.fff} -> {rowCounts[i]} total rows");

            if (i > 0)
            {
                Assert.True(watermarks[i] > watermarks[i - 1],
                    $"Watermark should increase from cycle {i} to {i + 1}");
            }
        }
        _output.WriteLine("");

        // Verify Iceberg snapshots were created
        var catalog = new FilesystemCatalog(_warehousePath, _loggerFactory.CreateLogger<FilesystemCatalog>());
        var tableExists = catalog.TableExists("orders_incremental");
        Assert.True(tableExists, "Iceberg table should exist");

        var metadata = await catalog.LoadTableMetadataAsync("orders_incremental");
        _output.WriteLine($"=== Iceberg Snapshots ===");
        _output.WriteLine($"Total Snapshots: {metadata.Snapshots.Count}");
        Assert.True(metadata.Snapshots.Count >= 5, "Should have at least 5 snapshots (one per cycle)");

        foreach (var snapshot in metadata.Snapshots)
        {
            _output.WriteLine($"  Snapshot {snapshot.SnapshotId}: {snapshot.TimestampMs} ms");
        }

        // Verify final data consistency
        var finalSourceCount = await GetSourceRowCount("Orders");
        var finalTargetCount = await GetTargetRowCount("Orders");
        Assert.Equal(finalSourceCount, finalTargetCount);
        Assert.Equal(2100, finalTargetCount);

        _output.WriteLine("");
        _output.WriteLine("=== Summary ===");
        _output.WriteLine($"Total Cycles: 5");
        _output.WriteLine($"Total Rows Synced: {rowCounts.Last()}");
        _output.WriteLine($"Source Row Count: {finalSourceCount}");
        _output.WriteLine($"Target Row Count: {finalTargetCount}");
        _output.WriteLine($"Data Integrity: {(finalSourceCount == finalTargetCount ? "PASSED" : "FAILED")}");
    }

    [Fact]
    public async Task Should_Handle_Zero_New_Records_In_Sync_Cycle()
    {
        // Demonstrates idempotent behavior - re-running sync with no new data

        // Arrange
        await CreateOrdersTable();
        await CreateTargetTable("Orders");

        var coordinator = CreateCoordinator();
        var options = new SyncOptions
        {
            PrimaryKeyColumn = "OrderId",
            WatermarkColumn = "ModifiedTimestamp",
            WarehousePath = _warehousePath,
            WatermarkDirectory = _watermarkPath
        };

        // Cycle 1: Initial load - 500 orders
        var baseDate = new DateTime(2024, 11, 1, 0, 0, 0);
        await InsertOrders(1, 500, baseDate);

        var result1 = await coordinator.SyncAsync(
            GetConnectionString(_sourceDatabase),
            "Orders",
            "orders_zero_test",
            GetConnectionString(_targetDatabase),
            "Orders",
            options);

        Assert.True(result1.Success);
        Assert.Equal(500, result1.RowsExtracted);

        // Cycle 2: No new data - should handle gracefully
        await Task.Delay(50);

        var result2 = await coordinator.SyncAsync(
            GetConnectionString(_sourceDatabase),
            "Orders",
            "orders_zero_test",
            GetConnectionString(_targetDatabase),
            "Orders",
            options);

        Assert.True(result2.Success, "Sync should succeed even with 0 new records");
        Assert.Equal(0, result2.RowsExtracted);

        // Target count should remain unchanged
        var targetCount = await GetTargetRowCount("Orders");
        Assert.Equal(500, targetCount);

        _output.WriteLine($"Cycle 1: {result1.RowsExtracted} rows");
        _output.WriteLine($"Cycle 2: {result2.RowsExtracted} rows (no new data)");
        _output.WriteLine($"Final Target Count: {targetCount}");
    }

    [Fact]
    public async Task Should_Maintain_Data_Consistency_Across_All_Cycles()
    {
        // Verify that cumulative data from all cycles matches source

        // Arrange
        await CreateOrdersTable();
        await CreateTargetTable("Orders");

        var coordinator = CreateCoordinator();
        var options = new SyncOptions
        {
            PrimaryKeyColumn = "OrderId",
            WatermarkColumn = "ModifiedTimestamp",
            WarehousePath = _warehousePath,
            WatermarkDirectory = _watermarkPath
        };

        // Run 3 cycles with varying batch sizes
        var baseDate = new DateTime(2024, 11, 1, 0, 0, 0);

        // Cycle 1: 300 orders
        await InsertOrders(1, 300, baseDate);
        await coordinator.SyncAsync(
            GetConnectionString(_sourceDatabase), "Orders", "consistency_test",
            GetConnectionString(_targetDatabase), "Orders", options);

        // Cycle 2: 150 orders
        await Task.Delay(50);
        await InsertOrders(301, 450, baseDate.AddDays(1));
        await coordinator.SyncAsync(
            GetConnectionString(_sourceDatabase), "Orders", "consistency_test",
            GetConnectionString(_targetDatabase), "Orders", options);

        // Cycle 3: 100 orders
        await Task.Delay(50);
        await InsertOrders(451, 550, baseDate.AddDays(2));
        var result3 = await coordinator.SyncAsync(
            GetConnectionString(_sourceDatabase), "Orders", "consistency_test",
            GetConnectionString(_targetDatabase), "Orders", options);

        // Assert - Full data consistency check
        var sourceCount = await GetSourceRowCount("Orders");
        var targetCount = await GetTargetRowCount("Orders");

        Assert.Equal(550, sourceCount);
        Assert.Equal(550, targetCount);
        Assert.Equal(sourceCount, targetCount);

        // Verify specific order IDs exist in target
        var targetOrderIds = await GetTargetOrderIds();
        Assert.Equal(550, targetOrderIds.Count);
        Assert.Contains(1, targetOrderIds);      // First order
        Assert.Contains(300, targetOrderIds);    // Boundary
        Assert.Contains(450, targetOrderIds);    // Boundary
        Assert.Contains(550, targetOrderIds);    // Last order
        Assert.DoesNotContain(0, targetOrderIds);    // Invalid ID
        Assert.DoesNotContain(551, targetOrderIds);  // Not inserted

        _output.WriteLine($"Source Count: {sourceCount}");
        _output.WriteLine($"Target Count: {targetCount}");
        _output.WriteLine($"Data Consistency: VERIFIED");
    }

    #region Helper Methods

    private IncrementalSyncCoordinator CreateCoordinator()
    {
        return new IncrementalSyncCoordinator(_loggerFactory);
    }

    private string GetConnectionString(string database)
    {
        var builder = new SqlConnectionStringBuilder(_connectionString)
        {
            InitialCatalog = database
        };
        return builder.ConnectionString;
    }

    private async Task CreateOrdersTable()
    {
        await using var conn = new SqlConnection(GetConnectionString(_sourceDatabase));
        await conn.OpenAsync();

        var cmd = conn.CreateCommand();
        cmd.CommandText = @"
            CREATE TABLE Orders (
                OrderId INT PRIMARY KEY,
                CustomerName NVARCHAR(100) NOT NULL,
                OrderDate DATE NOT NULL,
                TotalAmount DECIMAL(18, 2) NOT NULL,
                ModifiedTimestamp DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
            );";
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task CreateTargetTable(string tableName)
    {
        await using var conn = new SqlConnection(GetConnectionString(_targetDatabase));
        await conn.OpenAsync();

        var cmd = conn.CreateCommand();
        cmd.CommandText = $@"
            CREATE TABLE {tableName} (
                OrderId INT PRIMARY KEY,
                CustomerName NVARCHAR(100) NOT NULL,
                OrderDate DATE NOT NULL,
                TotalAmount DECIMAL(18, 2) NOT NULL,
                ModifiedTimestamp DATETIME2 NOT NULL
            );";
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task InsertOrders(int startId, int endId, DateTime modifiedDate)
    {
        await using var conn = new SqlConnection(GetConnectionString(_sourceDatabase));
        await conn.OpenAsync();

        for (int i = startId; i <= endId; i++)
        {
            var cmd = conn.CreateCommand();
            cmd.CommandText = @"
                INSERT INTO Orders (OrderId, CustomerName, OrderDate, TotalAmount, ModifiedTimestamp)
                VALUES (@OrderId, @CustomerName, @OrderDate, @TotalAmount, @ModifiedTimestamp);";

            cmd.Parameters.AddWithValue("@OrderId", i);
            cmd.Parameters.AddWithValue("@CustomerName", $"Customer {i}");
            cmd.Parameters.AddWithValue("@OrderDate", modifiedDate.Date);
            cmd.Parameters.AddWithValue("@TotalAmount", (i % 100) * 9.99m);
            cmd.Parameters.AddWithValue("@ModifiedTimestamp", modifiedDate.AddSeconds(i - startId));

            await cmd.ExecuteNonQueryAsync();
        }
    }

    private async Task<int> GetSourceRowCount(string tableName)
    {
        await using var conn = new SqlConnection(GetConnectionString(_sourceDatabase));
        await conn.OpenAsync();

        var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM {tableName};";
        return (int)await cmd.ExecuteScalarAsync()!;
    }

    private async Task<int> GetTargetRowCount(string tableName)
    {
        await using var conn = new SqlConnection(GetConnectionString(_targetDatabase));
        await conn.OpenAsync();

        var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM {tableName};";
        return (int)await cmd.ExecuteScalarAsync()!;
    }

    private async Task<List<int>> GetTargetOrderIds()
    {
        await using var conn = new SqlConnection(GetConnectionString(_targetDatabase));
        await conn.OpenAsync();

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT OrderId FROM Orders ORDER BY OrderId;";

        var reader = await cmd.ExecuteReaderAsync();
        var ids = new List<int>();
        while (await reader.ReadAsync())
        {
            ids.Add(reader.GetInt32(0));
        }
        return ids;
    }

    public void Dispose()
    {
        try
        {
            using var connection = new SqlConnection(_connectionString);
            connection.Open();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = $@"
                IF EXISTS (SELECT * FROM sys.databases WHERE name = '{_sourceDatabase}')
                BEGIN
                    ALTER DATABASE [{_sourceDatabase}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                    DROP DATABASE [{_sourceDatabase}];
                END
                IF EXISTS (SELECT * FROM sys.databases WHERE name = '{_targetDatabase}')
                BEGIN
                    ALTER DATABASE [{_targetDatabase}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                    DROP DATABASE [{_targetDatabase}];
                END";
            cmd.ExecuteNonQuery();
        }
        catch { /* Cleanup errors are acceptable */ }

        try
        {
            if (Directory.Exists(_warehousePath))
                Directory.Delete(_warehousePath, recursive: true);
            if (Directory.Exists(_watermarkPath))
                Directory.Delete(_watermarkPath, recursive: true);
        }
        catch { /* Cleanup errors are acceptable */ }

        _loggerFactory.Dispose();
    }

    #endregion
}
