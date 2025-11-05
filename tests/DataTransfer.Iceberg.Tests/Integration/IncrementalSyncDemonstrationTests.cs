using DataTransfer.Iceberg.Catalog;
using DataTransfer.Iceberg.ChangeDetection;
using DataTransfer.Iceberg.Integration;
using DataTransfer.Iceberg.Models;
using DataTransfer.Iceberg.Watermarks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using Xunit.Abstractions;

namespace DataTransfer.Iceberg.Tests.Integration;

/// <summary>
/// DEMONSTRATION: Iceberg Incremental Sync Feature
///
/// This test showcases the key differentiator of Iceberg integration:
/// watermark-based incremental synchronization across multiple cycles.
///
/// FEATURE: Only transfer data that changed since last sync
/// BENEFIT: Efficient data pipeline for large, frequently-updated tables
/// USE CASE: Daily batch sync of operational database to analytics warehouse
/// </summary>
public class IncrementalSyncDemonstrationTests : IDisposable
{
    private readonly string _connectionString;
    private readonly string _sourceDatabase;
    private readonly string _targetDatabase;
    private readonly string _warehousePath;
    private readonly string _watermarkPath;
    private readonly ITestOutputHelper _output;

    public IncrementalSyncDemonstrationTests(ITestOutputHelper output)
    {
        _output = output;
        _connectionString = "Server=localhost;User Id=sa;Password=IcebergDemo@2024;TrustServerCertificate=true;";
        _sourceDatabase = $"DemoSource_{Guid.NewGuid():N}";
        _targetDatabase = $"DemoTarget_{Guid.NewGuid():N}";
        _warehousePath = Path.Combine(Path.GetTempPath(), "iceberg-demo", Guid.NewGuid().ToString());
        _watermarkPath = Path.Combine(Path.GetTempPath(), "watermarks-demo", Guid.NewGuid().ToString());

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
        }
        catch
        {
            // Best effort cleanup
        }

        if (Directory.Exists(_warehousePath))
        {
            Directory.Delete(_warehousePath, true);
        }
        if (Directory.Exists(_watermarkPath))
        {
            Directory.Delete(_watermarkPath, true);
        }
    }

    /// <summary>
    /// COMPLETE INCREMENTAL SYNC DEMONSTRATION
    ///
    /// Scenario: E-commerce orders table that receives:
    /// - Day 1: 100 initial orders (first sync)
    /// - Day 2: 50 new orders (incremental sync)
    /// - Day 3: 30 more orders (incremental sync)
    ///
    /// Expected Behavior:
    /// - Cycle 1: Transfer all 100 rows (full sync)
    /// - Cycle 2: Transfer only 50 new rows (watermark at end of Cycle 1)
    /// - Cycle 3: Transfer only 30 new rows (watermark at end of Cycle 2)
    ///
    /// Total transferred: 180 rows across 3 syncs
    /// Total actual transfers: 100 + 50 + 30 = 180 (not 100+150+180!)
    ///
    /// This demonstrates efficient incremental loading.
    /// </summary>
    [Fact]
    public async Task Should_Demonstrate_Three_Cycle_Incremental_Sync_With_Watermark_Tracking()
    {
        _output.WriteLine("╔═══════════════════════════════════════════════════════════╗");
        _output.WriteLine("║  ICEBERG INCREMENTAL SYNC DEMONSTRATION                   ║");
        _output.WriteLine("╚═══════════════════════════════════════════════════════════╝");
        _output.WriteLine("");

        // ==========================
        // SETUP: Create Orders table
        // ==========================
        await CreateOrdersTable();
        _output.WriteLine("✓ Orders table created with OrderId, CustomerId, Amount, OrderDate");
        _output.WriteLine("");

        var coordinator = CreateCoordinator();
        var options = new SyncOptions
        {
            PrimaryKeyColumn = "OrderId",
            WatermarkColumn = "OrderDate",
            WarehousePath = _warehousePath,
            WatermarkDirectory = _watermarkPath
        };

        // ========================================
        // CYCLE 1: Initial Sync - Day 1 (100 rows)
        // ========================================
        _output.WriteLine("─────────────────────────────────────────────────────────");
        _output.WriteLine("CYCLE 1: Initial Sync (Day 1)");
        _output.WriteLine("─────────────────────────────────────────────────────────");

        await InsertOrders(1, 100, DateTime.UtcNow.AddDays(-2)); // Day 1 orders
        _output.WriteLine($"  Inserted: 100 orders (IDs 1-100)");

        var result1 = await coordinator.SyncAsync(
            GetConnectionString(_sourceDatabase),
            "Orders",
            "orders_incremental_sync",
            GetConnectionString(_targetDatabase),
            "Orders",
            options);

        Assert.True(result1.Success, $"Cycle 1 failed: {result1.ErrorMessage}");
        Assert.Equal(100, result1.RowsExtracted);
        _output.WriteLine($"  Extracted: {result1.RowsExtracted} rows");
        _output.WriteLine($"  Imported: {result1.RowsImported} rows");
        _output.WriteLine($"  Duration: {result1.Duration.TotalSeconds:F2}s");

        var targetCount1 = await GetTargetRowCount();
        Assert.Equal(100, targetCount1);
        _output.WriteLine($"  ✓ Target table now has {targetCount1} rows");

        // Check watermark
        var watermarkStore = new FileWatermarkStore(_watermarkPath);
        var watermark1 = await watermarkStore.GetWatermarkAsync("orders_incremental_sync");
        Assert.NotNull(watermark1);
        _output.WriteLine($"  ✓ Watermark saved: {watermark1.LastSyncTimestamp:yyyy-MM-dd HH:mm:ss}");
        _output.WriteLine("");

        // Small delay to ensure timestamp separation
        await Task.Delay(1000);

        // ========================================
        // CYCLE 2: Incremental Sync - Day 2 (+50 rows)
        // ========================================
        _output.WriteLine("─────────────────────────────────────────────────────────");
        _output.WriteLine("CYCLE 2: Incremental Sync (Day 2) - NEW ORDERS");
        _output.WriteLine("─────────────────────────────────────────────────────────");

        await InsertOrders(101, 150, DateTime.UtcNow.AddDays(-1)); // Day 2 orders
        _output.WriteLine($"  Inserted: 50 new orders (IDs 101-150)");
        _output.WriteLine($"  Source table now has 150 total rows");

        var result2 = await coordinator.SyncAsync(
            GetConnectionString(_sourceDatabase),
            "Orders",
            "orders_incremental_sync",
            GetConnectionString(_targetDatabase),
            "Orders",
            options);

        Assert.True(result2.Success, $"Cycle 2 failed: {result2.ErrorMessage}");
        Assert.Equal(50, result2.RowsExtracted); // ← KEY: Only 50, not 150!
        _output.WriteLine($"  Extracted: {result2.RowsExtracted} rows (only new data!)");
        _output.WriteLine($"  Imported: {result2.RowsImported} rows");
        _output.WriteLine($"  Duration: {result2.Duration.TotalSeconds:F2}s");

        var targetCount2 = await GetTargetRowCount();
        Assert.Equal(150, targetCount2);
        _output.WriteLine($"  ✓ Target table now has {targetCount2} rows (100 + 50)");

        var watermark2 = await watermarkStore.GetWatermarkAsync("orders_incremental_sync");
        Assert.NotNull(watermark2);
        Assert.True(watermark2.LastSyncTimestamp > watermark1.LastSyncTimestamp);
        _output.WriteLine($"  ✓ Watermark updated: {watermark2.LastSyncTimestamp:yyyy-MM-dd HH:mm:ss}");
        _output.WriteLine("");

        // Small delay to ensure timestamp separation
        await Task.Delay(1000);

        // ========================================
        // CYCLE 3: Incremental Sync - Day 3 (+30 rows)
        // ========================================
        _output.WriteLine("─────────────────────────────────────────────────────────");
        _output.WriteLine("CYCLE 3: Incremental Sync (Day 3) - MORE NEW ORDERS");
        _output.WriteLine("─────────────────────────────────────────────────────────");

        await InsertOrders(151, 180, DateTime.UtcNow); // Day 3 orders
        _output.WriteLine($"  Inserted: 30 new orders (IDs 151-180)");
        _output.WriteLine($"  Source table now has 180 total rows");

        var result3 = await coordinator.SyncAsync(
            GetConnectionString(_sourceDatabase),
            "Orders",
            "orders_incremental_sync",
            GetConnectionString(_targetDatabase),
            "Orders",
            options);

        Assert.True(result3.Success, $"Cycle 3 failed: {result3.ErrorMessage}");
        Assert.Equal(30, result3.RowsExtracted); // ← KEY: Only 30, not 180!
        _output.WriteLine($"  Extracted: {result3.RowsExtracted} rows (only new data!)");
        _output.WriteLine($"  Imported: {result3.RowsImported} rows");
        _output.WriteLine($"  Duration: {result3.Duration.TotalSeconds:F2}s");

        var targetCount3 = await GetTargetRowCount();
        Assert.Equal(180, targetCount3);
        _output.WriteLine($"  ✓ Target table now has {targetCount3} rows (150 + 30)");

        var watermark3 = await watermarkStore.GetWatermarkAsync("orders_incremental_sync");
        Assert.NotNull(watermark3);
        Assert.True(watermark3.LastSyncTimestamp > watermark2.LastSyncTimestamp);
        _output.WriteLine($"  ✓ Watermark updated: {watermark3.LastSyncTimestamp:yyyy-MM-dd HH:mm:ss}");
        _output.WriteLine("");

        // ========================================
        // SUMMARY: Efficiency Demonstration
        // ========================================
        _output.WriteLine("╔═══════════════════════════════════════════════════════════╗");
        _output.WriteLine("║  INCREMENTAL SYNC SUMMARY                                 ║");
        _output.WriteLine("╚═══════════════════════════════════════════════════════════╝");
        _output.WriteLine("");
        _output.WriteLine($"  Cycle 1: Transferred {result1.RowsExtracted} rows (full sync)");
        _output.WriteLine($"  Cycle 2: Transferred {result2.RowsExtracted} rows (incremental)");
        _output.WriteLine($"  Cycle 3: Transferred {result3.RowsExtracted} rows (incremental)");
        _output.WriteLine($"  ─────────────────────────────────────────────────────────");
        _output.WriteLine($"  Total Transferred: {result1.RowsExtracted + result2.RowsExtracted + result3.RowsExtracted} rows");
        _output.WriteLine($"  Final Row Count: {targetCount3} rows");
        _output.WriteLine("");
        _output.WriteLine("  KEY BENEFIT:");
        _output.WriteLine("  Without watermarks: Would transfer 100 + 150 + 180 = 430 rows");
        _output.WriteLine("  With watermarks:    Transferred  100 +  50 +  30 = 180 rows");
        _output.WriteLine($"  Efficiency Gain:    {(1 - 180.0 / 430) * 100:F1}% reduction in data transfer!");
        _output.WriteLine("");
        _output.WriteLine("╚═══════════════════════════════════════════════════════════╝");

        // Verify all three watermarks progressed
        Assert.True(watermark2.LastSyncTimestamp > watermark1.LastSyncTimestamp,
            "Watermark should increase between cycles");
        Assert.True(watermark3.LastSyncTimestamp > watermark2.LastSyncTimestamp,
            "Watermark should increase between cycles");
    }

    private async Task CreateOrdersTable()
    {
        var createSql = @"
            CREATE TABLE Orders (
                OrderId INT PRIMARY KEY,
                CustomerId INT NOT NULL,
                Amount DECIMAL(18,2) NOT NULL,
                OrderDate DATETIME2 NOT NULL DEFAULT GETUTCDATE()
            )";

        await using var sourceConn = new SqlConnection(GetConnectionString(_sourceDatabase));
        await sourceConn.OpenAsync();
        await using var sourceCmd = new SqlCommand(createSql, sourceConn);
        await sourceCmd.ExecuteNonQueryAsync();

        await using var targetConn = new SqlConnection(GetConnectionString(_targetDatabase));
        await targetConn.OpenAsync();
        await using var targetCmd = new SqlCommand(createSql, targetConn);
        await targetCmd.ExecuteNonQueryAsync();
    }

    private async Task InsertOrders(int startId, int endId, DateTime orderDate)
    {
        await using var conn = new SqlConnection(GetConnectionString(_sourceDatabase));
        await conn.OpenAsync();

        for (int id = startId; id <= endId; id++)
        {
            var insertSql = @"
                INSERT INTO Orders (OrderId, CustomerId, Amount, OrderDate)
                VALUES (@OrderId, @CustomerId, @Amount, @OrderDate)";

            await using var cmd = new SqlCommand(insertSql, conn);
            cmd.Parameters.AddWithValue("@OrderId", id);
            cmd.Parameters.AddWithValue("@CustomerId", (id % 100) + 1);
            cmd.Parameters.AddWithValue("@Amount", (id * 10.5m) % 1000);
            cmd.Parameters.AddWithValue("@OrderDate", orderDate.AddMinutes(id));
            await cmd.ExecuteNonQueryAsync();
        }
    }

    private async Task<int> GetTargetRowCount()
    {
        await using var conn = new SqlConnection(GetConnectionString(_targetDatabase));
        await conn.OpenAsync();
        await using var cmd = new SqlCommand("SELECT COUNT(*) FROM Orders", conn);
        return (int)await cmd.ExecuteScalarAsync();
    }

    private string GetConnectionString(string database)
    {
        var builder = new SqlConnectionStringBuilder(_connectionString)
        {
            InitialCatalog = database
        };
        return builder.ConnectionString;
    }

    private IncrementalSyncCoordinator CreateCoordinator()
    {
        var catalog = new FilesystemCatalog(_warehousePath, NullLogger<FilesystemCatalog>.Instance);
        var watermarkStore = new FileWatermarkStore(_watermarkPath);
        var changeDetection = new TimestampChangeDetection("OrderDate");

        return new IncrementalSyncCoordinator(
            catalog,
            watermarkStore,
            changeDetection,
            NullLogger<IncrementalSyncCoordinator>.Instance);
    }
}
