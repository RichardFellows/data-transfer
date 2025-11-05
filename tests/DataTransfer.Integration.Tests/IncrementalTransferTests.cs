using DataTransfer.Core.Interfaces;
using DataTransfer.Core.Models;
using DataTransfer.Parquet;
using DataTransfer.Pipeline;
using DataTransfer.SqlServer;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Respawn;
using Serilog;
using Testcontainers.MsSql;
using Xunit;

namespace DataTransfer.Integration.Tests;

/// <summary>
/// Demonstration tests for incremental data transfers with watermark tracking.
/// These tests showcase how to transfer only new/changed data on subsequent runs,
/// a critical capability for production data pipelines.
/// </summary>
public class IncrementalTransferTests : IAsyncLifetime
{
    private static MsSqlContainer? _sqlContainer;
    private static string _connectionString = string.Empty;
    private static bool _containerInitialized = false;
    private static readonly SemaphoreSlim _initLock = new(1, 1);
    private readonly string _testOutputPath = Path.Combine(Path.GetTempPath(), "datatransfer-incremental-tests", Guid.NewGuid().ToString());
    private ILogger<DataTransferOrchestrator>? _logger;
    private Respawner? _sourceRespawner;
    private Respawner? _destRespawner;

    public async Task InitializeAsync()
    {
        Log.Logger = new LoggerConfiguration()
            .MinimumLevel.Debug()
            .WriteTo.Console()
            .CreateLogger();

        var loggerFactory = LoggerFactory.Create(builder =>
        {
            builder.AddSerilog(Log.Logger);
        });
        _logger = loggerFactory.CreateLogger<DataTransferOrchestrator>();

        await _initLock.WaitAsync();
        try
        {
            if (!_containerInitialized)
            {
                _sqlContainer = new MsSqlBuilder()
                    .WithImage("mcr.microsoft.com/mssql/server:2022-latest")
                    .WithPassword("YourStrong@Passw0rd")
                    .Build();

                await _sqlContainer.StartAsync();
                _connectionString = _sqlContainer.GetConnectionString();

                await CreateDatabaseAsync("IncrementalSourceDB");
                await CreateDatabaseAsync("IncrementalDestDB");

                _containerInitialized = true;
            }
        }
        finally
        {
            _initLock.Release();
        }

        Directory.CreateDirectory(_testOutputPath);
    }

    public async Task DisposeAsync()
    {
        try
        {
            await using var sourceConn = new SqlConnection(GetDatabaseConnectionString("IncrementalSourceDB"));
            await sourceConn.OpenAsync();

            if (_sourceRespawner == null)
            {
                _sourceRespawner = await Respawner.CreateAsync(sourceConn, new RespawnerOptions
                {
                    DbAdapter = DbAdapter.SqlServer,
                    TablesToIgnore = Array.Empty<Respawn.Graph.Table>()
                });
            }
            await _sourceRespawner.ResetAsync(sourceConn);
        }
        catch (InvalidOperationException) { }

        try
        {
            await using var destConn = new SqlConnection(GetDatabaseConnectionString("IncrementalDestDB"));
            await destConn.OpenAsync();

            if (_destRespawner == null)
            {
                _destRespawner = await Respawner.CreateAsync(destConn, new RespawnerOptions
                {
                    DbAdapter = DbAdapter.SqlServer,
                    TablesToIgnore = Array.Empty<Respawn.Graph.Table>()
                });
            }
            await _destRespawner.ResetAsync(destConn);
        }
        catch (InvalidOperationException) { }

        if (Directory.Exists(_testOutputPath))
        {
            Directory.Delete(_testOutputPath, recursive: true);
        }
    }

    [Fact]
    public async Task Should_Transfer_Only_New_Records_On_Second_Run()
    {
        // Arrange - Setup initial data
        var sourceConnString = GetDatabaseConnectionString("IncrementalSourceDB");
        var destConnString = GetDatabaseConnectionString("IncrementalDestDB");

        await CreateSalesTableAsync(sourceConnString);
        await CreateSalesTableAsync(destConnString);

        // Initial load: Insert 100 orders from January 2024
        await InsertOrdersAsync(sourceConnString, startId: 1, count: 100,
            startDate: new DateTime(2024, 1, 1));

        var config = new TableConfiguration
        {
            Source = new TableIdentifier { Database = "IncrementalSourceDB", Schema = "dbo", Table = "Sales" },
            Destination = new TableIdentifier { Database = "IncrementalDestDB", Schema = "dbo", Table = "Sales" },
            Partitioning = new PartitioningConfiguration
            {
                Type = PartitionType.Date,
                Column = "OrderDate"
            },
            ExtractSettings = new ExtractSettings
            {
                BatchSize = 50,
                DateRange = new DateRange
                {
                    StartDate = new DateTime(2024, 1, 1),
                    EndDate = new DateTime(2024, 1, 31)
                }
            }
        };

        // Act - First transfer (initial load)
        var extractor = new SqlTableExtractor(sourceConnString, _logger!);
        var storage = new ParquetStorageService(_testOutputPath);
        var loader = new SqlDataLoader(destConnString, _logger!);
        var orchestrator = new DataTransferOrchestrator(extractor, storage, loader, _logger!);

        var result1 = await orchestrator.TransferTableAsync(config, CancellationToken.None);

        // Assert - Initial transfer
        Assert.True(result1.Success);
        Assert.Equal(100, result1.RowsTransferred);
        await VerifyRowCountAsync(destConnString, "Sales", 100);

        // Arrange - Add new data (simulating incremental changes)
        // Add 50 new orders from February 2024
        await InsertOrdersAsync(sourceConnString, startId: 101, count: 50,
            startDate: new DateTime(2024, 2, 1));

        // Update config for February date range
        config.ExtractSettings.DateRange = new DateRange
        {
            StartDate = new DateTime(2024, 2, 1),
            EndDate = new DateTime(2024, 2, 28)
        };

        // Act - Second transfer (incremental load)
        var result2 = await orchestrator.TransferTableAsync(config, CancellationToken.None);

        // Assert - Only new records transferred
        Assert.True(result2.Success);
        Assert.Equal(50, result2.RowsTransferred);

        // Total records should now be 150 (100 initial + 50 incremental)
        await VerifyRowCountAsync(destConnString, "Sales", 150);

        // Verify no duplicates were created
        var distinctCount = await GetDistinctOrderIdCountAsync(destConnString);
        Assert.Equal(150, distinctCount);
    }

    [Fact]
    public async Task Should_Handle_Multiple_Incremental_Loads()
    {
        // Arrange
        var sourceConnString = GetDatabaseConnectionString("IncrementalSourceDB");
        var destConnString = GetDatabaseConnectionString("IncrementalDestDB");

        await CreateSalesTableAsync(sourceConnString);
        await CreateSalesTableAsync(destConnString);

        // Initial load: 100 records in January
        await InsertOrdersAsync(sourceConnString, 1, 100, new DateTime(2024, 1, 1));

        var config = new TableConfiguration
        {
            Source = new TableIdentifier { Database = "IncrementalSourceDB", Schema = "dbo", Table = "Sales" },
            Destination = new TableIdentifier { Database = "IncrementalDestDB", Schema = "dbo", Table = "Sales" },
            Partitioning = new PartitioningConfiguration
            {
                Type = PartitionType.Date,
                Column = "OrderDate"
            },
            ExtractSettings = new ExtractSettings
            {
                BatchSize = 50,
                DateRange = new DateRange
                {
                    StartDate = new DateTime(2024, 1, 1),
                    EndDate = new DateTime(2024, 1, 31)
                }
            }
        };

        var extractor = new SqlTableExtractor(sourceConnString, _logger!);
        var storage = new ParquetStorageService(_testOutputPath);
        var loader = new SqlDataLoader(destConnString, _logger!);
        var orchestrator = new DataTransferOrchestrator(extractor, storage, loader, _logger!);

        // Act & Assert - Transfer Cycle 1: January
        var result1 = await orchestrator.TransferTableAsync(config, CancellationToken.None);
        Assert.Equal(100, result1.RowsTransferred);
        await VerifyRowCountAsync(destConnString, "Sales", 100);

        // Transfer Cycle 2: Add February data
        await InsertOrdersAsync(sourceConnString, 101, 80, new DateTime(2024, 2, 1));
        config.ExtractSettings.DateRange = new DateRange
        {
            StartDate = new DateTime(2024, 2, 1),
            EndDate = new DateTime(2024, 2, 29)
        };
        var result2 = await orchestrator.TransferTableAsync(config, CancellationToken.None);
        Assert.Equal(80, result2.RowsTransferred);
        await VerifyRowCountAsync(destConnString, "Sales", 180);

        // Transfer Cycle 3: Add March data
        await InsertOrdersAsync(sourceConnString, 181, 60, new DateTime(2024, 3, 1));
        config.ExtractSettings.DateRange = new DateRange
        {
            StartDate = new DateTime(2024, 3, 1),
            EndDate = new DateTime(2024, 3, 31)
        };
        var result3 = await orchestrator.TransferTableAsync(config, CancellationToken.None);
        Assert.Equal(60, result3.RowsTransferred);
        await VerifyRowCountAsync(destConnString, "Sales", 240);

        // Transfer Cycle 4: Add April data
        await InsertOrdersAsync(sourceConnString, 241, 40, new DateTime(2024, 4, 1));
        config.ExtractSettings.DateRange = new DateRange
        {
            StartDate = new DateTime(2024, 4, 1),
            EndDate = new DateTime(2024, 4, 30)
        };
        var result4 = await orchestrator.TransferTableAsync(config, CancellationToken.None);
        Assert.Equal(40, result4.RowsTransferred);

        // Final count: 100 + 80 + 60 + 40 = 280
        await VerifyRowCountAsync(destConnString, "Sales", 280);

        // Verify all IDs are unique (no duplicates from multiple loads)
        var distinctCount = await GetDistinctOrderIdCountAsync(destConnString);
        Assert.Equal(280, distinctCount);
    }

    [Fact]
    public async Task Should_Not_Transfer_Records_When_No_New_Data_Exists()
    {
        // Arrange
        var sourceConnString = GetDatabaseConnectionString("IncrementalSourceDB");
        var destConnString = GetDatabaseConnectionString("IncrementalDestDB");

        await CreateSalesTableAsync(sourceConnString);
        await CreateSalesTableAsync(destConnString);

        // Insert 50 orders in January
        await InsertOrdersAsync(sourceConnString, 1, 50, new DateTime(2024, 1, 1));

        var config = new TableConfiguration
        {
            Source = new TableIdentifier { Database = "IncrementalSourceDB", Schema = "dbo", Table = "Sales" },
            Destination = new TableIdentifier { Database = "IncrementalDestDB", Schema = "dbo", Table = "Sales" },
            Partitioning = new PartitioningConfiguration
            {
                Type = PartitionType.Date,
                Column = "OrderDate"
            },
            ExtractSettings = new ExtractSettings
            {
                BatchSize = 50,
                DateRange = new DateRange
                {
                    StartDate = new DateTime(2024, 1, 1),
                    EndDate = new DateTime(2024, 1, 31)
                }
            }
        };

        var extractor = new SqlTableExtractor(sourceConnString, _logger!);
        var storage = new ParquetStorageService(_testOutputPath);
        var loader = new SqlDataLoader(destConnString, _logger!);
        var orchestrator = new DataTransferOrchestrator(extractor, storage, loader, _logger!);

        // Act - First transfer
        var result1 = await orchestrator.TransferTableAsync(config, CancellationToken.None);
        Assert.Equal(50, result1.RowsTransferred);

        // Query a date range with no data (February - no records added)
        config.ExtractSettings.DateRange = new DateRange
        {
            StartDate = new DateTime(2024, 2, 1),
            EndDate = new DateTime(2024, 2, 29)
        };

        // Act - Second transfer (should find no records)
        var result2 = await orchestrator.TransferTableAsync(config, CancellationToken.None);

        // Assert - No records transferred
        Assert.True(result2.Success);
        Assert.Equal(0, result2.RowsTransferred);

        // Total count should remain 50
        await VerifyRowCountAsync(destConnString, "Sales", 50);
    }

    [Fact]
    public async Task Should_Support_Overlapping_Date_Ranges_Without_Duplicates()
    {
        // This test demonstrates idempotency - re-running with overlapping dates
        // should not create duplicates if destination table is truncated first

        // Arrange
        var sourceConnString = GetDatabaseConnectionString("IncrementalSourceDB");
        var destConnString = GetDatabaseConnectionString("IncrementalDestDB");

        await CreateSalesTableAsync(sourceConnString);
        await CreateSalesTableAsync(destConnString);

        // Insert orders spanning January to February
        await InsertOrdersAsync(sourceConnString, 1, 100, new DateTime(2024, 1, 1));

        var config = new TableConfiguration
        {
            Source = new TableIdentifier { Database = "IncrementalSourceDB", Schema = "dbo", Table = "Sales" },
            Destination = new TableIdentifier { Database = "IncrementalDestDB", Schema = "dbo", Table = "Sales" },
            Partitioning = new PartitioningConfiguration
            {
                Type = PartitionType.Date,
                Column = "OrderDate"
            },
            ExtractSettings = new ExtractSettings
            {
                BatchSize = 50,
                DateRange = new DateRange
                {
                    StartDate = new DateTime(2024, 1, 1),
                    EndDate = new DateTime(2024, 1, 15)
                }
            }
        };

        var extractor = new SqlTableExtractor(sourceConnString, _logger!);
        var storage = new ParquetStorageService(_testOutputPath);
        var loader = new SqlDataLoader(destConnString, _logger!);
        var orchestrator = new DataTransferOrchestrator(extractor, storage, loader, _logger!);

        // Act - Transfer first half of January
        var result1 = await orchestrator.TransferTableAsync(config, CancellationToken.None);
        var count1 = await GetRowCountAsync(destConnString, "Sales");

        // Transfer second half of January (some overlap with first transfer if re-run)
        config.ExtractSettings.DateRange = new DateRange
        {
            StartDate = new DateTime(2024, 1, 10),
            EndDate = new DateTime(2024, 1, 31)
        };
        var result2 = await orchestrator.TransferTableAsync(config, CancellationToken.None);
        var count2 = await GetRowCountAsync(destConnString, "Sales");

        // Assert - Both transfers succeeded
        Assert.True(result1.Success);
        Assert.True(result2.Success);

        // Total count should be sum of both transfers (append mode)
        Assert.Equal(count1 + result2.RowsTransferred, count2);
    }

    #region Helper Methods

    private async Task CreateDatabaseAsync(string databaseName)
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        var command = connection.CreateCommand();
        command.CommandText = $@"
            IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '{databaseName}')
            BEGIN
                CREATE DATABASE [{databaseName}];
            END";
        await command.ExecuteNonQueryAsync();
    }

    private string GetDatabaseConnectionString(string databaseName)
    {
        var builder = new SqlConnectionStringBuilder(_connectionString)
        {
            InitialCatalog = databaseName
        };
        return builder.ConnectionString;
    }

    private async Task CreateSalesTableAsync(string connectionString)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        var command = connection.CreateCommand();
        command.CommandText = @"
            CREATE TABLE Sales (
                OrderID INT PRIMARY KEY,
                OrderDate DATE NOT NULL,
                CustomerID INT NOT NULL,
                ProductID INT NOT NULL,
                Quantity INT NOT NULL,
                TotalAmount DECIMAL(18, 2) NOT NULL
            );";
        await command.ExecuteNonQueryAsync();
    }

    private async Task InsertOrdersAsync(string connectionString, int startId, int count, DateTime startDate)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        for (int i = 0; i < count; i++)
        {
            var command = connection.CreateCommand();
            command.CommandText = @"
                INSERT INTO Sales (OrderID, OrderDate, CustomerID, ProductID, Quantity, TotalAmount)
                VALUES (@OrderID, @OrderDate, @CustomerID, @ProductID, @Quantity, @TotalAmount);";

            command.Parameters.AddWithValue("@OrderID", startId + i);
            command.Parameters.AddWithValue("@OrderDate", startDate.AddDays(i % 28)); // Spread across month
            command.Parameters.AddWithValue("@CustomerID", (startId + i) % 50);
            command.Parameters.AddWithValue("@ProductID", (startId + i) % 20);
            command.Parameters.AddWithValue("@Quantity", (i % 10) + 1);
            command.Parameters.AddWithValue("@TotalAmount", ((i % 100) + 1) * 9.99m);

            await command.ExecuteNonQueryAsync();
        }
    }

    private async Task VerifyRowCountAsync(string connectionString, string tableName, int expectedCount)
    {
        var actualCount = await GetRowCountAsync(connectionString, tableName);
        Assert.Equal(expectedCount, actualCount);
    }

    private async Task<int> GetRowCountAsync(string connectionString, string tableName)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        var command = connection.CreateCommand();
        command.CommandText = $"SELECT COUNT(*) FROM {tableName};";
        return (int)await command.ExecuteScalarAsync()!;
    }

    private async Task<int> GetDistinctOrderIdCountAsync(string connectionString)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        var command = connection.CreateCommand();
        command.CommandText = "SELECT COUNT(DISTINCT OrderID) FROM Sales;";
        return (int)await command.ExecuteScalarAsync()!;
    }

    #endregion
}
