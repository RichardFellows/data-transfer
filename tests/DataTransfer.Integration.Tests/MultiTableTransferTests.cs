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
/// Demonstrates multi-table transfer with different partition strategies.
/// This test showcases real-world usage where multiple tables with different
/// partitioning needs are transferred in a single operation.
/// </summary>
public class MultiTableTransferTests : IAsyncLifetime
{
    private static MsSqlContainer? _sqlContainer;
    private static string _connectionString = string.Empty;
    private static bool _containerInitialized = false;
    private static readonly SemaphoreSlim _initLock = new(1, 1);
    private readonly string _testOutputPath = Path.Combine(Path.GetTempPath(), "datatransfer-multitable-tests", Guid.NewGuid().ToString());
    private ILogger<DataTransferOrchestrator>? _logger;
    private Respawner? _sourceRespawner;
    private Respawner? _destRespawner;

    public async Task InitializeAsync()
    {
        // Set up Serilog for test logging
        Log.Logger = new LoggerConfiguration()
            .MinimumLevel.Debug()
            .WriteTo.Console()
            .CreateLogger();

        var loggerFactory = LoggerFactory.Create(builder =>
        {
            builder.AddSerilog(Log.Logger);
        });
        _logger = loggerFactory.CreateLogger<DataTransferOrchestrator>();

        // Initialize shared container once for all tests
        await _initLock.WaitAsync();
        try
        {
            if (!_containerInitialized)
            {
                // Start SQL Server container (shared across all tests)
                _sqlContainer = new MsSqlBuilder()
                    .WithImage("mcr.microsoft.com/mssql/server:2022-latest")
                    .WithPassword("YourStrong@Passw0rd")
                    .Build();

                await _sqlContainer.StartAsync();
                _connectionString = _sqlContainer.GetConnectionString();

                // Create test databases once
                await CreateDatabaseAsync("SourceDB");
                await CreateDatabaseAsync("DestDB");

                _containerInitialized = true;
            }
        }
        finally
        {
            _initLock.Release();
        }

        // Create output directory for this test
        Directory.CreateDirectory(_testOutputPath);
    }

    public async Task DisposeAsync()
    {
        // Reset databases after each test to ensure clean state
        try
        {
            await using var sourceConn = new SqlConnection(GetDatabaseConnectionString("SourceDB"));
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
        catch (InvalidOperationException)
        {
            // No tables to reset - this is OK
        }

        try
        {
            await using var destConn = new SqlConnection(GetDatabaseConnectionString("DestDB"));
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
        catch (InvalidOperationException)
        {
            // No tables to reset - this is OK
        }

        // Clean up test output directory
        if (Directory.Exists(_testOutputPath))
        {
            Directory.Delete(_testOutputPath, recursive: true);
        }
    }

    private static async Task CreateDatabaseAsync(string databaseName)
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        using var command = new SqlCommand($"CREATE DATABASE [{databaseName}]", connection);
        await command.ExecuteNonQueryAsync();
    }

    private static string GetDatabaseConnectionString(string databaseName)
    {
        var builder = new SqlConnectionStringBuilder(_connectionString)
        {
            InitialCatalog = databaseName
        };
        return builder.ConnectionString;
    }

    /// <summary>
    /// FEATURE DEMONSTRATION: Multi-table transfer with all 4 partition strategies
    ///
    /// This test demonstrates:
    /// 1. Date-partitioned table (Orders) - for time-series data
    /// 2. IntDate-partitioned table (Sales) - for legacy systems with int dates
    /// 3. SCD2 table (Customers) - for slowly changing dimensions with history tracking
    /// 4. Static table (Products) - for reference/lookup tables
    ///
    /// USE CASE: Real-world scenario where a data warehouse ingests multiple tables
    /// with different partitioning requirements in a single batch operation.
    /// </summary>
    [Fact]
    public async Task Should_Transfer_Multiple_Tables_With_Different_Partition_Strategies()
    {
        // Arrange
        var sourceConnStr = GetDatabaseConnectionString("SourceDB");
        var destConnStr = GetDatabaseConnectionString("DestDB");

        // === Table 1: Orders (Date Partitioned) ===
        await CreateTableAsync(sourceConnStr, @"
            CREATE TABLE [dbo].[Orders] (
                OrderId INT PRIMARY KEY,
                OrderDate DATE NOT NULL,
                CustomerName NVARCHAR(100),
                Amount DECIMAL(18,2)
            )");

        await InsertDataAsync(sourceConnStr, @"
            INSERT INTO [dbo].[Orders] (OrderId, OrderDate, CustomerName, Amount) VALUES
            (1, '2024-01-15', 'Customer A', 100.50),
            (2, '2024-01-16', 'Customer B', 250.75),
            (3, '2024-01-17', 'Customer C', 500.00),
            (4, '2024-02-01', 'Customer D', 150.25),
            (5, '2024-02-15', 'Customer E', 300.00)");

        await CreateTableAsync(destConnStr, @"
            CREATE TABLE [dbo].[Orders] (
                OrderId INT PRIMARY KEY,
                OrderDate DATE NOT NULL,
                CustomerName NVARCHAR(100),
                Amount DECIMAL(18,2)
            )");

        // === Table 2: Sales (IntDate Partitioned) ===
        await CreateTableAsync(sourceConnStr, @"
            CREATE TABLE [dbo].[Sales] (
                SaleId INT PRIMARY KEY,
                SaleDate INT NOT NULL,
                ProductName NVARCHAR(100),
                Quantity INT
            )");

        await InsertDataAsync(sourceConnStr, @"
            INSERT INTO [dbo].[Sales] (SaleId, SaleDate, ProductName, Quantity) VALUES
            (1, 20240115, 'Product A', 10),
            (2, 20240116, 'Product B', 25),
            (3, 20240201, 'Product C', 15),
            (4, 20240215, 'Product D', 30)");

        await CreateTableAsync(destConnStr, @"
            CREATE TABLE [dbo].[Sales] (
                SaleId INT PRIMARY KEY,
                SaleDate INT NOT NULL,
                ProductName NVARCHAR(100),
                Quantity INT
            )");

        // === Table 3: Customers (SCD2 - Slowly Changing Dimension) ===
        await CreateTableAsync(sourceConnStr, @"
            CREATE TABLE [dbo].[Customers] (
                CustomerId INT,
                CustomerName NVARCHAR(100),
                EffectiveDate DATE NOT NULL,
                ExpirationDate DATE NULL,
                IsCurrent BIT,
                PRIMARY KEY (CustomerId, EffectiveDate)
            )");

        await InsertDataAsync(sourceConnStr, @"
            INSERT INTO [dbo].[Customers] (CustomerId, CustomerName, EffectiveDate, ExpirationDate, IsCurrent) VALUES
            (1, 'Customer A v1', '2024-01-01', '2024-02-01', 0),
            (1, 'Customer A v2', '2024-02-01', NULL, 1),
            (2, 'Customer B v1', '2024-01-15', NULL, 1)");

        await CreateTableAsync(destConnStr, @"
            CREATE TABLE [dbo].[Customers] (
                CustomerId INT,
                CustomerName NVARCHAR(100),
                EffectiveDate DATE NOT NULL,
                ExpirationDate DATE NULL,
                IsCurrent BIT,
                PRIMARY KEY (CustomerId, EffectiveDate)
            )");

        // === Table 4: Products (Static - No Partitioning) ===
        await CreateTableAsync(sourceConnStr, @"
            CREATE TABLE [dbo].[Products] (
                ProductId INT PRIMARY KEY,
                ProductName NVARCHAR(100),
                Category NVARCHAR(50),
                Price DECIMAL(18,2)
            )");

        await InsertDataAsync(sourceConnStr, @"
            INSERT INTO [dbo].[Products] (ProductId, ProductName, Category, Price) VALUES
            (1, 'Laptop', 'Electronics', 999.99),
            (2, 'Mouse', 'Electronics', 25.50),
            (3, 'Desk', 'Furniture', 350.00)");

        await CreateTableAsync(destConnStr, @"
            CREATE TABLE [dbo].[Products] (
                ProductId INT PRIMARY KEY,
                ProductName NVARCHAR(100),
                Category NVARCHAR(50),
                Price DECIMAL(18,2)
            )");

        // Configure all tables
        var tableConfigs = new List<TableConfiguration>
        {
            // 1. Date-partitioned Orders
            new TableConfiguration
            {
                Source = new TableIdentifier { Database = "SourceDB", Schema = "dbo", Table = "Orders" },
                Destination = new TableIdentifier { Database = "DestDB", Schema = "dbo", Table = "Orders" },
                Partitioning = new PartitioningConfiguration
                {
                    Type = PartitionType.Date,
                    Column = "OrderDate"
                },
                ExtractSettings = new ExtractSettings
                {
                    DateRange = new DateRange
                    {
                        StartDate = new DateTime(2024, 1, 1),
                        EndDate = new DateTime(2024, 12, 31)
                    }
                }
            },
            // 2. IntDate-partitioned Sales
            new TableConfiguration
            {
                Source = new TableIdentifier { Database = "SourceDB", Schema = "dbo", Table = "Sales" },
                Destination = new TableIdentifier { Database = "DestDB", Schema = "dbo", Table = "Sales" },
                Partitioning = new PartitioningConfiguration
                {
                    Type = PartitionType.IntDate,
                    Column = "SaleDate",
                    Format = "yyyyMMdd"
                },
                ExtractSettings = new ExtractSettings
                {
                    DateRange = new DateRange
                    {
                        StartDate = new DateTime(2024, 1, 1),
                        EndDate = new DateTime(2024, 12, 31)
                    }
                }
            },
            // 3. SCD2 Customers
            new TableConfiguration
            {
                Source = new TableIdentifier { Database = "SourceDB", Schema = "dbo", Table = "Customers" },
                Destination = new TableIdentifier { Database = "DestDB", Schema = "dbo", Table = "Customers" },
                Partitioning = new PartitioningConfiguration
                {
                    Type = PartitionType.Scd2,
                    Column = "EffectiveDate",
                    Format = "ExpirationDate"
                },
                ExtractSettings = new ExtractSettings
                {
                    DateRange = new DateRange
                    {
                        StartDate = new DateTime(2024, 1, 1),
                        EndDate = new DateTime(2024, 12, 31)
                    }
                }
            },
            // 4. Static Products
            new TableConfiguration
            {
                Source = new TableIdentifier { Database = "SourceDB", Schema = "dbo", Table = "Products" },
                Destination = new TableIdentifier { Database = "DestDB", Schema = "dbo", Table = "Products" },
                Partitioning = new PartitioningConfiguration
                {
                    Type = PartitionType.Static
                },
                ExtractSettings = new ExtractSettings
                {
                    DateRange = new DateRange
                    {
                        StartDate = DateTime.MinValue,
                        EndDate = DateTime.MaxValue
                    }
                }
            }
        };

        var queryBuilder = new SqlQueryBuilder();
        var extractor = new SqlTableExtractor(queryBuilder);
        var storage = new ParquetStorage(_testOutputPath);
        var loader = new SqlDataLoader(queryBuilder);
        var orchestrator = new DataTransferOrchestrator(extractor, storage, loader, _logger!);

        // Act - Transfer all tables
        var results = new List<TransferResult>();
        foreach (var tableConfig in tableConfigs)
        {
            var result = await orchestrator.TransferTableAsync(tableConfig, sourceConnStr, destConnStr);
            results.Add(result);
        }

        // Assert - All transfers should succeed
        Assert.All(results, r => Assert.True(r.Success, $"Transfer failed: {r.ErrorMessage}"));

        // Verify Orders (Date partitioned) - should have 5 rows
        var ordersData = await ReadTableDataAsync(destConnStr, "SELECT * FROM [dbo].[Orders] ORDER BY OrderId");
        Assert.Equal(5, ordersData.Count);
        Assert.Equal(5, results[0].RowsLoaded);
        _logger!.LogInformation("✓ Orders (Date): {Count} rows transferred", ordersData.Count);

        // Verify Sales (IntDate partitioned) - should have 4 rows
        var salesData = await ReadTableDataAsync(destConnStr, "SELECT * FROM [dbo].[Sales] ORDER BY SaleId");
        Assert.Equal(4, salesData.Count);
        Assert.Equal(4, results[1].RowsLoaded);
        _logger.LogInformation("✓ Sales (IntDate): {Count} rows transferred", salesData.Count);

        // Verify Customers (SCD2) - should have 2 current records
        var customersData = await ReadTableDataAsync(destConnStr,
            "SELECT * FROM [dbo].[Customers] ORDER BY CustomerId, EffectiveDate");
        Assert.Equal(2, customersData.Count); // Only current versions
        Assert.Equal(2, results[2].RowsLoaded);
        _logger.LogInformation("✓ Customers (SCD2): {Count} current versions transferred", customersData.Count);

        // Verify Products (Static) - should have 3 rows
        var productsData = await ReadTableDataAsync(destConnStr, "SELECT * FROM [dbo].[Products] ORDER BY ProductId");
        Assert.Equal(3, productsData.Count);
        Assert.Equal(3, results[3].RowsLoaded);
        _logger.LogInformation("✓ Products (Static): {Count} rows transferred", productsData.Count);

        // Verify total row counts
        var totalExtracted = results.Sum(r => r.RowsExtracted);
        var totalLoaded = results.Sum(r => r.RowsLoaded);
        Assert.Equal(14, totalExtracted); // 5 + 4 + 2 + 3
        Assert.Equal(14, totalLoaded);

        _logger.LogInformation("════════════════════════════════════════");
        _logger.LogInformation("Multi-Table Transfer Complete");
        _logger.LogInformation("  Tables: {Count}", tableConfigs.Count);
        _logger.LogInformation("  Total Rows: {Total}", totalLoaded);
        _logger.LogInformation("  Strategies Used: Date, IntDate, SCD2, Static");
        _logger.LogInformation("════════════════════════════════════════");
    }

    private async Task CreateTableAsync(string connectionString, string createTableSql)
    {
        using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();
        using var command = new SqlCommand(createTableSql, connection);
        await command.ExecuteNonQueryAsync();
    }

    private async Task InsertDataAsync(string connectionString, string insertSql)
    {
        using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();
        using var command = new SqlCommand(insertSql, connection);
        await command.ExecuteNonQueryAsync();
    }

    private async Task<List<Dictionary<string, object>>> ReadTableDataAsync(string connectionString, string querySql)
    {
        var results = new List<Dictionary<string, object>>();

        using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();
        using var command = new SqlCommand(querySql, connection);
        using var reader = await command.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            var row = new Dictionary<string, object>();
            for (int i = 0; i < reader.FieldCount; i++)
            {
                row[reader.GetName(i)] = reader.IsDBNull(i) ? null! : reader.GetValue(i);
            }
            results.Add(row);
        }

        return results;
    }
}
