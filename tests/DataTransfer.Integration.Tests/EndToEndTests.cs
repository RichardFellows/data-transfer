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

public class EndToEndTests : IAsyncLifetime
{
    private static MsSqlContainer? _sqlContainer;
    private static string _connectionString = string.Empty;
    private static bool _containerInitialized = false;
    private static readonly SemaphoreSlim _initLock = new(1, 1);
    private readonly string _testOutputPath = Path.Combine(Path.GetTempPath(), "datatransfer-integration-tests", Guid.NewGuid().ToString());
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
        // Create Respawn checkpoints on-demand (after tables are created by tests)
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
            // No tables to reset - this is OK for first test
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
            // No tables to reset - this is OK for first test
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

    [Fact]
    public async Task FullPipeline_Should_Transfer_DatePartitioned_Table()
    {
        // Arrange
        var sourceConnStr = GetDatabaseConnectionString("SourceDB");
        var destConnStr = GetDatabaseConnectionString("DestDB");

        // Create source table
        await CreateTableAsync(sourceConnStr, @"
            CREATE TABLE [dbo].[Orders] (
                OrderId INT PRIMARY KEY,
                OrderDate DATE NOT NULL,
                CustomerName NVARCHAR(100),
                Amount DECIMAL(18,2)
            )");

        // Insert test data
        await InsertDataAsync(sourceConnStr, @"
            INSERT INTO [dbo].[Orders] (OrderId, OrderDate, CustomerName, Amount) VALUES
            (1, '2024-01-15', 'Customer A', 100.50),
            (2, '2024-01-16', 'Customer B', 250.75),
            (3, '2024-01-17', 'Customer C', 500.00),
            (4, '2024-02-01', 'Customer D', 150.25),
            (5, '2024-02-15', 'Customer E', 300.00)");

        // Create destination table
        await CreateTableAsync(destConnStr, @"
            CREATE TABLE [dbo].[Orders] (
                OrderId INT PRIMARY KEY,
                OrderDate DATE NOT NULL,
                CustomerName NVARCHAR(100),
                Amount DECIMAL(18,2)
            )");

        var tableConfig = new TableConfiguration
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
        };

        var queryBuilder = new SqlQueryBuilder();
        var extractor = new SqlTableExtractor(queryBuilder);
        var storage = new ParquetStorage(_testOutputPath);
        var loader = new SqlDataLoader(queryBuilder);
        var orchestrator = new DataTransferOrchestrator(extractor, storage, loader, _logger!);

        // Act
        var result = await orchestrator.TransferTableAsync(tableConfig, sourceConnStr, destConnStr);

        // Assert
        Assert.True(result.Success, $"Transfer failed: {result.ErrorMessage}");
        Assert.Equal(5, result.RowsExtracted);
        Assert.Equal(5, result.RowsLoaded);

        // Verify data in destination
        var destData = await ReadTableDataAsync(destConnStr, "SELECT * FROM [dbo].[Orders] ORDER BY OrderId");
        Assert.Equal(5, destData.Count);
        Assert.Equal(1, destData[0]["OrderId"]);
        Assert.Equal("Customer A", destData[0]["CustomerName"]);
        Assert.Equal(100.50m, destData[0]["Amount"]);
    }

    [Fact]
    public async Task FullPipeline_Should_Transfer_IntDatePartitioned_Table()
    {
        // Arrange
        var sourceConnStr = GetDatabaseConnectionString("SourceDB");
        var destConnStr = GetDatabaseConnectionString("DestDB");

        // Create source table
        await CreateTableAsync(sourceConnStr, @"
            CREATE TABLE [dbo].[Sales] (
                SaleId INT PRIMARY KEY,
                SaleDate INT NOT NULL,
                ProductName NVARCHAR(100),
                Quantity INT
            )");

        // Insert test data with int dates (YYYYMMDD format)
        await InsertDataAsync(sourceConnStr, @"
            INSERT INTO [dbo].[Sales] (SaleId, SaleDate, ProductName, Quantity) VALUES
            (1, 20240115, 'Product A', 10),
            (2, 20240116, 'Product B', 25),
            (3, 20240201, 'Product C', 15),
            (4, 20240215, 'Product D', 30)");

        // Create destination table
        await CreateTableAsync(destConnStr, @"
            CREATE TABLE [dbo].[Sales] (
                SaleId INT PRIMARY KEY,
                SaleDate INT NOT NULL,
                ProductName NVARCHAR(100),
                Quantity INT
            )");

        var tableConfig = new TableConfiguration
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
        };

        var queryBuilder = new SqlQueryBuilder();
        var extractor = new SqlTableExtractor(queryBuilder);
        var storage = new ParquetStorage(_testOutputPath);
        var loader = new SqlDataLoader(queryBuilder);
        var orchestrator = new DataTransferOrchestrator(extractor, storage, loader, _logger!);

        // Act
        var result = await orchestrator.TransferTableAsync(tableConfig, sourceConnStr, destConnStr);

        // Assert
        Assert.True(result.Success, $"Transfer failed: {result.ErrorMessage}");
        Assert.Equal(4, result.RowsExtracted);
        Assert.Equal(4, result.RowsLoaded);

        // Verify data in destination
        var destData = await ReadTableDataAsync(destConnStr, "SELECT * FROM [dbo].[Sales] ORDER BY SaleId");
        Assert.Equal(4, destData.Count);
        Assert.Equal(1, destData[0]["SaleId"]);
        Assert.Equal(20240115, destData[0]["SaleDate"]);
        Assert.Equal("Product A", destData[0]["ProductName"]);
    }

    [Fact]
    public async Task FullPipeline_Should_Transfer_Scd2Table()
    {
        // Arrange
        var sourceConnStr = GetDatabaseConnectionString("SourceDB");
        var destConnStr = GetDatabaseConnectionString("DestDB");

        // Create source table
        await CreateTableAsync(sourceConnStr, @"
            CREATE TABLE [dbo].[Customers] (
                CustomerId INT,
                CustomerName NVARCHAR(100),
                EffectiveDate DATE NOT NULL,
                ExpirationDate DATE NULL,
                IsCurrent BIT,
                PRIMARY KEY (CustomerId, EffectiveDate)
            )");

        // Insert test data with SCD2 structure
        await InsertDataAsync(sourceConnStr, @"
            INSERT INTO [dbo].[Customers] (CustomerId, CustomerName, EffectiveDate, ExpirationDate, IsCurrent) VALUES
            (1, 'Customer A v1', '2024-01-01', '2024-02-01', 0),
            (1, 'Customer A v2', '2024-02-01', NULL, 1),
            (2, 'Customer B v1', '2024-01-15', NULL, 1),
            (3, 'Customer C v1', '2024-01-01', '2024-03-01', 0),
            (3, 'Customer C v2', '2024-03-01', NULL, 1)");

        // Create destination table
        await CreateTableAsync(destConnStr, @"
            CREATE TABLE [dbo].[Customers] (
                CustomerId INT,
                CustomerName NVARCHAR(100),
                EffectiveDate DATE NOT NULL,
                ExpirationDate DATE NULL,
                IsCurrent BIT,
                PRIMARY KEY (CustomerId, EffectiveDate)
            )");

        var tableConfig = new TableConfiguration
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
        };

        var queryBuilder = new SqlQueryBuilder();
        var extractor = new SqlTableExtractor(queryBuilder);
        var storage = new ParquetStorage(_testOutputPath);
        var loader = new SqlDataLoader(queryBuilder);
        var orchestrator = new DataTransferOrchestrator(extractor, storage, loader, _logger!);

        // Act
        var result = await orchestrator.TransferTableAsync(tableConfig, sourceConnStr, destConnStr);

        // Assert
        Assert.True(result.Success, $"Transfer failed: {result.ErrorMessage}");
        // SCD2 strategy filters for records "current" at endDate (ExpirationDate > endDate OR NULL)
        // So only records with NULL ExpirationDate are extracted: Customer 1 v2, Customer 2 v1, Customer 3 v2
        Assert.Equal(3, result.RowsExtracted);
        Assert.Equal(3, result.RowsLoaded);

        // Verify data in destination
        var destData = await ReadTableDataAsync(destConnStr,
            "SELECT * FROM [dbo].[Customers] ORDER BY CustomerId, EffectiveDate");
        Assert.Equal(3, destData.Count);

        // Verify we have the current versions only
        var customer1Records = destData.Where(r => (int)r["CustomerId"] == 1).ToList();
        Assert.Single(customer1Records);
        Assert.Equal("Customer A v2", customer1Records[0]["CustomerName"]);

        var customer2Records = destData.Where(r => (int)r["CustomerId"] == 2).ToList();
        Assert.Single(customer2Records);

        var customer3Records = destData.Where(r => (int)r["CustomerId"] == 3).ToList();
        Assert.Single(customer3Records);
        Assert.Equal("Customer C v2", customer3Records[0]["CustomerName"]);
    }

    [Fact]
    public async Task FullPipeline_Should_Transfer_StaticTable()
    {
        // Arrange
        var sourceConnStr = GetDatabaseConnectionString("SourceDB");
        var destConnStr = GetDatabaseConnectionString("DestDB");

        // Create source table
        await CreateTableAsync(sourceConnStr, @"
            CREATE TABLE [dbo].[Products] (
                ProductId INT PRIMARY KEY,
                ProductName NVARCHAR(100),
                Category NVARCHAR(50),
                Price DECIMAL(18,2)
            )");

        // Insert test data
        await InsertDataAsync(sourceConnStr, @"
            INSERT INTO [dbo].[Products] (ProductId, ProductName, Category, Price) VALUES
            (1, 'Laptop', 'Electronics', 999.99),
            (2, 'Mouse', 'Electronics', 25.50),
            (3, 'Desk', 'Furniture', 350.00),
            (4, 'Chair', 'Furniture', 150.00),
            (5, 'Monitor', 'Electronics', 299.99),
            (6, 'Keyboard', 'Electronics', 75.00)");

        // Create destination table
        await CreateTableAsync(destConnStr, @"
            CREATE TABLE [dbo].[Products] (
                ProductId INT PRIMARY KEY,
                ProductName NVARCHAR(100),
                Category NVARCHAR(50),
                Price DECIMAL(18,2)
            )");

        var tableConfig = new TableConfiguration
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
        };

        var queryBuilder = new SqlQueryBuilder();
        var extractor = new SqlTableExtractor(queryBuilder);
        var storage = new ParquetStorage(_testOutputPath);
        var loader = new SqlDataLoader(queryBuilder);
        var orchestrator = new DataTransferOrchestrator(extractor, storage, loader, _logger!);

        // Act
        var result = await orchestrator.TransferTableAsync(tableConfig, sourceConnStr, destConnStr);

        // Assert
        Assert.True(result.Success, $"Transfer failed: {result.ErrorMessage}");
        Assert.Equal(6, result.RowsExtracted);
        Assert.Equal(6, result.RowsLoaded);

        // Verify data in destination
        var destData = await ReadTableDataAsync(destConnStr, "SELECT * FROM [dbo].[Products] ORDER BY ProductId");
        Assert.Equal(6, destData.Count);
        Assert.Equal(1, destData[0]["ProductId"]);
        Assert.Equal("Laptop", destData[0]["ProductName"]);
        Assert.Equal("Electronics", destData[0]["Category"]);
        Assert.Equal(999.99m, destData[0]["Price"]);

        // Verify all categories
        var electronics = destData.Where(r => r["Category"].ToString() == "Electronics").ToList();
        Assert.Equal(4, electronics.Count);
    }

    [Fact]
    public async Task FullPipeline_Should_Handle_EmptyTable()
    {
        // Arrange
        var sourceConnStr = GetDatabaseConnectionString("SourceDB");
        var destConnStr = GetDatabaseConnectionString("DestDB");

        // Create source table (empty)
        await CreateTableAsync(sourceConnStr, @"
            CREATE TABLE [dbo].[EmptyTable] (
                Id INT PRIMARY KEY,
                Name NVARCHAR(100)
            )");

        // Create destination table
        await CreateTableAsync(destConnStr, @"
            CREATE TABLE [dbo].[EmptyTable] (
                Id INT PRIMARY KEY,
                Name NVARCHAR(100)
            )");

        var tableConfig = new TableConfiguration
        {
            Source = new TableIdentifier { Database = "SourceDB", Schema = "dbo", Table = "EmptyTable" },
            Destination = new TableIdentifier { Database = "DestDB", Schema = "dbo", Table = "EmptyTable" },
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
        };

        var queryBuilder = new SqlQueryBuilder();
        var extractor = new SqlTableExtractor(queryBuilder);
        var storage = new ParquetStorage(_testOutputPath);
        var loader = new SqlDataLoader(queryBuilder);
        var orchestrator = new DataTransferOrchestrator(extractor, storage, loader, _logger!);

        // Act
        var result = await orchestrator.TransferTableAsync(tableConfig, sourceConnStr, destConnStr);

        // Assert
        Assert.True(result.Success, $"Transfer failed: {result.ErrorMessage}");
        Assert.Equal(0, result.RowsExtracted);
        Assert.Equal(0, result.RowsLoaded);

        // Verify destination is empty
        var destData = await ReadTableDataAsync(destConnStr, "SELECT * FROM [dbo].[EmptyTable]");
        Assert.Empty(destData);
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
