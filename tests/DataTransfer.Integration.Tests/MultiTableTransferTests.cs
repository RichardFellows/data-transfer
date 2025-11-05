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
/// Demonstration tests for multi-table transfers with mixed partition strategies.
/// These tests showcase real-world scenarios where multiple tables with different
/// partitioning approaches are transferred in a single operation.
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

                // Create test databases
                await CreateDatabaseAsync("MultiSourceDB");
                await CreateDatabaseAsync("MultiDestDB");

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
        // Reset databases after each test
        try
        {
            await using var sourceConn = new SqlConnection(GetDatabaseConnectionString("MultiSourceDB"));
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
        catch (InvalidOperationException) { /* No tables to reset */ }

        try
        {
            await using var destConn = new SqlConnection(GetDatabaseConnectionString("MultiDestDB"));
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
        catch (InvalidOperationException) { /* No tables to reset */ }

        // Clean up test output directory
        if (Directory.Exists(_testOutputPath))
        {
            Directory.Delete(_testOutputPath, recursive: true);
        }
    }

    [Fact]
    public async Task Should_Transfer_Multiple_Tables_With_Different_Partition_Strategies()
    {
        // Arrange - Create 4 tables with different partition strategies
        var sourceConnString = GetDatabaseConnectionString("MultiSourceDB");
        var destConnString = GetDatabaseConnectionString("MultiDestDB");

        // Table 1: Date-partitioned Orders (fact table)
        await CreateOrdersTableAsync(sourceConnString);
        await CreateOrdersTableAsync(destConnString);
        await InsertDatePartitionedOrdersAsync(sourceConnString, 100);

        // Table 2: IntDate-partitioned Transactions (data warehouse style)
        await CreateTransactionsTableAsync(sourceConnString);
        await CreateTransactionsTableAsync(destConnString);
        await InsertIntDateTransactionsAsync(sourceConnString, 75);

        // Table 3: SCD2 Customer dimension
        await CreateCustomersSCD2TableAsync(sourceConnString);
        await CreateCustomersSCD2TableAsync(destConnString);
        await InsertSCD2CustomersAsync(sourceConnString, 50);

        // Table 4: Static reference table (Countries)
        await CreateCountriesTableAsync(sourceConnString);
        await CreateCountriesTableAsync(destConnString);
        await InsertStaticCountriesAsync(sourceConnString, 25);

        // Create configurations for all 4 tables
        var configurations = new List<TableConfiguration>
        {
            // Date-partitioned Orders
            new TableConfiguration
            {
                Source = new TableIdentifier { Database = "MultiSourceDB", Schema = "dbo", Table = "Orders" },
                Destination = new TableIdentifier { Database = "MultiDestDB", Schema = "dbo", Table = "Orders" },
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
                        EndDate = new DateTime(2024, 12, 31)
                    }
                }
            },
            // IntDate-partitioned Transactions
            new TableConfiguration
            {
                Source = new TableIdentifier { Database = "MultiSourceDB", Schema = "dbo", Table = "Transactions" },
                Destination = new TableIdentifier { Database = "MultiDestDB", Schema = "dbo", Table = "Transactions" },
                Partitioning = new PartitioningConfiguration
                {
                    Type = PartitionType.IntDate,
                    Column = "TransactionDateKey",
                    Format = "yyyyMMdd"
                },
                ExtractSettings = new ExtractSettings
                {
                    BatchSize = 50,
                    DateRange = new DateRange
                    {
                        StartDate = new DateTime(2024, 1, 1),
                        EndDate = new DateTime(2024, 12, 31)
                    }
                }
            },
            // SCD2 Customers
            new TableConfiguration
            {
                Source = new TableIdentifier { Database = "MultiSourceDB", Schema = "dbo", Table = "Customers" },
                Destination = new TableIdentifier { Database = "MultiDestDB", Schema = "dbo", Table = "Customers" },
                Partitioning = new PartitioningConfiguration
                {
                    Type = PartitionType.Scd2,
                    ScdEffectiveDateColumn = "EffectiveDate",
                    ScdExpirationDateColumn = "ExpirationDate"
                },
                ExtractSettings = new ExtractSettings
                {
                    BatchSize = 50,
                    DateRange = new DateRange
                    {
                        StartDate = new DateTime(2024, 1, 1),
                        EndDate = new DateTime(2024, 12, 31)
                    }
                }
            },
            // Static Countries
            new TableConfiguration
            {
                Source = new TableIdentifier { Database = "MultiSourceDB", Schema = "dbo", Table = "Countries" },
                Destination = new TableIdentifier { Database = "MultiDestDB", Schema = "dbo", Table = "Countries" },
                Partitioning = new PartitioningConfiguration
                {
                    Type = PartitionType.Static
                },
                ExtractSettings = new ExtractSettings
                {
                    BatchSize = 25
                }
            }
        };

        // Act - Transfer all tables
        var results = new List<TransferResult>();
        foreach (var config in configurations)
        {
            var extractor = new SqlTableExtractor(sourceConnString, _logger!);
            var storage = new ParquetStorageService(_testOutputPath);
            var loader = new SqlDataLoader(destConnString, _logger!);
            var orchestrator = new DataTransferOrchestrator(extractor, storage, loader, _logger!);

            var result = await orchestrator.TransferTableAsync(config, CancellationToken.None);
            results.Add(result);
        }

        // Assert - All transfers should succeed
        Assert.All(results, result => Assert.True(result.Success, $"Transfer failed: {result.ErrorMessage}"));

        // Verify row counts for each table
        await VerifyRowCountAsync(destConnString, "Orders", 100);
        await VerifyRowCountAsync(destConnString, "Transactions", 75);
        await VerifyRowCountAsync(destConnString, "Customers", 50);
        await VerifyRowCountAsync(destConnString, "Countries", 25);

        // Verify total rows transferred
        var totalTransferred = results.Sum(r => r.RowsTransferred);
        Assert.Equal(250, totalTransferred); // 100 + 75 + 50 + 25

        // Verify partitioning was applied correctly
        var orderParquetFiles = Directory.GetFiles(_testOutputPath, "*.parquet", SearchOption.AllDirectories)
            .Where(f => f.Contains("Orders")).ToList();
        var transactionParquetFiles = Directory.GetFiles(_testOutputPath, "*.parquet", SearchOption.AllDirectories)
            .Where(f => f.Contains("Transactions")).ToList();

        Assert.NotEmpty(orderParquetFiles); // Should have date-partitioned files
        Assert.NotEmpty(transactionParquetFiles); // Should have intdate-partitioned files
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

    private async Task CreateOrdersTableAsync(string connectionString)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        var command = connection.CreateCommand();
        command.CommandText = @"
            CREATE TABLE Orders (
                OrderID INT PRIMARY KEY,
                OrderDate DATE NOT NULL,
                CustomerID INT NOT NULL,
                TotalAmount DECIMAL(18, 2) NOT NULL
            );";
        await command.ExecuteNonQueryAsync();
    }

    private async Task InsertDatePartitionedOrdersAsync(string connectionString, int count)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        for (int i = 1; i <= count; i++)
        {
            var command = connection.CreateCommand();
            command.CommandText = @"
                INSERT INTO Orders (OrderID, OrderDate, CustomerID, TotalAmount)
                VALUES (@OrderID, @OrderDate, @CustomerID, @TotalAmount);";
            command.Parameters.AddWithValue("@OrderID", i);
            command.Parameters.AddWithValue("@OrderDate", new DateTime(2024, 1, 1).AddDays(i % 365));
            command.Parameters.AddWithValue("@CustomerID", i % 50);
            command.Parameters.AddWithValue("@TotalAmount", i * 10.5m);
            await command.ExecuteNonQueryAsync();
        }
    }

    private async Task CreateTransactionsTableAsync(string connectionString)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        var command = connection.CreateCommand();
        command.CommandText = @"
            CREATE TABLE Transactions (
                TransactionID INT PRIMARY KEY,
                TransactionDateKey INT NOT NULL,
                Amount DECIMAL(18, 2) NOT NULL,
                AccountID INT NOT NULL
            );";
        await command.ExecuteNonQueryAsync();
    }

    private async Task InsertIntDateTransactionsAsync(string connectionString, int count)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        for (int i = 1; i <= count; i++)
        {
            var command = connection.CreateCommand();
            var date = new DateTime(2024, 1, 1).AddDays(i % 365);
            var dateKey = date.Year * 10000 + date.Month * 100 + date.Day; // YYYYMMDD

            command.CommandText = @"
                INSERT INTO Transactions (TransactionID, TransactionDateKey, Amount, AccountID)
                VALUES (@TransactionID, @TransactionDateKey, @Amount, @AccountID);";
            command.Parameters.AddWithValue("@TransactionID", i);
            command.Parameters.AddWithValue("@TransactionDateKey", dateKey);
            command.Parameters.AddWithValue("@Amount", i * 5.75m);
            command.Parameters.AddWithValue("@AccountID", i % 30);
            await command.ExecuteNonQueryAsync();
        }
    }

    private async Task CreateCustomersSCD2TableAsync(string connectionString)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        var command = connection.CreateCommand();
        command.CommandText = @"
            CREATE TABLE Customers (
                CustomerID INT NOT NULL,
                CustomerName NVARCHAR(100) NOT NULL,
                Status NVARCHAR(20) NOT NULL,
                EffectiveDate DATE NOT NULL,
                ExpirationDate DATE NULL,
                PRIMARY KEY (CustomerID, EffectiveDate)
            );";
        await command.ExecuteNonQueryAsync();
    }

    private async Task InsertSCD2CustomersAsync(string connectionString, int count)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        for (int i = 1; i <= count; i++)
        {
            var command = connection.CreateCommand();
            command.CommandText = @"
                INSERT INTO Customers (CustomerID, CustomerName, Status, EffectiveDate, ExpirationDate)
                VALUES (@CustomerID, @CustomerName, @Status, @EffectiveDate, @ExpirationDate);";
            command.Parameters.AddWithValue("@CustomerID", i);
            command.Parameters.AddWithValue("@CustomerName", $"Customer {i}");
            command.Parameters.AddWithValue("@Status", i % 2 == 0 ? "Active" : "Inactive");
            command.Parameters.AddWithValue("@EffectiveDate", new DateTime(2024, 1, 1));
            command.Parameters.AddWithValue("@ExpirationDate", DBNull.Value); // Current record
            await command.ExecuteNonQueryAsync();
        }
    }

    private async Task CreateCountriesTableAsync(string connectionString)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        var command = connection.CreateCommand();
        command.CommandText = @"
            CREATE TABLE Countries (
                CountryID INT PRIMARY KEY,
                CountryCode NVARCHAR(3) NOT NULL,
                CountryName NVARCHAR(100) NOT NULL
            );";
        await command.ExecuteNonQueryAsync();
    }

    private async Task InsertStaticCountriesAsync(string connectionString, int count)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        var countries = new[] { "USA", "CAN", "MEX", "GBR", "FRA", "DEU", "ITA", "ESP", "JPN", "CHN",
                                "AUS", "BRA", "IND", "RUS", "ZAF", "KOR", "NLD", "SWE", "NOR", "DNK",
                                "FIN", "POL", "BEL", "AUT", "CHE" };

        for (int i = 0; i < Math.Min(count, countries.Length); i++)
        {
            var command = connection.CreateCommand();
            command.CommandText = @"
                INSERT INTO Countries (CountryID, CountryCode, CountryName)
                VALUES (@CountryID, @CountryCode, @CountryName);";
            command.Parameters.AddWithValue("@CountryID", i + 1);
            command.Parameters.AddWithValue("@CountryCode", countries[i]);
            command.Parameters.AddWithValue("@CountryName", $"Country {countries[i]}");
            await command.ExecuteNonQueryAsync();
        }
    }

    private async Task VerifyRowCountAsync(string connectionString, string tableName, int expectedCount)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        var command = connection.CreateCommand();
        command.CommandText = $"SELECT COUNT(*) FROM {tableName};";
        var actualCount = (int)await command.ExecuteScalarAsync()!;

        Assert.Equal(expectedCount, actualCount);
    }

    #endregion
}
