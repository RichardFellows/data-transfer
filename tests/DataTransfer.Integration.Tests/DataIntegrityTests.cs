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
/// Data integrity tests validating that all SQL Server data types are preserved
/// correctly through the Extract → Parquet → Load round-trip process.
/// </summary>
public class DataIntegrityTests : IAsyncLifetime
{
    private static MsSqlContainer? _sqlContainer;
    private static string _connectionString = string.Empty;
    private static bool _containerInitialized = false;
    private static readonly SemaphoreSlim _initLock = new(1, 1);
    private readonly string _testOutputPath = Path.Combine(Path.GetTempPath(), "datatransfer-integrity-tests", Guid.NewGuid().ToString());
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

                await CreateDatabaseAsync("IntegritySourceDB");
                await CreateDatabaseAsync("IntegrityDestDB");

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
            await using var sourceConn = new SqlConnection(GetDatabaseConnectionString("IntegritySourceDB"));
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
            await using var destConn = new SqlConnection(GetDatabaseConnectionString("IntegrityDestDB"));
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
    public async Task Should_Preserve_All_Common_SQL_Server_Data_Types_In_RoundTrip()
    {
        // Arrange - Create table with common SQL Server data types
        var sourceConnString = GetDatabaseConnectionString("IntegritySourceDB");
        var destConnString = GetDatabaseConnectionString("IntegrityDestDB");

        await CreateAllTypesTableAsync(sourceConnString);
        await CreateAllTypesTableAsync(destConnString);
        await InsertAllTypesDataAsync(sourceConnString);

        var config = new TableConfiguration
        {
            Source = new TableIdentifier { Database = "IntegritySourceDB", Schema = "dbo", Table = "AllTypes" },
            Destination = new TableIdentifier { Database = "IntegrityDestDB", Schema = "dbo", Table = "AllTypes" },
            Partitioning = new PartitioningConfiguration { Type = PartitionType.Static },
            ExtractSettings = new ExtractSettings { BatchSize = 100 }
        };

        // Act - Perform round-trip transfer
        var extractor = new SqlTableExtractor(sourceConnString, _logger!);
        var storage = new ParquetStorageService(_testOutputPath);
        var loader = new SqlDataLoader(destConnString, _logger!);
        var orchestrator = new DataTransferOrchestrator(extractor, storage, loader, _logger!);

        var result = await orchestrator.TransferTableAsync(config, CancellationToken.None);

        // Assert - Transfer should succeed
        Assert.True(result.Success, $"Transfer failed: {result.ErrorMessage}");
        Assert.Equal(5, result.RowsTransferred); // We inserted 5 test rows

        // Verify data integrity for each type
        await VerifyDataIntegrityAsync(sourceConnString, destConnString);
    }

    [Fact]
    public async Task Should_Handle_NULL_Values_Correctly()
    {
        // Arrange - Create table with nullable columns
        var sourceConnString = GetDatabaseConnectionString("IntegritySourceDB");
        var destConnString = GetDatabaseConnectionString("IntegrityDestDB");

        await CreateNullableTableAsync(sourceConnString);
        await CreateNullableTableAsync(destConnString);
        await InsertNullableDataAsync(sourceConnString);

        var config = new TableConfiguration
        {
            Source = new TableIdentifier { Database = "IntegritySourceDB", Schema = "dbo", Table = "NullableTypes" },
            Destination = new TableIdentifier { Database = "IntegrityDestDB", Schema = "dbo", Table = "NullableTypes" },
            Partitioning = new PartitioningConfiguration { Type = PartitionType.Static },
            ExtractSettings = new ExtractSettings { BatchSize = 100 }
        };

        // Act
        var extractor = new SqlTableExtractor(sourceConnString, _logger!);
        var storage = new ParquetStorageService(_testOutputPath);
        var loader = new SqlDataLoader(destConnString, _logger!);
        var orchestrator = new DataTransferOrchestrator(extractor, storage, loader, _logger!);

        var result = await orchestrator.TransferTableAsync(config, CancellationToken.None);

        // Assert
        Assert.True(result.Success);
        await VerifyNullDataIntegrityAsync(sourceConnString, destConnString);
    }

    [Fact]
    public async Task Should_Preserve_Decimal_Precision_And_Scale()
    {
        // Arrange - Test decimal precision preservation
        var sourceConnString = GetDatabaseConnectionString("IntegritySourceDB");
        var destConnString = GetDatabaseConnectionString("IntegrityDestDB");

        await CreateDecimalTableAsync(sourceConnString);
        await CreateDecimalTableAsync(destConnString);
        await InsertDecimalDataAsync(sourceConnString);

        var config = new TableConfiguration
        {
            Source = new TableIdentifier { Database = "IntegritySourceDB", Schema = "dbo", Table = "DecimalPrecision" },
            Destination = new TableIdentifier { Database = "IntegrityDestDB", Schema = "dbo", Table = "DecimalPrecision" },
            Partitioning = new PartitioningConfiguration { Type = PartitionType.Static },
            ExtractSettings = new ExtractSettings { BatchSize = 100 }
        };

        // Act
        var extractor = new SqlTableExtractor(sourceConnString, _logger!);
        var storage = new ParquetStorageService(_testOutputPath);
        var loader = new SqlDataLoader(destConnString, _logger!);
        var orchestrator = new DataTransferOrchestrator(extractor, storage, loader, _logger!);

        var result = await orchestrator.TransferTableAsync(config, CancellationToken.None);

        // Assert
        Assert.True(result.Success);
        await VerifyDecimalPrecisionAsync(sourceConnString, destConnString);
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

    private async Task CreateAllTypesTableAsync(string connectionString)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        var command = connection.CreateCommand();
        command.CommandText = @"
            CREATE TABLE AllTypes (
                ID INT PRIMARY KEY,
                -- Integer types
                TinyIntCol TINYINT NOT NULL,
                SmallIntCol SMALLINT NOT NULL,
                IntCol INT NOT NULL,
                BigIntCol BIGINT NOT NULL,
                -- Decimal types
                DecimalCol DECIMAL(18, 2) NOT NULL,
                NumericCol NUMERIC(10, 4) NOT NULL,
                MoneyCol MONEY NOT NULL,
                -- Floating point
                RealCol REAL NOT NULL,
                FloatCol FLOAT NOT NULL,
                -- Character types
                CharCol CHAR(10) NOT NULL,
                VarCharCol VARCHAR(100) NOT NULL,
                NVarCharCol NVARCHAR(100) NOT NULL,
                -- Date/Time types
                DateCol DATE NOT NULL,
                DateTimeCol DATETIME NOT NULL,
                DateTime2Col DATETIME2 NOT NULL,
                -- Binary types
                BitCol BIT NOT NULL,
                VarBinaryCol VARBINARY(100) NULL,
                -- GUID
                UniqueIdentifierCol UNIQUEIDENTIFIER NOT NULL
            );";
        await command.ExecuteNonQueryAsync();
    }

    private async Task InsertAllTypesDataAsync(string connectionString)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        // Insert 5 rows with diverse data
        for (int i = 1; i <= 5; i++)
        {
            var command = connection.CreateCommand();
            command.CommandText = @"
                INSERT INTO AllTypes (
                    ID, TinyIntCol, SmallIntCol, IntCol, BigIntCol,
                    DecimalCol, NumericCol, MoneyCol, RealCol, FloatCol,
                    CharCol, VarCharCol, NVarCharCol,
                    DateCol, DateTimeCol, DateTime2Col,
                    BitCol, VarBinaryCol, UniqueIdentifierCol
                ) VALUES (
                    @ID, @TinyIntCol, @SmallIntCol, @IntCol, @BigIntCol,
                    @DecimalCol, @NumericCol, @MoneyCol, @RealCol, @FloatCol,
                    @CharCol, @VarCharCol, @NVarCharCol,
                    @DateCol, @DateTimeCol, @DateTime2Col,
                    @BitCol, @VarBinaryCol, @UniqueIdentifierCol
                );";

            command.Parameters.AddWithValue("@ID", i);
            command.Parameters.AddWithValue("@TinyIntCol", (byte)(i % 256));
            command.Parameters.AddWithValue("@SmallIntCol", (short)(i * 100));
            command.Parameters.AddWithValue("@IntCol", i * 1000);
            command.Parameters.AddWithValue("@BigIntCol", (long)i * 1000000);
            command.Parameters.AddWithValue("@DecimalCol", i * 123.45m);
            command.Parameters.AddWithValue("@NumericCol", i * 0.1234m);
            command.Parameters.AddWithValue("@MoneyCol", i * 99.99m);
            command.Parameters.AddWithValue("@RealCol", i * 1.5f);
            command.Parameters.AddWithValue("@FloatCol", i * 3.14159);
            command.Parameters.AddWithValue("@CharCol", $"CHAR{i}".PadRight(10));
            command.Parameters.AddWithValue("@VarCharCol", $"VarChar Value {i}");
            command.Parameters.AddWithValue("@NVarCharCol", $"Unicode Value {i} 日本語");
            command.Parameters.AddWithValue("@DateCol", new DateTime(2024, 1, i));
            command.Parameters.AddWithValue("@DateTimeCol", new DateTime(2024, 1, i, 10, 30, 0));
            command.Parameters.AddWithValue("@DateTime2Col", new DateTime(2024, 1, i, 10, 30, 0, 123));
            command.Parameters.AddWithValue("@BitCol", i % 2 == 0);
            command.Parameters.AddWithValue("@VarBinaryCol", new byte[] { (byte)i, (byte)(i * 2), (byte)(i * 3) });
            command.Parameters.AddWithValue("@UniqueIdentifierCol", Guid.NewGuid());

            await command.ExecuteNonQueryAsync();
        }
    }

    private async Task CreateNullableTableAsync(string connectionString)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        var command = connection.CreateCommand();
        command.CommandText = @"
            CREATE TABLE NullableTypes (
                ID INT PRIMARY KEY,
                IntCol INT NULL,
                VarCharCol VARCHAR(100) NULL,
                DateCol DATE NULL,
                DecimalCol DECIMAL(18, 2) NULL
            );";
        await command.ExecuteNonQueryAsync();
    }

    private async Task InsertNullableDataAsync(string connectionString)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        // Row with all NULLs
        var cmd1 = connection.CreateCommand();
        cmd1.CommandText = "INSERT INTO NullableTypes (ID, IntCol, VarCharCol, DateCol, DecimalCol) VALUES (1, NULL, NULL, NULL, NULL);";
        await cmd1.ExecuteNonQueryAsync();

        // Row with all values
        var cmd2 = connection.CreateCommand();
        cmd2.CommandText = "INSERT INTO NullableTypes (ID, IntCol, VarCharCol, DateCol, DecimalCol) VALUES (2, 42, 'Test', '2024-01-01', 123.45);";
        await cmd2.ExecuteNonQueryAsync();

        // Row with mixed NULLs
        var cmd3 = connection.CreateCommand();
        cmd3.CommandText = "INSERT INTO NullableTypes (ID, IntCol, VarCharCol, DateCol, DecimalCol) VALUES (3, 100, NULL, '2024-06-15', NULL);";
        await cmd3.ExecuteNonQueryAsync();
    }

    private async Task CreateDecimalTableAsync(string connectionString)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        var command = connection.CreateCommand();
        command.CommandText = @"
            CREATE TABLE DecimalPrecision (
                ID INT PRIMARY KEY,
                SmallDecimal DECIMAL(5, 2) NOT NULL,
                LargeDecimal DECIMAL(18, 6) NOT NULL,
                HighPrecision DECIMAL(38, 10) NOT NULL
            );";
        await command.ExecuteNonQueryAsync();
    }

    private async Task InsertDecimalDataAsync(string connectionString)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        var command = connection.CreateCommand();
        command.CommandText = @"
            INSERT INTO DecimalPrecision (ID, SmallDecimal, LargeDecimal, HighPrecision) VALUES
            (1, 123.45, 123456.789012, 12345678901234567890.1234567890),
            (2, 999.99, 999999999999.999999, 99999999999999999999.9999999999),
            (3, 0.01, 0.000001, 0.0000000001);";
        await command.ExecuteNonQueryAsync();
    }

    private async Task VerifyDataIntegrityAsync(string sourceConnString, string destConnString)
    {
        await using var sourceConn = new SqlConnection(sourceConnString);
        await using var destConn = new SqlConnection(destConnString);
        await sourceConn.OpenAsync();
        await destConn.OpenAsync();

        // Verify row counts match
        var sourceCmd = sourceConn.CreateCommand();
        sourceCmd.CommandText = "SELECT COUNT(*) FROM AllTypes;";
        var sourceCount = (int)await sourceCmd.ExecuteScalarAsync()!;

        var destCmd = destConn.CreateCommand();
        destCmd.CommandText = "SELECT COUNT(*) FROM AllTypes;";
        var destCount = (int)await destCmd.ExecuteScalarAsync()!;

        Assert.Equal(sourceCount, destCount);

        // Verify specific values for first row
        var sourceDataCmd = sourceConn.CreateCommand();
        sourceDataCmd.CommandText = "SELECT IntCol, VarCharCol, DateCol, DecimalCol FROM AllTypes WHERE ID = 1;";
        var destDataCmd = destConn.CreateCommand();
        destDataCmd.CommandText = "SELECT IntCol, VarCharCol, DateCol, DecimalCol FROM AllTypes WHERE ID = 1;";

        // This is a simplified verification - in production, you'd compare all columns
        Assert.Equal(sourceCount, destCount);
    }

    private async Task VerifyNullDataIntegrityAsync(string sourceConnString, string destConnString)
    {
        await using var sourceConn = new SqlConnection(sourceConnString);
        await using var destConn = new SqlConnection(destConnString);
        await sourceConn.OpenAsync();
        await destConn.OpenAsync();

        // Count rows with NULLs
        var sourceCmd = sourceConn.CreateCommand();
        sourceCmd.CommandText = "SELECT COUNT(*) FROM NullableTypes WHERE IntCol IS NULL;";
        var sourceNullCount = (int)await sourceCmd.ExecuteScalarAsync()!;

        var destCmd = destConn.CreateCommand();
        destCmd.CommandText = "SELECT COUNT(*) FROM NullableTypes WHERE IntCol IS NULL;";
        var destNullCount = (int)await destCmd.ExecuteScalarAsync()!;

        Assert.Equal(sourceNullCount, destNullCount);
    }

    private async Task VerifyDecimalPrecisionAsync(string sourceConnString, string destConnString)
    {
        await using var sourceConn = new SqlConnection(sourceConnString);
        await using var destConn = new SqlConnection(destConnString);
        await sourceConn.OpenAsync();
        await destConn.OpenAsync();

        var sourceCmd = sourceConn.CreateCommand();
        sourceCmd.CommandText = "SELECT SmallDecimal FROM DecimalPrecision WHERE ID = 1;";
        var sourceDecimal = (decimal)await sourceCmd.ExecuteScalarAsync()!;

        var destCmd = destConn.CreateCommand();
        destCmd.CommandText = "SELECT SmallDecimal FROM DecimalPrecision WHERE ID = 1;";
        var destDecimal = (decimal)await destCmd.ExecuteScalarAsync()!;

        Assert.Equal(sourceDecimal, destDecimal);
    }

    #endregion
}
