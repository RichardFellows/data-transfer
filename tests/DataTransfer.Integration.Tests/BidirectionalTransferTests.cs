using DataTransfer.Core.Interfaces;
using DataTransfer.Core.Models;
using DataTransfer.Iceberg.Catalog;
using DataTransfer.Iceberg.ChangeDetection;
using DataTransfer.Iceberg.Integration;
using DataTransfer.Iceberg.Readers;
using DataTransfer.Iceberg.Watermarks;
using DataTransfer.Iceberg.Writers;
using DataTransfer.Parquet;
using DataTransfer.Pipeline;
using DataTransfer.SqlServer;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Testcontainers.MsSql;
using Xunit;

namespace DataTransfer.Integration.Tests;

/// <summary>
/// Integration tests for bi-directional transfers (SQL ↔ Parquet)
/// </summary>
public class BidirectionalTransferTests : IAsyncLifetime
{
    private MsSqlContainer? _sqlContainer;
    private string? _connectionString;
    private UnifiedTransferOrchestrator? _orchestrator;
    private readonly string _parquetBasePath;

    public BidirectionalTransferTests()
    {
        _parquetBasePath = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        Directory.CreateDirectory(_parquetBasePath);
    }

    public async Task InitializeAsync()
    {
        _sqlContainer = new MsSqlBuilder()
            .WithImage("mcr.microsoft.com/mssql/server:2022-latest")
            .Build();

        await _sqlContainer.StartAsync();
        _connectionString = _sqlContainer.GetConnectionString();

        // Set up orchestrator
        var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
        var sqlQueryBuilder = new SqlQueryBuilder();
        var sqlExtractor = new SqlTableExtractor(sqlQueryBuilder);
        var sqlLoader = new SqlDataLoader(sqlQueryBuilder);
        var parquetStorage = new ParquetStorage(_parquetBasePath);
        var parquetExtractor = new ParquetExtractor(_parquetBasePath);
        var parquetWriter = new ParquetWriter(parquetStorage);

        // Iceberg components
        var icebergWarehousePath = Path.Combine(_parquetBasePath, "iceberg-warehouse");
        var catalog = new FilesystemCatalog(icebergWarehousePath, loggerFactory.CreateLogger<FilesystemCatalog>());
        var icebergExporter = new SqlServerToIcebergExporter(catalog, loggerFactory.CreateLogger<SqlServerToIcebergExporter>());
        var watermarkStore = new FileWatermarkStore(Path.Combine(icebergWarehousePath, ".watermarks"));
        var changeDetection = new TimestampChangeDetection("UpdatedAt"); // Default watermark column
        var icebergAppender = new IcebergAppender(catalog, loggerFactory.CreateLogger<IcebergAppender>());
        var icebergReader = new IcebergReader(catalog, loggerFactory.CreateLogger<IcebergReader>());
        var sqlImporter = new SqlServerImporter(loggerFactory.CreateLogger<SqlServerImporter>());
        var incrementalSync = new IncrementalSyncCoordinator(
            changeDetection,
            icebergAppender,
            icebergReader,
            sqlImporter,
            watermarkStore,
            loggerFactory.CreateLogger<IncrementalSyncCoordinator>());

        // Configuration
        var configBuilder = new ConfigurationBuilder();
        configBuilder.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Iceberg:WarehousePath"] = icebergWarehousePath
        });
        var configuration = configBuilder.Build();

        _orchestrator = new UnifiedTransferOrchestrator(
            sqlExtractor,
            parquetExtractor,
            sqlLoader,
            parquetWriter,
            icebergExporter,
            incrementalSync,
            catalog,
            configuration,
            loggerFactory.CreateLogger<UnifiedTransferOrchestrator>());
    }

    public async Task DisposeAsync()
    {
        if (_sqlContainer != null)
        {
            await _sqlContainer.DisposeAsync();
        }

        if (Directory.Exists(_parquetBasePath))
        {
            Directory.Delete(_parquetBasePath, true);
        }
    }

    [Fact]
    public async Task SqlToParquet_Should_Export_Data_Successfully()
    {
        // Arrange
        await CreateTestTableAsync("ExportTest", 100);

        var config = new TransferConfiguration
        {
            TransferType = TransferType.SqlToParquet,
            Source = new SourceConfiguration
            {
                Type = SourceType.SqlServer,
                ConnectionString = _connectionString!,
                Table = new TableIdentifier { Database = "master", Schema = "dbo", Table = "ExportTest" }
            },
            Destination = new DestinationConfiguration
            {
                Type = DestinationType.Parquet,
                ParquetPath = "export_test.parquet"
            }
        };

        // Act
        var result = await _orchestrator!.ExecuteTransferAsync(config);

        // Assert
        Assert.True(result.Success, $"Transfer failed: {result.ErrorMessage}");
        Assert.Equal(100, result.RowsExtracted);
        Assert.Equal(100, result.RowsLoaded);
        Assert.NotNull(result.ParquetFilePath);

        // Verify Parquet file exists
        var parquetFiles = Directory.GetFiles(_parquetBasePath, "*.parquet", SearchOption.AllDirectories);
        Assert.NotEmpty(parquetFiles);
    }

    [Fact]
    public async Task ParquetToSql_Should_Import_Data_Successfully()
    {
        // Arrange - First export data to Parquet
        await CreateTestTableAsync("SourceTable", 50);
        var exportConfig = new TransferConfiguration
        {
            TransferType = TransferType.SqlToParquet,
            Source = new SourceConfiguration
            {
                Type = SourceType.SqlServer,
                ConnectionString = _connectionString!,
                Table = new TableIdentifier { Database = "master", Schema = "dbo", Table = "SourceTable" }
            },
            Destination = new DestinationConfiguration
            {
                Type = DestinationType.Parquet,
                ParquetPath = "import_test.parquet"
            }
        };
        var exportResult = await _orchestrator!.ExecuteTransferAsync(exportConfig);
        Assert.True(exportResult.Success);

        // Create destination table
        await CreateTestTableAsync("DestinationTable", 0);

        // Get the actual Parquet file path
        var parquetFiles = Directory.GetFiles(_parquetBasePath, "import_test.parquet", SearchOption.AllDirectories);
        Assert.NotEmpty(parquetFiles);
        var parquetFilePath = Path.GetRelativePath(_parquetBasePath, parquetFiles[0]);

        // Act - Import from Parquet
        var importConfig = new TransferConfiguration
        {
            TransferType = TransferType.ParquetToSql,
            Source = new SourceConfiguration
            {
                Type = SourceType.Parquet,
                ParquetPath = parquetFilePath
            },
            Destination = new DestinationConfiguration
            {
                Type = DestinationType.SqlServer,
                ConnectionString = _connectionString!,
                Table = new TableIdentifier { Database = "master", Schema = "dbo", Table = "DestinationTable" }
            }
        };
        var importResult = await _orchestrator!.ExecuteTransferAsync(importConfig);

        // Assert
        Assert.True(importResult.Success, $"Import failed: {importResult.ErrorMessage}");
        Assert.Equal(50, importResult.RowsExtracted);
        Assert.Equal(50, importResult.RowsLoaded);

        // Verify data in destination
        var destCount = await GetRowCountAsync("DestinationTable");
        Assert.Equal(50, destCount);
    }

    [Fact]
    public async Task RoundTrip_SqlToParquetToSql_Should_Maintain_Data_Integrity()
    {
        // Arrange
        await CreateTestTableWithDataAsync("RoundTripSource", 75);
        await CreateTestTableAsync("RoundTripDest", 0);

        // Act 1: Export to Parquet
        var exportConfig = new TransferConfiguration
        {
            TransferType = TransferType.SqlToParquet,
            Source = new SourceConfiguration
            {
                Type = SourceType.SqlServer,
                ConnectionString = _connectionString!,
                Table = new TableIdentifier { Database = "master", Schema = "dbo", Table = "RoundTripSource" }
            },
            Destination = new DestinationConfiguration
            {
                Type = DestinationType.Parquet,
                ParquetPath = "roundtrip.parquet"
            }
        };
        var exportResult = await _orchestrator!.ExecuteTransferAsync(exportConfig);
        Assert.True(exportResult.Success);

        // Get Parquet file path
        var parquetFiles = Directory.GetFiles(_parquetBasePath, "roundtrip.parquet", SearchOption.AllDirectories);
        var parquetFilePath = Path.GetRelativePath(_parquetBasePath, parquetFiles[0]);

        // Act 2: Import from Parquet
        var importConfig = new TransferConfiguration
        {
            TransferType = TransferType.ParquetToSql,
            Source = new SourceConfiguration
            {
                Type = SourceType.Parquet,
                ParquetPath = parquetFilePath
            },
            Destination = new DestinationConfiguration
            {
                Type = DestinationType.SqlServer,
                ConnectionString = _connectionString!,
                Table = new TableIdentifier { Database = "master", Schema = "dbo", Table = "RoundTripDest" }
            }
        };
        var importResult = await _orchestrator!.ExecuteTransferAsync(importConfig);

        // Assert
        Assert.True(exportResult.Success);
        Assert.True(importResult.Success);
        Assert.Equal(75, exportResult.RowsExtracted);
        Assert.Equal(75, importResult.RowsLoaded);

        // Verify data integrity
        var sourceData = await GetTableDataAsync("RoundTripSource");
        var destData = await GetTableDataAsync("RoundTripDest");
        Assert.Equal(sourceData.Count, destData.Count);
    }

    private async Task CreateTestTableAsync(string tableName, int rowCount)
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        var createTableSql = $@"
            CREATE TABLE {tableName} (
                Id INT PRIMARY KEY,
                Name NVARCHAR(100),
                Value DECIMAL(18,2),
                CreatedDate DATETIME
            )";

        await using var createCmd = new SqlCommand(createTableSql, connection);
        await createCmd.ExecuteNonQueryAsync();

        for (int i = 1; i <= rowCount; i++)
        {
            var insertSql = $@"
                INSERT INTO {tableName} (Id, Name, Value, CreatedDate)
                VALUES ({i}, 'Item{i}', {i * 10.5}, GETDATE())";

            await using var insertCmd = new SqlCommand(insertSql, connection);
            await insertCmd.ExecuteNonQueryAsync();
        }
    }

    private async Task CreateTestTableWithDataAsync(string tableName, int rowCount)
    {
        await CreateTestTableAsync(tableName, rowCount);
    }

    private async Task<int> GetRowCountAsync(string tableName)
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        await using var cmd = new SqlCommand($"SELECT COUNT(*) FROM {tableName}", connection);
        return (int)await cmd.ExecuteScalarAsync()!;
    }

    private async Task<List<Dictionary<string, object>>> GetTableDataAsync(string tableName)
    {
        var data = new List<Dictionary<string, object>>();

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        await using var cmd = new SqlCommand($"SELECT * FROM {tableName} ORDER BY Id", connection);
        await using var reader = await cmd.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            var row = new Dictionary<string, object>();
            for (int i = 0; i < reader.FieldCount; i++)
            {
                row[reader.GetName(i)] = reader.GetValue(i);
            }
            data.Add(row);
        }

        return data;
    }
}
