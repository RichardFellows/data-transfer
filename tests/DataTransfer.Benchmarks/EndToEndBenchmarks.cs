using BenchmarkDotNet.Attributes;
using DataTransfer.Core.Models;
using DataTransfer.Parquet;
using DataTransfer.Pipeline;
using DataTransfer.SqlServer;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging.Abstractions;
using Testcontainers.MsSql;

namespace DataTransfer.Benchmarks;

[MemoryDiagnoser]
public class EndToEndBenchmarks
{
    private MsSqlContainer? _sqlContainer;
    private string _connectionString = null!;
    private string _parquetPath = null!;
    private TableConfiguration? _tableConfig;

    [GlobalSetup]
    public async Task Setup()
    {
        // Start SQL Server container
        _sqlContainer = new MsSqlBuilder()
            .WithImage("mcr.microsoft.com/mssql/server:2022-latest")
            .WithPassword("YourStrong@Passw0rd")
            .Build();

        await _sqlContainer.StartAsync();
        _connectionString = _sqlContainer.GetConnectionString();

        _parquetPath = Path.Combine(Path.GetTempPath(), "benchmark_parquet", Guid.NewGuid().ToString());
        Directory.CreateDirectory(_parquetPath);

        // Create and populate test table
        await using var dbConnection = new SqlConnection(_connectionString);
        await dbConnection.OpenAsync();

        await using (var cmd = new SqlCommand(@"
            CREATE TABLE dbo.TestData (
                Id INT PRIMARY KEY,
                Name NVARCHAR(100),
                Amount DECIMAL(18,2),
                CreatedDate DATE
            )", dbConnection))
        {
            await cmd.ExecuteNonQueryAsync();
        }

        // Insert 10K rows
        for (int i = 0; i < 100; i++)
        {
            var values = new List<string>();
            for (int j = 0; j < 100; j++)
            {
                int id = i * 100 + j;
                values.Add($"({id}, 'Name{id}', {id * 1.5:F2}, '2024-01-15')");
            }

            await using var cmd = new SqlCommand($@"
                INSERT INTO dbo.TestData (Id, Name, Amount, CreatedDate)
                VALUES {string.Join(",", values)}", dbConnection);
            await cmd.ExecuteNonQueryAsync();
        }

        // Create destination table
        await using (var cmd = new SqlCommand(@"
            CREATE TABLE dbo.TestDataDest (
                Id INT PRIMARY KEY,
                Name NVARCHAR(100),
                Amount DECIMAL(18,2),
                CreatedDate DATE
            )", dbConnection))
        {
            await cmd.ExecuteNonQueryAsync();
        }

        _tableConfig = new TableConfiguration
        {
            Source = new TableIdentifier { Database = "master", Schema = "dbo", Table = "TestData" },
            Destination = new TableIdentifier { Database = "master", Schema = "dbo", Table = "TestDataDest" },
            Partitioning = new PartitioningConfiguration { Type = PartitionType.Static },
            ExtractSettings = new ExtractSettings
            {
                DateRange = new DateRange
                {
                    StartDate = new DateTime(2024, 1, 1),
                    EndDate = new DateTime(2024, 12, 31)
                }
            }
        };
    }

    [IterationSetup]
    public void IterationSetup()
    {
        // Truncate destination table before each iteration
        using var connection = new SqlConnection(_connectionString);
        connection.Open();
        using var cmd = new SqlCommand("TRUNCATE TABLE dbo.TestDataDest", connection);
        cmd.ExecuteNonQuery();

        // Clean parquet directory
        if (Directory.Exists(_parquetPath))
        {
            foreach (var file in Directory.GetFiles(_parquetPath, "*.parquet", SearchOption.AllDirectories))
            {
                File.Delete(file);
            }
        }
    }

    [Benchmark]
    public async Task Transfer_10K_Rows()
    {
        var queryBuilder = new SqlQueryBuilder();
        var extractor = new SqlTableExtractor(queryBuilder);
        var storage = new ParquetStorage(_parquetPath);
        var loader = new SqlDataLoader(queryBuilder);
        var orchestrator = new DataTransferOrchestrator(extractor, storage, loader, NullLogger<DataTransferOrchestrator>.Instance);

        await orchestrator.TransferTableAsync(_tableConfig!, _connectionString, _connectionString);
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        // Clean up parquet directory
        if (Directory.Exists(_parquetPath))
        {
            Directory.Delete(_parquetPath, true);
        }

        // Stop and dispose SQL Server container
        if (_sqlContainer != null)
        {
            await _sqlContainer.StopAsync();
            await _sqlContainer.DisposeAsync();
        }
    }
}
