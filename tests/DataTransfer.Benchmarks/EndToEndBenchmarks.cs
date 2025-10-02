using BenchmarkDotNet.Attributes;
using DataTransfer.Core.Models;
using DataTransfer.Parquet;
using DataTransfer.Pipeline;
using DataTransfer.SqlServer;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging.Abstractions;

namespace DataTransfer.Benchmarks;

[MemoryDiagnoser]
public class EndToEndBenchmarks
{
    private const string ConnectionString = "Server=(localdb)\\mssqllocaldb;Integrated Security=true;TrustServerCertificate=true;";
    private string _dbConnectionString = null!;
    private string _parquetPath = null!;
    private TableConfiguration? _tableConfig;

    [GlobalSetup]
    public async Task Setup()
    {
        // Create benchmark database
        await using var connection = new SqlConnection(ConnectionString);
        await connection.OpenAsync();

        await using (var cmd = new SqlCommand(@"
            IF EXISTS (SELECT name FROM sys.databases WHERE name = 'BenchmarkDB')
                DROP DATABASE BenchmarkDB;
            CREATE DATABASE BenchmarkDB;", connection))
        {
            await cmd.ExecuteNonQueryAsync();
        }

        _dbConnectionString = ConnectionString + "Database=BenchmarkDB;";
        _parquetPath = Path.Combine(Path.GetTempPath(), "benchmark_parquet", Guid.NewGuid().ToString());
        Directory.CreateDirectory(_parquetPath);

        // Create and populate test table
        await using var dbConnection = new SqlConnection(_dbConnectionString);
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
            Source = new TableIdentifier { Database = "BenchmarkDB", Schema = "dbo", Table = "TestData" },
            Destination = new TableIdentifier { Database = "BenchmarkDB", Schema = "dbo", Table = "TestDataDest" },
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
    public async Task IterationSetup()
    {
        // Truncate destination table before each iteration
        await using var connection = new SqlConnection(_dbConnectionString);
        await connection.OpenAsync();
        await using var cmd = new SqlCommand("TRUNCATE TABLE dbo.TestDataDest", connection);
        await cmd.ExecuteNonQueryAsync();

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

        await orchestrator.TransferTableAsync(_tableConfig!, _dbConnectionString, _dbConnectionString);
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        // Clean up parquet directory
        if (Directory.Exists(_parquetPath))
        {
            Directory.Delete(_parquetPath, true);
        }

        // Drop database
        await using var connection = new SqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var cmd = new SqlCommand(@"
            IF EXISTS (SELECT name FROM sys.databases WHERE name = 'BenchmarkDB')
            BEGIN
                ALTER DATABASE BenchmarkDB SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                DROP DATABASE BenchmarkDB;
            END", connection);
        await cmd.ExecuteNonQueryAsync();
    }
}
