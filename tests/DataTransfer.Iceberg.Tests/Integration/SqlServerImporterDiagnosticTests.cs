using DataTransfer.Iceberg.Catalog;
using DataTransfer.Iceberg.Integration;
using DataTransfer.Iceberg.MergeStrategies;
using DataTransfer.Iceberg.Readers;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using Xunit.Abstractions;

namespace DataTransfer.Iceberg.Tests.Integration;

/// <summary>
/// Diagnostic tests to investigate SqlServerImporter issues
/// </summary>
public class SqlServerImporterDiagnosticTests : IDisposable
{
    private readonly string _connectionString;
    private readonly string _testDatabase;
    private readonly string _warehousePath;
    private readonly ITestOutputHelper _output;

    public SqlServerImporterDiagnosticTests(ITestOutputHelper output)
    {
        _output = output;
        _connectionString = "Server=localhost;User Id=sa;Password=IcebergDemo@2024;TrustServerCertificate=true;";
        _testDatabase = $"ImporterDiagnosticDb_{Guid.NewGuid():N}";
        _warehousePath = Path.Combine(Path.GetTempPath(), "importer-diagnostic", Guid.NewGuid().ToString());

        Directory.CreateDirectory(_warehousePath);
        EnsureDatabase();
    }

    private void EnsureDatabase()
    {
        using var connection = new SqlConnection(_connectionString);
        connection.Open();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = $@"
            IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '{_testDatabase}')
                CREATE DATABASE [{_testDatabase}];";
        cmd.ExecuteNonQuery();
    }

    [Fact]
    public async Task Diagnose_Import_From_Iceberg_Table()
    {
        // Arrange - Create target table
        var connString = $"{_connectionString}Database={_testDatabase};";
        await using var connection = new SqlConnection(connString);
        await connection.OpenAsync();

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
            IF OBJECT_ID('DiagnosticTable', 'U') IS NOT NULL DROP TABLE DiagnosticTable;
            CREATE TABLE DiagnosticTable (
                ProductId INT PRIMARY KEY,
                Name NVARCHAR(200),
                Price DECIMAL(18,2),
                ModifiedDate DATETIME2
            )";
        await cmd.ExecuteNonQueryAsync();

        // Create test Iceberg data
        var catalog = new FilesystemCatalog(_warehousePath, NullLogger<FilesystemCatalog>.Instance);
        var writer = new IcebergTableWriter(catalog, NullLogger<IcebergTableWriter>.Instance);

        var schema = new DataTransfer.Core.Models.Iceberg.IcebergSchema
        {
            SchemaId = 0,
            Type = "struct",
            Fields = new List<DataTransfer.Core.Models.Iceberg.IcebergField>
            {
                new() { Id = 1, Name = "ProductId", Required = true, Type = "int" },
                new() { Id = 2, Name = "Name", Required = false, Type = "string" },
                new() { Id = 3, Name = "Price", Required = false, Type = "double" },
                new() { Id = 4, Name = "ModifiedDate", Required = false, Type = "timestamp" }
            }
        };

        var testData = new List<Dictionary<string, object>>
        {
            new() { ["ProductId"] = 1, ["Name"] = "Test1", ["Price"] = 10.99, ["ModifiedDate"] = DateTime.UtcNow },
            new() { ["ProductId"] = 2, ["Name"] = "Test2", ["Price"] = 20.99, ["ModifiedDate"] = DateTime.UtcNow },
            new() { ["ProductId"] = 3, ["Name"] = "Test3", ["Price"] = 30.99, ["ModifiedDate"] = DateTime.UtcNow }
        };

        var writeResult = await writer.WriteTableAsync("diagnostic_test", schema, testData);
        _output.WriteLine($"Write result: Success={writeResult.Success}, RecordCount={writeResult.RecordCount}");

        // Act - Read from Iceberg and import
        var reader = new IcebergReader(catalog, NullLogger<IcebergReader>.Instance);
        var icebergData = reader.ReadTableAsync("diagnostic_test");

        // Count what we're reading
        int readCount = 0;
        await foreach (var row in icebergData)
        {
            readCount++;
            _output.WriteLine($"Read row {readCount}: ProductId={row["ProductId"]}, Name={row["Name"]}");
        }
        _output.WriteLine($"Total rows read from Iceberg: {readCount}");

        // Now try import
        var importer = new SqlServerImporter(NullLogger<SqlServerImporter>.Instance);
        var mergeStrategy = new UpsertMergeStrategy("ProductId");

        // Re-read for import (can't reuse consumed enumerable)
        var icebergDataForImport = reader.ReadTableAsync("diagnostic_test");
        var importResult = await importer.ImportAsync(icebergDataForImport, connString, "DiagnosticTable", mergeStrategy);

        _output.WriteLine($"Import result: Success={importResult.Success}, RowsImported={importResult.RowsImported}");
        _output.WriteLine($"Import result: RowsInserted={importResult.RowsInserted}, RowsUpdated={importResult.RowsUpdated}");

        // Verify
        cmd.CommandText = "SELECT COUNT(*) FROM DiagnosticTable";
        var targetCount = (int)await cmd.ExecuteScalarAsync()!;
        _output.WriteLine($"Target table has {targetCount} rows");

        Assert.Equal(3, importResult.RowsImported);
        Assert.Equal(3, targetCount);
    }

    public void Dispose()
    {
        try
        {
            using var connection = new SqlConnection(_connectionString);
            connection.Open();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = $@"
                IF EXISTS (SELECT * FROM sys.databases WHERE name = '{_testDatabase}')
                    DROP DATABASE [{_testDatabase}];";
            cmd.ExecuteNonQuery();

            if (Directory.Exists(_warehousePath))
                Directory.Delete(_warehousePath, true);
        }
        catch
        {
            // Ignore cleanup errors
        }
    }
}
