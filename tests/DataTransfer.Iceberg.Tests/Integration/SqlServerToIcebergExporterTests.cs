using DataTransfer.Iceberg.Catalog;
using DataTransfer.Iceberg.Integration;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace DataTransfer.Iceberg.Tests.Integration;

/// <summary>
/// Integration tests for SQL Server to Iceberg export
/// NOTE: These tests require SQL Server LocalDB (Windows only) or a SQL Server test container
/// Tests will be skipped if SQL Server is not available
/// </summary>
public class SqlServerToIcebergExporterTests : IDisposable
{
    private readonly string _tempWarehouse;
    private readonly FilesystemCatalog _catalog;
    private readonly string _connectionString;
    private readonly string _testDatabaseName;
    private readonly bool _sqlServerAvailable;

    public SqlServerToIcebergExporterTests()
    {
        _tempWarehouse = Path.Combine(Path.GetTempPath(), $"iceberg-sqlserver-tests-{Guid.NewGuid()}");
        _catalog = new FilesystemCatalog(_tempWarehouse, NullLogger<FilesystemCatalog>.Instance);

        // Use SQL Server LocalDB for testing (Windows only)
        _testDatabaseName = $"IcebergTest_{Guid.NewGuid():N}";
        _connectionString = $"Server=(localdb)\\mssqllocaldb;Integrated Security=true;TrustServerCertificate=true";

        // Check if SQL Server is available
        _sqlServerAvailable = IsSqlServerAvailable();
    }

    private bool IsSqlServerAvailable()
    {
        try
        {
            using var connection = new SqlConnection(_connectionString);
            connection.Open();
            return true;
        }
        catch (PlatformNotSupportedException)
        {
            return false;
        }
        catch
        {
            return false;
        }
    }


    [Fact]
    public async Task Should_Export_Simple_Table_From_SqlServer_To_Iceberg()
    {
        if (!_sqlServerAvailable)
        {
            // Test is effectively skipped - passes without running
            return;
        }

        // Arrange
        await CreateTestDatabase();
        await CreateTestTable("SimpleTable", @"
            CREATE TABLE SimpleTable (
                id INT NOT NULL PRIMARY KEY,
                name NVARCHAR(100),
                created_at DATETIME2
            )
        ");
        await InsertTestData("SimpleTable", @"
            INSERT INTO SimpleTable (id, name, created_at) VALUES
            (1, 'Alice', '2024-01-01T10:00:00'),
            (2, 'Bob', '2024-01-02T11:00:00'),
            (3, 'Charlie', '2024-01-03T12:00:00')
        ");

        var exporter = new SqlServerToIcebergExporter(_catalog, NullLogger<SqlServerToIcebergExporter>.Instance);
        var dbConnectionString = $"{_connectionString};Database={_testDatabaseName}";

        // Act
        var result = await exporter.ExportTableAsync(
            dbConnectionString,
            "SimpleTable",
            "simple_iceberg_table"
        );

        // Assert
        Assert.True(result.Success, $"Export failed: {result.ErrorMessage}");
        Assert.Equal(3, result.RecordCount);
        Assert.True(_catalog.TableExists("simple_iceberg_table"));

        // Verify metadata
        var metadata = _catalog.LoadTable("simple_iceberg_table");
        Assert.NotNull(metadata);
        Assert.Equal(3, metadata.Schemas[0].Fields.Count);
        Assert.Contains(metadata.Schemas[0].Fields, f => f.Name == "id" && f.Type.ToString() == "int");
        Assert.Contains(metadata.Schemas[0].Fields, f => f.Name == "name" && f.Type.ToString() == "string");
        Assert.Contains(metadata.Schemas[0].Fields, f => f.Name == "created_at" && f.Type.ToString() == "timestamp");
    }

    [Fact]
    public async Task Should_Export_Table_With_Various_SqlServer_Types()
    {
        if (!_sqlServerAvailable) return;

        // Arrange
        await CreateTestDatabase();
        await CreateTestTable("TypesTable", @"
            CREATE TABLE TypesTable (
                id BIGINT NOT NULL PRIMARY KEY,
                flag BIT,
                small_num SMALLINT,
                big_num BIGINT,
                price DECIMAL(18, 2),
                description VARCHAR(255),
                unique_id UNIQUEIDENTIFIER,
                created_date DATE,
                updated_timestamp DATETIMEOFFSET
            )
        ");
        await InsertTestData("TypesTable", @"
            INSERT INTO TypesTable VALUES
            (1, 1, 100, 999999999, 123.45, 'Test description', NEWID(), '2024-01-01', '2024-01-01T10:00:00+00:00')
        ");

        var exporter = new SqlServerToIcebergExporter(_catalog, NullLogger<SqlServerToIcebergExporter>.Instance);
        var dbConnectionString = $"{_connectionString};Database={_testDatabaseName}";

        // Act
        var result = await exporter.ExportTableAsync(
            dbConnectionString,
            "TypesTable",
            "types_iceberg_table"
        );

        // Assert
        Assert.True(result.Success, $"Export failed: {result.ErrorMessage}");
        Assert.Equal(1, result.RecordCount);

        var metadata = _catalog.LoadTable("types_iceberg_table");
        Assert.NotNull(metadata);
        Assert.Equal(9, metadata.Schemas[0].Fields.Count);
    }

    [Fact]
    public async Task Should_Export_Table_With_Custom_Query()
    {
        if (!_sqlServerAvailable) return;

        // Arrange
        await CreateTestDatabase();
        await CreateTestTable("FullTable", @"
            CREATE TABLE FullTable (
                id INT PRIMARY KEY,
                status VARCHAR(50),
                amount DECIMAL(10, 2)
            )
        ");
        await InsertTestData("FullTable", @"
            INSERT INTO FullTable VALUES
            (1, 'active', 100.00),
            (2, 'inactive', 200.00),
            (3, 'active', 300.00)
        ");

        var exporter = new SqlServerToIcebergExporter(_catalog, NullLogger<SqlServerToIcebergExporter>.Instance);
        var dbConnectionString = $"{_connectionString};Database={_testDatabaseName}";
        var customQuery = "SELECT id, amount FROM FullTable WHERE status = 'active'";

        // Act
        var result = await exporter.ExportTableAsync(
            dbConnectionString,
            "FullTable",
            "filtered_iceberg_table",
            customQuery
        );

        // Assert
        Assert.True(result.Success);
        Assert.Equal(2, result.RecordCount); // Only 'active' rows

        var metadata = _catalog.LoadTable("filtered_iceberg_table");
        Assert.Equal(2, metadata.Schemas[0].Fields.Count); // Only id and amount columns
    }

    [Fact]
    public async Task Should_Handle_Empty_Table()
    {
        if (!_sqlServerAvailable) return;

        // Arrange
        await CreateTestDatabase();
        await CreateTestTable("EmptyTable", @"
            CREATE TABLE EmptyTable (
                id INT PRIMARY KEY,
                value VARCHAR(50)
            )
        ");

        var exporter = new SqlServerToIcebergExporter(_catalog, NullLogger<SqlServerToIcebergExporter>.Instance);
        var dbConnectionString = $"{_connectionString};Database={_testDatabaseName}";

        // Act
        var result = await exporter.ExportTableAsync(
            dbConnectionString,
            "EmptyTable",
            "empty_iceberg_table"
        );

        // Assert
        Assert.True(result.Success);
        Assert.Equal(0, result.RecordCount);
    }

    [Fact]
    public async Task Should_Return_Error_On_Invalid_Connection_String()
    {
        if (!_sqlServerAvailable) return;

        // Arrange
        var exporter = new SqlServerToIcebergExporter(_catalog, NullLogger<SqlServerToIcebergExporter>.Instance);
        var invalidConnectionString = "Server=nonexistent;Database=invalid";

        // Act
        var result = await exporter.ExportTableAsync(
            invalidConnectionString,
            "NonExistentTable",
            "test_table"
        );

        // Assert
        Assert.False(result.Success);
        Assert.NotEmpty(result.ErrorMessage);
    }

    [Fact]
    public async Task Should_Return_Error_On_Nonexistent_Table()
    {
        if (!_sqlServerAvailable) return;

        // Arrange
        await CreateTestDatabase();

        var exporter = new SqlServerToIcebergExporter(_catalog, NullLogger<SqlServerToIcebergExporter>.Instance);
        var dbConnectionString = $"{_connectionString};Database={_testDatabaseName}";

        // Act
        var result = await exporter.ExportTableAsync(
            dbConnectionString,
            "NonExistentTable",
            "test_table"
        );

        // Assert
        Assert.False(result.Success);
        Assert.Contains("Invalid object name", result.ErrorMessage);
    }

    [Fact]
    public async Task Should_Preserve_Nullability_In_Schema()
    {
        if (!_sqlServerAvailable) return;

        // Arrange
        await CreateTestDatabase();
        await CreateTestTable("NullableTable", @"
            CREATE TABLE NullableTable (
                id INT NOT NULL PRIMARY KEY,
                required_field VARCHAR(50) NOT NULL,
                optional_field VARCHAR(50) NULL
            )
        ");
        await InsertTestData("NullableTable", @"
            INSERT INTO NullableTable (id, required_field, optional_field) VALUES
            (1, 'required', NULL)
        ");

        var exporter = new SqlServerToIcebergExporter(_catalog, NullLogger<SqlServerToIcebergExporter>.Instance);
        var dbConnectionString = $"{_connectionString};Database={_testDatabaseName}";

        // Act
        var result = await exporter.ExportTableAsync(
            dbConnectionString,
            "NullableTable",
            "nullable_iceberg_table"
        );

        // Assert
        Assert.True(result.Success);

        var metadata = _catalog.LoadTable("nullable_iceberg_table");
        var idField = metadata.Schemas[0].Fields.First(f => f.Name == "id");
        var requiredField = metadata.Schemas[0].Fields.First(f => f.Name == "required_field");
        var optionalField = metadata.Schemas[0].Fields.First(f => f.Name == "optional_field");

        Assert.True(idField.Required);
        Assert.True(requiredField.Required);
        Assert.False(optionalField.Required);
    }

    [Fact]
    public async Task Should_Export_Large_Table_Efficiently()
    {
        if (!_sqlServerAvailable) return;

        // Arrange
        await CreateTestDatabase();
        await CreateTestTable("LargeTable", @"
            CREATE TABLE LargeTable (
                id INT PRIMARY KEY,
                data VARCHAR(100)
            )
        ");

        // Insert 1000 rows
        var insertStatements = new List<string>();
        for (int i = 1; i <= 1000; i++)
        {
            insertStatements.Add($"({i}, 'data_{i}')");
        }
        await InsertTestData("LargeTable",
            $"INSERT INTO LargeTable (id, data) VALUES {string.Join(",", insertStatements)}");

        var exporter = new SqlServerToIcebergExporter(_catalog, NullLogger<SqlServerToIcebergExporter>.Instance);
        var dbConnectionString = $"{_connectionString};Database={_testDatabaseName}";

        // Act
        var result = await exporter.ExportTableAsync(
            dbConnectionString,
            "LargeTable",
            "large_iceberg_table"
        );

        // Assert
        Assert.True(result.Success);
        Assert.Equal(1000, result.RecordCount);
    }

    // Helper methods
    private async Task CreateTestDatabase()
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        using var command = new SqlCommand($"CREATE DATABASE [{_testDatabaseName}]", connection);
        await command.ExecuteNonQueryAsync();
    }

    private async Task CreateTestTable(string tableName, string createSql)
    {
        var dbConnectionString = $"{_connectionString};Database={_testDatabaseName}";
        using var connection = new SqlConnection(dbConnectionString);
        await connection.OpenAsync();

        using var command = new SqlCommand(createSql, connection);
        await command.ExecuteNonQueryAsync();
    }

    private async Task InsertTestData(string tableName, string insertSql)
    {
        var dbConnectionString = $"{_connectionString};Database={_testDatabaseName}";
        using var connection = new SqlConnection(dbConnectionString);
        await connection.OpenAsync();

        using var command = new SqlCommand(insertSql, connection);
        await command.ExecuteNonQueryAsync();
    }

    public void Dispose()
    {
        // Clean up test database (only if SQL Server is available)
        if (_sqlServerAvailable)
        {
            try
            {
                using var connection = new SqlConnection(_connectionString);
                connection.Open();
                using var command = new SqlCommand($@"
                    IF EXISTS (SELECT name FROM sys.databases WHERE name = '{_testDatabaseName}')
                    BEGIN
                        ALTER DATABASE [{_testDatabaseName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                        DROP DATABASE [{_testDatabaseName}];
                    END", connection);
                command.ExecuteNonQuery();
            }
            catch
            {
                // Ignore cleanup errors
            }
        }

        // Clean up warehouse
        if (Directory.Exists(_tempWarehouse))
        {
            try
            {
                Directory.Delete(_tempWarehouse, recursive: true);
            }
            catch
            {
                // Ignore cleanup errors
            }
        }
    }
}
