using DataTransfer.Iceberg.Catalog;
using DataTransfer.Iceberg.ChangeDetection;
using DataTransfer.Iceberg.Integration;
using DataTransfer.Iceberg.Models;
using DataTransfer.Iceberg.Readers;
using DataTransfer.Iceberg.Watermarks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace DataTransfer.Iceberg.Tests.Integration;

public class IncrementalSyncCoordinatorTests : IDisposable
{
    private readonly string _connectionString;
    private readonly string _sourceDatabase = "SyncSourceDb";
    private readonly string _targetDatabase = "SyncTargetDb";
    private readonly string _sourceTable = "Customers";
    private readonly string _targetTable = "Customers";
    private readonly string _icebergTable = "customers_sync";
    private readonly string _warehousePath;
    private readonly string _watermarkPath;

    public IncrementalSyncCoordinatorTests()
    {
        _connectionString = "Server=localhost;User Id=sa;Password=IcebergDemo@2024;TrustServerCertificate=true;";
        _warehousePath = Path.Combine(Path.GetTempPath(), "iceberg-sync-test", Guid.NewGuid().ToString());
        _watermarkPath = Path.Combine(Path.GetTempPath(), "watermarks-test", Guid.NewGuid().ToString());

        Directory.CreateDirectory(_warehousePath);
        Directory.CreateDirectory(_watermarkPath);

        EnsureDatabases();
    }

    private void EnsureDatabases()
    {
        using var connection = new SqlConnection(_connectionString);
        connection.Open();

        using var cmd = connection.CreateCommand();

        // Create source database
        cmd.CommandText = $@"
            IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '{_sourceDatabase}')
            BEGIN
                CREATE DATABASE [{_sourceDatabase}]
            END";
        cmd.ExecuteNonQuery();

        // Create target database
        cmd.CommandText = $@"
            IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '{_targetDatabase}')
            BEGIN
                CREATE DATABASE [{_targetDatabase}]
            END";
        cmd.ExecuteNonQuery();
    }

    [Fact]
    public async Task Should_Complete_Full_Sync_Workflow()
    {
        // Arrange
        await CreateSourceTable();
        await InsertSourceData(1000);
        await CreateTargetTable();

        var coordinator = CreateCoordinator();
        var options = new SyncOptions
        {
            PrimaryKeyColumn = "Id",
            WatermarkColumn = "ModifiedDate",
            WarehousePath = _warehousePath,
            WatermarkDirectory = _watermarkPath
        };

        // Act
        var result = await coordinator.SyncAsync(
            GetConnectionString(_sourceDatabase),
            _sourceTable,
            _icebergTable,
            GetConnectionString(_targetDatabase),
            _targetTable,
            options);

        // Assert
        Assert.True(result.Success, $"Sync failed: {result.ErrorMessage}");
        Assert.Equal(1000, result.RowsExtracted);
        Assert.Equal(1000, result.RowsAppended);
        Assert.Equal(1000, result.RowsImported);
        Assert.True(result.NewSnapshotId > 0);
        Assert.NotNull(result.NewWatermark);

        var targetCount = await GetTargetRowCount();
        Assert.Equal(1000, targetCount);
    }

    [Fact]
    public async Task Should_Handle_Incremental_Sync()
    {
        // Arrange
        await CreateSourceTable();
        await InsertSourceData(1000);
        await CreateTargetTable();

        var coordinator = CreateCoordinator();
        var options = new SyncOptions
        {
            PrimaryKeyColumn = "Id",
            WatermarkColumn = "ModifiedDate",
            WarehousePath = _warehousePath,
            WatermarkDirectory = _watermarkPath
        };

        // First sync (initial)
        await coordinator.SyncAsync(
            GetConnectionString(_sourceDatabase),
            _sourceTable,
            _icebergTable,
            GetConnectionString(_targetDatabase),
            _targetTable,
            options);

        // Add more data
        await Task.Delay(100); // Ensure different timestamps
        await InsertSourceData(100, startId: 1001);

        // Act - Second sync (incremental)
        var result = await coordinator.SyncAsync(
            GetConnectionString(_sourceDatabase),
            _sourceTable,
            _icebergTable,
            GetConnectionString(_targetDatabase),
            _targetTable,
            options);

        // Assert
        Assert.True(result.Success, $"Incremental sync failed: {result.ErrorMessage}");
        Assert.Equal(100, result.RowsExtracted);
        Assert.Equal(100, result.RowsAppended);

        var targetCount = await GetTargetRowCount();
        Assert.Equal(1100, targetCount);
    }

    [Fact]
    public async Task Should_Handle_No_Changes()
    {
        // Arrange
        await CreateSourceTable();
        await InsertSourceData(100);
        await CreateTargetTable();

        var coordinator = CreateCoordinator();
        var options = new SyncOptions
        {
            PrimaryKeyColumn = "Id",
            WatermarkColumn = "ModifiedDate",
            WarehousePath = _warehousePath,
            WatermarkDirectory = _watermarkPath
        };

        // First sync
        await coordinator.SyncAsync(
            GetConnectionString(_sourceDatabase),
            _sourceTable,
            _icebergTable,
            GetConnectionString(_targetDatabase),
            _targetTable,
            options);

        // Act - Second sync with no changes
        var result = await coordinator.SyncAsync(
            GetConnectionString(_sourceDatabase),
            _sourceTable,
            _icebergTable,
            GetConnectionString(_targetDatabase),
            _targetTable,
            options);

        // Assert
        Assert.True(result.Success);
        Assert.Equal(0, result.RowsExtracted);
    }

    [Fact]
    public async Task Should_Handle_Updates()
    {
        // Arrange
        await CreateSourceTable();
        await InsertSourceData(100);
        await CreateTargetTable();

        var coordinator = CreateCoordinator();
        var options = new SyncOptions
        {
            PrimaryKeyColumn = "Id",
            WatermarkColumn = "ModifiedDate",
            WarehousePath = _warehousePath,
            WatermarkDirectory = _watermarkPath
        };

        // First sync
        await coordinator.SyncAsync(
            GetConnectionString(_sourceDatabase),
            _sourceTable,
            _icebergTable,
            GetConnectionString(_targetDatabase),
            _targetTable,
            options);

        // Update some rows
        await Task.Delay(100);
        await UpdateSourceData(new[] { 1, 2, 3 });

        // Act - Sync updates
        var result = await coordinator.SyncAsync(
            GetConnectionString(_sourceDatabase),
            _sourceTable,
            _icebergTable,
            GetConnectionString(_targetDatabase),
            _targetTable,
            options);

        // Assert
        Assert.True(result.Success, $"Update sync failed: {result.ErrorMessage}");
        Assert.Equal(3, result.RowsExtracted);

        var targetCount = await GetTargetRowCount();
        Assert.Equal(100, targetCount); // Still 100 rows (updates, not inserts)
    }

    // Helper methods

    private IncrementalSyncCoordinator CreateCoordinator()
    {
        var catalog = new FilesystemCatalog(_warehousePath, NullLogger<FilesystemCatalog>.Instance);
        var changeDetection = new TimestampChangeDetection("ModifiedDate");
        var appender = new IcebergAppender(catalog, NullLogger<IcebergAppender>.Instance);
        var reader = new IcebergReader(catalog, NullLogger<IcebergReader>.Instance);
        var importer = new SqlServerImporter(NullLogger<SqlServerImporter>.Instance);
        var watermarkStore = new FileWatermarkStore(_watermarkPath);

        return new IncrementalSyncCoordinator(
            changeDetection,
            appender,
            reader,
            importer,
            watermarkStore,
            NullLogger<IncrementalSyncCoordinator>.Instance);
    }

    private string GetConnectionString(string database)
    {
        return $"Server=localhost;Database={database};User Id=sa;Password=IcebergDemo@2024;TrustServerCertificate=true;";
    }

    private async Task CreateSourceTable()
    {
        await using var connection = new SqlConnection(GetConnectionString(_sourceDatabase));
        await connection.OpenAsync();

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $@"
            IF OBJECT_ID('{_sourceTable}', 'U') IS NOT NULL
                DROP TABLE {_sourceTable};

            CREATE TABLE {_sourceTable} (
                Id INT PRIMARY KEY,
                Name NVARCHAR(100),
                Email NVARCHAR(100),
                ModifiedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE()
            )";
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task CreateTargetTable()
    {
        await using var connection = new SqlConnection(GetConnectionString(_targetDatabase));
        await connection.OpenAsync();

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $@"
            IF OBJECT_ID('{_targetTable}', 'U') IS NOT NULL
                DROP TABLE {_targetTable};

            CREATE TABLE {_targetTable} (
                Id INT PRIMARY KEY,
                Name NVARCHAR(100),
                Email NVARCHAR(100),
                ModifiedDate DATETIME2
            )";
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task InsertSourceData(int count, int startId = 1)
    {
        await using var connection = new SqlConnection(GetConnectionString(_sourceDatabase));
        await connection.OpenAsync();

        for (int i = 0; i < count; i++)
        {
            int id = startId + i;
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = $@"
                INSERT INTO {_sourceTable} (Id, Name, Email, ModifiedDate)
                VALUES (@Id, @Name, @Email, GETUTCDATE())";
            cmd.Parameters.AddWithValue("@Id", id);
            cmd.Parameters.AddWithValue("@Name", $"Customer{id}");
            cmd.Parameters.AddWithValue("@Email", $"customer{id}@example.com");
            await cmd.ExecuteNonQueryAsync();
        }
    }

    private async Task UpdateSourceData(int[] ids)
    {
        await using var connection = new SqlConnection(GetConnectionString(_sourceDatabase));
        await connection.OpenAsync();

        foreach (var id in ids)
        {
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = $@"
                UPDATE {_sourceTable}
                SET Name = @Name, ModifiedDate = GETUTCDATE()
                WHERE Id = @Id";
            cmd.Parameters.AddWithValue("@Id", id);
            cmd.Parameters.AddWithValue("@Name", $"UpdatedCustomer{id}");
            await cmd.ExecuteNonQueryAsync();
        }
    }

    private async Task<int> GetTargetRowCount()
    {
        await using var connection = new SqlConnection(GetConnectionString(_targetDatabase));
        await connection.OpenAsync();

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM {_targetTable}";
        return (int)await cmd.ExecuteScalarAsync()!;
    }

    public void Dispose()
    {
        // Cleanup
        try
        {
            using var connection = new SqlConnection(_connectionString);
            connection.Open();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = $@"
                IF EXISTS (SELECT * FROM sys.databases WHERE name = '{_sourceDatabase}')
                    DROP DATABASE [{_sourceDatabase}];
                IF EXISTS (SELECT * FROM sys.databases WHERE name = '{_targetDatabase}')
                    DROP DATABASE [{_targetDatabase}];";
            cmd.ExecuteNonQuery();

            if (Directory.Exists(_warehousePath))
                Directory.Delete(_warehousePath, true);
            if (Directory.Exists(_watermarkPath))
                Directory.Delete(_watermarkPath, true);
        }
        catch
        {
            // Ignore cleanup errors
        }
    }
}
