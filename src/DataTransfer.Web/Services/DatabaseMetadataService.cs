using System.Data;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using DataTransfer.Web.Models;

namespace DataTransfer.Web.Services;

/// <summary>
/// Service for querying SQL Server metadata (databases, schemas, tables)
/// </summary>
public class DatabaseMetadataService
{
    private readonly ILogger<DatabaseMetadataService> _logger;
    private const int ConnectionTimeoutSeconds = 5;

    public DatabaseMetadataService(ILogger<DatabaseMetadataService> logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// Tests if a connection string is valid and can connect to SQL Server
    /// </summary>
    public bool TestConnection(string connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            return false;
        }

        try
        {
            using var connection = new SqlConnection(connectionString);
            connection.Open();
            return connection.State == ConnectionState.Open;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Connection test failed for connection string");
            return false;
        }
    }

    /// <summary>
    /// Gets list of databases from SQL Server (excludes system databases)
    /// </summary>
    public async Task<List<string>> GetDatabasesAsync(string connectionString)
    {
        var databases = new List<string>();

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        var query = "SELECT name FROM sys.databases WHERE database_id > 4 ORDER BY name";
        await using var command = new SqlCommand(query, connection);
        await using var reader = await command.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            databases.Add(reader.GetString(0));
        }

        return databases;
    }

    /// <summary>
    /// Gets list of schemas in a specific database
    /// </summary>
    public async Task<List<string>> GetSchemasAsync(string connectionString, string database)
    {
        var schemas = new List<string>();

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        var query = @"
            SELECT DISTINCT TABLE_SCHEMA
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_CATALOG = @database
            ORDER BY TABLE_SCHEMA";

        await using var command = new SqlCommand(query, connection);
        command.Parameters.AddWithValue("@database", database);
        await using var reader = await command.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            schemas.Add(reader.GetString(0));
        }

        return schemas;
    }

    /// <summary>
    /// Gets list of tables in a specific database and schema
    /// </summary>
    public async Task<List<TableInfo>> GetTablesAsync(string connectionString, string database, string schema)
    {
        var tables = new List<TableInfo>();

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        var query = @"
            SELECT TABLE_NAME, TABLE_TYPE
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_CATALOG = @database AND TABLE_SCHEMA = @schema
            ORDER BY TABLE_NAME";

        await using var command = new SqlCommand(query, connection);
        command.Parameters.AddWithValue("@database", database);
        command.Parameters.AddWithValue("@schema", schema);
        await using var reader = await command.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            tables.Add(new TableInfo
            {
                Name = reader.GetString(0),
                Type = reader.GetString(1)
            });
        }

        return tables;
    }
}
