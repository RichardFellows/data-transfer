using System.Data;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using DataTransfer.Web.Models;

namespace DataTransfer.Web.Services;

/// <summary>
/// Service for querying SQL Server metadata (databases, schemas, tables)
/// Includes 5-minute caching to improve performance
/// </summary>
public class DatabaseMetadataService
{
    private readonly ILogger<DatabaseMetadataService> _logger;
    private const int ConnectionTimeoutSeconds = 5;
    private const int CacheExpirationMinutes = 5;

    // Cache storage: Key = "{connectionString}|{database}|{schema}", Value = (Data, ExpiresAt)
    private readonly Dictionary<string, (object Data, DateTime ExpiresAt)> _cache = new();
    private readonly object _cacheLock = new();

    public DatabaseMetadataService(ILogger<DatabaseMetadataService> logger)
    {
        _logger = logger;
    }

    private T? GetFromCache<T>(string key) where T : class
    {
        lock (_cacheLock)
        {
            if (_cache.TryGetValue(key, out var cached) && cached.ExpiresAt > DateTime.UtcNow)
            {
                _logger.LogDebug("Cache hit for key: {Key}", key);
                return cached.Data as T;
            }

            if (_cache.ContainsKey(key))
            {
                _logger.LogDebug("Cache expired for key: {Key}", key);
                _cache.Remove(key);
            }

            return null;
        }
    }

    private void SaveToCache<T>(string key, T data) where T : class
    {
        lock (_cacheLock)
        {
            var expiresAt = DateTime.UtcNow.AddMinutes(CacheExpirationMinutes);
            _cache[key] = (data, expiresAt);
            _logger.LogDebug("Cached data for key: {Key}, expires at: {ExpiresAt}", key, expiresAt);
        }
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
    /// Results are cached for 5 minutes
    /// </summary>
    public async Task<List<string>> GetDatabasesAsync(string connectionString)
    {
        var cacheKey = $"databases|{connectionString}";
        var cached = GetFromCache<List<string>>(cacheKey);
        if (cached != null)
        {
            return cached;
        }

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

        SaveToCache(cacheKey, databases);
        return databases;
    }

    /// <summary>
    /// Gets list of schemas in a specific database
    /// Results are cached for 5 minutes
    /// </summary>
    public async Task<List<string>> GetSchemasAsync(string connectionString, string database)
    {
        var cacheKey = $"schemas|{connectionString}|{database}";
        var cached = GetFromCache<List<string>>(cacheKey);
        if (cached != null)
        {
            return cached;
        }

        var schemas = new List<string>();

        // Modify connection string to include the database
        var builder = new SqlConnectionStringBuilder(connectionString)
        {
            InitialCatalog = database
        };

        await using var connection = new SqlConnection(builder.ConnectionString);
        await connection.OpenAsync();

        var query = @"
            SELECT DISTINCT TABLE_SCHEMA
            FROM INFORMATION_SCHEMA.TABLES
            ORDER BY TABLE_SCHEMA";

        await using var command = new SqlCommand(query, connection);
        await using var reader = await command.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            schemas.Add(reader.GetString(0));
        }

        SaveToCache(cacheKey, schemas);
        return schemas;
    }

    /// <summary>
    /// Gets list of tables in a specific database and schema
    /// Results are cached for 5 minutes
    /// </summary>
    public async Task<List<TableInfo>> GetTablesAsync(string connectionString, string database, string schema)
    {
        var cacheKey = $"tables|{connectionString}|{database}|{schema}";
        var cached = GetFromCache<List<TableInfo>>(cacheKey);
        if (cached != null)
        {
            return cached;
        }

        var tables = new List<TableInfo>();

        // Modify connection string to include the database
        var builder = new SqlConnectionStringBuilder(connectionString)
        {
            InitialCatalog = database
        };

        await using var connection = new SqlConnection(builder.ConnectionString);
        await connection.OpenAsync();

        var query = @"
            SELECT TABLE_NAME, TABLE_TYPE
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = @schema
            ORDER BY TABLE_NAME";

        await using var command = new SqlCommand(query, connection);
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

        SaveToCache(cacheKey, tables);
        return tables;
    }
}
