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

    /// <summary>
    /// Gets preview data for a specific table (schema + 10 sample rows)
    /// Results are cached for 5 minutes
    /// </summary>
    public async Task<DataPreview> GetTablePreviewAsync(string connectionString, string database, string schema, string tableName)
    {
        var cacheKey = $"preview|{connectionString}|{database}|{schema}|{tableName}";
        var cached = GetFromCache<DataPreview>(cacheKey);
        if (cached != null)
        {
            return cached;
        }

        var preview = new DataPreview();

        // Modify connection string to include the database
        var builder = new SqlConnectionStringBuilder(connectionString)
        {
            InitialCatalog = database
        };

        await using var connection = new SqlConnection(builder.ConnectionString);
        await connection.OpenAsync();

        // Get column metadata
        var columnsQuery = @"
            SELECT
                COLUMN_NAME,
                DATA_TYPE +
                    CASE
                        WHEN CHARACTER_MAXIMUM_LENGTH IS NOT NULL
                        THEN '(' + CAST(CHARACTER_MAXIMUM_LENGTH AS VARCHAR) + ')'
                        WHEN NUMERIC_PRECISION IS NOT NULL
                        THEN '(' + CAST(NUMERIC_PRECISION AS VARCHAR) + ',' + CAST(NUMERIC_SCALE AS VARCHAR) + ')'
                        ELSE ''
                    END AS DATA_TYPE,
                IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @tableName
            ORDER BY ORDINAL_POSITION";

        await using (var command = new SqlCommand(columnsQuery, connection))
        {
            command.Parameters.AddWithValue("@schema", schema);
            command.Parameters.AddWithValue("@tableName", tableName);
            await using var reader = await command.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                preview.Columns.Add(new ColumnInfo
                {
                    Name = reader.GetString(0),
                    DataType = reader.GetString(1),
                    IsNullable = reader.GetString(2) == "YES"
                });
            }
        }

        // Get sample data (top 10 rows)
        var dataQuery = $"SELECT TOP 10 * FROM [{schema}].[{tableName}]";
        await using (var command = new SqlCommand(dataQuery, connection))
        {
            await using var reader = await command.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                var row = new Dictionary<string, object?>();
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    var value = reader.IsDBNull(i) ? null : reader.GetValue(i);
                    row[reader.GetName(i)] = value;
                }
                preview.Rows.Add(row);
            }
        }

        // Get total row count
        var countQuery = $"SELECT COUNT(*) FROM [{schema}].[{tableName}]";
        await using (var command = new SqlCommand(countQuery, connection))
        {
            var result = await command.ExecuteScalarAsync();
            preview.TotalRowCount = result != null ? Convert.ToInt64(result) : null;
        }

        SaveToCache(cacheKey, preview);
        return preview;
    }
}
