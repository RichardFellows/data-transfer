using DataTransfer.SqlServer.Models;
using Microsoft.Data.SqlClient;

namespace DataTransfer.SqlServer;

/// <summary>
/// Service for discovering SQL Server database schema and metadata
/// </summary>
public class SqlSchemaDiscovery
{
    private readonly string _connectionString;

    public SqlSchemaDiscovery(string connectionString)
    {
        ArgumentNullException.ThrowIfNull(connectionString);

        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new ArgumentException("Connection string cannot be empty or whitespace", nameof(connectionString));
        }

        _connectionString = connectionString;
    }

    /// <summary>
    /// Discover all tables and columns in the database
    /// </summary>
    public async Task<DatabaseInfo> DiscoverDatabaseAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var databaseName = connection.Database;
        var serverVersion = connection.ServerVersion;

        var tables = await DiscoverTablesAsync(connection, cancellationToken);

        return new DatabaseInfo
        {
            DatabaseName = databaseName,
            ServerVersion = $"Microsoft SQL Server {serverVersion}",
            Tables = tables
        };
    }

    /// <summary>
    /// Discover a specific table's schema
    /// </summary>
    public async Task<TableInfo?> DiscoverTableAsync(
        string schema,
        string tableName,
        CancellationToken cancellationToken = default)
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var table = await DiscoverSingleTableAsync(connection, schema, tableName, cancellationToken);
        return table;
    }

    private async Task<List<TableInfo>> DiscoverTablesAsync(
        SqlConnection connection,
        CancellationToken cancellationToken)
    {
        const string query = """
            SELECT
                s.name AS SchemaName,
                t.name AS TableName,
                SUM(p.rows) AS RowCount
            FROM
                sys.tables t
                INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                INNER JOIN sys.partitions p ON t.object_id = p.object_id
            WHERE
                p.index_id IN (0, 1) -- Heap or clustered index
                AND t.is_ms_shipped = 0 -- Exclude system tables
            GROUP BY
                s.name, t.name
            ORDER BY
                s.name, t.name;
            """;

        var tables = new List<TableInfo>();

        await using var command = new SqlCommand(query, connection);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            var schema = reader.GetString(0);
            var tableName = reader.GetString(1);
            var rowCount = reader.GetInt64(2);

            var columns = await DiscoverColumnsAsync(connection, schema, tableName, cancellationToken);

            tables.Add(new TableInfo
            {
                Schema = schema,
                TableName = tableName,
                RowCount = rowCount,
                Columns = columns
            });
        }

        return tables;
    }

    private async Task<TableInfo?> DiscoverSingleTableAsync(
        SqlConnection connection,
        string schema,
        string tableName,
        CancellationToken cancellationToken)
    {
        const string query = """
            SELECT
                s.name AS SchemaName,
                t.name AS TableName,
                SUM(p.rows) AS RowCount
            FROM
                sys.tables t
                INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                INNER JOIN sys.partitions p ON t.object_id = p.object_id
            WHERE
                p.index_id IN (0, 1)
                AND t.is_ms_shipped = 0
                AND s.name = @Schema
                AND t.name = @TableName
            GROUP BY
                s.name, t.name;
            """;

        await using var command = new SqlCommand(query, connection);
        command.Parameters.AddWithValue("@Schema", schema);
        command.Parameters.AddWithValue("@TableName", tableName);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        if (!await reader.ReadAsync(cancellationToken))
        {
            return null; // Table not found
        }

        var schemaName = reader.GetString(0);
        var table = reader.GetString(1);
        var rowCount = reader.GetInt64(2);

        await reader.CloseAsync();

        var columns = await DiscoverColumnsAsync(connection, schemaName, table, cancellationToken);

        return new TableInfo
        {
            Schema = schemaName,
            TableName = table,
            RowCount = rowCount,
            Columns = columns
        };
    }

    private async Task<List<ColumnInfo>> DiscoverColumnsAsync(
        SqlConnection connection,
        string schema,
        string tableName,
        CancellationToken cancellationToken)
    {
        const string query = """
            SELECT
                c.name AS ColumnName,
                t.name AS DataType,
                c.is_nullable AS IsNullable,
                c.max_length AS MaxLength,
                c.precision AS Precision,
                c.scale AS Scale
            FROM
                sys.columns c
                INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
                INNER JOIN sys.tables tab ON c.object_id = tab.object_id
                INNER JOIN sys.schemas s ON tab.schema_id = s.schema_id
            WHERE
                s.name = @Schema
                AND tab.name = @TableName
            ORDER BY
                c.column_id;
            """;

        var columns = new List<ColumnInfo>();

        // Need to use a separate connection for nested queries
        await using var nestedConnection = new SqlConnection(connection.ConnectionString);
        await nestedConnection.OpenAsync(cancellationToken);

        await using var command = new SqlCommand(query, nestedConnection);
        command.Parameters.AddWithValue("@Schema", schema);
        command.Parameters.AddWithValue("@TableName", tableName);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            columns.Add(new ColumnInfo
            {
                ColumnName = reader.GetString(0),
                DataType = reader.GetString(1),
                IsNullable = reader.GetBoolean(2),
                MaxLength = reader.GetInt16(3),
                Precision = reader.GetByte(4),
                Scale = reader.GetByte(5)
            });
        }

        return columns;
    }

    /// <summary>
    /// Test if the connection string is valid
    /// </summary>
    public async Task<bool> TestConnectionAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            await using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync(cancellationToken);
            return true;
        }
        catch
        {
            return false;
        }
    }
}
