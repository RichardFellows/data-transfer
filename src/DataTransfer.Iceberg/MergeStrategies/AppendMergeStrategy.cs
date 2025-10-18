using System.Text;
using DataTransfer.Iceberg.Models;
using Microsoft.Data.SqlClient;

namespace DataTransfer.Iceberg.MergeStrategies;

/// <summary>
/// Append-only merge strategy (INSERT new rows only, skip existing)
/// </summary>
public class AppendMergeStrategy : IMergeStrategy
{
    private readonly string _primaryKeyColumn;

    public AppendMergeStrategy(string primaryKeyColumn)
    {
        _primaryKeyColumn = primaryKeyColumn ?? throw new ArgumentNullException(nameof(primaryKeyColumn));
    }

    public async Task<MergeResult> MergeAsync(
        SqlConnection connection,
        string targetTable,
        string tempTable,
        CancellationToken cancellationToken = default)
    {
        // Get column list from temp table
        var columns = await GetTableColumns(connection, tempTable, cancellationToken);

        // Build INSERT statement that only inserts rows that don't already exist
        var insertSql = BuildInsertSql(targetTable, tempTable, columns);

        await using var command = new SqlCommand(insertSql, connection);
        command.CommandTimeout = 300; // 5 minutes for large datasets

        var rowsInserted = await command.ExecuteNonQueryAsync(cancellationToken);

        return new MergeResult
        {
            Inserted = rowsInserted,
            Updated = 0 // Append strategy never updates
        };
    }

    private string BuildInsertSql(
        string targetTable,
        string tempTable,
        List<string> columns)
    {
        var sql = new StringBuilder();

        // INSERT INTO target ... SELECT FROM temp WHERE NOT EXISTS
        sql.Append($"INSERT INTO {targetTable} (");
        sql.Append(string.Join(", ", columns));
        sql.AppendLine(")");
        sql.Append("SELECT ");
        sql.Append(string.Join(", ", columns));
        sql.AppendLine($" FROM {tempTable} AS source");
        sql.AppendLine("WHERE NOT EXISTS (");
        sql.AppendLine($"    SELECT 1 FROM {targetTable} AS target");
        sql.AppendLine($"    WHERE target.{_primaryKeyColumn} = source.{_primaryKeyColumn}");
        sql.AppendLine(");");

        return sql.ToString();
    }

    private async Task<List<string>> GetTableColumns(
        SqlConnection connection,
        string tableName,
        CancellationToken cancellationToken)
    {
        var columns = new List<string>();

        // Use OBJECT_ID() which works for temp tables in the same session
        var sql = tableName.StartsWith("#")
            ? $@"
                SELECT c.name
                FROM tempdb.sys.columns c
                WHERE c.object_id = OBJECT_ID('tempdb..{tableName}')
                ORDER BY c.column_id"
            : $@"
                SELECT c.name
                FROM sys.columns c
                WHERE c.object_id = OBJECT_ID('{tableName}')
                ORDER BY c.column_id";

        await using var command = new SqlCommand(sql, connection);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            columns.Add(reader.GetString(0));
        }

        return columns;
    }
}
