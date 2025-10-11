using System.Text;
using DataTransfer.Iceberg.Models;
using Microsoft.Data.SqlClient;

namespace DataTransfer.Iceberg.MergeStrategies;

/// <summary>
/// MERGE-based upsert strategy (INSERT if not exists, UPDATE if exists)
/// </summary>
public class UpsertMergeStrategy : IMergeStrategy
{
    private readonly string _primaryKeyColumn;

    public UpsertMergeStrategy(string primaryKeyColumn)
    {
        _primaryKeyColumn = primaryKeyColumn ?? throw new ArgumentNullException(nameof(primaryKeyColumn));
    }

    public async Task<MergeResult> MergeAsync(
        SqlConnection connection,
        string targetTable,
        string tempTable,
        CancellationToken cancellationToken = default)
    {
        // Get column list from temp table (excluding primary key for UPDATE SET clause)
        var columns = await GetTableColumns(connection, tempTable, cancellationToken);
        var updateColumns = columns.Where(c => c != _primaryKeyColumn).ToList();

        // Build MERGE statement with OUTPUT clause to capture inserted/updated counts
        var mergeSql = BuildMergeSql(targetTable, tempTable, columns, updateColumns);

        await using var command = new SqlCommand(mergeSql, connection);
        command.CommandTimeout = 300; // 5 minutes for large datasets

        // Execute merge and capture results
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        int inserted = 0;
        int updated = 0;

        while (await reader.ReadAsync(cancellationToken))
        {
            var action = reader.GetString(0);
            if (action == "INSERT")
                inserted++;
            else if (action == "UPDATE")
                updated++;
        }

        return new MergeResult
        {
            Inserted = inserted,
            Updated = updated
        };
    }

    private string BuildMergeSql(
        string targetTable,
        string tempTable,
        List<string> allColumns,
        List<string> updateColumns)
    {
        var sql = new StringBuilder();

        // MERGE statement
        sql.AppendLine($"MERGE {targetTable} AS target");
        sql.AppendLine($"USING {tempTable} AS source");
        sql.AppendLine($"ON target.{_primaryKeyColumn} = source.{_primaryKeyColumn}");

        // WHEN MATCHED (update existing rows)
        if (updateColumns.Any())
        {
            sql.AppendLine("WHEN MATCHED THEN");
            sql.Append("    UPDATE SET ");
            sql.AppendLine(string.Join(", ", updateColumns.Select(c => $"target.{c} = source.{c}")));
        }

        // WHEN NOT MATCHED (insert new rows)
        sql.AppendLine("WHEN NOT MATCHED THEN");
        sql.Append("    INSERT (");
        sql.Append(string.Join(", ", allColumns));
        sql.AppendLine(")");
        sql.Append("    VALUES (");
        sql.Append(string.Join(", ", allColumns.Select(c => $"source.{c}")));
        sql.AppendLine(")");

        // OUTPUT clause to capture what happened
        sql.AppendLine("OUTPUT $action;");

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
