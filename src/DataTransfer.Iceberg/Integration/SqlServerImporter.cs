using System.Data;
using System.Runtime.CompilerServices;
using DataTransfer.Iceberg.MergeStrategies;
using DataTransfer.Iceberg.Models;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace DataTransfer.Iceberg.Integration;

/// <summary>
/// Imports data from Iceberg tables into SQL Server
/// </summary>
public class SqlServerImporter
{
    private readonly ILogger<SqlServerImporter> _logger;

    public SqlServerImporter(ILogger<SqlServerImporter> logger)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <summary>
    /// Imports data from Iceberg into SQL Server table
    /// </summary>
    /// <param name="data">Data stream from Iceberg</param>
    /// <param name="connectionString">SQL Server connection string</param>
    /// <param name="tableName">Target table name</param>
    /// <param name="mergeStrategy">Strategy for merging data</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Import result with row counts</returns>
    public async Task<ImportResult> ImportAsync(
        IAsyncEnumerable<Dictionary<string, object>> data,
        string connectionString,
        string tableName,
        IMergeStrategy mergeStrategy,
        CancellationToken cancellationToken = default)
    {
        try
        {
            _logger.LogInformation("Starting import to {TableName}", tableName);

            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken);

            // 1. Create temp table with same schema as target
            var tempTable = await CreateTempTable(connection, tableName, cancellationToken);
            _logger.LogDebug("Created temp table: {TempTable}", tempTable);

            // 2. Bulk copy data to temp table
            var rowCount = await BulkCopyToTemp(data, connection, tempTable, cancellationToken);
            _logger.LogInformation("Bulk copied {RowCount} rows to temp table", rowCount);

            // 3. Execute merge strategy
            var mergeResult = await mergeStrategy.MergeAsync(connection, tableName, tempTable, cancellationToken);
            _logger.LogInformation(
                "Merge complete: {Inserted} inserted, {Updated} updated",
                mergeResult.Inserted,
                mergeResult.Updated);

            return new ImportResult
            {
                Success = true,
                RowsImported = rowCount,
                RowsInserted = mergeResult.Inserted,
                RowsUpdated = mergeResult.Updated
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to import data to {TableName}", tableName);
            return new ImportResult
            {
                Success = false,
                ErrorMessage = ex.Message
            };
        }
    }

    private async Task<string> CreateTempTable(
        SqlConnection connection,
        string tableName,
        CancellationToken cancellationToken)
    {
        var tempTable = $"#Temp_{tableName}_{Guid.NewGuid():N}";

        var createSql = $@"
            SELECT TOP 0 *
            INTO {tempTable}
            FROM {tableName}";

        await using var command = new SqlCommand(createSql, connection);
        await command.ExecuteNonQueryAsync(cancellationToken);

        return tempTable;
    }

    private async Task<int> BulkCopyToTemp(
        IAsyncEnumerable<Dictionary<string, object>> data,
        SqlConnection connection,
        string tempTable,
        CancellationToken cancellationToken)
    {
        // Materialize data into DataTable for SqlBulkCopy
        var dataTable = await MaterializeDataTable(data, cancellationToken);

        if (dataTable.Rows.Count == 0)
        {
            _logger.LogWarning("No data to import");
            return 0;
        }

        using var bulkCopy = new SqlBulkCopy(connection)
        {
            DestinationTableName = tempTable,
            BatchSize = 10000,
            BulkCopyTimeout = 300 // 5 minutes
        };

        // Map columns
        foreach (DataColumn column in dataTable.Columns)
        {
            bulkCopy.ColumnMappings.Add(column.ColumnName, column.ColumnName);
        }

        await bulkCopy.WriteToServerAsync(dataTable, cancellationToken);

        return dataTable.Rows.Count;
    }

    private async Task<DataTable> MaterializeDataTable(
        IAsyncEnumerable<Dictionary<string, object>> data,
        CancellationToken cancellationToken)
    {
        var dataTable = new DataTable();
        bool schemaCreated = false;
        int rowsProcessed = 0;

        _logger.LogDebug("Starting to materialize data table");

        try
        {
            await foreach (var row in data.WithCancellation(cancellationToken))
            {
                if (!schemaCreated)
                {
                    // Create schema from first row
                    _logger.LogDebug("Creating schema from first row with {ColumnCount} columns", row.Count);
                    foreach (var kvp in row)
                    {
                        var columnType = InferColumnType(kvp.Value);
                        dataTable.Columns.Add(kvp.Key, columnType);
                        _logger.LogDebug("Added column: {ColumnName} ({ColumnType})", kvp.Key, columnType.Name);
                    }
                    schemaCreated = true;
                }

                // Add row
                var dataRow = dataTable.NewRow();
                foreach (var kvp in row)
                {
                    dataRow[kvp.Key] = kvp.Value ?? DBNull.Value;
                }
                dataTable.Rows.Add(dataRow);
                rowsProcessed++;

                if (rowsProcessed % 100 == 0)
                {
                    _logger.LogDebug("Materialized {RowCount} rows so far", rowsProcessed);
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error materializing data table after {RowCount} rows", rowsProcessed);
            throw;
        }

        _logger.LogInformation("Materialized {TotalRows} rows into DataTable", rowsProcessed);
        return dataTable;
    }

    private Type InferColumnType(object? value)
    {
        if (value == null)
            return typeof(string); // Default to string for nulls

        return value switch
        {
            int => typeof(int),
            long => typeof(long),
            double => typeof(double),
            float => typeof(float),
            decimal => typeof(decimal),
            bool => typeof(bool),
            DateTime => typeof(DateTime),
            DateTimeOffset => typeof(DateTimeOffset),
            byte[] => typeof(byte[]),
            _ => typeof(string)
        };
    }
}
