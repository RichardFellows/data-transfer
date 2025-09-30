using System.Data;
using System.Text.Json;
using DataTransfer.Core.Interfaces;
using DataTransfer.Core.Models;
using Microsoft.Data.SqlClient;

namespace DataTransfer.SqlServer;

public class SqlDataLoader : IDataLoader
{
    private readonly SqlQueryBuilder _queryBuilder;

    public SqlDataLoader(SqlQueryBuilder queryBuilder)
    {
        _queryBuilder = queryBuilder ?? throw new ArgumentNullException(nameof(queryBuilder));
    }

    public async Task<LoadResult> LoadAsync(
        TableConfiguration tableConfig,
        string connectionString,
        Stream inputStream,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(tableConfig);
        ArgumentNullException.ThrowIfNull(inputStream);

        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new ArgumentException("Connection string cannot be empty", nameof(connectionString));
        }

        var result = new LoadResult
        {
            StartTime = DateTime.UtcNow,
            Success = false
        };

        try
        {
            inputStream.Position = 0;

            var jsonData = await JsonDocument.ParseAsync(inputStream, cancellationToken: cancellationToken);
            var rows = jsonData.RootElement;

            if (rows.ValueKind != JsonValueKind.Array)
            {
                throw new InvalidOperationException("Input stream must contain a JSON array");
            }

            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken);

            // Get the first row to determine schema
            if (rows.GetArrayLength() == 0)
            {
                result.RowsLoaded = 0;
                result.Success = true;
                result.EndTime = DateTime.UtcNow;
                return result;
            }

            var firstRow = rows[0];
            var columns = firstRow.EnumerateObject().Select(p => p.Name).ToArray();

            // Create DataTable for bulk copy
            var dataTable = new DataTable();
            foreach (var column in columns)
            {
                dataTable.Columns.Add(column, typeof(object));
            }

            // Populate DataTable
            foreach (var row in rows.EnumerateArray())
            {
                var dataRow = dataTable.NewRow();

                foreach (var property in row.EnumerateObject())
                {
                    if (property.Value.ValueKind == JsonValueKind.Null)
                    {
                        dataRow[property.Name] = DBNull.Value;
                    }
                    else
                    {
                        dataRow[property.Name] = GetValue(property.Value);
                    }
                }

                dataTable.Rows.Add(dataRow);
            }

            // Bulk insert using SqlBulkCopy
            var destinationTable = $"[{tableConfig.Destination.Database}].[{tableConfig.Destination.Schema}].[{tableConfig.Destination.Table}]";

            using var bulkCopy = new SqlBulkCopy(connection)
            {
                DestinationTableName = destinationTable,
                BatchSize = 10000,
                BulkCopyTimeout = 300 // 5 minutes
            };

            foreach (var column in columns)
            {
                bulkCopy.ColumnMappings.Add(column, column);
            }

            await bulkCopy.WriteToServerAsync(dataTable, cancellationToken);

            result.RowsLoaded = dataTable.Rows.Count;
            result.Success = true;
        }
        catch (OperationCanceledException)
        {
            result.ErrorMessage = "Load was cancelled";
            throw;
        }
        catch (Exception ex)
        {
            result.ErrorMessage = ex.Message;
            throw;
        }
        finally
        {
            result.EndTime = DateTime.UtcNow;
        }

        return result;
    }

    private static object GetValue(JsonElement element)
    {
        return element.ValueKind switch
        {
            JsonValueKind.String => element.GetString() ?? string.Empty,
            JsonValueKind.Number => element.TryGetInt32(out var intVal) ? intVal :
                                   element.TryGetInt64(out var longVal) ? longVal :
                                   element.TryGetDecimal(out var decVal) ? decVal :
                                   element.GetDouble(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.Null => DBNull.Value,
            _ => element.GetRawText()
        };
    }
}
