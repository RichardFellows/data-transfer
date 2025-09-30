using System.Data;
using System.Text.Json;
using DataTransfer.Core.Interfaces;
using DataTransfer.Core.Models;
using DataTransfer.Core.Strategies;
using Microsoft.Data.SqlClient;

namespace DataTransfer.SqlServer;

public class SqlTableExtractor : ITableExtractor
{
    private readonly SqlQueryBuilder _queryBuilder;

    public SqlTableExtractor(SqlQueryBuilder queryBuilder)
    {
        _queryBuilder = queryBuilder ?? throw new ArgumentNullException(nameof(queryBuilder));
    }

    public async Task<ExtractionResult> ExtractAsync(
        TableConfiguration tableConfig,
        string connectionString,
        Stream outputStream,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(tableConfig);
        ArgumentNullException.ThrowIfNull(outputStream);

        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new ArgumentException("Connection string cannot be empty", nameof(connectionString));
        }

        var result = new ExtractionResult
        {
            StartTime = DateTime.UtcNow,
            Success = false
        };

        try
        {
            var strategy = PartitionStrategyFactory.Create(tableConfig.Partitioning);
            var startDate = tableConfig.ExtractSettings.DateRange.StartDate;
            var endDate = tableConfig.ExtractSettings.DateRange.EndDate;

            var query = _queryBuilder.BuildSelectQuery(tableConfig, strategy, startDate, endDate);

            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken);

            await using var command = new SqlCommand(query, connection)
            {
                CommandTimeout = 300 // 5 minutes
            };

            await using var reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken);

            long rowCount = 0;
            await using var writer = new StreamWriter(outputStream, leaveOpen: true);
            await using var jsonWriter = new Utf8JsonWriter(outputStream, new JsonWriterOptions
            {
                Indented = false
            });

            jsonWriter.WriteStartArray();

            while (await reader.ReadAsync(cancellationToken))
            {
                cancellationToken.ThrowIfCancellationRequested();

                jsonWriter.WriteStartObject();

                for (int i = 0; i < reader.FieldCount; i++)
                {
                    var fieldName = reader.GetName(i);
                    jsonWriter.WritePropertyName(fieldName);

                    if (reader.IsDBNull(i))
                    {
                        jsonWriter.WriteNullValue();
                    }
                    else
                    {
                        WriteValue(jsonWriter, reader, i);
                    }
                }

                jsonWriter.WriteEndObject();
                rowCount++;
            }

            jsonWriter.WriteEndArray();
            await jsonWriter.FlushAsync(cancellationToken);

            result.RowsExtracted = rowCount;
            result.Success = true;
        }
        catch (OperationCanceledException)
        {
            result.ErrorMessage = "Extraction was cancelled";
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

    private static void WriteValue(Utf8JsonWriter writer, SqlDataReader reader, int ordinal)
    {
        var value = reader.GetValue(ordinal);
        var fieldType = reader.GetFieldType(ordinal);

        if (fieldType == typeof(string))
        {
            writer.WriteStringValue(reader.GetString(ordinal));
        }
        else if (fieldType == typeof(int))
        {
            writer.WriteNumberValue(reader.GetInt32(ordinal));
        }
        else if (fieldType == typeof(long))
        {
            writer.WriteNumberValue(reader.GetInt64(ordinal));
        }
        else if (fieldType == typeof(decimal))
        {
            writer.WriteNumberValue(reader.GetDecimal(ordinal));
        }
        else if (fieldType == typeof(double))
        {
            writer.WriteNumberValue(reader.GetDouble(ordinal));
        }
        else if (fieldType == typeof(float))
        {
            writer.WriteNumberValue(reader.GetFloat(ordinal));
        }
        else if (fieldType == typeof(bool))
        {
            writer.WriteBooleanValue(reader.GetBoolean(ordinal));
        }
        else if (fieldType == typeof(DateTime))
        {
            writer.WriteStringValue(reader.GetDateTime(ordinal).ToString("O"));
        }
        else if (fieldType == typeof(Guid))
        {
            writer.WriteStringValue(reader.GetGuid(ordinal).ToString());
        }
        else
        {
            writer.WriteStringValue(value?.ToString() ?? string.Empty);
        }
    }
}
