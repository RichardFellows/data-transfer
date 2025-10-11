using DataTransfer.Core.Models.Iceberg;
using Microsoft.Extensions.Logging;
using ParquetSharp;
using System.Runtime.CompilerServices;
using System.Text.Json;

namespace DataTransfer.Iceberg.Readers;

/// <summary>
/// Reads Parquet data files and reconstructs rows from columnar storage
/// </summary>
public class IcebergParquetReader
{
    private readonly ILogger<IcebergParquetReader> _logger;

    public IcebergParquetReader(ILogger<IcebergParquetReader> logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// Reads all rows from a Parquet file as dictionaries
    /// Streams data efficiently without loading entire file into memory
    /// </summary>
    /// <param name="filePath">Path to Parquet file</param>
    /// <param name="schema">Iceberg schema for the table</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Async enumerable of rows as dictionaries</returns>
    public async IAsyncEnumerable<Dictionary<string, object>> ReadAsync(
        string filePath,
        IcebergSchema schema,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        using var reader = new ParquetFileReader(filePath);
        var rowGroupCount = reader.FileMetaData.NumRowGroups;

        _logger.LogDebug("Reading Parquet file {Path} with {RowGroups} row groups",
            filePath, rowGroupCount);

        for (int rg = 0; rg < rowGroupCount; rg++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            using var rowGroupReader = reader.RowGroup(rg);
            var rowCount = rowGroupReader.MetaData.NumRows;

            // Read all columns into arrays (columnar storage)
            var columnData = new List<Array>();
            for (int colIndex = 0; colIndex < schema.Fields.Count; colIndex++)
            {
                var field = schema.Fields[colIndex];
                using var columnReader = rowGroupReader.Column(colIndex);
                var values = ReadColumnValues(columnReader, field, rowCount);
                columnData.Add(values);
            }

            // Reconstruct rows from columnar data
            for (long rowIndex = 0; rowIndex < rowCount; rowIndex++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var row = new Dictionary<string, object>();
                for (int colIndex = 0; colIndex < schema.Fields.Count; colIndex++)
                {
                    var fieldName = schema.Fields[colIndex].Name;
                    var value = columnData[colIndex].GetValue(rowIndex);
                    row[fieldName] = value!;
                }

                yield return row;
            }
        }

        await Task.CompletedTask; // Satisfy async method signature
    }

    /// <summary>
    /// Reads all values from a column based on Iceberg field type
    /// </summary>
    private Array ReadColumnValues(ColumnReader columnReader, IcebergField field, long rowCount)
    {
        var fieldType = field.Type;

        // Handle JsonElement from deserialization
        if (fieldType is JsonElement jsonElement &&
            jsonElement.ValueKind == JsonValueKind.String)
        {
            fieldType = jsonElement.GetString()!;
        }

        if (fieldType is not string primitiveType)
        {
            throw new NotSupportedException($"Complex types not yet supported: {fieldType}");
        }

        switch (primitiveType)
        {
            case "boolean":
                return field.Required
                    ? columnReader.LogicalReader<bool>().ReadAll((int)rowCount)
                    : columnReader.LogicalReader<bool?>().ReadAll((int)rowCount);

            case "int":
                return field.Required
                    ? columnReader.LogicalReader<int>().ReadAll((int)rowCount)
                    : columnReader.LogicalReader<int?>().ReadAll((int)rowCount);

            case "long":
                return field.Required
                    ? columnReader.LogicalReader<long>().ReadAll((int)rowCount)
                    : columnReader.LogicalReader<long?>().ReadAll((int)rowCount);

            case "float":
                return field.Required
                    ? columnReader.LogicalReader<float>().ReadAll((int)rowCount)
                    : columnReader.LogicalReader<float?>().ReadAll((int)rowCount);

            case "double":
                return field.Required
                    ? columnReader.LogicalReader<double>().ReadAll((int)rowCount)
                    : columnReader.LogicalReader<double?>().ReadAll((int)rowCount);

            case "string":
                // Strings are always nullable in Parquet (reference type)
                return columnReader.LogicalReader<string>().ReadAll((int)rowCount);

            case "date":
                return field.Required
                    ? columnReader.LogicalReader<DateTime>().ReadAll((int)rowCount)
                    : columnReader.LogicalReader<DateTime?>().ReadAll((int)rowCount);

            case "timestamp":
            case "timestamptz":
                return field.Required
                    ? columnReader.LogicalReader<DateTime>().ReadAll((int)rowCount)
                    : columnReader.LogicalReader<DateTime?>().ReadAll((int)rowCount);

            case "binary":
                return columnReader.LogicalReader<byte[]>().ReadAll((int)rowCount);

            case "uuid":
                // UUIDs stored as FixedLenByteArray(16)
                var byteArrays = columnReader.LogicalReader<byte[]>().ReadAll((int)rowCount);
                var guids = new object[byteArrays.Length];
                for (int i = 0; i < byteArrays.Length; i++)
                {
                    guids[i] = byteArrays[i] != null ? new Guid(byteArrays[i]) : null!;
                }
                return guids;

            default:
                throw new NotSupportedException($"Iceberg type {primitiveType} is not supported for Parquet reading");
        }
    }
}
