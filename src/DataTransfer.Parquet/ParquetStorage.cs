using System.Text.Json;
using DataTransfer.Core.Interfaces;
using Parquet;
using Parquet.Data;
using Parquet.Schema;

namespace DataTransfer.Parquet;

public class ParquetStorage : IParquetStorage
{
    private readonly string _basePath;

    public ParquetStorage(string basePath)
    {
        if (string.IsNullOrWhiteSpace(basePath))
        {
            throw new ArgumentException("Base path cannot be empty", nameof(basePath));
        }

        _basePath = basePath;
    }

    public async Task WriteAsync(
        Stream dataStream,
        string filePath,
        DateTime partitionDate,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(dataStream);

        if (string.IsNullOrWhiteSpace(filePath))
        {
            throw new ArgumentException("File path cannot be empty", nameof(filePath));
        }

        cancellationToken.ThrowIfCancellationRequested();

        // Build partition path: year=YYYY/month=MM/day=DD
        var partitionPath = $"year={partitionDate.Year:D4}/month={partitionDate.Month:D2}/day={partitionDate.Day:D2}";
        var fullDirectoryPath = Path.Combine(_basePath, partitionPath);
        var fullFilePath = Path.Combine(fullDirectoryPath, filePath);

        // Create directory if it doesn't exist
        Directory.CreateDirectory(fullDirectoryPath);

        // Parse JSON data
        dataStream.Position = 0;
        var jsonDocument = await JsonDocument.ParseAsync(dataStream, cancellationToken: cancellationToken);
        var rows = jsonDocument.RootElement;

        if (rows.ValueKind != JsonValueKind.Array)
        {
            throw new InvalidOperationException("Input data must be a JSON array");
        }

        if (rows.GetArrayLength() == 0)
        {
            // Create empty file
            await File.WriteAllTextAsync(fullFilePath, string.Empty, cancellationToken);
            return;
        }

        // Infer schema and collect column data
        var (schema, columnData) = InferSchemaAndCollectData(rows);

        // Create DataColumn objects with properly typed arrays
        var dataColumns = CreateDataColumns(schema.GetDataFields(), columnData);

        // Write to Parquet file with compression
        await using var fileStream = File.Create(fullFilePath);
        await using var parquetWriter = await global::Parquet.ParquetWriter.CreateAsync(schema, fileStream, cancellationToken: cancellationToken);
        parquetWriter.CompressionMethod = CompressionMethod.Snappy;

        using var rowGroup = parquetWriter.CreateRowGroup();
        foreach (var column in dataColumns)
        {
            await rowGroup.WriteColumnAsync(column, cancellationToken);
        }
    }

    public async Task<Stream> ReadAsync(string filePath, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(filePath))
        {
            throw new ArgumentException("File path cannot be empty", nameof(filePath));
        }

        var fullFilePath = Path.Combine(_basePath, filePath);

        if (!File.Exists(fullFilePath))
        {
            throw new FileNotFoundException($"Parquet file not found: {fullFilePath}", fullFilePath);
        }

        cancellationToken.ThrowIfCancellationRequested();

        // Check if file is empty (created for 0 rows)
        var fileInfo = new FileInfo(fullFilePath);
        if (fileInfo.Length == 0)
        {
            // Return empty JSON array
            var emptyMemoryStream = new MemoryStream();
            await JsonSerializer.SerializeAsync(emptyMemoryStream, new List<Dictionary<string, object?>>(), cancellationToken: cancellationToken);
            emptyMemoryStream.Position = 0;
            return emptyMemoryStream;
        }

        // Read Parquet file
        await using var fileStream = File.OpenRead(fullFilePath);
        using var parquetReader = await global::Parquet.ParquetReader.CreateAsync(fileStream, cancellationToken: cancellationToken);

        // Read all row groups
        var jsonArray = new List<Dictionary<string, object?>>();

        for (int i = 0; i < parquetReader.RowGroupCount; i++)
        {
            using var rowGroupReader = parquetReader.OpenRowGroupReader(i);
            var dataFields = parquetReader.Schema.GetDataFields();

            // Read all columns
            var columnData = new Dictionary<string, Array>();
            foreach (var field in dataFields)
            {
                var column = await rowGroupReader.ReadColumnAsync(field, cancellationToken);
                columnData[field.Name] = column.Data;
            }

            // Convert to JSON objects
            if (columnData.Values.Any())
            {
                var rowCount = columnData.Values.First().Length;
                for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
                {
                    var row = new Dictionary<string, object?>();
                    foreach (var field in dataFields)
                    {
                        var value = columnData[field.Name].GetValue(rowIndex);
                        row[field.Name] = value;
                    }
                    jsonArray.Add(row);
                }
            }
        }

        // Convert to JSON stream
        var memoryStream = new MemoryStream();
        await JsonSerializer.SerializeAsync(memoryStream, jsonArray, cancellationToken: cancellationToken);
        memoryStream.Position = 0;
        return memoryStream;
    }

    private static (ParquetSchema schema, Dictionary<string, List<object?>> columnData) InferSchemaAndCollectData(JsonElement rows)
    {
        // Get schema from first row
        var firstRow = rows[0];
        var fields = new List<DataField>();
        var columnData = new Dictionary<string, List<object?>>();

        foreach (var property in firstRow.EnumerateObject())
        {
            var fieldType = InferDataFieldType(property.Value);
            fields.Add(new DataField(property.Name, fieldType));
            columnData[property.Name] = new List<object?>();
        }

        var schema = new ParquetSchema(fields);

        // Collect data for each column
        foreach (var row in rows.EnumerateArray())
        {
            foreach (var property in row.EnumerateObject())
            {
                if (columnData.ContainsKey(property.Name))
                {
                    columnData[property.Name].Add(GetValue(property.Value));
                }
            }
        }

        return (schema, columnData);
    }

    private static List<DataColumn> CreateDataColumns(DataField[] fields, Dictionary<string, List<object?>> columnData)
    {
        var dataColumns = new List<DataColumn>();
        foreach (var field in fields)
        {
            var values = columnData[field.Name];
            var typedArray = ConvertToTypedArray(values, field.ClrType);
            dataColumns.Add(new DataColumn(field, typedArray));
        }
        return dataColumns;
    }

    private static Type InferDataFieldType(JsonElement element)
    {
        return element.ValueKind switch
        {
            JsonValueKind.String => typeof(string),
            JsonValueKind.Number => element.TryGetInt32(out _) ? typeof(int) :
                                   element.TryGetInt64(out _) ? typeof(long) :
                                   typeof(double),
            JsonValueKind.True or JsonValueKind.False => typeof(bool),
            JsonValueKind.Null => typeof(string),
            _ => typeof(string)
        };
    }

    private static object? GetValue(JsonElement element)
    {
        return element.ValueKind switch
        {
            JsonValueKind.String => element.GetString(),
            JsonValueKind.Number => element.TryGetInt32(out var intVal) ? intVal :
                                   element.TryGetInt64(out var longVal) ? (object)longVal :
                                   element.GetDouble(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.Null => null,
            _ => element.GetRawText()
        };
    }

    private static Array ConvertToTypedArray(List<object?> values, Type targetType)
    {
        if (targetType == typeof(int))
        {
            return values.Select(v => v == null ? 0 : (int)v).ToArray();
        }
        else if (targetType == typeof(long))
        {
            return values.Select(v => v == null ? 0L : (long)v).ToArray();
        }
        else if (targetType == typeof(double))
        {
            return values.Select(v => v == null ? 0.0 : (double)v).ToArray();
        }
        else if (targetType == typeof(bool))
        {
            return values.Select(v => v == null ? false : (bool)v).ToArray();
        }
        else if (targetType == typeof(string))
        {
            return values.Select(v => v as string).ToArray();
        }
        else
        {
            return values.ToArray();
        }
    }
}
