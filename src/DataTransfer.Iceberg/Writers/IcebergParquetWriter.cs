using DataTransfer.Core.Models.Iceberg;
using DataTransfer.Iceberg.Models;
using ParquetSharp;
using ParquetSharp.Schema;
using System.Text.Json;

namespace DataTransfer.Iceberg.Writers;

/// <summary>
/// Writes Parquet files with Iceberg-compliant schema (embedded field-ids)
/// Uses ParquetSharp low-level GroupNode API to inject field-id metadata
/// </summary>
public class IcebergParquetWriter : IDisposable
{
    private readonly ParquetFileWriter _writer;
    private readonly IcebergSchema _schema;
    private readonly string _filePath;
    private long _recordCount;
    private readonly List<object[]> _buffer;
    private const int DefaultBufferSize = 1000; // Rows to buffer before writing

    /// <summary>
    /// Creates a new Iceberg-compliant Parquet writer
    /// </summary>
    /// <param name="path">Path to the output Parquet file</param>
    /// <param name="schema">Iceberg schema with field-ids</param>
    public IcebergParquetWriter(string path, IcebergSchema schema)
    {
        _schema = schema;
        _filePath = path;
        _recordCount = 0;
        _buffer = new List<object[]>();

        // CRITICAL: Use GroupNode to embed field-id metadata
        var groupNode = BuildIcebergCompliantSchema(schema);

        // Create writer properties with Snappy compression (Iceberg default)
        using var propertiesBuilder = new WriterPropertiesBuilder();
        propertiesBuilder.Compression(Compression.Snappy);
        using var writerProperties = propertiesBuilder.Build();

        _writer = new ParquetFileWriter(path, groupNode, writerProperties);
    }

    /// <summary>
    /// Builds Parquet schema with mandatory Iceberg field-id annotations
    /// This is the key to Iceberg compatibility
    /// </summary>
    private GroupNode BuildIcebergCompliantSchema(IcebergSchema schema)
    {
        var nodes = new List<Node>();

        foreach (var field in schema.Fields)
        {
            var parquetType = MapIcebergTypeToParquetType(field.Type, out var logicalType, out var typeLength);
            var repetition = field.Required ? Repetition.Required : Repetition.Optional;

            // CRITICAL: PrimitiveNode constructor that accepts fieldId parameter
            // This is what enables Iceberg schema evolution
            Node node = new PrimitiveNode(
                name: field.Name,
                repetition: repetition,
                logicalType: logicalType ?? LogicalType.None(),
                physicalType: parquetType,
                primitiveLength: typeLength,
                fieldId: field.Id  // ← This is what enables Iceberg schema evolution
            );

            nodes.Add(node);
        }

        return new GroupNode("schema", Repetition.Required, nodes.ToArray());
    }

    /// <summary>
    /// Maps Iceberg types to ParquetSharp physical types
    /// </summary>
    /// <param name="icebergType">Iceberg type (string or object for complex types)</param>
    /// <param name="logicalType">Output logical type annotation (if applicable)</param>
    /// <param name="typeLength">Output type length for FixedLenByteArray</param>
    /// <returns>Parquet physical type</returns>
    private PhysicalType MapIcebergTypeToParquetType(object icebergType, out LogicalType? logicalType, out int typeLength)
    {
        logicalType = null;
        typeLength = -1;

        // Handle JsonElement from deserialization
        if (icebergType is JsonElement jsonElement)
        {
            if (jsonElement.ValueKind == JsonValueKind.String)
            {
                // Primitive type stored as string
                icebergType = jsonElement.GetString()!;
            }
            // Complex types would be objects, handle them below
        }

        if (icebergType is string primitiveType)
        {
            switch (primitiveType)
            {
                case "boolean":
                    return PhysicalType.Boolean;

                case "int":
                    return PhysicalType.Int32;

                case "long":
                    return PhysicalType.Int64;

                case "float":
                    return PhysicalType.Float;

                case "double":
                    return PhysicalType.Double;

                case "string":
                    logicalType = LogicalType.String();
                    return PhysicalType.ByteArray;

                case "binary":
                    return PhysicalType.ByteArray;

                case "uuid":
                    logicalType = LogicalType.Uuid();
                    typeLength = 16;
                    return PhysicalType.FixedLenByteArray;

                case "date":
                    logicalType = LogicalType.Date();
                    return PhysicalType.Int32;  // Days since epoch

                case "timestamp":
                    logicalType = LogicalType.Timestamp(true, TimeUnit.Micros);  // Microseconds, UTC
                    return PhysicalType.Int64;

                case "timestamptz":
                    logicalType = LogicalType.Timestamp(true, TimeUnit.Micros);  // Microseconds, UTC
                    return PhysicalType.Int64;

                default:
                    throw new NotSupportedException($"Iceberg type {primitiveType} is not supported for Parquet mapping");
            }
        }

        // Handle complex types (like decimal)
        var complexType = icebergType.GetType();
        var typeProperty = complexType.GetProperty("type")?.GetValue(icebergType) as string;

        if (typeProperty == "decimal")
        {
            var precision = (int)(complexType.GetProperty("precision")?.GetValue(icebergType) ?? 18);
            var scale = (int)(complexType.GetProperty("scale")?.GetValue(icebergType) ?? 0);

            logicalType = LogicalType.Decimal(precision, scale);

            // Determine physical type based on precision
            // Parquet uses different physical types based on precision requirements
            if (precision <= 9)
            {
                return PhysicalType.Int32;
            }
            else if (precision <= 18)
            {
                return PhysicalType.Int64;
            }
            else
            {
                // Use fixed-length byte array for high precision decimals
                typeLength = (int)Math.Ceiling((precision * Math.Log(10) + Math.Log(2)) / Math.Log(256));
                return PhysicalType.FixedLenByteArray;
            }
        }

        throw new NotSupportedException($"Iceberg complex type {icebergType} is not supported for Parquet mapping");
    }

    /// <summary>
    /// Writes a single row of data to the Parquet file
    /// Buffers rows and flushes when buffer is full
    /// </summary>
    /// <param name="values">Array of values matching schema field order</param>
    public void WriteRow(object[] values)
    {
        if (values.Length != _schema.Fields.Count)
        {
            throw new ArgumentException(
                $"Value count ({values.Length}) does not match schema field count ({_schema.Fields.Count})");
        }

        _buffer.Add(values);
        _recordCount++;

        // Flush buffer when it reaches the threshold
        if (_buffer.Count >= DefaultBufferSize)
        {
            FlushBuffer();
        }
    }

    /// <summary>
    /// Flushes buffered rows to Parquet file as a row group
    /// </summary>
    private void FlushBuffer()
    {
        if (_buffer.Count == 0)
            return;

        using var rowGroup = _writer.AppendRowGroup();

        // Write each column's data
        for (int colIndex = 0; colIndex < _schema.Fields.Count; colIndex++)
        {
            var field = _schema.Fields[colIndex];
            using var columnWriter = rowGroup.NextColumn();

            // Extract column values from all buffered rows
            var columnValues = new object[_buffer.Count];
            for (int rowIndex = 0; rowIndex < _buffer.Count; rowIndex++)
            {
                columnValues[rowIndex] = _buffer[rowIndex][colIndex];
            }

            // Write column data based on type
            WriteColumnBatch(columnWriter, field, columnValues);
        }

        _buffer.Clear();
    }

    /// <summary>
    /// Writes a batch of values for a single column
    /// </summary>
    private void WriteColumnBatch(ColumnWriter columnWriter, IcebergField field, object[] values)
    {
        var fieldType = field.Type;

        // Handle JsonElement from deserialization
        if (fieldType is JsonElement jsonElement && jsonElement.ValueKind == JsonValueKind.String)
        {
            fieldType = jsonElement.GetString()!;
        }

        if (fieldType is string primitiveType)
        {
            switch (primitiveType)
            {
                case "boolean":
                    if (field.Required)
                    {
                        var boolWriter = columnWriter.LogicalWriter<bool>();
                        boolWriter.WriteBatch(values.Select(v => Convert.ToBoolean(v)).ToArray());
                    }
                    else
                    {
                        var nullableBoolWriter = columnWriter.LogicalWriter<bool?>();
                        nullableBoolWriter.WriteBatch(values.Select(v => v != null ? Convert.ToBoolean(v) : (bool?)null).ToArray());
                    }
                    break;

                case "int":
                    if (field.Required)
                    {
                        var intWriter = columnWriter.LogicalWriter<int>();
                        intWriter.WriteBatch(values.Select(v => Convert.ToInt32(v)).ToArray());
                    }
                    else
                    {
                        var nullableIntWriter = columnWriter.LogicalWriter<int?>();
                        nullableIntWriter.WriteBatch(values.Select(v => v != null ? Convert.ToInt32(v) : (int?)null).ToArray());
                    }
                    break;

                case "long":
                    if (field.Required)
                    {
                        var longWriter = columnWriter.LogicalWriter<long>();
                        longWriter.WriteBatch(values.Select(v => Convert.ToInt64(v)).ToArray());
                    }
                    else
                    {
                        var nullableLongWriter = columnWriter.LogicalWriter<long?>();
                        nullableLongWriter.WriteBatch(values.Select(v => v != null ? Convert.ToInt64(v) : (long?)null).ToArray());
                    }
                    break;

                case "float":
                    if (field.Required)
                    {
                        var floatWriter = columnWriter.LogicalWriter<float>();
                        floatWriter.WriteBatch(values.Select(v => Convert.ToSingle(v)).ToArray());
                    }
                    else
                    {
                        var nullableFloatWriter = columnWriter.LogicalWriter<float?>();
                        nullableFloatWriter.WriteBatch(values.Select(v => v != null ? Convert.ToSingle(v) : (float?)null).ToArray());
                    }
                    break;

                case "double":
                    if (field.Required)
                    {
                        var doubleWriter = columnWriter.LogicalWriter<double>();
                        doubleWriter.WriteBatch(values.Select(v => Convert.ToDouble(v)).ToArray());
                    }
                    else
                    {
                        var nullableDoubleWriter = columnWriter.LogicalWriter<double?>();
                        nullableDoubleWriter.WriteBatch(values.Select(v => v != null ? Convert.ToDouble(v) : (double?)null).ToArray());
                    }
                    break;

                case "date":
                    var dateWriter = columnWriter.LogicalWriter<int>();
                    var dates = values.Select(v =>
                    {
                        if (v == null) return 0;
                        var dt = v is DateTime dateTime ? dateTime : Convert.ToDateTime(v);
                        return (int)(dt.Date - new DateTime(1970, 1, 1)).TotalDays;
                    }).ToArray();
                    dateWriter.WriteBatch(dates);
                    break;

                case "timestamp":
                case "timestamptz":
                    var timestampWriter = columnWriter.LogicalWriter<DateTime>();
                    var timestamps = values.Select(v =>
                    {
                        if (v == null) return DateTime.MinValue;
                        if (v is DateTime dt) return dt.ToUniversalTime();
                        if (v is DateTimeOffset dto) return dto.UtcDateTime;
                        return Convert.ToDateTime(v).ToUniversalTime();
                    }).ToArray();
                    timestampWriter.WriteBatch(timestamps);
                    break;

                case "string":
                    var stringWriter = columnWriter.LogicalWriter<string>();
                    stringWriter.WriteBatch(values.Select(v => v?.ToString() ?? string.Empty).ToArray());
                    break;

                case "binary":
                    var binaryWriter = columnWriter.LogicalWriter<byte[]>();
                    binaryWriter.WriteBatch(values.Select(v => v as byte[] ?? Array.Empty<byte>()).ToArray());
                    break;

                case "uuid":
                    var uuidWriter = columnWriter.LogicalWriter<byte[]>();
                    var uuids = values.Select(v =>
                    {
                        if (v == null) return new byte[16];
                        return v is Guid guid ? guid.ToByteArray() : Guid.Empty.ToByteArray();
                    }).ToArray();
                    uuidWriter.WriteBatch(uuids);
                    break;

                default:
                    throw new NotSupportedException($"Writing values of type {primitiveType} is not yet supported");
            }
        }
        else
        {
            // Handle complex types like decimal
            throw new NotSupportedException($"Writing complex types is not yet supported");
        }
    }

    /// <summary>
    /// Closes the writer and returns file metadata for manifest generation
    /// Flushes any remaining buffered data before closing
    /// </summary>
    public DataFileMetadata Close()
    {
        // Flush any remaining buffered data
        FlushBuffer();

        _writer.Close();

        var fileInfo = new FileInfo(_filePath);
        return new DataFileMetadata
        {
            FilePath = _filePath,
            FileSizeInBytes = fileInfo.Length,
            RecordCount = _recordCount
        };
    }

    public void Dispose()
    {
        _writer?.Dispose();
    }
}
