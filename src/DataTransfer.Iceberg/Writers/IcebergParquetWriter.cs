using DataTransfer.Core.Models.Iceberg;
using DataTransfer.Iceberg.Models;
using ParquetSharp;
using ParquetSharp.Schema;

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
    /// Closes the writer and returns file metadata for manifest generation
    /// </summary>
    public DataFileMetadata Close()
    {
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
