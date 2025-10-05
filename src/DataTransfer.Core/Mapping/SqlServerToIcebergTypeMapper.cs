using System.Data;

namespace DataTransfer.Core.Mapping;

/// <summary>
/// Maps SQL Server data types to Iceberg primitive types
/// Reference: Apache Iceberg Type System
/// </summary>
public static class SqlServerToIcebergTypeMapper
{
    /// <summary>
    /// Maps a SQL Server data type to its corresponding Iceberg type
    /// </summary>
    /// <param name="sqlType">SQL Server data type</param>
    /// <param name="precision">Precision for decimal types (optional)</param>
    /// <param name="scale">Scale for decimal types (optional)</param>
    /// <returns>Iceberg type representation (string for primitives, object for complex types)</returns>
    /// <exception cref="NotSupportedException">Thrown when the SQL type is not supported</exception>
    public static object MapType(SqlDbType sqlType, int? precision = null, int? scale = null)
    {
        return sqlType switch
        {
            // Integer types
            SqlDbType.BigInt => "long",
            SqlDbType.Int => "int",
            SqlDbType.SmallInt => "int",      // Fits in int32
            SqlDbType.TinyInt => "int",       // Fits in int32

            // Boolean
            SqlDbType.Bit => "boolean",

            // Floating point
            SqlDbType.Float => "double",      // SQL Server Float is 64-bit (double precision)
            SqlDbType.Real => "float",        // SQL Server Real is 32-bit (single precision)

            // Decimal - requires precision/scale object
            SqlDbType.Decimal => new { type = "decimal", precision = precision ?? 18, scale = scale ?? 0 },
            SqlDbType.Money => new { type = "decimal", precision = precision ?? 19, scale = scale ?? 4 },
            SqlDbType.SmallMoney => new { type = "decimal", precision = precision ?? 10, scale = scale ?? 4 },

            // Date/Time types
            SqlDbType.Date => "date",
            SqlDbType.DateTime => "timestamp",
            SqlDbType.SmallDateTime => "timestamp",
            SqlDbType.DateTime2 => "timestamp",
            SqlDbType.DateTimeOffset => "timestamptz",

            // String types
            SqlDbType.Char => "string",
            SqlDbType.NChar => "string",
            SqlDbType.VarChar => "string",
            SqlDbType.NVarChar => "string",
            SqlDbType.Text => "string",
            SqlDbType.NText => "string",

            // Binary types
            SqlDbType.Binary => "binary",
            SqlDbType.VarBinary => "binary",
            SqlDbType.Image => "binary",

            // UUID
            SqlDbType.UniqueIdentifier => "uuid",

            // Unsupported types
            SqlDbType.Timestamp => throw new NotSupportedException(
                $"SQL type {sqlType} is not supported for Iceberg mapping. " +
                "Timestamp is a SQL Server internal type and should not be used for data storage."),

            SqlDbType.Variant => throw new NotSupportedException(
                $"SQL type {sqlType} is not supported for Iceberg mapping. " +
                "Sql_variant is a SQL Server-specific type with no Iceberg equivalent."),

            SqlDbType.Xml => throw new NotSupportedException(
                $"SQL type {sqlType} is not supported for Iceberg mapping. " +
                "Consider storing XML as string type."),

            SqlDbType.Udt => throw new NotSupportedException(
                $"SQL type {sqlType} is not supported for Iceberg mapping. " +
                "User-defined types must be mapped individually."),

            SqlDbType.Structured => throw new NotSupportedException(
                $"SQL type {sqlType} is not supported for Iceberg mapping. " +
                "Table-valued parameters are not supported."),

            _ => throw new NotSupportedException(
                $"SQL type {sqlType} is not supported for Iceberg mapping")
        };
    }
}
