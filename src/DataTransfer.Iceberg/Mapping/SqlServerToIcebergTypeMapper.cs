using System.Data;

namespace DataTransfer.Iceberg.Mapping;

/// <summary>
/// Maps SQL Server data types to Iceberg primitive types
/// Reference: Iceberg specification type mapping
/// </summary>
public static class SqlServerToIcebergTypeMapper
{
    /// <summary>
    /// Maps SqlDbType to Iceberg type representation
    /// </summary>
    /// <param name="sqlType">SQL Server data type</param>
    /// <param name="precision">Optional precision for decimal types</param>
    /// <param name="scale">Optional scale for decimal types</param>
    /// <returns>Iceberg type (string for primitives, object for complex types)</returns>
    public static object MapType(SqlDbType sqlType, int? precision = null, int? scale = null)
    {
        return sqlType switch
        {
            // Integer types
            SqlDbType.BigInt => "long",
            SqlDbType.Int => "int",
            SqlDbType.SmallInt => "int",
            SqlDbType.TinyInt => "int",

            // Boolean
            SqlDbType.Bit => "boolean",

            // Floating point
            SqlDbType.Float => "double",
            SqlDbType.Real => "float",

            // Decimal - requires precision/scale object
            SqlDbType.Decimal => new { type = "decimal", precision = precision ?? 18, scale = scale ?? 0 },
            SqlDbType.Money => new { type = "decimal", precision = 19, scale = 4 },
            SqlDbType.SmallMoney => new { type = "decimal", precision = 10, scale = 4 },

            // Date/Time
            SqlDbType.Date => "date",
            SqlDbType.DateTime => "timestamp",
            SqlDbType.SmallDateTime => "timestamp",
            SqlDbType.DateTime2 => "timestamp",
            SqlDbType.DateTimeOffset => "timestamptz",
            SqlDbType.Time => "long", // Store as microseconds

            // String types
            SqlDbType.Char => "string",
            SqlDbType.NChar => "string",
            SqlDbType.VarChar => "string",
            SqlDbType.NVarChar => "string",
            SqlDbType.Text => "string",
            SqlDbType.NText => "string",

            // Binary
            SqlDbType.Binary => "binary",
            SqlDbType.VarBinary => "binary",
            SqlDbType.Image => "binary",

            // UUID
            SqlDbType.UniqueIdentifier => "uuid",

            _ => throw new NotSupportedException($"SQL type {sqlType} is not supported for Iceberg mapping")
        };
    }
}
