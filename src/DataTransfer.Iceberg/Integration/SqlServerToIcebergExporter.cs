using System.Data;
using DataTransfer.Core.Models.Iceberg;
using DataTransfer.Iceberg.Catalog;
using DataTransfer.Iceberg.Mapping;
using DataTransfer.Iceberg.Models;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace DataTransfer.Iceberg.Integration;

/// <summary>
/// Exports SQL Server table data directly to Iceberg table format
/// </summary>
public class SqlServerToIcebergExporter
{
    private readonly FilesystemCatalog _catalog;
    private readonly ILogger<SqlServerToIcebergExporter> _logger;

    public SqlServerToIcebergExporter(FilesystemCatalog catalog, ILogger<SqlServerToIcebergExporter> logger)
    {
        _catalog = catalog;
        _logger = logger;
    }

    /// <summary>
    /// Exports a SQL Server table to Iceberg format
    /// </summary>
    /// <param name="connectionString">SQL Server connection string</param>
    /// <param name="sourceTableName">Source SQL Server table name</param>
    /// <param name="icebergTableName">Destination Iceberg table name</param>
    /// <param name="query">Optional custom query (if null, selects all from table)</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Result containing success status and metadata</returns>
    public async Task<IcebergWriteResult> ExportTableAsync(
        string connectionString,
        string sourceTableName,
        string icebergTableName,
        string? query = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            _logger.LogInformation(
                "Starting export from SQL Server table {SourceTable} to Iceberg table {IcebergTable}",
                sourceTableName,
                icebergTableName);

            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken);

            // Use provided query or default to SELECT *
            var sqlQuery = query ?? $"SELECT * FROM {sourceTableName}";

            await using var command = new SqlCommand(sqlQuery, connection)
            {
                CommandTimeout = 300 // 5 minutes
            };

            await using var reader = await command.ExecuteReaderAsync(cancellationToken);

            // Infer Iceberg schema from SQL Server schema
            var schema = InferIcebergSchemaFromReader(reader);

            // Read all data into memory (TODO: support streaming for large tables)
            var data = new List<Dictionary<string, object>>();
            while (await reader.ReadAsync(cancellationToken))
            {
                var row = new Dictionary<string, object>();
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    var fieldName = reader.GetName(i);
                    row[fieldName] = reader.IsDBNull(i) ? null! : reader.GetValue(i);
                }
                data.Add(row);
            }

            _logger.LogInformation("Read {RowCount} rows from SQL Server", data.Count);

            // Write to Iceberg
            var writerLogger = (ILogger<IcebergTableWriter>)(object)_logger;
            var writer = new IcebergTableWriter(_catalog, writerLogger);
            var result = await writer.WriteTableAsync(icebergTableName, schema, data, cancellationToken);

            if (result.Success)
            {
                _logger.LogInformation(
                    "Successfully exported {RowCount} rows to Iceberg table {IcebergTable}",
                    result.RecordCount,
                    icebergTableName);
            }

            return result;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to export SQL Server table {SourceTable}", sourceTableName);
            return new IcebergWriteResult
            {
                Success = false,
                ErrorMessage = $"Failed to export table: {ex.Message}"
            };
        }
    }

    /// <summary>
    /// Infers Iceberg schema from SqlDataReader metadata
    /// Assigns sequential field IDs starting at 1
    /// </summary>
    private IcebergSchema InferIcebergSchemaFromReader(SqlDataReader reader)
    {
        var schema = new IcebergSchema
        {
            SchemaId = 0,
            Type = "struct",
            Fields = new List<IcebergField>()
        };

        var schemaTable = reader.GetSchemaTable();
        if (schemaTable == null)
        {
            throw new InvalidOperationException("Could not retrieve schema from SqlDataReader");
        }

        int fieldId = 1;
        foreach (DataRow row in schemaTable.Rows)
        {
            var columnName = row["ColumnName"]?.ToString() ?? $"col_{fieldId}";
            var dataType = (Type?)row["DataType"];
            var allowNull = row["AllowDBNull"] != DBNull.Value && (bool)row["AllowDBNull"];

            if (dataType == null)
            {
                _logger.LogWarning("Skipping column {ColumnName} with null data type", columnName);
                continue;
            }

            // Map .NET type to SQL type for Iceberg mapping
            var sqlType = GetSqlDbType(dataType);
            var icebergType = SqlServerToIcebergTypeMapper.MapType(sqlType);

            schema.Fields.Add(new IcebergField
            {
                Id = fieldId++,
                Name = columnName,
                Required = !allowNull,
                Type = icebergType
            });
        }

        _logger.LogDebug("Inferred Iceberg schema with {FieldCount} fields", schema.Fields.Count);

        return schema;
    }

    /// <summary>
    /// Maps .NET Type to SqlDbType for Iceberg type mapping
    /// </summary>
    private SqlDbType GetSqlDbType(Type type)
    {
        return Type.GetTypeCode(type) switch
        {
            TypeCode.Boolean => SqlDbType.Bit,
            TypeCode.Byte => SqlDbType.TinyInt,
            TypeCode.Int16 => SqlDbType.SmallInt,
            TypeCode.Int32 => SqlDbType.Int,
            TypeCode.Int64 => SqlDbType.BigInt,
            TypeCode.Single => SqlDbType.Real,
            TypeCode.Double => SqlDbType.Float,
            TypeCode.Decimal => SqlDbType.Decimal,
            TypeCode.DateTime => SqlDbType.DateTime2,
            TypeCode.String => SqlDbType.NVarChar,
            _ => type == typeof(Guid) ? SqlDbType.UniqueIdentifier :
                 type == typeof(byte[]) ? SqlDbType.VarBinary :
                 type == typeof(DateTimeOffset) ? SqlDbType.DateTimeOffset :
                 SqlDbType.NVarChar // Default to string
        };
    }
}
