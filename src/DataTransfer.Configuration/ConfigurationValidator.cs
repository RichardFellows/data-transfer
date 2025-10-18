using DataTransfer.Core.Models;

namespace DataTransfer.Configuration;

public class ConfigurationValidator
{
    public ValidationResult Validate(DataTransferConfiguration config)
    {
        var result = new ValidationResult();

        ValidateConnections(config.Connections, result);
        ValidateTables(config.Tables, result);
        ValidateStorage(config.Storage, result);

        return result;
    }

    private void ValidateConnections(ConnectionConfiguration connections, ValidationResult result)
    {
        if (string.IsNullOrWhiteSpace(connections.Source))
        {
            result.AddError("Source connection string is required");
        }

        if (string.IsNullOrWhiteSpace(connections.Destination))
        {
            result.AddError("Destination connection string is required");
        }
    }

    private void ValidateTables(List<TableConfiguration> tables, ValidationResult result)
    {
        if (tables.Count == 0)
        {
            result.AddError("At least one table configuration is required");
            return;
        }

        for (int i = 0; i < tables.Count; i++)
        {
            ValidateTable(tables[i], i, result);
        }
    }

    private void ValidateTable(TableConfiguration table, int index, ValidationResult result)
    {
        var tablePrefix = $"Table[{index}]";

        // Validate source
        if (string.IsNullOrWhiteSpace(table.Source.Database))
        {
            result.AddError($"{tablePrefix}: Source database is required");
        }

        if (string.IsNullOrWhiteSpace(table.Source.Schema))
        {
            result.AddError($"{tablePrefix}: Source schema is required");
        }

        if (string.IsNullOrWhiteSpace(table.Source.Table))
        {
            result.AddError($"{tablePrefix}: Source table is required");
        }

        // Validate destination
        if (string.IsNullOrWhiteSpace(table.Destination.Database))
        {
            result.AddError($"{tablePrefix}: Destination database is required");
        }

        if (string.IsNullOrWhiteSpace(table.Destination.Schema))
        {
            result.AddError($"{tablePrefix}: Destination schema is required");
        }

        if (string.IsNullOrWhiteSpace(table.Destination.Table))
        {
            result.AddError($"{tablePrefix}: Destination table is required");
        }

        // Validate partitioning
        ValidatePartitioning(table.Partitioning, tablePrefix, result);

        // Validate extract settings
        ValidateExtractSettings(table.ExtractSettings, tablePrefix, result);
    }

    private void ValidatePartitioning(PartitioningConfiguration partitioning, string tablePrefix, ValidationResult result)
    {
        // Date, IntDate, and Scd2 require a column
        if (partitioning.Type == PartitionType.Date ||
            partitioning.Type == PartitionType.IntDate ||
            partitioning.Type == PartitionType.Scd2)
        {
            if (string.IsNullOrWhiteSpace(partitioning.Column))
            {
                result.AddError($"{tablePrefix}: Partition column is required for {partitioning.Type} partition type");
            }
        }

        // IntDate has a default format, so we don't need to validate it
        // Scd2 uses Format for expiration column, which has a default
    }

    private void ValidateExtractSettings(ExtractSettings extractSettings, string tablePrefix, ValidationResult result)
    {
        if (extractSettings == null)
        {
            return; // ExtractSettings is optional
        }

        // Validate RowLimit must be positive if specified
        if (extractSettings.RowLimit.HasValue && extractSettings.RowLimit.Value <= 0)
        {
            result.AddError($"{tablePrefix}: RowLimit must be greater than zero (got {extractSettings.RowLimit.Value})");
        }

        // Validate WhereClause doesn't contain dangerous SQL keywords (basic SQL injection prevention)
        if (!string.IsNullOrWhiteSpace(extractSettings.WhereClause))
        {
            var dangerousKeywords = new[] { ";--", "DROP", "DELETE", "TRUNCATE", "ALTER", "CREATE", "EXEC", "EXECUTE", "xp_" };
            var upperClause = extractSettings.WhereClause.ToUpperInvariant();

            foreach (var keyword in dangerousKeywords)
            {
                if (upperClause.Contains(keyword.ToUpperInvariant()))
                {
                    result.AddError($"{tablePrefix}: WhereClause contains potentially dangerous SQL keyword: {keyword}");
                }
            }

            // Note: If user includes WHERE keyword, it will be part of the clause (not ideal but won't break)
            // Future enhancement: Add AddWarning method to ValidationResult
        }
    }

    private void ValidateStorage(StorageConfiguration storage, ValidationResult result)
    {
        if (string.IsNullOrWhiteSpace(storage.BasePath))
        {
            result.AddError("Storage BasePath is required");
        }
    }

    /// <summary>
    /// Validates a TransferConfiguration for bi-directional transfers
    /// </summary>
    public ValidationResult ValidateTransfer(TransferConfiguration config)
    {
        var result = new ValidationResult();

        switch (config.TransferType)
        {
            case TransferType.SqlToParquet:
                ValidateSqlToParquetTransfer(config, result);
                break;

            case TransferType.ParquetToSql:
                ValidateParquetToSqlTransfer(config, result);
                break;

            case TransferType.SqlToSql:
                ValidateSqlToSqlTransfer(config, result);
                break;

            case TransferType.SqlToIceberg:
                ValidateSqlToIcebergTransfer(config, result);
                break;

            case TransferType.IcebergToSql:
                ValidateIcebergToSqlTransfer(config, result);
                break;

            case TransferType.SqlToIcebergIncremental:
                ValidateSqlToIcebergIncrementalTransfer(config, result);
                break;

            default:
                result.AddError($"Transfer type {config.TransferType} is not supported");
                break;
        }

        return result;
    }

    private void ValidateSqlToParquetTransfer(TransferConfiguration config, ValidationResult result)
    {
        // Validate source is SQL Server
        if (config.Source.Type != SourceType.SqlServer)
        {
            result.AddError("Source must be SqlServer for SqlToParquet transfer");
        }

        if (string.IsNullOrWhiteSpace(config.Source.ConnectionString))
        {
            result.AddError("Source connection string is required");
        }

        if (config.Source.Table == null)
        {
            result.AddError("Source table is required");
        }

        // Validate destination is Parquet
        if (config.Destination.Type != DestinationType.Parquet)
        {
            result.AddError("Destination must be Parquet for SqlToParquet transfer");
        }

        if (string.IsNullOrWhiteSpace(config.Destination.ParquetPath))
        {
            result.AddError("Destination Parquet path is required");
        }
    }

    private void ValidateParquetToSqlTransfer(TransferConfiguration config, ValidationResult result)
    {
        // Validate source is Parquet
        if (config.Source.Type != SourceType.Parquet)
        {
            result.AddError("Source must be Parquet for ParquetToSql transfer");
        }

        if (string.IsNullOrWhiteSpace(config.Source.ParquetPath))
        {
            result.AddError("Source Parquet path is required");
        }

        // Validate destination is SQL Server
        if (config.Destination.Type != DestinationType.SqlServer)
        {
            result.AddError("Destination must be SqlServer for ParquetToSql transfer");
        }

        if (string.IsNullOrWhiteSpace(config.Destination.ConnectionString))
        {
            result.AddError("Destination connection string is required");
        }

        if (config.Destination.Table == null)
        {
            result.AddError("Destination table is required");
        }
    }

    private void ValidateSqlToSqlTransfer(TransferConfiguration config, ValidationResult result)
    {
        // Validate source is SQL Server
        if (config.Source.Type != SourceType.SqlServer)
        {
            result.AddError("Source must be SqlServer for SqlToSql transfer");
        }

        if (string.IsNullOrWhiteSpace(config.Source.ConnectionString))
        {
            result.AddError("Source connection string is required");
        }

        if (config.Source.Table == null)
        {
            result.AddError("Source table is required");
        }

        // Validate destination is SQL Server
        if (config.Destination.Type != DestinationType.SqlServer)
        {
            result.AddError("Destination must be SqlServer for SqlToSql transfer");
        }

        if (string.IsNullOrWhiteSpace(config.Destination.ConnectionString))
        {
            result.AddError("Destination connection string is required");
        }

        if (config.Destination.Table == null)
        {
            result.AddError("Destination table is required");
        }
    }

    private void ValidateSqlToIcebergTransfer(TransferConfiguration config, ValidationResult result)
    {
        // Validate source is SQL Server
        if (config.Source.Type != SourceType.SqlServer)
        {
            result.AddError("Source must be SqlServer for SqlToIceberg transfer");
        }

        if (string.IsNullOrWhiteSpace(config.Source.ConnectionString))
        {
            result.AddError("Source connection string is required");
        }

        if (config.Source.Table == null)
        {
            result.AddError("Source table is required");
        }

        // Validate destination is Iceberg
        if (config.Destination.Type != DestinationType.Iceberg)
        {
            result.AddError("Destination must be Iceberg for SqlToIceberg transfer");
        }

        if (config.Destination.IcebergTable == null)
        {
            result.AddError("Destination Iceberg table configuration is required");
        }
        else if (string.IsNullOrWhiteSpace(config.Destination.IcebergTable.TableName))
        {
            result.AddError("Destination Iceberg table name is required");
        }
    }

    private void ValidateIcebergToSqlTransfer(TransferConfiguration config, ValidationResult result)
    {
        // Validate source is Iceberg
        if (config.Source.Type != SourceType.Iceberg)
        {
            result.AddError("Source must be Iceberg for IcebergToSql transfer");
        }

        if (config.Source.IcebergTable == null)
        {
            result.AddError("Source Iceberg table configuration is required");
        }
        else if (string.IsNullOrWhiteSpace(config.Source.IcebergTable.TableName))
        {
            result.AddError("Source Iceberg table name is required");
        }

        // Validate destination is SQL Server
        if (config.Destination.Type != DestinationType.SqlServer)
        {
            result.AddError("Destination must be SqlServer for IcebergToSql transfer");
        }

        if (string.IsNullOrWhiteSpace(config.Destination.ConnectionString))
        {
            result.AddError("Destination connection string is required");
        }

        if (config.Destination.Table == null)
        {
            result.AddError("Destination table is required");
        }
    }

    private void ValidateSqlToIcebergIncrementalTransfer(TransferConfiguration config, ValidationResult result)
    {
        // Validate source is SQL Server
        if (config.Source.Type != SourceType.SqlServer)
        {
            result.AddError("Source must be SqlServer for SqlToIcebergIncremental transfer");
        }

        if (string.IsNullOrWhiteSpace(config.Source.ConnectionString))
        {
            result.AddError("Source connection string is required");
        }

        if (config.Source.Table == null)
        {
            result.AddError("Source table is required");
        }

        // Validate destination is Iceberg
        if (config.Destination.Type != DestinationType.Iceberg)
        {
            result.AddError("Destination must be Iceberg for SqlToIcebergIncremental transfer");
        }

        if (config.Destination.IcebergTable == null)
        {
            result.AddError("Destination Iceberg table configuration is required");
            return; // Can't validate incremental options if IcebergTable is null
        }

        if (string.IsNullOrWhiteSpace(config.Destination.IcebergTable.TableName))
        {
            result.AddError("Destination Iceberg table name is required");
        }

        // Validate incremental sync options are present
        if (config.Destination.IcebergTable.IncrementalSync == null)
        {
            result.AddError("Incremental sync configuration is required for SqlToIcebergIncremental transfer");
            return;
        }

        var incrementalSync = config.Destination.IcebergTable.IncrementalSync;

        // Validate required incremental sync fields
        if (string.IsNullOrWhiteSpace(incrementalSync.PrimaryKeyColumn))
        {
            result.AddError("Primary key column is required for incremental sync");
        }

        if (string.IsNullOrWhiteSpace(incrementalSync.WatermarkColumn))
        {
            result.AddError("Watermark column is required for incremental sync");
        }

        // Validate merge strategy
        var validStrategies = new[] { "upsert", "append" };
        if (!string.IsNullOrWhiteSpace(incrementalSync.MergeStrategy) &&
            !validStrategies.Contains(incrementalSync.MergeStrategy.ToLower()))
        {
            result.AddError($"Merge strategy must be one of: {string.Join(", ", validStrategies)}");
        }

        // Validate watermark type
        var validWatermarkTypes = new[] { "timestamp", "integer" };
        if (!string.IsNullOrWhiteSpace(incrementalSync.WatermarkType) &&
            !validWatermarkTypes.Contains(incrementalSync.WatermarkType.ToLower()))
        {
            result.AddError($"Watermark type must be one of: {string.Join(", ", validWatermarkTypes)}");
        }
    }
}
