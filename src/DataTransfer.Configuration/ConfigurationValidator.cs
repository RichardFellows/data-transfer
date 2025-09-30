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

    private void ValidateStorage(StorageConfiguration storage, ValidationResult result)
    {
        if (string.IsNullOrWhiteSpace(storage.BasePath))
        {
            result.AddError("Storage BasePath is required");
        }
    }
}
