using DataTransfer.Iceberg.Catalog;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace DataTransfer.Web.Services;

/// <summary>
/// Service for managing and listing Iceberg tables
/// </summary>
public class IcebergTableService
{
    private readonly FilesystemCatalog _catalog;
    private readonly ILogger<IcebergTableService> _logger;
    private readonly string _warehousePath;

    public IcebergTableService(
        FilesystemCatalog catalog,
        IConfiguration configuration,
        ILogger<IcebergTableService> logger)
    {
        _catalog = catalog ?? throw new ArgumentNullException(nameof(catalog));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        ArgumentNullException.ThrowIfNull(configuration);

        _warehousePath = configuration["Iceberg:WarehousePath"] ?? "./iceberg-warehouse";
    }

    /// <summary>
    /// Gets a list of all available Iceberg tables in the warehouse
    /// </summary>
    public List<IcebergTableInfo> GetAvailableTables()
    {
        var tables = new List<IcebergTableInfo>();

        try
        {
            if (!Directory.Exists(_warehousePath))
            {
                _logger.LogWarning("Iceberg warehouse path does not exist: {WarehousePath}", _warehousePath);
                return tables;
            }

            // List all directories in warehouse (each is a table)
            var directories = Directory.GetDirectories(_warehousePath);

            foreach (var dir in directories)
            {
                var tableName = Path.GetFileName(dir);

                // Skip system directories
                if (tableName.StartsWith("."))
                    continue;

                var metadataPath = Path.Combine(dir, "metadata");
                if (!Directory.Exists(metadataPath))
                    continue;

                // Try to load table metadata
                try
                {
                    var table = _catalog.LoadTable(tableName);

                    // Get current snapshot if available
                    var snapshot = table?.CurrentSnapshotId.HasValue == true
                        ? table.Snapshots?.FirstOrDefault(s => s.SnapshotId == table.CurrentSnapshotId.Value)
                        : null;

                    tables.Add(new IcebergTableInfo
                    {
                        TableName = tableName,
                        TablePath = dir,
                        MetadataPath = metadataPath,
                        HasSnapshot = snapshot != null,
                        SnapshotId = snapshot?.SnapshotId,
                        LastModified = Directory.GetLastWriteTime(metadataPath),
                        RecordCount = null  // Would need to parse manifest files to get this
                    });
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Could not load metadata for table {TableName}", tableName);

                    // Add table with limited info
                    tables.Add(new IcebergTableInfo
                    {
                        TableName = tableName,
                        TablePath = dir,
                        MetadataPath = metadataPath,
                        HasSnapshot = false,
                        LastModified = Directory.GetLastWriteTime(metadataPath)
                    });
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error listing Iceberg tables from {WarehousePath}", _warehousePath);
        }

        return tables.OrderBy(t => t.TableName).ToList();
    }

    /// <summary>
    /// Checks if a table exists
    /// </summary>
    public bool TableExists(string tableName)
    {
        try
        {
            var tablePath = Path.Combine(_warehousePath, tableName);
            var metadataPath = Path.Combine(tablePath, "metadata");
            return Directory.Exists(metadataPath);
        }
        catch
        {
            return false;
        }
    }
}

/// <summary>
/// Information about an Iceberg table
/// </summary>
public class IcebergTableInfo
{
    public string TableName { get; set; } = string.Empty;
    public string TablePath { get; set; } = string.Empty;
    public string MetadataPath { get; set; } = string.Empty;
    public bool HasSnapshot { get; set; }
    public long? SnapshotId { get; set; }
    public DateTime LastModified { get; set; }
    public long? RecordCount { get; set; }

    public string DisplayName => HasSnapshot && SnapshotId.HasValue
        ? $"{TableName} (Snapshot {SnapshotId})"
        : TableName;
}
