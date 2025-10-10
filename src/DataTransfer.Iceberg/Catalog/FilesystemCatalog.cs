using System.Text.Json;
using DataTransfer.Core.Models.Iceberg;
using DataTransfer.Iceberg.Metadata;
using Microsoft.Extensions.Logging;

namespace DataTransfer.Iceberg.Catalog;

/// <summary>
/// Filesystem-based Iceberg catalog for managing table metadata and atomic commits
/// Provides ACID semantics via version-hint.txt atomic file operations
/// </summary>
public class FilesystemCatalog
{
    private readonly string _warehousePath;
    private readonly ILogger<FilesystemCatalog> _logger;
    private readonly TableMetadataGenerator _metadataGenerator;

    public FilesystemCatalog(string warehousePath, ILogger<FilesystemCatalog> logger)
    {
        _warehousePath = warehousePath;
        _logger = logger;
        _metadataGenerator = new TableMetadataGenerator();
    }

    /// <summary>
    /// Initializes directory structure for a new Iceberg table
    /// </summary>
    /// <param name="tableName">Name of the table (can include namespace like "db.table")</param>
    /// <returns>Full path to the table directory</returns>
    public string InitializeTable(string tableName)
    {
        var tablePath = Path.Combine(_warehousePath, tableName);
        var metadataPath = Path.Combine(tablePath, "metadata");
        var dataPath = Path.Combine(tablePath, "data");

        Directory.CreateDirectory(metadataPath);
        Directory.CreateDirectory(dataPath);

        _logger.LogInformation("Initialized Iceberg table at {TablePath}", tablePath);

        return tablePath;
    }

    /// <summary>
    /// Atomically commits a new snapshot to the catalog
    /// </summary>
    /// <param name="tableName">Name of the table</param>
    /// <param name="metadata">Table metadata to commit</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>True if commit succeeded, false otherwise</returns>
    public async Task<bool> CommitAsync(
        string tableName,
        IcebergTableMetadata metadata,
        CancellationToken cancellationToken = default)
    {
        var tablePath = Path.Combine(_warehousePath, tableName);
        var metadataDir = Path.Combine(tablePath, "metadata");

        try
        {
            // Determine next version number
            var version = GetNextVersion(metadataDir);
            var metadataFile = Path.Combine(metadataDir, $"v{version}.metadata.json");

            _logger.LogInformation(
                "Committing Iceberg snapshot version {Version} for table {Table}",
                version,
                tableName);

            // Write metadata file
            _metadataGenerator.WriteMetadata(metadata, metadataFile);

            // Atomic commit via version-hint.txt
            await AtomicVersionUpdate(metadataDir, version, cancellationToken);

            _logger.LogInformation(
                "Successfully committed Iceberg table {Table} version {Version}",
                tableName,
                version);

            return true;
        }
        catch (OperationCanceledException)
        {
            _logger.LogWarning("Commit cancelled for table {Table}", tableName);
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to commit Iceberg table {Table}", tableName);
            return false;
        }
    }

    /// <summary>
    /// Loads current table metadata using version-hint.txt
    /// </summary>
    /// <param name="tableName">Name of the table</param>
    /// <returns>Table metadata, or null if table doesn't exist</returns>
    public IcebergTableMetadata? LoadTable(string tableName)
    {
        var tablePath = Path.Combine(_warehousePath, tableName);
        var metadataDir = Path.Combine(tablePath, "metadata");
        var hintFile = Path.Combine(metadataDir, "version-hint.txt");

        if (!File.Exists(hintFile))
        {
            _logger.LogDebug("Table {Table} does not exist (no version-hint.txt)", tableName);
            return null;
        }

        try
        {
            var version = int.Parse(File.ReadAllText(hintFile).Trim());
            var metadataFile = Path.Combine(metadataDir, $"v{version}.metadata.json");

            if (!File.Exists(metadataFile))
            {
                _logger.LogWarning(
                    "Version-hint points to v{Version} but metadata file doesn't exist for table {Table}",
                    version,
                    tableName);
                return null;
            }

            var json = File.ReadAllText(metadataFile);
            var options = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
            var metadata = JsonSerializer.Deserialize<IcebergTableMetadata>(json, options);

            _logger.LogDebug("Loaded table {Table} version {Version}", tableName, version);

            return metadata;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to load table {Table}", tableName);
            return null;
        }
    }

    /// <summary>
    /// Gets the full path to a table directory
    /// </summary>
    /// <param name="tableName">Name of the table</param>
    /// <returns>Full path to the table directory</returns>
    public string GetTablePath(string tableName)
    {
        return Path.Combine(_warehousePath, tableName);
    }

    /// <summary>
    /// Checks if a table exists in the catalog
    /// </summary>
    /// <param name="tableName">Name of the table</param>
    /// <returns>True if table exists, false otherwise</returns>
    public bool TableExists(string tableName)
    {
        var tablePath = Path.Combine(_warehousePath, tableName);
        var hintFile = Path.Combine(tablePath, "metadata", "version-hint.txt");

        return File.Exists(hintFile);
    }

    /// <summary>
    /// Atomically updates version-hint.txt using filesystem move operation
    /// This provides ACID commit semantics on the same filesystem
    /// </summary>
    private async Task AtomicVersionUpdate(
        string metadataDir,
        int version,
        CancellationToken cancellationToken)
    {
        var hintFile = Path.Combine(metadataDir, "version-hint.txt");
        var tempHint = Path.Combine(metadataDir, $"version-hint.txt.{Guid.NewGuid()}");

        try
        {
            // Write to temporary file
            await File.WriteAllTextAsync(tempHint, version.ToString(), cancellationToken);

            // Atomic move (filesystem operation is atomic on same filesystem)
            File.Move(tempHint, hintFile, overwrite: true);

            _logger.LogDebug("Atomically updated version-hint.txt to version {Version}", version);
        }
        finally
        {
            // Cleanup temp file if it still exists
            if (File.Exists(tempHint))
            {
                try { File.Delete(tempHint); } catch { /* Ignore */ }
            }
        }
    }

    /// <summary>
    /// Gets the next version number by reading current version-hint.txt
    /// </summary>
    private int GetNextVersion(string metadataDir)
    {
        var hintFile = Path.Combine(metadataDir, "version-hint.txt");

        if (!File.Exists(hintFile))
        {
            _logger.LogDebug("No existing version-hint.txt, starting at version 1");
            return 1;
        }

        var currentVersionText = File.ReadAllText(hintFile).Trim();
        var currentVersion = int.Parse(currentVersionText);
        var nextVersion = currentVersion + 1;

        _logger.LogDebug("Current version: {Current}, next version: {Next}", currentVersion, nextVersion);

        return nextVersion;
    }
}
