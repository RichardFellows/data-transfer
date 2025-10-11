using Avro.File;
using Avro.Generic;
using DataTransfer.Iceberg.Catalog;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System.Runtime.CompilerServices;

namespace DataTransfer.Iceberg.Readers;

/// <summary>
/// Reads data from Iceberg tables by streaming Parquet files
/// Handles manifest chain traversal (metadata → manifest list → manifest → data files)
/// </summary>
public class IcebergReader
{
    private readonly FilesystemCatalog _catalog;
    private readonly ILogger<IcebergReader> _logger;

    public IcebergReader(FilesystemCatalog catalog, ILogger<IcebergReader> logger)
    {
        _catalog = catalog;
        _logger = logger;
    }

    /// <summary>
    /// Reads all data from the current snapshot of a table
    /// </summary>
    /// <param name="tableName">Name of the table to read</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Async enumerable of rows as dictionaries</returns>
    public async IAsyncEnumerable<Dictionary<string, object>> ReadTableAsync(
        string tableName,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("[READER] Starting enumeration for table {Table}", tableName);

        // 1. Load table metadata
        var metadata = _catalog.LoadTable(tableName);
        if (metadata == null)
        {
            throw new InvalidOperationException($"Table {tableName} does not exist");
        }

        _logger.LogInformation("[READER] Loaded metadata - Total snapshots: {Count}, Current snapshot: {SnapshotId}",
            metadata.Snapshots.Count, metadata.CurrentSnapshotId);

        // Handle empty table (no snapshots)
        if (metadata.CurrentSnapshotId == null)
        {
            _logger.LogDebug("Table {Table} has no data (no current snapshot)", tableName);
            yield break;
        }

        var currentSnapshot = metadata.Snapshots.First(s => s.SnapshotId == metadata.CurrentSnapshotId);
        var schema = metadata.Schemas.FirstOrDefault(s => s.SchemaId == metadata.CurrentSchemaId)
            ?? metadata.Schemas[0];

        _logger.LogInformation("[READER] Reading snapshot {SnapshotId}, manifest list: {ManifestList}",
            currentSnapshot.SnapshotId, currentSnapshot.ManifestList);

        // 2. Read manifest list
        var tablePath = _catalog.GetTablePath(tableName);
        var manifestListPath = Path.Combine(tablePath, currentSnapshot.ManifestList);

        _logger.LogDebug("[READER] Manifest list path: {Path}, Exists: {Exists}",
            manifestListPath, File.Exists(manifestListPath));

        var manifestPaths = ReadManifestList(manifestListPath);

        _logger.LogInformation("[READER] Found {Count} manifest files in manifest list", manifestPaths.Count);
        foreach (var manifestPath in manifestPaths)
        {
            _logger.LogDebug("[READER]   Manifest: {Path}", manifestPath);
        }

        // 3. Read all manifests to get data files
        var dataFiles = new List<string>();
        foreach (var manifestPath in manifestPaths)
        {
            var filesFromManifest = ReadManifest(tablePath, manifestPath);
            _logger.LogDebug("[READER]   Manifest {Path} contains {Count} data files", manifestPath, filesFromManifest.Count);
            dataFiles.AddRange(filesFromManifest);
        }

        _logger.LogInformation("[READER] Found {Count} data files across all manifests", dataFiles.Count);
        foreach (var dataFile in dataFiles)
        {
            _logger.LogDebug("[READER]   Data file: {Path}", dataFile);
        }

        if (dataFiles.Count == 0)
        {
            _logger.LogWarning("[READER] WARNING: No data files found! This will result in empty enumerable.");
            yield break;
        }

        // 4. Read all Parquet data files
        var parquetReader = new IcebergParquetReader(NullLogger<IcebergParquetReader>.Instance);
        int rowsYielded = 0;

        foreach (var dataFile in dataFiles)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var dataFilePath = Path.Combine(tablePath, dataFile);
            _logger.LogDebug("[READER] Starting to read Parquet file: {Path}", dataFilePath);

            int rowsFromFile = 0;
            await foreach (var row in parquetReader.ReadAsync(dataFilePath, schema, cancellationToken))
            {
                rowsYielded++;
                rowsFromFile++;

                if (rowsYielded <= 5 || rowsYielded % 100 == 0)
                {
                    _logger.LogDebug("[READER] Yielded row {Count}", rowsYielded);
                }

                yield return row;
            }

            _logger.LogDebug("[READER] Finished reading {Path}, yielded {Count} rows", dataFile, rowsFromFile);
        }

        _logger.LogInformation("[READER] Completed reading Iceberg table {Table}, total rows yielded: {Count}", tableName, rowsYielded);
    }

    /// <summary>
    /// Reads data from a specific snapshot of a table (time-travel support)
    /// </summary>
    /// <param name="tableName">Name of the table to read</param>
    /// <param name="snapshotId">ID of the snapshot to read</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Async enumerable of rows as dictionaries</returns>
    public async IAsyncEnumerable<Dictionary<string, object>> ReadSnapshotAsync(
        string tableName,
        long snapshotId,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Reading Iceberg table {Table} snapshot {Snapshot}", tableName, snapshotId);

        // 1. Load table metadata
        var metadata = _catalog.LoadTable(tableName);
        if (metadata == null)
        {
            throw new InvalidOperationException($"Table {tableName} does not exist");
        }

        // 2. Find specific snapshot
        var snapshot = metadata.Snapshots.FirstOrDefault(s => s.SnapshotId == snapshotId);
        if (snapshot == null)
        {
            throw new InvalidOperationException($"Snapshot {snapshotId} not found in table {tableName}");
        }

        var schema = metadata.Schemas.FirstOrDefault(s => s.SchemaId == metadata.CurrentSchemaId)
            ?? metadata.Schemas[0];

        // 3. Read manifest list for this snapshot
        var tablePath = _catalog.GetTablePath(tableName);
        var manifestListPath = Path.Combine(tablePath, snapshot.ManifestList);
        var manifestPaths = ReadManifestList(manifestListPath);

        // 4. Read manifests to get data files
        var dataFiles = new List<string>();
        foreach (var manifestPath in manifestPaths)
        {
            dataFiles.AddRange(ReadManifest(tablePath, manifestPath));
        }

        // 5. Read Parquet data files
        var parquetReader = new IcebergParquetReader(NullLogger<IcebergParquetReader>.Instance);

        foreach (var dataFile in dataFiles)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var dataFilePath = Path.Combine(tablePath, dataFile);
            await foreach (var row in parquetReader.ReadAsync(dataFilePath, schema, cancellationToken))
            {
                yield return row;
            }
        }

        _logger.LogInformation("Completed reading snapshot {Snapshot} from table {Table}", snapshotId, tableName);
    }

    /// <summary>
    /// Reads manifest list Avro file and returns paths to manifest files
    /// </summary>
    private List<string> ReadManifestList(string manifestListPath)
    {
        var manifestPaths = new List<string>();

        try
        {
            using var reader = DataFileReader<GenericRecord>.OpenReader(manifestListPath);

            while (reader.HasNext())
            {
                var record = reader.Next();
                var manifestPath = record["manifest_path"] as string;

                if (!string.IsNullOrEmpty(manifestPath))
                {
                    manifestPaths.Add(manifestPath);
                }
            }

            _logger.LogDebug("Read {Count} manifest entries from {Path}",
                manifestPaths.Count, manifestListPath);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to read manifest list: {Path}", manifestListPath);
            throw new InvalidOperationException($"Failed to read manifest list: {manifestListPath}", ex);
        }

        return manifestPaths;
    }

    /// <summary>
    /// Reads manifest Avro file and returns paths to data files
    /// </summary>
    private List<string> ReadManifest(string tablePath, string manifestRelativePath)
    {
        var dataFilePaths = new List<string>();
        var manifestPath = Path.Combine(tablePath, manifestRelativePath);

        try
        {
            using var reader = DataFileReader<GenericRecord>.OpenReader(manifestPath);

            while (reader.HasNext())
            {
                var entry = reader.Next();

                // Extract the nested data_file record
                var dataFile = entry["data_file"] as GenericRecord;

                if (dataFile != null)
                {
                    var filePath = dataFile["file_path"] as string;

                    if (!string.IsNullOrEmpty(filePath))
                    {
                        dataFilePaths.Add(filePath);
                    }
                }
            }

            _logger.LogDebug("Read {Count} data file entries from {Path}",
                dataFilePaths.Count, manifestRelativePath);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to read manifest: {Path}", manifestRelativePath);
            throw new InvalidOperationException($"Failed to read manifest: {manifestRelativePath}", ex);
        }

        return dataFilePaths;
    }
}
