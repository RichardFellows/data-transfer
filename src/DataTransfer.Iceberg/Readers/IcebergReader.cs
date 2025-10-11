using DataTransfer.Iceberg.Catalog;
using Microsoft.Extensions.Logging;
using System.Runtime.CompilerServices;

namespace DataTransfer.Iceberg.Readers;

/// <summary>
/// Reads data from Iceberg tables by streaming Parquet files
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
        // TODO: Implement read functionality
        throw new NotImplementedException("IcebergReader.ReadTableAsync is not yet implemented");
#pragma warning disable CS0162 // Unreachable code detected
        yield break;
#pragma warning restore CS0162
    }

    /// <summary>
    /// Reads data from a specific snapshot of a table
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
        // TODO: Implement snapshot read functionality
        throw new NotImplementedException("IcebergReader.ReadSnapshotAsync is not yet implemented");
#pragma warning disable CS0162 // Unreachable code detected
        yield break;
#pragma warning restore CS0162
    }
}
