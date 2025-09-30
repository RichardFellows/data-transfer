using DataTransfer.Core.Models;

namespace DataTransfer.Core.Interfaces;

public interface IDataLoader
{
    Task<LoadResult> LoadAsync(
        TableConfiguration tableConfig,
        string connectionString,
        Stream inputStream,
        CancellationToken cancellationToken = default);
}
