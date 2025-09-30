using DataTransfer.Core.Models;

namespace DataTransfer.Core.Interfaces;

public interface ITableExtractor
{
    Task<ExtractionResult> ExtractAsync(
        TableConfiguration tableConfig,
        string connectionString,
        Stream outputStream,
        CancellationToken cancellationToken = default);
}
