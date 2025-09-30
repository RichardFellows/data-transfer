namespace DataTransfer.Core.Interfaces;

public interface IParquetStorage
{
    Task WriteAsync(
        Stream dataStream,
        string filePath,
        DateTime partitionDate,
        CancellationToken cancellationToken = default);

    Task<Stream> ReadAsync(
        string filePath,
        CancellationToken cancellationToken = default);
}
