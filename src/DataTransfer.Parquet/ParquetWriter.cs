using System.Text.Json;
using DataTransfer.Core.Interfaces;

namespace DataTransfer.Parquet;

/// <summary>
/// Writes data directly to Parquet files (simplified wrapper around IParquetStorage)
/// </summary>
public class ParquetWriter : IParquetWriter
{
    private readonly IParquetStorage _storage;

    public ParquetWriter(IParquetStorage storage)
    {
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
    }

    public async Task<int> WriteToParquetAsync(
        Stream dataStream,
        string outputPath,
        DateTime? partitionDate = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(dataStream);

        if (string.IsNullOrWhiteSpace(outputPath))
        {
            throw new ArgumentException("Output path cannot be empty", nameof(outputPath));
        }

        cancellationToken.ThrowIfCancellationRequested();

        var fileName = Path.GetFileName(outputPath);
        var partition = partitionDate ?? DateTime.UtcNow;

        // Write to Parquet using the storage
        await _storage.WriteAsync(dataStream, fileName, partition, cancellationToken);

        // Read back the data to count rows
        // This is necessary because WriteAsync doesn't return row count
        var partitionPath = $"year={partition.Year:D4}/month={partition.Month:D2}/day={partition.Day:D2}";
        var fullPath = $"{partitionPath}/{fileName}";

        await using var readStream = await _storage.ReadAsync(fullPath, cancellationToken);

        // Count rows in JSON array
        dataStream.Position = 0;
        using var jsonDoc = await JsonDocument.ParseAsync(readStream, cancellationToken: cancellationToken);

        if (jsonDoc.RootElement.ValueKind == JsonValueKind.Array)
        {
            return jsonDoc.RootElement.GetArrayLength();
        }

        return 0;
    }
}
