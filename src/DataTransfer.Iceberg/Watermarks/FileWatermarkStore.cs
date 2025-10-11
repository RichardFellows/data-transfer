using System.Text.Json;
using DataTransfer.Iceberg.Models;

namespace DataTransfer.Iceberg.Watermarks;

/// <summary>
/// Stores watermarks as JSON files on filesystem
/// </summary>
public class FileWatermarkStore : IWatermarkStore
{
    private readonly string _watermarkDirectory;

    public FileWatermarkStore(string watermarkDirectory)
    {
        _watermarkDirectory = watermarkDirectory ?? throw new ArgumentNullException(nameof(watermarkDirectory));

        // Ensure directory exists
        if (!Directory.Exists(_watermarkDirectory))
        {
            Directory.CreateDirectory(_watermarkDirectory);
        }
    }

    public async Task<Watermark?> GetWatermarkAsync(string tableName)
    {
        var filePath = Path.Combine(_watermarkDirectory, $"{tableName}.json");

        if (!File.Exists(filePath))
        {
            return null;
        }

        var json = await File.ReadAllTextAsync(filePath);
        return JsonSerializer.Deserialize<Watermark>(json);
    }

    public async Task SetWatermarkAsync(string tableName, Watermark watermark)
    {
        var filePath = Path.Combine(_watermarkDirectory, $"{tableName}.json");
        var options = new JsonSerializerOptions { WriteIndented = true };
        var json = JsonSerializer.Serialize(watermark, options);
        await File.WriteAllTextAsync(filePath, json);
    }
}
