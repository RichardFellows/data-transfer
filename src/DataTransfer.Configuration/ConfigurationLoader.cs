using System.Text.Json;
using System.Text.Json.Serialization;
using DataTransfer.Core.Models;

namespace DataTransfer.Configuration;

public class ConfigurationLoader
{
    private static readonly JsonSerializerOptions _jsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        Converters = { new JsonStringEnumConverter(JsonNamingPolicy.CamelCase) },
        ReadCommentHandling = JsonCommentHandling.Skip,
        AllowTrailingCommas = true
    };

    public DataTransferConfiguration Load(string filePath)
    {
        if (!File.Exists(filePath))
        {
            throw new FileNotFoundException($"Configuration file not found: {filePath}", filePath);
        }

        var jsonContent = File.ReadAllText(filePath);

        var config = JsonSerializer.Deserialize<DataTransferConfiguration>(jsonContent, _jsonOptions);

        if (config == null)
        {
            throw new InvalidOperationException("Failed to deserialize configuration");
        }

        return config;
    }

    public async Task<DataTransferConfiguration> LoadAsync(string filePath, CancellationToken cancellationToken = default)
    {
        if (!File.Exists(filePath))
        {
            throw new FileNotFoundException($"Configuration file not found: {filePath}", filePath);
        }

        using var stream = File.OpenRead(filePath);
        var config = await JsonSerializer.DeserializeAsync<DataTransferConfiguration>(stream, _jsonOptions, cancellationToken);

        if (config == null)
        {
            throw new InvalidOperationException("Failed to deserialize configuration");
        }

        return config;
    }
}
