namespace DataTransfer.Core.Models;

public class StorageConfiguration
{
    public string BasePath { get; set; } = string.Empty;
    public string Compression { get; set; } = "snappy";
}
