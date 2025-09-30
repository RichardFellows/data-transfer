namespace DataTransfer.Core.Models;

public class DataTransferConfiguration
{
    public ConnectionConfiguration Connections { get; set; } = new ConnectionConfiguration();
    public List<TableConfiguration> Tables { get; set; } = new List<TableConfiguration>();
    public StorageConfiguration Storage { get; set; } = new StorageConfiguration();
}
