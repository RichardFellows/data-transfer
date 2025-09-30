namespace DataTransfer.Core.Models;

public class TableConfiguration
{
    public TableIdentifier Source { get; set; } = new TableIdentifier();
    public TableIdentifier Destination { get; set; } = new TableIdentifier();
    public PartitioningConfiguration Partitioning { get; set; } = new PartitioningConfiguration();
    public ExtractSettings ExtractSettings { get; set; } = new ExtractSettings();
}
