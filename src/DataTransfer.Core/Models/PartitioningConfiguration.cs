namespace DataTransfer.Core.Models;

public class PartitioningConfiguration
{
    public PartitionType Type { get; set; }
    public string? Column { get; set; }
    public string? Format { get; set; }
}
