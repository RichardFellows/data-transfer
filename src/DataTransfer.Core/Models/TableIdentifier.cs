namespace DataTransfer.Core.Models;

public class TableIdentifier
{
    public string Database { get; set; } = string.Empty;
    public string Schema { get; set; } = string.Empty;
    public string Table { get; set; } = string.Empty;

    public string FullyQualifiedName => $"{Database}.{Schema}.{Table}";
}
