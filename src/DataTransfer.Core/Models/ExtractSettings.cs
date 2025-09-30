namespace DataTransfer.Core.Models;

public class ExtractSettings
{
    public int BatchSize { get; set; }
    public DateRange DateRange { get; set; } = new DateRange();
}
