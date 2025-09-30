namespace DataTransfer.Core.Models;

public class ExtractionResult
{
    public long RowsExtracted { get; set; }
    public DateTime StartTime { get; set; }
    public DateTime EndTime { get; set; }
    public TimeSpan Duration => EndTime - StartTime;
    public bool Success { get; set; }
    public string? ErrorMessage { get; set; }
}
