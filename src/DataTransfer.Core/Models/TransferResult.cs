namespace DataTransfer.Core.Models;

public class TransferResult
{
    public bool Success { get; set; }
    public long RowsExtracted { get; set; }
    public long RowsLoaded { get; set; }
    public DateTime StartTime { get; set; }
    public DateTime EndTime { get; set; }
    public TimeSpan Duration => EndTime - StartTime;
    public string? ErrorMessage { get; set; }
    public string? ParquetFilePath { get; set; }
}
