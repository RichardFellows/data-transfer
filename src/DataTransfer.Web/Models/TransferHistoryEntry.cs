using DataTransfer.Core.Models;

namespace DataTransfer.Web.Models;

/// <summary>
/// Represents a historical transfer operation
/// </summary>
public class TransferHistoryEntry
{
    public string Id { get; set; } = string.Empty;
    public TransferType TransferType { get; set; }
    public DateTime StartTime { get; set; }
    public DateTime? EndTime { get; set; }
    public long RowsTransferred { get; set; }
    public bool Success { get; set; }
    public string? ErrorMessage { get; set; }
    public string? SourceInfo { get; set; }
    public string? DestinationInfo { get; set; }
    public TimeSpan Duration => EndTime.HasValue ? EndTime.Value - StartTime : TimeSpan.Zero;
}
