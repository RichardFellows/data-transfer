using DataTransfer.Web.Models;

namespace DataTransfer.Web.Services;

/// <summary>
/// Manages transfer history in memory
/// </summary>
public class TransferHistoryService
{
    private readonly List<TransferHistoryEntry> _history = new();
    private readonly SemaphoreSlim _lock = new(1, 1);

    /// <summary>
    /// Adds a transfer to history
    /// </summary>
    public async Task AddAsync(TransferHistoryEntry entry)
    {
        await _lock.WaitAsync();
        try
        {
            _history.Add(entry);
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <summary>
    /// Gets all transfers ordered by start time descending
    /// </summary>
    public async Task<List<TransferHistoryEntry>> GetAllAsync()
    {
        await _lock.WaitAsync();
        try
        {
            return _history.OrderByDescending(h => h.StartTime).ToList();
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <summary>
    /// Gets a specific transfer by ID
    /// </summary>
    public async Task<TransferHistoryEntry?> GetByIdAsync(string id)
    {
        await _lock.WaitAsync();
        try
        {
            return _history.FirstOrDefault(h => h.Id == id);
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <summary>
    /// Gets transfer statistics
    /// </summary>
    public async Task<TransferStatistics> GetStatisticsAsync()
    {
        await _lock.WaitAsync();
        try
        {
            return new TransferStatistics
            {
                TotalTransfers = _history.Count,
                SuccessfulTransfers = _history.Count(h => h.Success),
                FailedTransfers = _history.Count(h => !h.Success),
                TotalRowsTransferred = _history.Where(h => h.Success).Sum(h => h.RowsTransferred),
                AverageDuration = _history.Any()
                    ? TimeSpan.FromMilliseconds(_history.Average(h => h.Duration.TotalMilliseconds))
                    : TimeSpan.Zero
            };
        }
        finally
        {
            _lock.Release();
        }
    }
}

/// <summary>
/// Transfer statistics summary
/// </summary>
public class TransferStatistics
{
    public int TotalTransfers { get; set; }
    public int SuccessfulTransfers { get; set; }
    public int FailedTransfers { get; set; }
    public long TotalRowsTransferred { get; set; }
    public TimeSpan AverageDuration { get; set; }
}
