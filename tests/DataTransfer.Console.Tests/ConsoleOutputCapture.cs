namespace DataTransfer.Console.Tests;

/// <summary>
/// Captures console output, error, and execution details for test reporting
/// </summary>
public class ConsoleOutputCapture
{
    public string TestName { get; set; } = string.Empty;
    public string StepName { get; set; } = string.Empty;
    public string Command { get; set; } = string.Empty;
    public string Arguments { get; set; } = string.Empty;
    public string StandardOutput { get; set; } = string.Empty;
    public string StandardError { get; set; } = string.Empty;
    public int ExitCode { get; set; }
    public TimeSpan Duration { get; set; }
    public DateTime Timestamp { get; set; } = DateTime.UtcNow;
    public bool Success => ExitCode == 0;
    public string Status => Success ? "✓ Passed" : "✗ Failed";
}
