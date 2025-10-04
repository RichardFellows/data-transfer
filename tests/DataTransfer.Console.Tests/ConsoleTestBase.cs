using CliWrap;
using CliWrap.Buffered;
using System.Diagnostics;
using System.Text.Json;
using Xunit;

namespace DataTransfer.Console.Tests;

/// <summary>
/// Base class for console integration tests with output capture and reporting
/// Similar to PlaywrightTestBase but for console applications
/// </summary>
public abstract class ConsoleTestBase : IAsyncLifetime
{
    protected static readonly string OutputDirectory = Path.Combine("test-results", "console-output");
    protected static readonly string MetadataFile = Path.Combine(OutputDirectory, "captures.json");
    protected static readonly List<ConsoleOutputCapture> Captures = new();
    protected const string ProjectPath = "src/DataTransfer.Console";

    public virtual Task InitializeAsync()
    {
        // Ensure output directory exists
        Directory.CreateDirectory(OutputDirectory);
        return Task.CompletedTask;
    }

    public virtual Task DisposeAsync()
    {
        // Save captures to metadata file for report generation
        SaveCapturesToMetadata();
        return Task.CompletedTask;
    }

    /// <summary>
    /// Executes a console command and captures output for reporting
    /// </summary>
    protected async Task<ConsoleOutputCapture> ExecuteConsoleCommandAsync(
        string arguments,
        string testName,
        string stepName,
        TimeSpan? timeout = null)
    {
        var stopwatch = Stopwatch.StartNew();

        var result = await Cli.Wrap("dotnet")
            .WithArguments($"run --project {ProjectPath} -- {arguments}")
            .WithWorkingDirectory("/home/richard/sonnet45")
            .WithValidation(CommandResultValidation.None) // Don't throw on non-zero exit
            .ExecuteBufferedAsync(cancellationToken: new CancellationTokenSource(timeout ?? TimeSpan.FromSeconds(30)).Token);

        stopwatch.Stop();

        var capture = new ConsoleOutputCapture
        {
            TestName = testName,
            StepName = stepName,
            Command = "dotnet",
            Arguments = $"run --project {ProjectPath} -- {arguments}",
            StandardOutput = result.StandardOutput,
            StandardError = result.StandardError,
            ExitCode = result.ExitCode,
            Duration = stopwatch.Elapsed
        };

        // Save output to individual files
        await SaveCaptureToFileAsync(capture);

        // Add to collection for report generation
        lock (Captures)
        {
            Captures.Add(capture);
        }

        return capture;
    }

    /// <summary>
    /// Saves captured output to individual text files
    /// </summary>
    private async Task SaveCaptureToFileAsync(ConsoleOutputCapture capture)
    {
        var fileName = $"{capture.TestName}_{capture.StepName}";
        var outputFile = Path.Combine(OutputDirectory, $"{fileName}_stdout.txt");
        var errorFile = Path.Combine(OutputDirectory, $"{fileName}_stderr.txt");
        var summaryFile = Path.Combine(OutputDirectory, $"{fileName}_summary.txt");

        await File.WriteAllTextAsync(outputFile, capture.StandardOutput);
        await File.WriteAllTextAsync(errorFile, capture.StandardError);

        var summary = $"""
            Test: {capture.TestName}
            Step: {capture.StepName}
            Command: {capture.Command} {capture.Arguments}
            Exit Code: {capture.ExitCode}
            Duration: {capture.Duration.TotalMilliseconds:F2}ms
            Status: {capture.Status}
            Timestamp: {capture.Timestamp:yyyy-MM-dd HH:mm:ss}
            """;

        await File.WriteAllTextAsync(summaryFile, summary);
    }

    /// <summary>
    /// Saves all captures to JSON metadata file for report generation
    /// </summary>
    private void SaveCapturesToMetadata()
    {
        try
        {
            var json = JsonSerializer.Serialize(Captures, new JsonSerializerOptions
            {
                WriteIndented = true
            });
            File.WriteAllText(MetadataFile, json);
        }
        catch (Exception ex)
        {
            System.Console.WriteLine($"Warning: Could not save captures metadata: {ex.Message}");
        }
    }

    /// <summary>
    /// Helper to execute console in interactive mode (no arguments)
    /// </summary>
    protected async Task<ConsoleOutputCapture> ExecuteInteractiveModeAsync(
        string testName,
        string stepName,
        string? input = null,
        TimeSpan? timeout = null)
    {
        // For interactive mode, we need to simulate stdin
        var stopwatch = Stopwatch.StartNew();

        var command = Cli.Wrap("dotnet")
            .WithArguments($"run --project {ProjectPath}")
            .WithWorkingDirectory("/home/richard/sonnet45")
            .WithValidation(CommandResultValidation.None);

        if (!string.IsNullOrEmpty(input))
        {
            command = command.WithStandardInputPipe(PipeSource.FromString(input));
        }

        var result = await command.ExecuteBufferedAsync(
            cancellationToken: new CancellationTokenSource(timeout ?? TimeSpan.FromSeconds(10)).Token);

        stopwatch.Stop();

        var capture = new ConsoleOutputCapture
        {
            TestName = testName,
            StepName = stepName,
            Command = "dotnet",
            Arguments = $"run --project {ProjectPath} (interactive mode)",
            StandardOutput = result.StandardOutput,
            StandardError = result.StandardError,
            ExitCode = result.ExitCode,
            Duration = stopwatch.Elapsed
        };

        await SaveCaptureToFileAsync(capture);

        lock (Captures)
        {
            Captures.Add(capture);
        }

        return capture;
    }
}
