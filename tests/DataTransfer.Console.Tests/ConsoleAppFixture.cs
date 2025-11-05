using CliWrap;
using CliWrap.Buffered;
using Xunit;

namespace DataTransfer.Console.Tests;

/// <summary>
/// xUnit CollectionFixture that builds the console app once before all tests run.
/// This eliminates the 'dotnet run' compilation overhead on each test execution.
/// </summary>
public class ConsoleAppFixture : IAsyncLifetime
{
    private const string ProjectPath = "src/DataTransfer.Console";

    /// <summary>
    /// Path to the pre-built console application binary
    /// </summary>
    public string BinaryPath { get; private set; } = string.Empty;

    /// <summary>
    /// Working directory for test execution (solution root)
    /// </summary>
    public string WorkingDirectory { get; }

    public ConsoleAppFixture()
    {
        // Find solution root by looking for .sln file
        var current = Directory.GetCurrentDirectory();
        while (current != null && !Directory.GetFiles(current, "*.sln").Any())
        {
            current = Directory.GetParent(current)?.FullName;
        }

        WorkingDirectory = current ?? Directory.GetCurrentDirectory();
        System.Console.WriteLine($"Solution root: {WorkingDirectory}");
    }

    public async Task InitializeAsync()
    {
        BinaryPath = Path.Combine(WorkingDirectory, ProjectPath, "bin/Debug/net8.0/DataTransfer.Console");

        // Check if binary already exists (pre-built)
        if (File.Exists(BinaryPath))
        {
            System.Console.WriteLine($"✓ Using existing console app binary: {BinaryPath}");
            return;
        }

        // Build console app if it doesn't exist
        System.Console.WriteLine("Building console application for tests...");

        var buildResult = await Cli.Wrap("dotnet")
            .WithArguments($"build {ProjectPath} -c Debug --nologo -v quiet")
            .WithWorkingDirectory(WorkingDirectory)
            .WithValidation(CliWrap.CommandResultValidation.None)
            .ExecuteBufferedAsync();

        if (buildResult.ExitCode != 0)
        {
            throw new InvalidOperationException(
                $"Failed to build console app. Exit code: {buildResult.ExitCode}\n" +
                $"Error: {buildResult.StandardError}");
        }

        if (!File.Exists(BinaryPath))
        {
            throw new FileNotFoundException($"Console app binary not found at: {BinaryPath}");
        }

        System.Console.WriteLine($"✓ Console app built successfully: {BinaryPath}");
    }

    public Task DisposeAsync()
    {
        // No cleanup needed - binary remains for potential debugging
        return Task.CompletedTask;
    }
}

/// <summary>
/// Collection definition for Layer 1 CLI tests (no database required)
/// </summary>
[CollectionDefinition("ConsoleApp")]
public class ConsoleAppCollection : ICollectionFixture<ConsoleAppFixture>
{
    // This class is never instantiated - it's just a marker for xUnit
}
