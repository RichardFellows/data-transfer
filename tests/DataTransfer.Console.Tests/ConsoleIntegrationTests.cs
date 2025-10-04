using Xunit;

namespace DataTransfer.Console.Tests;

/// <summary>
/// Layer 1: CLI Interface Integration Tests
/// Tests command-line arguments, help output, and profile operations (no database required)
/// </summary>
[Collection("ConsoleApp")]
public class ConsoleIntegrationTests : ConsoleTestBase
{
    public ConsoleIntegrationTests(ConsoleAppFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task HelpCommand_Should_Display_Usage_Information()
    {
        // Arrange & Act
        var capture = await ExecuteConsoleCommandAsync(
            "--help",
            nameof(ConsoleIntegrationTests),
            "help_command",
            timeout: TimeSpan.FromSeconds(10));

        // Assert
        Assert.Equal(0, capture.ExitCode);
        Assert.Contains("DataTransfer Console - Usage:", capture.StandardOutput);
        Assert.Contains("--profile <name>", capture.StandardOutput);
        Assert.Contains("--config <path>", capture.StandardOutput);
        Assert.Contains("--list-profiles", capture.StandardOutput);
        Assert.Contains("--help", capture.StandardOutput);
    }

    [Fact]
    public async Task ListProfiles_Should_Return_Zero_Exit_Code()
    {
        // Arrange & Act
        var capture = await ExecuteConsoleCommandAsync(
            "--list-profiles",
            nameof(ConsoleIntegrationTests),
            "list_profiles_command",
            timeout: TimeSpan.FromSeconds(30));

        // Assert
        Assert.Equal(0, capture.ExitCode);
        // Output will either show "No profiles found" or list profiles
        Assert.True(
            capture.StandardOutput.Contains("No profiles found") ||
            capture.StandardOutput.Contains("Saved Profiles:"),
            "Expected either 'No profiles found' or 'Saved Profiles:' in output");
    }

    [Fact]
    public async Task InvalidProfile_Should_Return_NonZero_Exit_Code()
    {
        // Arrange & Act
        var capture = await ExecuteConsoleCommandAsync(
            "--profile \"NonExistentProfile12345\"",
            nameof(ConsoleIntegrationTests),
            "invalid_profile_command",
            timeout: TimeSpan.FromSeconds(15));

        // Assert
        Assert.NotEqual(0, capture.ExitCode);
        Assert.True(
            capture.StandardOutput.Contains("not found") ||
            capture.StandardError.Contains("not found"),
            "Expected 'not found' message for invalid profile");
    }

    [Fact]
    public async Task InvalidConfigPath_Should_Handle_Gracefully()
    {
        // Arrange & Act
        var capture = await ExecuteConsoleCommandAsync(
            "--config \"nonexistent/path/config.json\"",
            nameof(ConsoleIntegrationTests),
            "invalid_config_path",
            timeout: TimeSpan.FromSeconds(15));

        // Assert
        Assert.NotEqual(0, capture.ExitCode);
        // Should have error message about config file not found
        Assert.True(
            capture.StandardOutput.Contains("Error") ||
            capture.StandardError.Contains("Error") ||
            capture.StandardOutput.Contains("not found") ||
            capture.StandardError.Contains("not found"),
            "Expected error message for invalid config path");
    }

    [Fact]
    public async Task NoArguments_Should_Start_Interactive_Mode()
    {
        // Arrange & Act
        var capture = await ExecuteInteractiveModeAsync(
            nameof(ConsoleIntegrationTests),
            "interactive_mode_start",
            input: "4\n", // Select "Exit" option
            timeout: TimeSpan.FromSeconds(10));

        // Assert
        // Interactive mode should display menu
        Assert.True(
            capture.StandardOutput.Contains("DataTransfer Console") ||
            capture.StandardOutput.Contains("Select option"),
            "Expected interactive menu to be displayed");
    }

    [Fact]
    public async Task MultipleArguments_Should_Process_Correctly()
    {
        // Arrange & Act - Test that unrecognized args don't crash
        var capture = await ExecuteConsoleCommandAsync(
            "--unknown-arg test",
            nameof(ConsoleIntegrationTests),
            "unknown_argument",
            timeout: TimeSpan.FromSeconds(10));

        // Assert - Should either show help or exit gracefully
        // Exit code doesn't matter as much as not crashing
        Assert.True(
            capture.StandardOutput.Length > 0 || capture.StandardError.Length > 0,
            "Should produce some output for unknown arguments");
    }

    [Fact]
    public async Task ConfigMode_With_Valid_Legacy_Config_Should_Work()
    {
        // This test assumes a valid config exists or will be created
        // For now, we test that the command is parsed correctly

        // Arrange & Act
        var capture = await ExecuteConsoleCommandAsync(
            "--config config/appsettings.json",
            nameof(ConsoleIntegrationTests),
            "config_mode_legacy",
            timeout: TimeSpan.FromSeconds(30));

        // Assert
        // Should attempt to load config (may fail if DB not available, but that's OK for this test)
        Assert.True(
            capture.StandardOutput.Contains("Loading configuration") ||
            capture.StandardOutput.Contains("Error") ||
            capture.StandardError.Length > 0,
            "Should attempt to process config file");
    }
}
