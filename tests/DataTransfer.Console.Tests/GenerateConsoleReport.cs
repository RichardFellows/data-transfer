using Xunit;

namespace DataTransfer.Console.Tests;

/// <summary>
/// Test class that generates HTML report from console test captures
/// Runs after all console integration tests complete
/// </summary>
[Collection("ConsoleReport")]
public class GenerateConsoleReport
{
    [Fact]
    public async Task GenerateHtmlReport()
    {
        var generator = new ConsoleReportGenerator();
        await generator.GenerateReportAsync();

        // This test always passes - it's just for report generation
        Assert.True(true);
    }
}
