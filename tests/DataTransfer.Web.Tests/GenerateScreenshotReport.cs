using Xunit;

namespace DataTransfer.Web.Tests;

/// <summary>
/// Test that runs after all other tests to generate the HTML screenshot report
/// </summary>
public class GenerateScreenshotReport
{
    [Fact]
    public async Task Generate_HTML_Report_From_Screenshots()
    {
        var generator = new ScreenshotReportGenerator();
        await generator.GenerateReportAsync();

        // Verify report was created
        var reportPath = "test-results/TestReport.html";
        Assert.True(File.Exists(reportPath), $"Report should be generated at {reportPath}");
    }
}
