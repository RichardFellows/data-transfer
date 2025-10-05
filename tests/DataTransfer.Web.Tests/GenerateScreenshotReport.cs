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

        // Verify report was created if screenshots exist
        var reportPath = "test-results/TestReport.html";
        var screenshotDir = "test-results/screenshots";

        if (Directory.Exists(screenshotDir) && Directory.GetFiles(screenshotDir, "*.png").Any())
        {
            Assert.True(File.Exists(reportPath), $"Report should be generated at {reportPath} when screenshots exist");
        }
        else
        {
            // No screenshots available - report generation is optional
            Assert.True(true, "No screenshots to generate report from");
        }
    }
}
