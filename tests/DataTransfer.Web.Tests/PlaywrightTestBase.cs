using Microsoft.Playwright;
using Xunit;

namespace DataTransfer.Web.Tests;

/// <summary>
/// Base class for Playwright tests with screenshot capture functionality
/// Screenshots are saved to test-results/screenshots with organized structure
/// </summary>
public abstract class PlaywrightTestBase : IAsyncLifetime
{
    protected IPlaywright? _playwright;
    protected IBrowser? _browser;
    protected const string BaseUrl = "http://localhost:5000";
    protected static readonly string ScreenshotDirectory = Path.Combine("test-results", "screenshots");

    public async Task InitializeAsync()
    {
        _playwright = await Playwright.CreateAsync();
        _browser = await _playwright.Chromium.LaunchAsync(new()
        {
            Headless = true
        });

        // Ensure screenshot directory exists
        Directory.CreateDirectory(ScreenshotDirectory);
    }

    public async Task DisposeAsync()
    {
        if (_browser != null)
        {
            await _browser.CloseAsync();
            await _browser.DisposeAsync();
        }
        _playwright?.Dispose();
    }

    /// <summary>
    /// Captures a screenshot with descriptive name and saves to organized directory
    /// </summary>
    /// <param name="page">The page to capture</param>
    /// <param name="testName">Name of the test (e.g., "HomePage_Should_Load")</param>
    /// <param name="stepName">Name of the step (e.g., "01_initial_load")</param>
    /// <param name="fullPage">Whether to capture the full scrollable page</param>
    protected async Task CaptureScreenshotAsync(IPage page, string testName, string stepName, bool fullPage = true)
    {
        var fileName = $"{testName}_{stepName}.png";
        var filePath = Path.Combine(ScreenshotDirectory, fileName);

        await page.ScreenshotAsync(new()
        {
            Path = filePath,
            FullPage = fullPage
        });

        // Also save metadata for report generation
        var metadataPath = Path.Combine(ScreenshotDirectory, "metadata.txt");
        var metadata = $"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss}|{testName}|{stepName}|{fileName}\n";
        await File.AppendAllTextAsync(metadataPath, metadata);
    }

    /// <summary>
    /// Captures a screenshot of a specific element
    /// </summary>
    protected async Task CaptureElementScreenshotAsync(ILocator element, string testName, string stepName)
    {
        var fileName = $"{testName}_{stepName}.png";
        var filePath = Path.Combine(ScreenshotDirectory, fileName);

        await element.ScreenshotAsync(new()
        {
            Path = filePath
        });

        var metadataPath = Path.Combine(ScreenshotDirectory, "metadata.txt");
        var metadata = $"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss}|{testName}|{stepName}|{fileName}\n";
        await File.AppendAllTextAsync(metadataPath, metadata);
    }
}
