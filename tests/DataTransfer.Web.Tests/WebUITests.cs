using Microsoft.Playwright;
using Xunit;

namespace DataTransfer.Web.Tests;

/// <summary>
/// End-to-end tests for the DataTransfer web UI
/// Tests are designed to verify expected behavior and capture current state
/// </summary>
public class WebUITests : IAsyncLifetime
{
    private IPlaywright? _playwright;
    private IBrowser? _browser;
    private const string BaseUrl = "http://localhost:5000";

    public async Task InitializeAsync()
    {
        _playwright = await Playwright.CreateAsync();
        _browser = await _playwright.Chromium.LaunchAsync(new()
        {
            Headless = true
        });
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

    [Fact]
    public async Task HomePage_Should_Load_And_Display_Dashboard()
    {
        // Arrange
        var page = await _browser!.NewPageAsync();

        try
        {
            // Act
            await page.GotoAsync(BaseUrl);

            // Assert - Page title
            await Assertions.Expect(page).ToHaveTitleAsync(new System.Text.RegularExpressions.Regex("Data Transfer Dashboard"));

            // Assert - Main heading
            var heading = page.Locator("h1");
            await Assertions.Expect(heading).ToHaveTextAsync("Data Transfer Dashboard");

            // Assert - Statistics cards should be present
            var statsCards = page.Locator(".card");
            await Assertions.Expect(statsCards).Not.ToHaveCountAsync(0);

            // Assert - Navigation links
            var newTransferLink = page.Locator("a[href='/transfer/new']");
            await Assertions.Expect(newTransferLink).ToBeVisibleAsync();

            var historyLink = page.Locator("a[href='/history']");
            await Assertions.Expect(historyLink).ToBeVisibleAsync();
        }
        finally
        {
            await page.CloseAsync();
        }
    }

    [Fact]
    public async Task NewTransferPage_Should_Load_With_Form_Elements()
    {
        // Arrange
        var page = await _browser!.NewPageAsync();

        try
        {
            // Act
            await page.GotoAsync($"{BaseUrl}/transfer/new");

            // Assert - Page loads
            await Assertions.Expect(page).ToHaveTitleAsync(new System.Text.RegularExpressions.Regex("New Transfer"));

            // Assert - Transfer type selector is present
            var transferTypeSelect = page.Locator("select");
            await Assertions.Expect(transferTypeSelect).ToBeVisibleAsync();

            // Assert - Submit button is present
            var submitButton = page.Locator("button[type='submit']");
            await Assertions.Expect(submitButton).ToBeVisibleAsync();
            await Assertions.Expect(submitButton).ToContainTextAsync("Execute Transfer");
        }
        finally
        {
            await page.CloseAsync();
        }
    }

    [Fact]
    public async Task NewTransferPage_SqlToParquet_Should_Show_Correct_Form_Fields()
    {
        // Arrange
        var page = await _browser!.NewPageAsync();

        try
        {
            // Act
            await page.GotoAsync($"{BaseUrl}/transfer/new");

            // Wait for page to be interactive
            await page.WaitForLoadStateAsync(LoadState.NetworkIdle);

            // Select SQL→Parquet transfer type
            await page.Locator("select").SelectOptionAsync("SqlToParquet");

            // Wait a moment for UI to update
            await page.WaitForTimeoutAsync(500);

            // Assert - SQL Server source section should be visible
            var sqlSourceHeader = page.Locator("text=SQL Server Source");
            await Assertions.Expect(sqlSourceHeader).ToBeVisibleAsync();

            // Assert - Parquet destination section should be visible
            var parquetDestHeader = page.Locator("text=Parquet Destination");
            await Assertions.Expect(parquetDestHeader).ToBeVisibleAsync();

            // Assert - Source connection string field
            var sourceConnectionString = page.Locator("label:text('Connection String')").First;
            await Assertions.Expect(sourceConnectionString).ToBeVisibleAsync();

            // Assert - Parquet file name field
            var parquetFileName = page.Locator("label:text('Parquet File Name')");
            await Assertions.Expect(parquetFileName).ToBeVisibleAsync();
        }
        finally
        {
            await page.CloseAsync();
        }
    }

    [Fact]
    public async Task NewTransferPage_ParquetToSql_Should_Show_Correct_Form_Fields()
    {
        // Arrange
        var page = await _browser!.NewPageAsync();

        try
        {
            // Act
            await page.GotoAsync($"{BaseUrl}/transfer/new");
            await page.WaitForLoadStateAsync(LoadState.NetworkIdle);

            // Select Parquet→SQL transfer type
            await page.Locator("select").SelectOptionAsync("ParquetToSql");
            await page.WaitForTimeoutAsync(500);

            // Assert - Parquet source section should be visible
            var parquetSourceHeader = page.Locator("text=Parquet Source");
            await Assertions.Expect(parquetSourceHeader).ToBeVisibleAsync();

            // Assert - SQL Server destination section should be visible
            var sqlDestHeader = page.Locator("text=SQL Server Destination");
            await Assertions.Expect(sqlDestHeader).ToBeVisibleAsync();

            // Assert - Parquet file path field
            var parquetPath = page.Locator("label:text('Parquet File Path')");
            await Assertions.Expect(parquetPath).ToBeVisibleAsync();

            // Assert - Destination connection string field
            var destConnectionString = page.Locator("label:text('Connection String')").Last;
            await Assertions.Expect(destConnectionString).ToBeVisibleAsync();
        }
        finally
        {
            await page.CloseAsync();
        }
    }

    [Fact]
    public async Task NewTransferPage_SqlToSql_Submit_Should_Show_Error_Or_NotImplemented()
    {
        // Arrange
        var page = await _browser!.NewPageAsync();

        try
        {
            // Act
            await page.GotoAsync($"{BaseUrl}/transfer/new");
            await page.WaitForLoadStateAsync(LoadState.NetworkIdle);

            // Select SQL→SQL transfer type
            await page.Locator("select").SelectOptionAsync("SqlToSql");
            await page.WaitForTimeoutAsync(500);

            // Fill in minimal form data (both source and destination SQL Server)
            var connectionStrings = page.Locator("input[class*='form-control']").Filter(new() { HasText = "" });

            // Try to submit (even with empty data, we want to see what error we get)
            var submitButton = page.Locator("button[type='submit']");
            await submitButton.ClickAsync();

            // Wait for response
            await page.WaitForTimeoutAsync(2000);

            // Assert - Should show some kind of error or not implemented message
            // This test documents the CURRENT behavior (NotImplementedException)
            var alertDanger = page.Locator(".alert-danger");
            var hasError = await alertDanger.CountAsync() > 0;

            // Expected behavior: Either shows validation error or NotImplementedException
            Assert.True(hasError, "Expected to see an error message for SQL→SQL transfer");

            // Document what error we see
            if (hasError)
            {
                var errorText = await alertDanger.First.TextContentAsync();
                // We expect to see error about SQL→SQL not being implemented in UnifiedTransferOrchestrator
                Assert.Contains("SQL→SQL", errorText);
                Assert.Contains("DataTransferOrchestrator", errorText);
            }
        }
        finally
        {
            await page.CloseAsync();
        }
    }

    [Fact]
    public async Task HistoryPage_Should_Load_And_Display_Table()
    {
        // Arrange
        var page = await _browser!.NewPageAsync();

        try
        {
            // Act
            await page.GotoAsync($"{BaseUrl}/history");

            // Assert - Page loads
            await Assertions.Expect(page).ToHaveTitleAsync(new System.Text.RegularExpressions.Regex("Transfer History"));

            // Assert - Main heading
            var heading = page.Locator("h1");
            await Assertions.Expect(heading).ToHaveTextAsync("Transfer History");

            // Assert - Either shows "No transfers yet" message or table with transfers
            var noTransfersAlert = page.Locator(".alert-info:has-text('No transfers yet')");
            var transferTable = page.Locator("table.table");

            var hasNoTransfers = await noTransfersAlert.CountAsync() > 0;
            var hasTable = await transferTable.CountAsync() > 0;

            // One of these should be true
            Assert.True(hasNoTransfers || hasTable,
                "History page should show either 'No transfers' message or a table with transfers");

            // If table exists, verify it has the expected columns
            if (hasTable)
            {
                var headers = page.Locator("table thead th");
                var headerCount = await headers.CountAsync();
                Assert.True(headerCount >= 7, $"Expected at least 7 column headers, got {headerCount}");

                // Verify column names
                await Assertions.Expect(headers.Nth(0)).ToContainTextAsync("Status");
                await Assertions.Expect(headers.Nth(1)).ToContainTextAsync("Type");
                await Assertions.Expect(headers.Nth(2)).ToContainTextAsync("Source");
                await Assertions.Expect(headers.Nth(3)).ToContainTextAsync("Destination");
                await Assertions.Expect(headers.Nth(4)).ToContainTextAsync("Rows");
                await Assertions.Expect(headers.Nth(5)).ToContainTextAsync("Duration");
                await Assertions.Expect(headers.Nth(6)).ToContainTextAsync("Start Time");
            }

            // Assert - Refresh button should be present (if we have the table/history section)
            var refreshButton = page.Locator("button:has-text('Refresh')");
            if (hasTable)
            {
                await Assertions.Expect(refreshButton).ToBeVisibleAsync();
            }
        }
        finally
        {
            await page.CloseAsync();
        }
    }

    [Fact]
    public async Task Navigation_Should_Work_Between_Pages()
    {
        // Arrange
        var page = await _browser!.NewPageAsync();

        try
        {
            // Start at home page
            await page.GotoAsync(BaseUrl);
            await page.WaitForLoadStateAsync(LoadState.NetworkIdle);

            // Act & Assert - Navigate to New Transfer
            var newTransferLink = page.Locator("a[href='/transfer/new']").First;
            await newTransferLink.ClickAsync();
            await page.WaitForLoadStateAsync(LoadState.NetworkIdle);
            await Assertions.Expect(page).ToHaveURLAsync($"{BaseUrl}/transfer/new");

            // Act & Assert - Navigate to History
            var historyNavLink = page.Locator("nav a[href='history']");
            await historyNavLink.ClickAsync();
            await page.WaitForLoadStateAsync(LoadState.NetworkIdle);
            await Assertions.Expect(page).ToHaveURLAsync($"{BaseUrl}/history");

            // Act & Assert - Navigate back to Home
            var homeLink = page.Locator("nav a[href='']").First;
            await homeLink.ClickAsync();
            await page.WaitForLoadStateAsync(LoadState.NetworkIdle);
            await Assertions.Expect(page).ToHaveURLAsync($"{BaseUrl}/");
        }
        finally
        {
            await page.CloseAsync();
        }
    }
}
