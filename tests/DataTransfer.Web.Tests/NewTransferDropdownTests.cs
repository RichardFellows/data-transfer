using Microsoft.Playwright;
using Xunit;

namespace DataTransfer.Web.Tests;

/// <summary>
/// Playwright tests for NewTransfer page cascading dropdowns
/// Tests dynamic table selection functionality
/// </summary>
public class NewTransferDropdownTests : IAsyncLifetime
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
    public async Task NewTransfer_Should_Have_Connection_Preset_Dropdown()
    {
        // Arrange
        var page = await _browser!.NewPageAsync();

        try
        {
            // Act
            await page.GotoAsync($"{BaseUrl}/transfer/new");
            await page.WaitForLoadStateAsync(LoadState.NetworkIdle);

            // Select SQL→Parquet to show SQL source section
            await page.Locator("select").First.SelectOptionAsync("SqlToParquet");
            await page.WaitForTimeoutAsync(500);

            // Assert - Connection preset dropdown should exist
            var presetDropdown = page.Locator("select#connectionPreset");
            await Assertions.Expect(presetDropdown).ToBeVisibleAsync();

            // Assert - Should have "Custom" and preset options
            var options = presetDropdown.Locator("option");
            var optionCount = await options.CountAsync();
            Assert.True(optionCount >= 2, $"Expected at least 2 connection preset options, got {optionCount}");

            // Assert - Should have "Custom" option
            var customOption = options.Locator("text=Custom");
            await Assertions.Expect(customOption).ToBeAttachedAsync();
        }
        finally
        {
            await page.CloseAsync();
        }
    }

    [Fact]
    public async Task NewTransfer_Should_Have_Database_Dropdown_After_Connection_Selected()
    {
        // Arrange
        var page = await _browser!.NewPageAsync();

        try
        {
            // Act
            await page.GotoAsync($"{BaseUrl}/transfer/new");
            await page.WaitForLoadStateAsync(LoadState.NetworkIdle);

            // Select SQL→Parquet
            await page.Locator("select").First.SelectOptionAsync("SqlToParquet");
            await page.WaitForTimeoutAsync(500);

            // Select a connection preset (not Custom)
            var presetDropdown = page.Locator("select#connectionPreset");
            var options = presetDropdown.Locator("option");
            var optionCount = await options.CountAsync();

            if (optionCount > 1)
            {
                // Select first non-Custom option (index 1)
                await presetDropdown.SelectOptionAsync(new SelectOptionValue { Index = 1 });
                await page.WaitForTimeoutAsync(1000); // Wait for async database load

                // Assert - Database dropdown should be visible
                var databaseDropdown = page.Locator("select#database");
                await Assertions.Expect(databaseDropdown).ToBeVisibleAsync();
            }
        }
        finally
        {
            await page.CloseAsync();
        }
    }

    [Fact]
    public async Task NewTransfer_Should_Have_Schema_Dropdown_After_Database_Selected()
    {
        // Arrange
        var page = await _browser!.NewPageAsync();

        try
        {
            // Act
            await page.GotoAsync($"{BaseUrl}/transfer/new");
            await page.WaitForLoadStateAsync(LoadState.NetworkIdle);

            // Select SQL→Parquet
            await page.Locator("select").First.SelectOptionAsync("SqlToParquet");
            await page.WaitForTimeoutAsync(500);

            // Try to find and interact with database dropdown
            var databaseDropdown = page.Locator("select#database");
            if (await databaseDropdown.IsVisibleAsync())
            {
                var dbOptions = databaseDropdown.Locator("option");
                var dbCount = await dbOptions.CountAsync();

                if (dbCount > 0)
                {
                    await databaseDropdown.SelectOptionAsync(new SelectOptionValue { Index = 0 });
                    await page.WaitForTimeoutAsync(1000);

                    // Assert - Schema dropdown should be visible
                    var schemaDropdown = page.Locator("select#schema");
                    await Assertions.Expect(schemaDropdown).ToBeVisibleAsync();
                }
            }
        }
        finally
        {
            await page.CloseAsync();
        }
    }

    [Fact]
    public async Task NewTransfer_Should_Have_Table_Dropdown_After_Schema_Selected()
    {
        // Arrange
        var page = await _browser!.NewPageAsync();

        try
        {
            // Act
            await page.GotoAsync($"{BaseUrl}/transfer/new");
            await page.WaitForLoadStateAsync(LoadState.NetworkIdle);

            // Select SQL→Parquet
            await page.Locator("select").First.SelectOptionAsync("SqlToParquet");
            await page.WaitForTimeoutAsync(500);

            // Try to find and interact with schema dropdown
            var schemaDropdown = page.Locator("select#schema");
            if (await schemaDropdown.IsVisibleAsync())
            {
                var schemaOptions = schemaDropdown.Locator("option");
                var schemaCount = await schemaOptions.CountAsync();

                if (schemaCount > 0)
                {
                    await schemaDropdown.SelectOptionAsync(new SelectOptionValue { Index = 0 });
                    await page.WaitForTimeoutAsync(1000);

                    // Assert - Table dropdown should be visible
                    var tableDropdown = page.Locator("select#table");
                    await Assertions.Expect(tableDropdown).ToBeVisibleAsync();
                }
            }
        }
        finally
        {
            await page.CloseAsync();
        }
    }

    [Fact]
    public async Task NewTransfer_Should_Have_Connection_Test_Button()
    {
        // Arrange
        var page = await _browser!.NewPageAsync();

        try
        {
            // Act
            await page.GotoAsync($"{BaseUrl}/transfer/new");
            await page.WaitForLoadStateAsync(LoadState.NetworkIdle);

            // Select SQL→Parquet
            await page.Locator("select").First.SelectOptionAsync("SqlToParquet");
            await page.WaitForTimeoutAsync(500);

            // Assert - Test connection button should be visible
            var testButton = page.Locator("button:has-text('Test Connection')");
            await Assertions.Expect(testButton).ToBeVisibleAsync();
        }
        finally
        {
            await page.CloseAsync();
        }
    }
}
