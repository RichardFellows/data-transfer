using Microsoft.Playwright;
using Xunit;

namespace DataTransfer.Web.Tests;

/// <summary>
/// End-to-end workflow tests showing complete data transfer operations
/// Captures screenshots at each step to document the process
/// </summary>
[Collection("WebApplication")]
public class WorkflowTests : PlaywrightTestBase
{
    public WorkflowTests(WebApplicationFixture webFixture) : base(webFixture)
    {
    }

    [Fact]
    public async Task Workflow_SqlToParquet_Complete_Transfer()
    {
        // This test demonstrates the complete workflow of extracting data from SQL Server to Parquet
        var page = await _browser!.NewPageAsync();

        try
        {
            // Step 1: Navigate to New Transfer page
            await page.GotoAsync($"{BaseUrl}/transfer/new");
            await page.WaitForLoadStateAsync(LoadState.NetworkIdle);
            await CaptureScreenshotAsync(page, "Workflow_SqlToParquet", "01_new_transfer_page");

            // Step 2: Select SQL→Parquet transfer type
            await page.Locator("select").First.SelectOptionAsync("SqlToParquet");
            await page.WaitForTimeoutAsync(500);
            await CaptureScreenshotAsync(page, "Workflow_SqlToParquet", "02_transfer_type_selected");

            // Step 3: Select connection preset (LocalDemo)
            var presetDropdown = page.Locator("select#connectionPreset");
            var presetOptions = presetDropdown.Locator("option");
            var presetCount = await presetOptions.CountAsync();

            if (presetCount > 1)
            {
                // Select first preset (index 1, skipping "Custom" at index 0)
                await presetDropdown.SelectOptionAsync(new SelectOptionValue { Index = 1 });
                await page.WaitForTimeoutAsync(1000);
                await CaptureScreenshotAsync(page, "Workflow_SqlToParquet", "03_connection_preset_selected");

                // Step 4: Test connection
                var testButton = page.Locator("button:has-text('Test Connection')");
                await testButton.ClickAsync();
                await page.WaitForTimeoutAsync(2000); // Wait for connection test and database load
                await CaptureScreenshotAsync(page, "Workflow_SqlToParquet", "04_connection_tested");

                // Step 5: Select database (if available)
                var databaseDropdown = page.Locator("select#database");
                if (await databaseDropdown.IsVisibleAsync())
                {
                    var dbOptions = databaseDropdown.Locator("option");
                    var dbCount = await dbOptions.CountAsync();

                    if (dbCount > 1)
                    {
                        // Select first database (index 1, skipping placeholder)
                        await databaseDropdown.SelectOptionAsync(new SelectOptionValue { Index = 1 });
                        await page.WaitForTimeoutAsync(1500); // Wait for schema load
                        await CaptureScreenshotAsync(page, "Workflow_SqlToParquet", "05_database_selected");

                        // Step 6: Schema should auto-select to "dbo" - capture it
                        var schemaDropdown = page.Locator("select#schema");
                        if (await schemaDropdown.IsVisibleAsync())
                        {
                            await page.WaitForTimeoutAsync(1500); // Wait for table load
                            await CaptureScreenshotAsync(page, "Workflow_SqlToParquet", "06_schema_auto_selected");

                            // Step 7: Select a table (if available)
                            var tableDropdown = page.Locator("select#table");
                            if (await tableDropdown.IsVisibleAsync())
                            {
                                var tableOptions = tableDropdown.Locator("option");
                                var tableCount = await tableOptions.CountAsync();

                                if (tableCount > 1)
                                {
                                    // Select first table (index 1, skipping placeholder)
                                    await tableDropdown.SelectOptionAsync(new SelectOptionValue { Index = 1 });
                                    await page.WaitForTimeoutAsync(500);
                                    await CaptureScreenshotAsync(page, "Workflow_SqlToParquet", "07_table_selected");

                                    // Step 8: Enter Parquet file name
                                    // Use more specific selector to find the Parquet File Name input
                                    var parquetFileNameLabel = page.Locator("label:has-text('Parquet File Name')");
                                    var parquetFileNameInput = parquetFileNameLabel.Locator("..").Locator("input");
                                    await parquetFileNameInput.FillAsync("workflow_export");
                                    await page.WaitForTimeoutAsync(500);

                                    await CaptureScreenshotAsync(page, "Workflow_SqlToParquet", "08_parquet_filename_entered");

                                    // Step 9: Show the complete form ready to submit
                                    await CaptureScreenshotAsync(page, "Workflow_SqlToParquet", "09_ready_to_execute");

                                    // Step 10: Click Execute Transfer button
                                    var executeButton = page.Locator("button[type='submit']");
                                    await executeButton.ClickAsync();
                                    await page.WaitForTimeoutAsync(1000);
                                    await CaptureScreenshotAsync(page, "Workflow_SqlToParquet", "10_transfer_executing");

                                    // Step 11: Wait for transfer to complete (or timeout)
                                    // Look for success or error message
                                    await page.WaitForTimeoutAsync(10000); // Give it time to process
                                    await CaptureScreenshotAsync(page, "Workflow_SqlToParquet", "11_transfer_complete");
                                }
                            }
                        }
                    }
                }
            }
        }
        finally
        {
            await page.CloseAsync();
        }
    }

    [Fact]
    public async Task Workflow_ParquetToSql_Complete_Transfer()
    {
        // This test demonstrates the complete workflow of loading Parquet data into SQL Server
        var page = await _browser!.NewPageAsync();

        try
        {
            // Step 1: Navigate to New Transfer page
            await page.GotoAsync($"{BaseUrl}/transfer/new");
            await page.WaitForLoadStateAsync(LoadState.NetworkIdle);
            await CaptureScreenshotAsync(page, "Workflow_ParquetToSql", "01_new_transfer_page");

            // Step 2: Select Parquet→SQL transfer type
            await page.Locator("select").First.SelectOptionAsync("ParquetToSql");
            await page.WaitForTimeoutAsync(500);
            await CaptureScreenshotAsync(page, "Workflow_ParquetToSql", "02_transfer_type_selected");

            // Step 3: Select Parquet file from dropdown
            var parquetFileDropdown = page.Locator("select#parquetFile");
            string? selectedParquetFile = null;

            if (await parquetFileDropdown.IsVisibleAsync())
            {
                var fileOptions = parquetFileDropdown.Locator("option");
                var fileCount = await fileOptions.CountAsync();

                if (fileCount > 1)
                {
                    // Select first available file (index 1, skipping placeholder)
                    await parquetFileDropdown.SelectOptionAsync(new SelectOptionValue { Index = 1 });
                    await page.WaitForTimeoutAsync(500);

                    // Get the selected file name to help match with destination table
                    selectedParquetFile = await parquetFileDropdown.InputValueAsync();

                    await CaptureScreenshotAsync(page, "Workflow_ParquetToSql", "03_parquet_file_selected");
                }
            }

            // Step 4: Select destination connection preset
            var destPresetDropdown = page.Locator("select#destConnectionPreset");
            if (await destPresetDropdown.IsVisibleAsync())
            {
                var presetOptions = destPresetDropdown.Locator("option");
                var presetCount = await presetOptions.CountAsync();

                if (presetCount > 1)
                {
                    // Select first preset (index 1, skipping "Custom")
                    await destPresetDropdown.SelectOptionAsync(new SelectOptionValue { Index = 1 });
                    await page.WaitForTimeoutAsync(1000);
                    await CaptureScreenshotAsync(page, "Workflow_ParquetToSql", "04_destination_preset_selected");

                    // Step 5: Test destination connection
                    var testButtons = page.Locator("button:has-text('Test Connection')");
                    var testButtonCount = await testButtons.CountAsync();
                    if (testButtonCount > 0)
                    {
                        // Click the destination test button (should be the last one)
                        await testButtons.Last.ClickAsync();
                        await page.WaitForTimeoutAsync(2000);
                        await CaptureScreenshotAsync(page, "Workflow_ParquetToSql", "05_destination_connection_tested");

                        // Step 6: Select destination database
                        var destDatabaseDropdown = page.Locator("select#destDatabase");
                        if (await destDatabaseDropdown.IsVisibleAsync())
                        {
                            var dbOptions = destDatabaseDropdown.Locator("option");
                            var dbCount = await dbOptions.CountAsync();

                            if (dbCount > 1)
                            {
                                // Select first database
                                await destDatabaseDropdown.SelectOptionAsync(new SelectOptionValue { Index = 1 });
                                await page.WaitForTimeoutAsync(1500);
                                await CaptureScreenshotAsync(page, "Workflow_ParquetToSql", "06_destination_database_selected");

                                // Step 7: Schema should auto-select to "dbo"
                                var destSchemaDropdown = page.Locator("select#destSchema");
                                if (await destSchemaDropdown.IsVisibleAsync())
                                {
                                    await page.WaitForTimeoutAsync(1500);
                                    await CaptureScreenshotAsync(page, "Workflow_ParquetToSql", "07_destination_schema_auto_selected");

                                    // Step 8: Select destination table
                                    var destTableDropdown = page.Locator("select#destTable");
                                    if (await destTableDropdown.IsVisibleAsync())
                                    {
                                        var tableOptions = destTableDropdown.Locator("option");
                                        var tableCount = await tableOptions.CountAsync();

                                        if (tableCount > 1)
                                        {
                                            // Try to find a table matching the Parquet file name
                                            int selectedIndex = 1;
                                            string? matchingTableName = null;

                                            // Extract table name from Parquet file path (e.g., "year=2025/.../customers.parquet" -> "customers")
                                            if (!string.IsNullOrEmpty(selectedParquetFile))
                                            {
                                                var fileName = selectedParquetFile.Split('/').Last();
                                                matchingTableName = System.IO.Path.GetFileNameWithoutExtension(fileName);
                                            }

                                            // Look for a table with matching name, or skip system tables
                                            for (int i = 1; i < tableCount; i++)
                                            {
                                                var optionText = await tableOptions.Nth(i).TextContentAsync();
                                                if (optionText != null)
                                                {
                                                    // First priority: find table matching Parquet file name
                                                    if (matchingTableName != null &&
                                                        optionText.Contains(matchingTableName, StringComparison.OrdinalIgnoreCase))
                                                    {
                                                        selectedIndex = i;
                                                        break;
                                                    }

                                                    // Second priority: skip system tables (MS*, spt_*, sys*, dt_*)
                                                    bool isSystemTable = optionText.StartsWith("MS", StringComparison.OrdinalIgnoreCase) ||
                                                                        optionText.StartsWith("spt_", StringComparison.OrdinalIgnoreCase) ||
                                                                        optionText.StartsWith("sys", StringComparison.OrdinalIgnoreCase) ||
                                                                        optionText.StartsWith("dt_", StringComparison.OrdinalIgnoreCase);

                                                    if (!isSystemTable && selectedIndex == 1)
                                                    {
                                                        selectedIndex = i;
                                                        // Don't break - keep looking for matching table name
                                                    }
                                                }
                                            }

                                            await destTableDropdown.SelectOptionAsync(new SelectOptionValue { Index = selectedIndex });
                                            await page.WaitForTimeoutAsync(500);
                                            await CaptureScreenshotAsync(page, "Workflow_ParquetToSql", "08_destination_table_selected");

                                            // Step 9: Show complete form ready to execute
                                            await CaptureScreenshotAsync(page, "Workflow_ParquetToSql", "09_ready_to_execute");

                                            // Step 10: Click Execute Transfer button
                                            var executeButton = page.Locator("button[type='submit']");
                                            await executeButton.ClickAsync();
                                            await page.WaitForTimeoutAsync(1000);
                                            await CaptureScreenshotAsync(page, "Workflow_ParquetToSql", "10_transfer_executing");

                                            // Step 11: Wait for transfer to complete
                                            await page.WaitForTimeoutAsync(10000);
                                            await CaptureScreenshotAsync(page, "Workflow_ParquetToSql", "11_transfer_complete");
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        finally
        {
            await page.CloseAsync();
        }
    }

    [Fact]
    public async Task Workflow_Check_Transfer_History()
    {
        // This test shows how to view transfer history after completing transfers
        var page = await _browser!.NewPageAsync();

        try
        {
            // Navigate to History page
            await page.GotoAsync($"{BaseUrl}/history");
            await page.WaitForLoadStateAsync(LoadState.NetworkIdle);
            await page.WaitForTimeoutAsync(1000);
            await CaptureScreenshotAsync(page, "Workflow_History", "01_history_page_with_transfers");

            // If there's a table with transfers, capture it
            var transferTable = page.Locator("table.table");
            if (await transferTable.IsVisibleAsync())
            {
                // Capture the table showing completed transfers
                await CaptureElementScreenshotAsync(transferTable, "Workflow_History", "02_transfer_table_detail");
            }

            // Click refresh button to show it working
            var refreshButton = page.Locator("button:has-text('Refresh')");
            if (await refreshButton.IsVisibleAsync())
            {
                await refreshButton.ClickAsync();
                await page.WaitForTimeoutAsync(500);
                await CaptureScreenshotAsync(page, "Workflow_History", "03_after_refresh");
            }
        }
        finally
        {
            await page.CloseAsync();
        }
    }
}
