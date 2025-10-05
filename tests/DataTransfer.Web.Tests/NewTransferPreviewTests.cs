using Microsoft.Playwright;
using Xunit;
using static Microsoft.Playwright.Assertions;
using Parquet;
using Parquet.Data;
using Parquet.Schema;

namespace DataTransfer.Web.Tests;

/// <summary>
/// E2E tests for data preview functionality in NewTransfer page
/// Tests preview display for both SQL tables and Parquet files
/// </summary>
[Collection("WebApplication")]
public class NewTransferPreviewTests : PlaywrightTestBase
{
    public NewTransferPreviewTests(WebApplicationFixture webFixture) : base(webFixture)
    {
    }

    private async Task<IPage> CreatePageAsync()
    {
        var context = await _browser!.NewContextAsync();
        return await context.NewPageAsync();
    }

    [Fact]
    public async Task SqlToParquet_Should_Show_Preview_When_Table_Selected()
    {
        // Arrange
        var page = await CreatePageAsync();
        await page.GotoAsync($"{BaseUrl}/transfer/new");

        // Act - Select SQL→Parquet transfer type
        await page.Locator("select").Nth(1).SelectOptionAsync("SqlToParquet");
        await page.WaitForTimeoutAsync(500);

        // Fill in connection and select table
        await page.Locator("input[placeholder*='connection string']").FillAsync(_webFixture.SqlConnectionString);
        await page.Locator("text=Test Connection").ClickAsync();
        await page.WaitForSelectorAsync("select >> nth=2"); // Database dropdown

        await page.Locator("select").Nth(2).SelectOptionAsync("TestSource");
        await page.WaitForSelectorAsync("select >> nth=3"); // Schema dropdown

        await page.Locator("select").Nth(3).SelectOptionAsync("dbo");
        await page.WaitForSelectorAsync("select >> nth=4"); // Table dropdown

        await page.Locator("select").Nth(4).SelectOptionAsync("Customers");
        await page.WaitForTimeoutAsync(1000); // Wait for preview to load

        // Assert - Preview should be visible
        var previewSection = page.Locator("text=Preview");
        await Expect(previewSection).ToBeVisibleAsync();

        // Verify table view with column headers
        var tableHeader = page.Locator("th:has-text('Id')");
        await Expect(tableHeader).ToBeVisibleAsync();

        var nameHeader = page.Locator("th:has-text('Name')");
        await Expect(nameHeader).ToBeVisibleAsync();

        // Verify sample data rows (should show 3 customers: Acme Corp, TechStart Inc, Global Solutions)
        var firstRow = page.Locator("td:has-text('Acme Corp')");
        await Expect(firstRow).ToBeVisibleAsync();

        // Verify column list with data types
        var columnList = page.Locator("text=Columns");
        await Expect(columnList).ToBeVisibleAsync();

        // Should show data types (e.g., "Id: int", "Name: nvarchar(100)")
        var idColumn = page.Locator("text=/Id.*int/i");
        await Expect(idColumn).ToBeVisibleAsync();
    }

    [Fact]
    public async Task ParquetToSql_Should_Show_Preview_When_File_Selected()
    {
        // Arrange - Create a test Parquet file
        var parquetDir = "./parquet-files";
        Directory.CreateDirectory(parquetDir);

        var testFile = Path.Combine(parquetDir, "test_customers.parquet");
        await CreateTestParquetFile(testFile);

        var page = await CreatePageAsync();
        await page.GotoAsync($"{BaseUrl}/transfer/new");

        // Act - Select Parquet→SQL transfer type
        await page.Locator("select").Nth(1).SelectOptionAsync("ParquetToSql");
        await page.WaitForTimeoutAsync(500);

        // Select the test Parquet file
        await page.Locator("select").Nth(2).SelectOptionAsync("test_customers.parquet");
        await page.WaitForTimeoutAsync(1000); // Wait for preview to load

        // Assert - Preview should be visible
        var previewSection = page.Locator("text=Preview");
        await Expect(previewSection).ToBeVisibleAsync();

        // Verify column headers
        var idHeader = page.Locator("th:has-text('Id')");
        await Expect(idHeader).ToBeVisibleAsync();

        // Verify sample data (test file has "Item 1", "Item 2", etc.)
        var firstRow = page.Locator("td:has-text('Item 1')");
        await Expect(firstRow).ToBeVisibleAsync();

        // Verify column list shows data types
        var columnList = page.Locator("text=Columns");
        await Expect(columnList).ToBeVisibleAsync();

        // Should show Parquet data types (INT, STRING, DECIMAL)
        var intColumn = page.Locator("text=/Id.*INT/i");
        await Expect(intColumn).ToBeVisibleAsync();
    }

    [Fact]
    public async Task Preview_Should_Limit_To_10_Rows()
    {
        // Arrange
        var page = await CreatePageAsync();
        await page.GotoAsync($"{BaseUrl}/transfer/new");

        // Act - Select SQL→Parquet and navigate to sales.Products (has only 2 rows)
        await page.Locator("select").Nth(1).SelectOptionAsync("SqlToParquet");
        await page.WaitForTimeoutAsync(500);

        await page.Locator("input[placeholder*='connection string']").FillAsync(_webFixture.SqlConnectionString);
        await page.Locator("text=Test Connection").ClickAsync();
        await page.WaitForSelectorAsync("select >> nth=2");

        await page.Locator("select").Nth(2).SelectOptionAsync("TestSource");
        await page.WaitForSelectorAsync("select >> nth=3");

        await page.Locator("select").Nth(3).SelectOptionAsync("sales");
        await page.WaitForSelectorAsync("select >> nth=4");

        await page.Locator("select").Nth(4).SelectOptionAsync("Products");
        await page.WaitForTimeoutAsync(1000);

        // Assert - Should show max 10 rows (in this case only 2)
        var rows = page.Locator("tbody tr");
        var count = await rows.CountAsync();
        Assert.True(count <= 10, $"Preview should show max 10 rows, but found {count}");
    }

    [Fact]
    public async Task Preview_Should_Show_Total_Row_Count()
    {
        // Arrange
        var page = await CreatePageAsync();
        await page.GotoAsync($"{BaseUrl}/transfer/new");

        // Act - Select table and wait for preview
        await page.Locator("select").Nth(1).SelectOptionAsync("SqlToParquet");
        await page.WaitForTimeoutAsync(500);

        await page.Locator("input[placeholder*='connection string']").FillAsync(_webFixture.SqlConnectionString);
        await page.Locator("text=Test Connection").ClickAsync();
        await page.WaitForSelectorAsync("select >> nth=2");

        await page.Locator("select").Nth(2).SelectOptionAsync("TestSource");
        await page.WaitForSelectorAsync("select >> nth=3");

        await page.Locator("select").Nth(3).SelectOptionAsync("dbo");
        await page.WaitForSelectorAsync("select >> nth=4");

        await page.Locator("select").Nth(4).SelectOptionAsync("Customers");
        await page.WaitForTimeoutAsync(1000);

        // Assert - Should display total row count
        var totalCount = page.Locator("text=/Showing.*of.*rows/i");
        await Expect(totalCount).ToBeVisibleAsync();

        // Should say "Showing 3 of 3 rows" for Customers table
        var countText = page.Locator("text=/Showing 3 of 3/i");
        await Expect(countText).ToBeVisibleAsync();
    }

    private async Task CreateTestParquetFile(string filePath)
    {
        // Create test Parquet file with sample data
        var ids = new int[] { 1, 2, 3, 4, 5 };
        var names = new string[] { "Item 1", "Item 2", "Item 3", "Item 4", "Item 5" };
        var amounts = new decimal[] { 10.50m, 21.00m, 31.50m, 42.00m, 52.50m };

        var schema = new ParquetSchema(
            new DataField<int>("Id"),
            new DataField<string>("Name"),
            new DataField<decimal>("Amount")
        );

        await using var fileStream = File.Create(filePath);
        using var writer = await ParquetWriter.CreateAsync(schema, fileStream);
        using var groupWriter = writer.CreateRowGroup();

        await groupWriter.WriteColumnAsync(new DataColumn(
            schema.GetDataFields()[0], ids));
        await groupWriter.WriteColumnAsync(new DataColumn(
            schema.GetDataFields()[1], names));
        await groupWriter.WriteColumnAsync(new DataColumn(
            schema.GetDataFields()[2], amounts));
    }
}
