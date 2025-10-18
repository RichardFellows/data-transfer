using DataTransfer.SqlServer;
using Xunit;

namespace DataTransfer.SqlServer.Tests;

/// <summary>
/// Integration tests for SqlSchemaDiscovery requiring real SQL Server
/// These tests are skipped by default unless SQL Server is available
/// </summary>
public class SqlSchemaDiscoveryIntegrationTests
{
    private const string TestConnectionString =
        "Server=localhost,1433;Database=IcebergDemo_Source;User Id=sa;Password=IcebergDemo@2024;TrustServerCertificate=true";

    private async Task<bool> IsSqlServerAvailableAsync()
    {
        try
        {
            var discovery = new SqlSchemaDiscovery(TestConnectionString);
            return await discovery.TestConnectionAsync();
        }
        catch
        {
            return false;
        }
    }

    [Fact(Skip = "Requires SQL Server - run manually when SQL Server is available")]
    public async Task DiscoverDatabase_ShouldReturnDatabaseInfo_WhenSqlServerIsAvailable()
    {
        // Arrange
        if (!await IsSqlServerAvailableAsync())
        {
            return; // Skip test if SQL Server not available
        }

        var discovery = new SqlSchemaDiscovery(TestConnectionString);

        // Act
        var dbInfo = await discovery.DiscoverDatabaseAsync();

        // Assert
        Assert.NotNull(dbInfo);
        Assert.Equal("IcebergDemo_Source", dbInfo.DatabaseName);
        Assert.Contains("Microsoft SQL Server", dbInfo.ServerVersion);
        Assert.NotEmpty(dbInfo.Tables);
    }

    [Fact(Skip = "Requires SQL Server - run manually when SQL Server is available")]
    public async Task DiscoverDatabase_ShouldFindExpectedTables_WhenDemoSchemaExists()
    {
        // Arrange
        if (!await IsSqlServerAvailableAsync())
        {
            return;
        }

        var discovery = new SqlSchemaDiscovery(TestConnectionString);

        // Act
        var dbInfo = await discovery.DiscoverDatabaseAsync();

        // Assert - Demo database should have Customers, Orders, Products tables
        var customers = dbInfo.FindTable("dbo", "Customers");
        var orders = dbInfo.FindTable("dbo", "Orders");
        var products = dbInfo.FindTable("dbo", "Products");

        Assert.NotNull(customers);
        Assert.NotNull(orders);
        Assert.NotNull(products);

        // Verify Customers table structure
        Assert.Contains(customers.Columns, c => c.ColumnName == "CustomerID");
        Assert.Contains(customers.Columns, c => c.ColumnName == "FirstName");
        Assert.Contains(customers.Columns, c => c.ColumnName == "CreatedAt");

        // Verify Orders has date column
        Assert.Contains(orders.Columns, c => c.ColumnName == "OrderDate");
    }

    [Fact(Skip = "Requires SQL Server - run manually when SQL Server is available")]
    public async Task DiscoverTable_ShouldSuggestDatePartition_ForOrdersTable()
    {
        // Arrange
        if (!await IsSqlServerAvailableAsync())
        {
            return;
        }

        var discovery = new SqlSchemaDiscovery(TestConnectionString);

        // Act
        var table = await discovery.DiscoverTableAsync("dbo", "Orders");

        // Assert
        Assert.NotNull(table);
        Assert.Equal("dbo", table.Schema);
        Assert.Equal("Orders", table.TableName);

        var suggestion = table.GetBestPartitionSuggestion();
        Assert.NotNull(suggestion);

        // Orders table has OrderDate column, should suggest date partitioning
        Assert.Equal("date", suggestion.PartitionType);
        Assert.Equal("OrderDate", suggestion.ColumnName);
    }

    [Fact(Skip = "Requires SQL Server - run manually when SQL Server is available")]
    public async Task DiscoverTable_ShouldSuggestStaticPartition_ForProductsTable()
    {
        // Arrange
        if (!await IsSqlServerAvailableAsync())
        {
            return;
        }

        var discovery = new SqlSchemaDiscovery(TestConnectionString);

        // Act
        var table = await discovery.DiscoverTableAsync("dbo", "Products");

        // Assert
        Assert.NotNull(table);

        var suggestion = table.GetBestPartitionSuggestion();
        Assert.NotNull(suggestion);

        // Products is a small reference table, might suggest static
        // (depending on row count in test database)
        Assert.Contains(suggestion.PartitionType, new[] { "static", "date" });
    }

    [Fact(Skip = "Requires SQL Server - run manually when SQL Server is available")]
    public async Task DiscoverTable_ShouldReturnNull_ForNonExistentTable()
    {
        // Arrange
        if (!await IsSqlServerAvailableAsync())
        {
            return;
        }

        var discovery = new SqlSchemaDiscovery(TestConnectionString);

        // Act
        var table = await discovery.DiscoverTableAsync("dbo", "NonExistentTable");

        // Assert
        Assert.Null(table);
    }

    [Fact(Skip = "Requires SQL Server - run manually when SQL Server is available")]
    public async Task GetTableSuggestions_ShouldReturnSimilarNames_ForPartialMatch()
    {
        // Arrange
        if (!await IsSqlServerAvailableAsync())
        {
            return;
        }

        var discovery = new SqlSchemaDiscovery(TestConnectionString);
        var dbInfo = await discovery.DiscoverDatabaseAsync();

        // Act
        var suggestions = dbInfo.GetTableSuggestions("Cust", maxResults: 5);

        // Assert
        Assert.NotEmpty(suggestions);
        Assert.Contains(suggestions, s => s.Contains("Customers", StringComparison.OrdinalIgnoreCase));
    }
}
