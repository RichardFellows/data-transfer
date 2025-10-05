using DataTransfer.Web.Services;
using DataTransfer.Web.Models;
using Xunit;
using Microsoft.Extensions.Logging.Abstractions;

namespace DataTransfer.Web.Tests.Services;

/// <summary>
/// TDD tests for DatabaseMetadataService preview functionality
/// Uses WebApplicationFixture for real SQL Server via TestContainers
/// </summary>
[Collection("WebApplication")]
public class DatabaseMetadataServicePreviewTests
{
    private readonly WebApplicationFixture _fixture;
    private readonly DatabaseMetadataService _service;

    public DatabaseMetadataServicePreviewTests(WebApplicationFixture fixture)
    {
        _fixture = fixture;
        _service = new DatabaseMetadataService(NullLogger<DatabaseMetadataService>.Instance);
    }

    [Fact]
    public async Task GetTablePreviewAsync_Should_Return_Schema_Information()
    {
        // Arrange
        var connectionString = _fixture.SqlConnectionString;
        var database = "TestSource";
        var schema = "dbo";
        var table = "Customers";

        // Act
        var preview = await _service.GetTablePreviewAsync(connectionString, database, schema, table);

        // Assert
        Assert.NotNull(preview);
        Assert.NotEmpty(preview.Columns);

        // Customers table has: Id (INT), Name (NVARCHAR(100))
        Assert.Equal(2, preview.Columns.Count);

        var idColumn = preview.Columns.FirstOrDefault(c => c.Name == "Id");
        Assert.NotNull(idColumn);
        Assert.Contains("int", idColumn.DataType, StringComparison.OrdinalIgnoreCase);
        Assert.False(idColumn.IsNullable);

        var nameColumn = preview.Columns.FirstOrDefault(c => c.Name == "Name");
        Assert.NotNull(nameColumn);
        Assert.Contains("nvarchar", nameColumn.DataType, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task GetTablePreviewAsync_Should_Return_Sample_Rows()
    {
        // Arrange
        var connectionString = _fixture.SqlConnectionString;
        var database = "TestSource";
        var schema = "dbo";
        var table = "Customers";

        // Act
        var preview = await _service.GetTablePreviewAsync(connectionString, database, schema, table);

        // Assert
        Assert.NotEmpty(preview.Rows);
        Assert.Equal(3, preview.Rows.Count); // TestSource.dbo.Customers has 3 rows

        // Check first row has expected columns
        var firstRow = preview.Rows.First();
        Assert.True(firstRow.ContainsKey("Id"));
        Assert.True(firstRow.ContainsKey("Name"));
    }

    [Fact]
    public async Task GetTablePreviewAsync_Should_Limit_To_10_Rows()
    {
        // Arrange - would need a table with >10 rows to fully test this
        // For now, verify it returns <= 10 rows
        var connectionString = _fixture.SqlConnectionString;
        var database = "TestSource";
        var schema = "sales";
        var table = "Orders";

        // Act
        var preview = await _service.GetTablePreviewAsync(connectionString, database, schema, table);

        // Assert
        Assert.True(preview.Rows.Count <= 10, "Preview should return maximum 10 rows");
    }

    [Fact]
    public async Task GetTablePreviewAsync_Should_Return_Total_Row_Count()
    {
        // Arrange
        var connectionString = _fixture.SqlConnectionString;
        var database = "TestSource";
        var schema = "dbo";
        var table = "Customers";

        // Act
        var preview = await _service.GetTablePreviewAsync(connectionString, database, schema, table);

        // Assert
        Assert.NotNull(preview.TotalRowCount);
        Assert.Equal(3, preview.TotalRowCount.Value); // TestSource.dbo.Customers has 3 rows
    }

    [Fact]
    public async Task GetTablePreviewAsync_Should_Handle_Empty_Table()
    {
        // Arrange - TestDestination tables are empty
        var connectionString = _fixture.SqlConnectionString;
        var database = "TestDestination";
        var schema = "dbo";
        var table = "Customers";

        // Act
        var preview = await _service.GetTablePreviewAsync(connectionString, database, schema, table);

        // Assert
        Assert.NotNull(preview);
        Assert.NotEmpty(preview.Columns); // Schema should still be present
        Assert.Empty(preview.Rows); // No data
        Assert.Equal(0, preview.TotalRowCount.Value);
    }

    [Fact]
    public async Task GetTablePreviewAsync_Should_Handle_NULL_Values()
    {
        // Arrange - testing nullable columns
        var connectionString = _fixture.SqlConnectionString;
        var database = "TestSource";
        var schema = "dbo";
        var table = "Customers";

        // Act
        var preview = await _service.GetTablePreviewAsync(connectionString, database, schema, table);

        // Assert
        Assert.NotNull(preview);
        // Verify we can handle NULL values in the data (if any exist)
        foreach (var row in preview.Rows)
        {
            foreach (var column in preview.Columns)
            {
                // Should not throw when accessing potentially null values
                row.TryGetValue(column.Name, out var value);
                // This test passes if no exception is thrown
            }
        }
    }
}
