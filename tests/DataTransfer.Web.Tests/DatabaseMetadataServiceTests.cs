using DataTransfer.Web.Services;
using DataTransfer.Web.Models;
using Xunit;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Data.SqlClient;

namespace DataTransfer.Web.Tests;

/// <summary>
/// Unit tests for DatabaseMetadataService
/// Tests database metadata queries and connection validation
/// </summary>
public class DatabaseMetadataServiceTests
{
    private readonly ILogger<DatabaseMetadataService> _logger;

    public DatabaseMetadataServiceTests()
    {
        _logger = NullLogger<DatabaseMetadataService>.Instance;
    }

    [Fact]
    public void TestConnection_WithInvalidConnectionString_ReturnsFalse()
    {
        // Arrange
        var service = new DatabaseMetadataService(_logger);
        var invalidConnectionString = "Server=invalid;Database=invalid;User=invalid;Password=invalid";

        // Act
        var result = service.TestConnection(invalidConnectionString);

        // Assert
        Assert.False(result);
    }

    [Fact]
    public void TestConnection_WithEmptyConnectionString_ReturnsFalse()
    {
        // Arrange
        var service = new DatabaseMetadataService(_logger);

        // Act
        var result = service.TestConnection(string.Empty);

        // Assert
        Assert.False(result);
    }

    [Fact]
    public async Task GetDatabasesAsync_WithInvalidConnection_ThrowsException()
    {
        // Arrange
        var service = new DatabaseMetadataService(_logger);
        var invalidConnectionString = "Server=invalid;Database=invalid;User=invalid;Password=invalid";

        // Act & Assert
        await Assert.ThrowsAsync<SqlException>(async () =>
        {
            await service.GetDatabasesAsync(invalidConnectionString);
        });
    }

    [Fact]
    public async Task GetSchemasAsync_WithInvalidConnection_ThrowsException()
    {
        // Arrange
        var service = new DatabaseMetadataService(_logger);
        var invalidConnectionString = "Server=invalid;Database=invalid;User=invalid;Password=invalid";

        // Act & Assert
        await Assert.ThrowsAsync<SqlException>(async () =>
        {
            await service.GetSchemasAsync(invalidConnectionString, "TestDB");
        });
    }

    [Fact]
    public async Task GetTablesAsync_WithInvalidConnection_ThrowsException()
    {
        // Arrange
        var service = new DatabaseMetadataService(_logger);
        var invalidConnectionString = "Server=invalid;Database=invalid;User=invalid;Password=invalid";

        // Act & Assert
        await Assert.ThrowsAsync<SqlException>(async () =>
        {
            await service.GetTablesAsync(invalidConnectionString, "TestDB", "dbo");
        });
    }
}
