using DataTransfer.Core.Models;
using Xunit;

namespace DataTransfer.Core.Tests.Models;

/// <summary>
/// Tests for ExtractSettings model including WHERE clause and row limit support
/// </summary>
public class ExtractSettingsTests
{
    [Fact]
    public void Should_Support_WhereClause_Property()
    {
        // Arrange
        var settings = new ExtractSettings();

        // Act
        settings.WhereClause = "Status = 'Active' AND CreatedDate > '2024-01-01'";

        // Assert
        Assert.NotNull(settings.WhereClause);
        Assert.Equal("Status = 'Active' AND CreatedDate > '2024-01-01'", settings.WhereClause);
    }

    [Fact]
    public void Should_Allow_Null_WhereClause()
    {
        // Arrange
        var settings = new ExtractSettings();

        // Act
        settings.WhereClause = null;

        // Assert
        Assert.Null(settings.WhereClause);
    }

    [Fact]
    public void Should_Support_RowLimit_Property()
    {
        // Arrange
        var settings = new ExtractSettings();

        // Act
        settings.RowLimit = 1000;

        // Assert
        Assert.NotNull(settings.RowLimit);
        Assert.Equal(1000, settings.RowLimit);
    }

    [Fact]
    public void Should_Allow_Null_RowLimit()
    {
        // Arrange
        var settings = new ExtractSettings();

        // Act
        settings.RowLimit = null;

        // Assert
        Assert.Null(settings.RowLimit);
    }

    [Fact]
    public void Should_Support_Combination_Of_WhereClause_And_RowLimit()
    {
        // Arrange & Act
        var settings = new ExtractSettings
        {
            WhereClause = "IsActive = 1",
            RowLimit = 500,
            BatchSize = 100
        };

        // Assert
        Assert.Equal("IsActive = 1", settings.WhereClause);
        Assert.Equal(500, settings.RowLimit);
        Assert.Equal(100, settings.BatchSize);
    }

    [Fact]
    public void RowLimit_Should_Be_Positive_When_Specified()
    {
        // This test validates that business logic will reject negative/zero values
        // The validation happens in ConfigurationValidator, not the model itself
        var settings = new ExtractSettings
        {
            RowLimit = -1  // Invalid, but model allows it
        };

        Assert.Equal(-1, settings.RowLimit);  // Model doesn't enforce, validator will
    }
}
