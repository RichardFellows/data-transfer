using DataTransfer.Core.Models;
using Xunit;

namespace DataTransfer.Core.Tests.Models;

/// <summary>
/// Tests for TransferResult model including row count verification support
/// </summary>
public class TransferResultTests
{
    [Fact]
    public void Should_Support_SourceRowCount_Property()
    {
        // Arrange & Act
        var result = new TransferResult
        {
            SourceRowCount = 1000
        };

        // Assert
        Assert.Equal(1000, result.SourceRowCount);
    }

    [Fact]
    public void Should_Support_DestinationRowCount_Property()
    {
        // Arrange & Act
        var result = new TransferResult
        {
            DestinationRowCount = 1000
        };

        // Assert
        Assert.Equal(1000, result.DestinationRowCount);
    }

    [Fact]
    public void Should_Allow_Null_SourceRowCount()
    {
        // Arrange & Act
        var result = new TransferResult
        {
            SourceRowCount = null
        };

        // Assert
        Assert.Null(result.SourceRowCount);
    }

    [Fact]
    public void Should_Allow_Null_DestinationRowCount()
    {
        // Arrange & Act
        var result = new TransferResult
        {
            DestinationRowCount = null
        };

        // Assert
        Assert.Null(result.DestinationRowCount);
    }

    [Fact]
    public void CountsMatch_Should_Return_True_When_Counts_Equal()
    {
        // Arrange
        var result = new TransferResult
        {
            SourceRowCount = 1000,
            DestinationRowCount = 1000
        };

        // Act & Assert
        Assert.True(result.CountsMatch);
    }

    [Fact]
    public void CountsMatch_Should_Return_False_When_Counts_Different()
    {
        // Arrange
        var result = new TransferResult
        {
            SourceRowCount = 1000,
            DestinationRowCount = 950
        };

        // Act & Assert
        Assert.False(result.CountsMatch);
    }

    [Fact]
    public void CountsMatch_Should_Return_False_When_Source_Is_Null()
    {
        // Arrange
        var result = new TransferResult
        {
            SourceRowCount = null,
            DestinationRowCount = 1000
        };

        // Act & Assert
        Assert.False(result.CountsMatch);
    }

    [Fact]
    public void CountsMatch_Should_Return_False_When_Destination_Is_Null()
    {
        // Arrange
        var result = new TransferResult
        {
            SourceRowCount = 1000,
            DestinationRowCount = null
        };

        // Act & Assert
        Assert.False(result.CountsMatch);
    }

    [Fact]
    public void CountsMatch_Should_Return_False_When_Both_Null()
    {
        // Arrange
        var result = new TransferResult
        {
            SourceRowCount = null,
            DestinationRowCount = null
        };

        // Act & Assert
        Assert.False(result.CountsMatch);
    }

    [Fact]
    public void Should_Support_ValidationMessage_Property()
    {
        // Arrange & Act
        var result = new TransferResult
        {
            ValidationMessage = "Row count mismatch: expected 1000, got 950"
        };

        // Assert
        Assert.Equal("Row count mismatch: expected 1000, got 950", result.ValidationMessage);
    }

    [Fact]
    public void Should_Calculate_CountDifference_When_Counts_Present()
    {
        // Arrange
        var result = new TransferResult
        {
            SourceRowCount = 1000,
            DestinationRowCount = 950
        };

        // Act & Assert
        Assert.Equal(-50, result.CountDifference);
    }

    [Fact]
    public void CountDifference_Should_Return_Null_When_Counts_Not_Available()
    {
        // Arrange
        var result = new TransferResult
        {
            SourceRowCount = null,
            DestinationRowCount = 950
        };

        // Act & Assert
        Assert.Null(result.CountDifference);
    }
}
