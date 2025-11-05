using DataTransfer.Core.Interfaces;
using DataTransfer.Core.Models;
using DataTransfer.Pipeline;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace DataTransfer.Pipeline.Tests;

/// <summary>
/// Demonstration tests for error handling and recovery scenarios in the data transfer pipeline.
/// These tests showcase how the system handles failures at different stages of the pipeline.
/// </summary>
public class ErrorRecoveryDemoTests
{
    [Fact]
    public async Task Should_Return_Failure_Result_When_Extraction_Fails()
    {
        // Arrange - Mock extractor that throws exception
        var mockExtractor = new Mock<ITableExtractor>();
        mockExtractor
            .Setup(x => x.ExtractDataAsync(It.IsAny<TableConfiguration>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("Database connection failed"));

        var mockStorage = new Mock<IParquetStorage>();
        var mockLoader = new Mock<IDataLoader>();
        var mockLogger = new Mock<ILogger<DataTransferOrchestrator>>();

        var orchestrator = new DataTransferOrchestrator(
            mockExtractor.Object,
            mockStorage.Object,
            mockLoader.Object,
            mockLogger.Object);

        var config = CreateSampleConfig();

        // Act
        var result = await orchestrator.TransferTableAsync(config, CancellationToken.None);

        // Assert - Should return failure result without crashing
        Assert.False(result.Success);
        Assert.Contains("Database connection failed", result.ErrorMessage);
        Assert.Equal(0, result.RowsTransferred);

        // Verify storage and loader were never called (fail fast)
        mockStorage.Verify(x => x.WriteDataAsync(
            It.IsAny<IAsyncEnumerable<string>>(),
            It.IsAny<TableConfiguration>(),
            It.IsAny<CancellationToken>()), Times.Never);

        mockLoader.Verify(x => x.LoadDataAsync(
            It.IsAny<IAsyncEnumerable<string>>(),
            It.IsAny<TableConfiguration>(),
            It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task Should_Handle_Parquet_Storage_Failure_Gracefully()
    {
        // Arrange - Mock storage that fails
        var mockExtractor = new Mock<ITableExtractor>();
        var testData = new[] { "{\"id\":1}", "{\"id\":2}", "{\"id\":3}" };
        mockExtractor
            .Setup(x => x.ExtractDataAsync(It.IsAny<TableConfiguration>(), It.IsAny<CancellationToken>()))
            .Returns(testData.ToAsyncEnumerable());

        var mockStorage = new Mock<IParquetStorage>();
        mockStorage
            .Setup(x => x.WriteDataAsync(
                It.IsAny<IAsyncEnumerable<string>>(),
                It.IsAny<TableConfiguration>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(new IOException("Disk full - cannot write Parquet file"));

        var mockLoader = new Mock<IDataLoader>();
        var mockLogger = new Mock<ILogger<DataTransferOrchestrator>>();

        var orchestrator = new DataTransferOrchestrator(
            mockExtractor.Object,
            mockStorage.Object,
            mockLoader.Object,
            mockLogger.Object);

        var config = CreateSampleConfig();

        // Act
        var result = await orchestrator.TransferTableAsync(config, CancellationToken.None);

        // Assert
        Assert.False(result.Success);
        Assert.Contains("Disk full", result.ErrorMessage);

        // Loader should never be called if storage fails
        mockLoader.Verify(x => x.LoadDataAsync(
            It.IsAny<IAsyncEnumerable<string>>(),
            It.IsAny<TableConfiguration>(),
            It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task Should_Handle_Data_Loading_Failure()
    {
        // Arrange - Mock loader that fails
        var mockExtractor = new Mock<ITableExtractor>();
        var testData = new[] { "{\"id\":1}", "{\"id\":2}" };
        mockExtractor
            .Setup(x => x.ExtractDataAsync(It.IsAny<TableConfiguration>(), It.IsAny<CancellationToken>()))
            .Returns(testData.ToAsyncEnumerable());

        var mockStorage = new Mock<IParquetStorage>();
        mockStorage
            .Setup(x => x.WriteDataAsync(
                It.IsAny<IAsyncEnumerable<string>>(),
                It.IsAny<TableConfiguration>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        mockStorage
            .Setup(x => x.ReadDataAsync(
                It.IsAny<TableConfiguration>(),
                It.IsAny<CancellationToken>()))
            .Returns(testData.ToAsyncEnumerable());

        var mockLoader = new Mock<IDataLoader>();
        mockLoader
            .Setup(x => x.LoadDataAsync(
                It.IsAny<IAsyncEnumerable<string>>(),
                It.IsAny<TableConfiguration>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(new SqlException("Destination table does not exist"));

        var mockLogger = new Mock<ILogger<DataTransferOrchestrator>>();

        var orchestrator = new DataTransferOrchestrator(
            mockExtractor.Object,
            mockStorage.Object,
            mockLoader.Object,
            mockLogger.Object);

        var config = CreateSampleConfig();

        // Act
        var result = await orchestrator.TransferTableAsync(config, CancellationToken.None);

        // Assert
        Assert.False(result.Success);
        Assert.Contains("Destination table does not exist", result.ErrorMessage);

        // Verify all stages were attempted in order
        mockExtractor.Verify(x => x.ExtractDataAsync(
            It.IsAny<TableConfiguration>(),
            It.IsAny<CancellationToken>()), Times.Once);

        mockStorage.Verify(x => x.WriteDataAsync(
            It.IsAny<IAsyncEnumerable<string>>(),
            It.IsAny<TableConfiguration>(),
            It.IsAny<CancellationToken>()), Times.Once);

        mockLoader.Verify(x => x.LoadDataAsync(
            It.IsAny<IAsyncEnumerable<string>>(),
            It.IsAny<TableConfiguration>(),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task Should_Respect_Cancellation_Token()
    {
        // Arrange - Mock extractor with delayed execution
        var mockExtractor = new Mock<ITableExtractor>();
        mockExtractor
            .Setup(x => x.ExtractDataAsync(It.IsAny<TableConfiguration>(), It.IsAny<CancellationToken>()))
            .Returns(async (TableConfiguration config, CancellationToken ct) =>
            {
                await Task.Delay(100, ct); // Simulate work
                return AsyncEnumerable.Empty<string>();
            });

        var mockStorage = new Mock<IParquetStorage>();
        var mockLoader = new Mock<IDataLoader>();
        var mockLogger = new Mock<ILogger<DataTransferOrchestrator>>();

        var orchestrator = new DataTransferOrchestrator(
            mockExtractor.Object,
            mockStorage.Object,
            mockLoader.Object,
            mockLogger.Object);

        var config = CreateSampleConfig();

        // Act - Cancel immediately
        var cts = new CancellationTokenSource();
        cts.Cancel();

        // Assert - Should throw OperationCanceledException
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await orchestrator.TransferTableAsync(config, cts.Token));
    }

    [Fact]
    public async Task Should_Demonstrate_Successful_Pipeline_Execution()
    {
        // This test demonstrates the happy path for comparison with error scenarios

        // Arrange - All mocks succeed
        var mockExtractor = new Mock<ITableExtractor>();
        var testData = new[] { "{\"id\":1,\"name\":\"Test1\"}", "{\"id\":2,\"name\":\"Test2\"}" };
        mockExtractor
            .Setup(x => x.ExtractDataAsync(It.IsAny<TableConfiguration>(), It.IsAny<CancellationToken>()))
            .Returns(testData.ToAsyncEnumerable());

        var mockStorage = new Mock<IParquetStorage>();
        mockStorage
            .Setup(x => x.WriteDataAsync(
                It.IsAny<IAsyncEnumerable<string>>(),
                It.IsAny<TableConfiguration>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        mockStorage
            .Setup(x => x.ReadDataAsync(
                It.IsAny<TableConfiguration>(),
                It.IsAny<CancellationToken>()))
            .Returns(testData.ToAsyncEnumerable());

        var mockLoader = new Mock<IDataLoader>();
        mockLoader
            .Setup(x => x.LoadDataAsync(
                It.IsAny<IAsyncEnumerable<string>>(),
                It.IsAny<TableConfiguration>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(2); // 2 rows loaded

        var mockLogger = new Mock<ILogger<DataTransferOrchestrator>>();

        var orchestrator = new DataTransferOrchestrator(
            mockExtractor.Object,
            mockStorage.Object,
            mockLoader.Object,
            mockLogger.Object);

        var config = CreateSampleConfig();

        // Act
        var result = await orchestrator.TransferTableAsync(config, CancellationToken.None);

        // Assert - Success
        Assert.True(result.Success);
        Assert.Null(result.ErrorMessage);
        Assert.Equal(2, result.RowsTransferred);

        // Verify complete pipeline execution
        mockExtractor.Verify(x => x.ExtractDataAsync(
            It.IsAny<TableConfiguration>(),
            It.IsAny<CancellationToken>()), Times.Once);

        mockStorage.Verify(x => x.WriteDataAsync(
            It.IsAny<IAsyncEnumerable<string>>(),
            It.IsAny<TableConfiguration>(),
            It.IsAny<CancellationToken>()), Times.Once);

        mockStorage.Verify(x => x.ReadDataAsync(
            It.IsAny<TableConfiguration>(),
            It.IsAny<CancellationToken>()), Times.Once);

        mockLoader.Verify(x => x.LoadDataAsync(
            It.IsAny<IAsyncEnumerable<string>>(),
            It.IsAny<TableConfiguration>(),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task Should_Propagate_Meaningful_Error_Messages()
    {
        // Demonstrates that error messages are preserved through the pipeline

        // Arrange - Extractor with specific error
        var mockExtractor = new Mock<ITableExtractor>();
        var specificErrorMessage = "Invalid SQL query: Column 'NonExistentColumn' not found in table 'Users'";
        mockExtractor
            .Setup(x => x.ExtractDataAsync(It.IsAny<TableConfiguration>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException(specificErrorMessage));

        var mockStorage = new Mock<IParquetStorage>();
        var mockLoader = new Mock<IDataLoader>();
        var mockLogger = new Mock<ILogger<DataTransferOrchestrator>>();

        var orchestrator = new DataTransferOrchestrator(
            mockExtractor.Object,
            mockStorage.Object,
            mockLoader.Object,
            mockLogger.Object);

        var config = CreateSampleConfig();

        // Act
        var result = await orchestrator.TransferTableAsync(config, CancellationToken.None);

        // Assert - Specific error message preserved
        Assert.False(result.Success);
        Assert.Contains("NonExistentColumn", result.ErrorMessage);
        Assert.Contains("Users", result.ErrorMessage);
    }

    #region Helper Methods

    private static TableConfiguration CreateSampleConfig()
    {
        return new TableConfiguration
        {
            Source = new TableIdentifier
            {
                Database = "SourceDB",
                Schema = "dbo",
                Table = "TestTable"
            },
            Destination = new TableIdentifier
            {
                Database = "DestDB",
                Schema = "dbo",
                Table = "TestTable"
            },
            Partitioning = new PartitioningConfiguration
            {
                Type = PartitionType.Static
            },
            ExtractSettings = new ExtractSettings
            {
                BatchSize = 1000
            }
        };
    }

    #endregion
}

// Helper exception for testing
public class SqlException : Exception
{
    public SqlException(string message) : base(message) { }
}
