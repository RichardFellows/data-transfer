using DataTransfer.Core.Interfaces;
using DataTransfer.Core.Models;
using DataTransfer.Pipeline;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace DataTransfer.Pipeline.Tests;

public class DataTransferOrchestratorTests
{
    [Fact]
    public void DataTransferOrchestrator_Should_Require_Dependencies()
    {
        Assert.Throws<ArgumentNullException>(() => new DataTransferOrchestrator(
            null!,
            Mock.Of<IParquetStorage>(),
            Mock.Of<IDataLoader>(),
            Mock.Of<ILogger<DataTransferOrchestrator>>()));

        Assert.Throws<ArgumentNullException>(() => new DataTransferOrchestrator(
            Mock.Of<ITableExtractor>(),
            null!,
            Mock.Of<IDataLoader>(),
            Mock.Of<ILogger<DataTransferOrchestrator>>()));

        Assert.Throws<ArgumentNullException>(() => new DataTransferOrchestrator(
            Mock.Of<ITableExtractor>(),
            Mock.Of<IParquetStorage>(),
            null!,
            Mock.Of<ILogger<DataTransferOrchestrator>>()));

        Assert.Throws<ArgumentNullException>(() => new DataTransferOrchestrator(
            Mock.Of<ITableExtractor>(),
            Mock.Of<IParquetStorage>(),
            Mock.Of<IDataLoader>(),
            null!));
    }

    [Fact]
    public async Task TransferTableAsync_Should_Throw_When_TableConfig_Null()
    {
        var orchestrator = CreateOrchestrator();

        await Assert.ThrowsAsync<ArgumentNullException>(async () =>
            await orchestrator.TransferTableAsync(null!, "source-conn", "dest-conn"));
    }

    [Fact]
    public async Task TransferTableAsync_Should_Throw_When_SourceConnectionString_Empty()
    {
        var orchestrator = CreateOrchestrator();
        var tableConfig = CreateTestTableConfig();

        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await orchestrator.TransferTableAsync(tableConfig, "", "dest-conn"));
    }

    [Fact]
    public async Task TransferTableAsync_Should_Throw_When_DestinationConnectionString_Empty()
    {
        var orchestrator = CreateOrchestrator();
        var tableConfig = CreateTestTableConfig();

        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await orchestrator.TransferTableAsync(tableConfig, "source-conn", ""));
    }

    [Fact]
    public async Task TransferTableAsync_Should_Extract_Data_From_Source()
    {
        var extractorMock = new Mock<ITableExtractor>();
        var storageMock = new Mock<IParquetStorage>();
        var loaderMock = new Mock<IDataLoader>();
        var logger = Mock.Of<ILogger<DataTransferOrchestrator>>();

        extractorMock
            .Setup(x => x.ExtractAsync(
                It.IsAny<TableConfiguration>(),
                It.IsAny<string>(),
                It.IsAny<Stream>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ExtractionResult { RowsExtracted = 100 });

        storageMock
            .Setup(x => x.ReadAsync(
                It.IsAny<string>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new MemoryStream());

        loaderMock
            .Setup(x => x.LoadAsync(
                It.IsAny<TableConfiguration>(),
                It.IsAny<string>(),
                It.IsAny<Stream>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new LoadResult { RowsLoaded = 100 });

        var orchestrator = new DataTransferOrchestrator(
            extractorMock.Object,
            storageMock.Object,
            loaderMock.Object,
            logger);

        var tableConfig = CreateTestTableConfig();
        await orchestrator.TransferTableAsync(tableConfig, "source-conn", "dest-conn");

        extractorMock.Verify(x => x.ExtractAsync(
            tableConfig,
            "source-conn",
            It.IsAny<Stream>(),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task TransferTableAsync_Should_Write_To_Parquet()
    {
        var extractorMock = new Mock<ITableExtractor>();
        var storageMock = new Mock<IParquetStorage>();
        var loaderMock = new Mock<IDataLoader>();
        var logger = Mock.Of<ILogger<DataTransferOrchestrator>>();

        extractorMock
            .Setup(x => x.ExtractAsync(
                It.IsAny<TableConfiguration>(),
                It.IsAny<string>(),
                It.IsAny<Stream>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ExtractionResult { RowsExtracted = 100 });

        storageMock
            .Setup(x => x.ReadAsync(
                It.IsAny<string>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new MemoryStream());

        loaderMock
            .Setup(x => x.LoadAsync(
                It.IsAny<TableConfiguration>(),
                It.IsAny<string>(),
                It.IsAny<Stream>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new LoadResult { RowsLoaded = 100 });

        var orchestrator = new DataTransferOrchestrator(
            extractorMock.Object,
            storageMock.Object,
            loaderMock.Object,
            logger);

        var tableConfig = CreateTestTableConfig();
        await orchestrator.TransferTableAsync(tableConfig, "source-conn", "dest-conn");

        storageMock.Verify(x => x.WriteAsync(
            It.IsAny<Stream>(),
            It.IsAny<string>(),
            It.IsAny<DateTime>(),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task TransferTableAsync_Should_Load_From_Parquet_To_Destination()
    {
        var extractorMock = new Mock<ITableExtractor>();
        var storageMock = new Mock<IParquetStorage>();
        var loaderMock = new Mock<IDataLoader>();
        var logger = Mock.Of<ILogger<DataTransferOrchestrator>>();

        extractorMock
            .Setup(x => x.ExtractAsync(
                It.IsAny<TableConfiguration>(),
                It.IsAny<string>(),
                It.IsAny<Stream>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ExtractionResult { RowsExtracted = 100 });

        storageMock
            .Setup(x => x.ReadAsync(
                It.IsAny<string>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new MemoryStream());

        loaderMock
            .Setup(x => x.LoadAsync(
                It.IsAny<TableConfiguration>(),
                It.IsAny<string>(),
                It.IsAny<Stream>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new LoadResult { RowsLoaded = 100 });

        var orchestrator = new DataTransferOrchestrator(
            extractorMock.Object,
            storageMock.Object,
            loaderMock.Object,
            logger);

        var tableConfig = CreateTestTableConfig();
        await orchestrator.TransferTableAsync(tableConfig, "source-conn", "dest-conn");

        loaderMock.Verify(x => x.LoadAsync(
            tableConfig,
            "dest-conn",
            It.IsAny<Stream>(),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task TransferTableAsync_Should_Return_TransferResult()
    {
        var extractorMock = new Mock<ITableExtractor>();
        var storageMock = new Mock<IParquetStorage>();
        var loaderMock = new Mock<IDataLoader>();
        var logger = Mock.Of<ILogger<DataTransferOrchestrator>>();

        extractorMock
            .Setup(x => x.ExtractAsync(
                It.IsAny<TableConfiguration>(),
                It.IsAny<string>(),
                It.IsAny<Stream>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ExtractionResult { RowsExtracted = 100 });

        storageMock
            .Setup(x => x.ReadAsync(
                It.IsAny<string>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new MemoryStream());

        loaderMock
            .Setup(x => x.LoadAsync(
                It.IsAny<TableConfiguration>(),
                It.IsAny<string>(),
                It.IsAny<Stream>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new LoadResult { RowsLoaded = 100 });

        var orchestrator = new DataTransferOrchestrator(
            extractorMock.Object,
            storageMock.Object,
            loaderMock.Object,
            logger);

        var tableConfig = CreateTestTableConfig();
        var result = await orchestrator.TransferTableAsync(tableConfig, "source-conn", "dest-conn");

        Assert.NotNull(result);
        Assert.Equal(100, result.RowsExtracted);
        Assert.Equal(100, result.RowsLoaded);
        Assert.True(result.Success);
    }

    [Fact]
    public async Task TransferTableAsync_Should_Handle_CancellationToken()
    {
        var extractorMock = new Mock<ITableExtractor>();
        var storageMock = new Mock<IParquetStorage>();
        var loaderMock = new Mock<IDataLoader>();
        var logger = Mock.Of<ILogger<DataTransferOrchestrator>>();

        var cts = new CancellationTokenSource();
        cts.Cancel();

        extractorMock
            .Setup(x => x.ExtractAsync(
                It.IsAny<TableConfiguration>(),
                It.IsAny<string>(),
                It.IsAny<Stream>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(new OperationCanceledException());

        var orchestrator = new DataTransferOrchestrator(
            extractorMock.Object,
            storageMock.Object,
            loaderMock.Object,
            logger);

        var tableConfig = CreateTestTableConfig();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await orchestrator.TransferTableAsync(tableConfig, "source-conn", "dest-conn", cts.Token));
    }

    private static DataTransferOrchestrator CreateOrchestrator()
    {
        return new DataTransferOrchestrator(
            Mock.Of<ITableExtractor>(),
            Mock.Of<IParquetStorage>(),
            Mock.Of<IDataLoader>(),
            Mock.Of<ILogger<DataTransferOrchestrator>>());
    }

    private static TableConfiguration CreateTestTableConfig()
    {
        return new TableConfiguration
        {
            Source = new TableIdentifier
            {
                Database = "TestDB",
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
            }
        };
    }
}
