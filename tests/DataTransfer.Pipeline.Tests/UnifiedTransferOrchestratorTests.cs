using DataTransfer.Core.Interfaces;
using DataTransfer.Core.Models;
using DataTransfer.Iceberg.Catalog;
using DataTransfer.Iceberg.Integration;
using DataTransfer.Pipeline;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace DataTransfer.Pipeline.Tests;

public class UnifiedTransferOrchestratorTests
{
    [Fact]
    public void UnifiedTransferOrchestrator_Should_Require_Dependencies()
    {
        var sqlExtractor = Mock.Of<ITableExtractor>();
        var parquetExtractor = Mock.Of<IParquetExtractor>();
        var sqlLoader = Mock.Of<IDataLoader>();
        var parquetWriter = Mock.Of<IParquetWriter>();
        var icebergExporter = Mock.Of<SqlServerToIcebergExporter>();
        var incrementalSync = Mock.Of<IncrementalSyncCoordinator>();
        var catalog = Mock.Of<FilesystemCatalog>();
        var config = Mock.Of<IConfiguration>();
        var logger = Mock.Of<ILogger<UnifiedTransferOrchestrator>>();

        Assert.Throws<ArgumentNullException>(() => new UnifiedTransferOrchestrator(
            null!, parquetExtractor, sqlLoader, parquetWriter, icebergExporter, incrementalSync, catalog, config, logger));

        Assert.Throws<ArgumentNullException>(() => new UnifiedTransferOrchestrator(
            sqlExtractor, null!, sqlLoader, parquetWriter, icebergExporter, incrementalSync, catalog, config, logger));

        Assert.Throws<ArgumentNullException>(() => new UnifiedTransferOrchestrator(
            sqlExtractor, parquetExtractor, null!, parquetWriter, icebergExporter, incrementalSync, catalog, config, logger));

        Assert.Throws<ArgumentNullException>(() => new UnifiedTransferOrchestrator(
            sqlExtractor, parquetExtractor, sqlLoader, null!, icebergExporter, incrementalSync, catalog, config, logger));

        Assert.Throws<ArgumentNullException>(() => new UnifiedTransferOrchestrator(
            sqlExtractor, parquetExtractor, sqlLoader, parquetWriter, null!, incrementalSync, catalog, config, logger));

        Assert.Throws<ArgumentNullException>(() => new UnifiedTransferOrchestrator(
            sqlExtractor, parquetExtractor, sqlLoader, parquetWriter, icebergExporter, null!, catalog, config, logger));

        Assert.Throws<ArgumentNullException>(() => new UnifiedTransferOrchestrator(
            sqlExtractor, parquetExtractor, sqlLoader, parquetWriter, icebergExporter, incrementalSync, null!, config, logger));

        Assert.Throws<ArgumentNullException>(() => new UnifiedTransferOrchestrator(
            sqlExtractor, parquetExtractor, sqlLoader, parquetWriter, icebergExporter, incrementalSync, catalog, null!, logger));
    }

    [Fact]
    public async Task ExecuteTransferAsync_SqlToParquet_Should_Extract_And_Write()
    {
        // Arrange
        var extractorMock = new Mock<ITableExtractor>();
        var parquetWriterMock = new Mock<IParquetWriter>();

        extractorMock
            .Setup(x => x.ExtractAsync(
                It.IsAny<TableConfiguration>(),
                It.IsAny<string>(),
                It.IsAny<Stream>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ExtractionResult { RowsExtracted = 100, Success = true });

        parquetWriterMock
            .Setup(x => x.WriteToParquetAsync(
                It.IsAny<Stream>(),
                It.IsAny<string>(),
                It.IsAny<DateTime?>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(100);

        var orchestrator = CreateOrchestrator(
            sqlExtractor: extractorMock.Object,
            parquetWriter: parquetWriterMock.Object);

        var config = new TransferConfiguration
        {
            TransferType = TransferType.SqlToParquet,
            Source = new SourceConfiguration
            {
                Type = SourceType.SqlServer,
                ConnectionString = "Server=localhost;",
                Table = new TableIdentifier { Database = "Test", Schema = "dbo", Table = "Orders" }
            },
            Destination = new DestinationConfiguration
            {
                Type = DestinationType.Parquet,
                ParquetPath = "output.parquet"
            }
        };

        // Act
        var result = await orchestrator.ExecuteTransferAsync(config);

        // Assert
        Assert.True(result.Success);
        Assert.Equal(100, result.RowsExtracted);
        Assert.Equal(100, result.RowsLoaded);
        Assert.NotNull(result.ParquetFilePath);

        extractorMock.Verify(x => x.ExtractAsync(
            It.IsAny<TableConfiguration>(),
            "Server=localhost;",
            It.IsAny<Stream>(),
            It.IsAny<CancellationToken>()), Times.Once);

        parquetWriterMock.Verify(x => x.WriteToParquetAsync(
            It.IsAny<Stream>(),
            "output.parquet",
            It.IsAny<DateTime?>(),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task ExecuteTransferAsync_ParquetToSql_Should_Extract_And_Load()
    {
        // Arrange
        var parquetExtractorMock = new Mock<IParquetExtractor>();
        var sqlLoaderMock = new Mock<IDataLoader>();

        parquetExtractorMock
            .Setup(x => x.ExtractFromParquetAsync(
                It.IsAny<string>(),
                It.IsAny<Stream>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ExtractionResult { RowsExtracted = 50, Success = true });

        sqlLoaderMock
            .Setup(x => x.LoadAsync(
                It.IsAny<TableConfiguration>(),
                It.IsAny<string>(),
                It.IsAny<Stream>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new LoadResult { RowsLoaded = 50, Success = true });

        var orchestrator = CreateOrchestrator(
            parquetExtractor: parquetExtractorMock.Object,
            sqlLoader: sqlLoaderMock.Object);

        var config = new TransferConfiguration
        {
            TransferType = TransferType.ParquetToSql,
            Source = new SourceConfiguration
            {
                Type = SourceType.Parquet,
                ParquetPath = "input.parquet"
            },
            Destination = new DestinationConfiguration
            {
                Type = DestinationType.SqlServer,
                ConnectionString = "Server=localhost;",
                Table = new TableIdentifier { Database = "Test", Schema = "dbo", Table = "Orders" }
            }
        };

        // Act
        var result = await orchestrator.ExecuteTransferAsync(config);

        // Assert
        Assert.True(result.Success);
        Assert.Equal(50, result.RowsExtracted);
        Assert.Equal(50, result.RowsLoaded);
        Assert.Equal("input.parquet", result.ParquetFilePath);

        parquetExtractorMock.Verify(x => x.ExtractFromParquetAsync(
            "input.parquet",
            It.IsAny<Stream>(),
            It.IsAny<CancellationToken>()), Times.Once);

        sqlLoaderMock.Verify(x => x.LoadAsync(
            It.IsAny<TableConfiguration>(),
            "Server=localhost;",
            It.IsAny<Stream>(),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task ExecuteTransferAsync_Should_Set_Timing_Information()
    {
        // Arrange
        var orchestrator = CreateOrchestrator();
        var config = CreateSqlToParquetConfig();

        // Act
        var result = await orchestrator.ExecuteTransferAsync(config);

        // Assert
        Assert.True(result.StartTime > DateTime.MinValue);
        Assert.True(result.EndTime > result.StartTime);
        Assert.True(result.Duration.TotalMilliseconds > 0);
    }

    [Fact]
    public async Task ExecuteTransferAsync_Should_Handle_Exceptions()
    {
        // Arrange
        var extractorMock = new Mock<ITableExtractor>();
        extractorMock
            .Setup(x => x.ExtractAsync(
                It.IsAny<TableConfiguration>(),
                It.IsAny<string>(),
                It.IsAny<Stream>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("Test error"));

        var orchestrator = CreateOrchestrator(sqlExtractor: extractorMock.Object);
        var config = CreateSqlToParquetConfig();

        // Act & Assert
        await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await orchestrator.ExecuteTransferAsync(config));
    }

    [Fact]
    public async Task ExecuteTransferAsync_Should_Throw_When_Config_Null()
    {
        // Arrange
        var orchestrator = CreateOrchestrator();

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(
            async () => await orchestrator.ExecuteTransferAsync(null!));
    }

    [Fact]
    public async Task ExecuteTransferAsync_SqlToSql_Should_Throw_NotImplementedException()
    {
        // Arrange
        var orchestrator = CreateOrchestrator();
        var config = new TransferConfiguration
        {
            TransferType = TransferType.SqlToSql,
            Source = new SourceConfiguration
            {
                Type = SourceType.SqlServer,
                ConnectionString = "Server=localhost;",
                Table = new TableIdentifier { Database = "Test", Schema = "dbo", Table = "Source" }
            },
            Destination = new DestinationConfiguration
            {
                Type = DestinationType.SqlServer,
                ConnectionString = "Server=localhost;",
                Table = new TableIdentifier { Database = "Test", Schema = "dbo", Table = "Dest" }
            }
        };

        // Act & Assert
        await Assert.ThrowsAsync<NotImplementedException>(
            async () => await orchestrator.ExecuteTransferAsync(config));
    }

    private static UnifiedTransferOrchestrator CreateOrchestrator(
        ITableExtractor? sqlExtractor = null,
        IParquetExtractor? parquetExtractor = null,
        IDataLoader? sqlLoader = null,
        IParquetWriter? parquetWriter = null,
        SqlServerToIcebergExporter? icebergExporter = null,
        IncrementalSyncCoordinator? incrementalSync = null,
        FilesystemCatalog? catalog = null,
        IConfiguration? configuration = null)
    {
        var config = configuration ?? CreateMockConfiguration();

        return new UnifiedTransferOrchestrator(
            sqlExtractor ?? CreateMockSqlExtractor(),
            parquetExtractor ?? CreateMockParquetExtractor(),
            sqlLoader ?? CreateMockSqlLoader(),
            parquetWriter ?? CreateMockParquetWriter(),
            icebergExporter ?? CreateMockIcebergExporter(),
            incrementalSync ?? CreateMockIncrementalSync(),
            catalog ?? CreateMockCatalog(),
            config,
            Mock.Of<ILogger<UnifiedTransferOrchestrator>>());
    }

    private static SqlServerToIcebergExporter CreateMockIcebergExporter()
    {
        return Mock.Of<SqlServerToIcebergExporter>();
    }

    private static IncrementalSyncCoordinator CreateMockIncrementalSync()
    {
        return Mock.Of<IncrementalSyncCoordinator>();
    }

    private static FilesystemCatalog CreateMockCatalog()
    {
        return Mock.Of<FilesystemCatalog>();
    }

    private static IConfiguration CreateMockConfiguration()
    {
        var mock = new Mock<IConfiguration>();
        mock.SetupGet(x => x["Iceberg:WarehousePath"]).Returns("./iceberg-warehouse");
        return mock.Object;
    }

    private static ITableExtractor CreateMockSqlExtractor()
    {
        var mock = new Mock<ITableExtractor>();
        mock.Setup(x => x.ExtractAsync(
                It.IsAny<TableConfiguration>(),
                It.IsAny<string>(),
                It.IsAny<Stream>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ExtractionResult { RowsExtracted = 10, Success = true });
        return mock.Object;
    }

    private static IParquetExtractor CreateMockParquetExtractor()
    {
        var mock = new Mock<IParquetExtractor>();
        mock.Setup(x => x.ExtractFromParquetAsync(
                It.IsAny<string>(),
                It.IsAny<Stream>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ExtractionResult { RowsExtracted = 10, Success = true });
        return mock.Object;
    }

    private static IDataLoader CreateMockSqlLoader()
    {
        var mock = new Mock<IDataLoader>();
        mock.Setup(x => x.LoadAsync(
                It.IsAny<TableConfiguration>(),
                It.IsAny<string>(),
                It.IsAny<Stream>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new LoadResult { RowsLoaded = 10, Success = true });
        return mock.Object;
    }

    private static IParquetWriter CreateMockParquetWriter()
    {
        var mock = new Mock<IParquetWriter>();
        mock.Setup(x => x.WriteToParquetAsync(
                It.IsAny<Stream>(),
                It.IsAny<string>(),
                It.IsAny<DateTime?>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(10);
        return mock.Object;
    }

    private static TransferConfiguration CreateSqlToParquetConfig()
    {
        return new TransferConfiguration
        {
            TransferType = TransferType.SqlToParquet,
            Source = new SourceConfiguration
            {
                Type = SourceType.SqlServer,
                ConnectionString = "Server=localhost;",
                Table = new TableIdentifier { Database = "Test", Schema = "dbo", Table = "Orders" }
            },
            Destination = new DestinationConfiguration
            {
                Type = DestinationType.Parquet,
                ParquetPath = "output.parquet"
            }
        };
    }
}
