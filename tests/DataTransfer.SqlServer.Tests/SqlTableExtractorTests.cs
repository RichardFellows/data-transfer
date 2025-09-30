using DataTransfer.Core.Interfaces;
using DataTransfer.Core.Models;
using DataTransfer.Core.Strategies;
using DataTransfer.SqlServer;
using Xunit;

namespace DataTransfer.SqlServer.Tests;

public class SqlTableExtractorTests
{
    [Fact]
    public void SqlTableExtractor_Should_Implement_ITableExtractor()
    {
        var extractor = new SqlTableExtractor(new SqlQueryBuilder());

        Assert.IsAssignableFrom<ITableExtractor>(extractor);
    }

    [Fact]
    public void SqlTableExtractor_Should_Require_QueryBuilder()
    {
        Assert.Throws<ArgumentNullException>(() => new SqlTableExtractor(null!));
    }

    [Fact]
    public async Task ExtractAsync_Should_Throw_When_TableConfig_Null()
    {
        var extractor = new SqlTableExtractor(new SqlQueryBuilder());
        var stream = new MemoryStream();

        await Assert.ThrowsAsync<ArgumentNullException>(async () =>
            await extractor.ExtractAsync(null!, "connectionString", stream));
    }

    [Fact]
    public async Task ExtractAsync_Should_Throw_When_ConnectionString_Empty()
    {
        var extractor = new SqlTableExtractor(new SqlQueryBuilder());
        var config = CreateTestConfiguration();
        var stream = new MemoryStream();

        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await extractor.ExtractAsync(config, "", stream));
    }

    [Fact]
    public async Task ExtractAsync_Should_Throw_When_Stream_Null()
    {
        var extractor = new SqlTableExtractor(new SqlQueryBuilder());
        var config = CreateTestConfiguration();

        await Assert.ThrowsAsync<ArgumentNullException>(async () =>
            await extractor.ExtractAsync(config, "connectionString", null!));
    }

    [Fact]
    public async Task ExtractAsync_Should_Handle_CancellationToken()
    {
        var extractor = new SqlTableExtractor(new SqlQueryBuilder());
        var config = CreateTestConfiguration();
        var stream = new MemoryStream();
        var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await extractor.ExtractAsync(config, "Server=invalid;", stream, cts.Token));
    }

    private static TableConfiguration CreateTestConfiguration()
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
                Table = "DestTable"
            },
            Partitioning = new PartitioningConfiguration
            {
                Type = PartitionType.Date,
                Column = "CreatedDate"
            },
            ExtractSettings = new ExtractSettings
            {
                BatchSize = 100000,
                DateRange = new DateRange
                {
                    StartDate = new DateTime(2024, 1, 1),
                    EndDate = new DateTime(2024, 12, 31)
                }
            }
        };
    }
}
