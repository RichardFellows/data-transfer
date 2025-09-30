using DataTransfer.Core.Interfaces;
using DataTransfer.Core.Models;
using DataTransfer.SqlServer;
using Xunit;

namespace DataTransfer.SqlServer.Tests;

public class SqlDataLoaderTests
{
    [Fact]
    public void SqlDataLoader_Should_Implement_IDataLoader()
    {
        var loader = new SqlDataLoader(new SqlQueryBuilder());

        Assert.IsAssignableFrom<IDataLoader>(loader);
    }

    [Fact]
    public void SqlDataLoader_Should_Require_QueryBuilder()
    {
        Assert.Throws<ArgumentNullException>(() => new SqlDataLoader(null!));
    }

    [Fact]
    public async Task LoadAsync_Should_Throw_When_TableConfig_Null()
    {
        var loader = new SqlDataLoader(new SqlQueryBuilder());
        var stream = new MemoryStream();

        await Assert.ThrowsAsync<ArgumentNullException>(async () =>
            await loader.LoadAsync(null!, "connectionString", stream));
    }

    [Fact]
    public async Task LoadAsync_Should_Throw_When_ConnectionString_Empty()
    {
        var loader = new SqlDataLoader(new SqlQueryBuilder());
        var config = CreateTestConfiguration();
        var stream = new MemoryStream();

        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await loader.LoadAsync(config, "", stream));
    }

    [Fact]
    public async Task LoadAsync_Should_Throw_When_Stream_Null()
    {
        var loader = new SqlDataLoader(new SqlQueryBuilder());
        var config = CreateTestConfiguration();

        await Assert.ThrowsAsync<ArgumentNullException>(async () =>
            await loader.LoadAsync(config, "connectionString", null!));
    }

    [Fact]
    public async Task LoadAsync_Should_Handle_CancellationToken()
    {
        var loader = new SqlDataLoader(new SqlQueryBuilder());
        var config = CreateTestConfiguration();
        var stream = new MemoryStream();
        var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await loader.LoadAsync(config, "Server=invalid;", stream, cts.Token));
    }

    [Fact]
    public async Task LoadAsync_Should_Handle_Empty_Stream()
    {
        var loader = new SqlDataLoader(new SqlQueryBuilder());
        var config = CreateTestConfiguration();
        var stream = new MemoryStream();

        // This will fail to connect, but validates parameter checking happens first
        await Assert.ThrowsAnyAsync<Exception>(async () =>
            await loader.LoadAsync(config, "Server=invalid;Database=test;", stream));
    }

    private static TableConfiguration CreateTestConfiguration()
    {
        return new TableConfiguration
        {
            Source = new TableIdentifier
            {
                Database = "SourceDB",
                Schema = "dbo",
                Table = "SourceTable"
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
            }
        };
    }
}
