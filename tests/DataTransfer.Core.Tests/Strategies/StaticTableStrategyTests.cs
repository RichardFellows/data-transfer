using DataTransfer.Core.Strategies;
using Xunit;

namespace DataTransfer.Core.Tests.Strategies;

public class StaticTableStrategyTests
{
    [Fact]
    public void StaticTableStrategy_Should_Return_Static_Partition_Path()
    {
        var strategy = new StaticTableStrategy();
        var date = new DateTime(2024, 3, 15);

        var path = strategy.GetPartitionPath(date);

        Assert.Equal("static", path);
    }

    [Fact]
    public void StaticTableStrategy_Should_Return_Empty_Where_Clause()
    {
        var strategy = new StaticTableStrategy();
        var startDate = new DateTime(2024, 1, 1);
        var endDate = new DateTime(2024, 12, 31);

        var whereClause = strategy.BuildWhereClause(startDate, endDate);

        Assert.Equal(string.Empty, whereClause);
    }

    [Fact]
    public void StaticTableStrategy_Should_Return_Same_Path_For_Any_Date()
    {
        var strategy = new StaticTableStrategy();
        var date1 = new DateTime(2024, 1, 1);
        var date2 = new DateTime(2025, 12, 31);

        var path1 = strategy.GetPartitionPath(date1);
        var path2 = strategy.GetPartitionPath(date2);

        Assert.Equal(path1, path2);
        Assert.Equal("static", path1);
    }
}
