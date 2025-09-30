using DataTransfer.Core.Strategies;
using Xunit;

namespace DataTransfer.Core.Tests.Strategies;

public class IntDatePartitionStrategyTests
{
    [Fact]
    public void IntDatePartitionStrategy_Should_Generate_Correct_Partition_Path()
    {
        var strategy = new IntDatePartitionStrategy("DateKey", "yyyyMMdd");
        var date = new DateTime(2024, 3, 15);

        var path = strategy.GetPartitionPath(date);

        Assert.Equal("year=2024/month=03/day=15", path);
    }

    [Fact]
    public void IntDatePartitionStrategy_Should_Build_Where_Clause_With_Integer_Format()
    {
        var strategy = new IntDatePartitionStrategy("DateKey", "yyyyMMdd");
        var startDate = new DateTime(2024, 3, 15);
        var endDate = new DateTime(2024, 3, 15);

        var whereClause = strategy.BuildWhereClause(startDate, endDate);

        Assert.Contains("DateKey", whereClause);
        Assert.Contains("20240315", whereClause);
    }

    [Fact]
    public void IntDatePartitionStrategy_Should_Build_Where_Clause_For_Date_Range()
    {
        var strategy = new IntDatePartitionStrategy("DateKey", "yyyyMMdd");
        var startDate = new DateTime(2024, 1, 1);
        var endDate = new DateTime(2024, 12, 31);

        var whereClause = strategy.BuildWhereClause(startDate, endDate);

        Assert.Contains("DateKey", whereClause);
        Assert.Contains("20240101", whereClause);
        Assert.Contains("20241231", whereClause);
    }

    [Fact]
    public void IntDatePartitionStrategy_Should_Support_Different_Formats()
    {
        var strategy = new IntDatePartitionStrategy("DateKey", "yyyyMM");
        var startDate = new DateTime(2024, 3, 1);
        var endDate = new DateTime(2024, 3, 31);

        var whereClause = strategy.BuildWhereClause(startDate, endDate);

        Assert.Contains("202403", whereClause);
    }
}
