using DataTransfer.Core.Models;
using DataTransfer.Core.Strategies;
using Xunit;

namespace DataTransfer.Core.Tests.Strategies;

public class DatePartitionStrategyTests
{
    [Fact]
    public void DatePartitionStrategy_Should_Generate_Correct_Partition_Path()
    {
        var strategy = new DatePartitionStrategy("CreatedDate");
        var date = new DateTime(2024, 3, 15);

        var path = strategy.GetPartitionPath(date);

        Assert.Equal("year=2024/month=03/day=15", path);
    }

    [Fact]
    public void DatePartitionStrategy_Should_Build_Where_Clause_For_Single_Day()
    {
        var strategy = new DatePartitionStrategy("CreatedDate");
        var startDate = new DateTime(2024, 3, 15);
        var endDate = new DateTime(2024, 3, 15);

        var whereClause = strategy.BuildWhereClause(startDate, endDate);

        Assert.Contains("CreatedDate", whereClause);
        Assert.Contains("2024-03-15", whereClause);
    }

    [Fact]
    public void DatePartitionStrategy_Should_Build_Where_Clause_For_Date_Range()
    {
        var strategy = new DatePartitionStrategy("CreatedDate");
        var startDate = new DateTime(2024, 1, 1);
        var endDate = new DateTime(2024, 12, 31);

        var whereClause = strategy.BuildWhereClause(startDate, endDate);

        Assert.Contains("CreatedDate", whereClause);
        Assert.Contains(">=", whereClause);
        Assert.Contains("<=", whereClause);
    }

    [Fact]
    public void DatePartitionStrategy_Should_Handle_Different_Column_Names()
    {
        var strategy = new DatePartitionStrategy("ModifiedDate");
        var startDate = new DateTime(2024, 1, 1);
        var endDate = new DateTime(2024, 1, 31);

        var whereClause = strategy.BuildWhereClause(startDate, endDate);

        Assert.Contains("ModifiedDate", whereClause);
    }
}
