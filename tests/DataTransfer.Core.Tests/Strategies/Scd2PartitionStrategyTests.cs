using DataTransfer.Core.Strategies;
using Xunit;

namespace DataTransfer.Core.Tests.Strategies;

public class Scd2PartitionStrategyTests
{
    [Fact]
    public void Scd2PartitionStrategy_Should_Generate_Correct_Partition_Path()
    {
        var strategy = new Scd2PartitionStrategy("EffectiveDate", "ExpirationDate");
        var date = new DateTime(2024, 3, 15);

        var path = strategy.GetPartitionPath(date);

        Assert.Equal("year=2024/month=03/day=15", path);
    }

    [Fact]
    public void Scd2PartitionStrategy_Should_Build_Where_Clause_With_Effective_And_Expiration()
    {
        var strategy = new Scd2PartitionStrategy("EffectiveDate", "ExpirationDate");
        var startDate = new DateTime(2024, 3, 15);
        var endDate = new DateTime(2024, 3, 15);

        var whereClause = strategy.BuildWhereClause(startDate, endDate);

        Assert.Contains("EffectiveDate", whereClause);
        Assert.Contains("ExpirationDate", whereClause);
    }

    [Fact]
    public void Scd2PartitionStrategy_Should_Handle_Null_Expiration_Date()
    {
        var strategy = new Scd2PartitionStrategy("EffectiveDate", "ExpirationDate");
        var startDate = new DateTime(2024, 1, 1);
        var endDate = new DateTime(2024, 12, 31);

        var whereClause = strategy.BuildWhereClause(startDate, endDate);

        Assert.Contains("IS NULL", whereClause, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Scd2PartitionStrategy_Should_Build_Where_Clause_For_Date_Range()
    {
        var strategy = new Scd2PartitionStrategy("EffectiveDate", "ExpirationDate");
        var startDate = new DateTime(2024, 1, 1);
        var endDate = new DateTime(2024, 12, 31);

        var whereClause = strategy.BuildWhereClause(startDate, endDate);

        Assert.Contains("EffectiveDate", whereClause);
        Assert.Contains("2024-01-01", whereClause);
        Assert.Contains("2024-12-31", whereClause);
    }
}
