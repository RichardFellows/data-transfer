using DataTransfer.Core.Strategies;
using Xunit;

namespace DataTransfer.Core.Tests.Strategies;

/// <summary>
/// Demonstration tests showing how all 4 partition strategies handle the same dates differently.
/// These tests serve as documentation and showcase the behavior differences between strategies.
/// </summary>
public class PartitionStrategyComparisonTests
{
    [Fact]
    public void Should_Generate_Different_Partition_Paths_For_Same_Date()
    {
        // Arrange - Use a common test date
        var testDate = new DateTime(2024, 11, 4);

        var dateStrategy = new DatePartitionStrategy("TransactionDate");
        var intDateStrategy = new IntDatePartitionStrategy("DateKey");
        var scd2Strategy = new Scd2PartitionStrategy("EffectiveDate", "ExpirationDate");
        var staticStrategy = new StaticTableStrategy();

        // Act - Generate partition paths for each strategy
        var datePath = dateStrategy.GetPartitionPath(testDate);
        var intDatePath = intDateStrategy.GetPartitionPath(testDate);
        var scd2Path = scd2Strategy.GetPartitionPath(testDate);
        var staticPath = staticStrategy.GetPartitionPath(testDate);

        // Assert - Each strategy produces different partition structure
        Assert.Equal("year=2024/month=11/day=04", datePath);
        Assert.Equal("date_key=20241104", intDatePath);
        Assert.Equal("effective_year=2024/effective_month=11/effective_day=04", scd2Path);
        Assert.Equal(string.Empty, staticPath); // Static tables don't partition
    }

    [Fact]
    public void Should_Handle_Leap_Year_February_29th()
    {
        // Arrange - Leap year date (2024 is a leap year)
        var leapDate = new DateTime(2024, 2, 29);

        var dateStrategy = new DatePartitionStrategy("CreatedDate");
        var intDateStrategy = new IntDatePartitionStrategy("DateKey");

        // Act
        var datePath = dateStrategy.GetPartitionPath(leapDate);
        var intDatePath = intDateStrategy.GetPartitionPath(leapDate);

        // Assert - Both should handle Feb 29 correctly
        Assert.Equal("year=2024/month=02/day=29", datePath);
        Assert.Equal("date_key=20240229", intDatePath);
    }

    [Fact]
    public void Should_Handle_Year_Boundary_December_31_To_January_1()
    {
        // Arrange - Year boundary dates
        var yearEnd = new DateTime(2023, 12, 31);
        var yearStart = new DateTime(2024, 1, 1);

        var strategy = new DatePartitionStrategy("OrderDate");

        // Act
        var endPath = strategy.GetPartitionPath(yearEnd);
        var startPath = strategy.GetPartitionPath(yearStart);
        var whereClause = strategy.BuildWhereClause(yearEnd, yearStart);

        // Assert - Should create separate partitions for different years
        Assert.Equal("year=2023/month=12/day=31", endPath);
        Assert.Equal("year=2024/month=01/day=01", startPath);
        Assert.Contains("OrderDate", whereClause);
        Assert.Contains("2023-12-31", whereClause);
        Assert.Contains("2024-01-01", whereClause);
    }

    [Fact]
    public void Should_Handle_Month_Boundary_January_31_To_February_1()
    {
        // Arrange - Month boundary in non-leap year
        var monthEnd = new DateTime(2023, 1, 31);
        var monthStart = new DateTime(2023, 2, 1);

        var dateStrategy = new DatePartitionStrategy("EventDate");
        var intDateStrategy = new IntDatePartitionStrategy("EventDateKey");

        // Act
        var dateEndPath = dateStrategy.GetPartitionPath(monthEnd);
        var dateStartPath = dateStrategy.GetPartitionPath(monthStart);
        var intDateEndPath = intDateStrategy.GetPartitionPath(monthEnd);
        var intDateStartPath = intDateStrategy.GetPartitionPath(monthStart);

        // Assert - Month changes should be reflected in paths
        Assert.Equal("year=2023/month=01/day=31", dateEndPath);
        Assert.Equal("year=2023/month=02/day=01", dateStartPath);
        Assert.Equal("date_key=20230131", intDateEndPath);
        Assert.Equal("date_key=20230201", intDateStartPath);
    }

    [Fact]
    public void Should_Generate_Different_Where_Clauses_For_Date_Range()
    {
        // Arrange - Same date range, different strategies
        var startDate = new DateTime(2024, 1, 1);
        var endDate = new DateTime(2024, 3, 31);

        var dateStrategy = new DatePartitionStrategy("TransactionDate");
        var intDateStrategy = new IntDatePartitionStrategy("TransactionDateKey");
        var scd2Strategy = new Scd2PartitionStrategy("EffectiveDate", "ExpirationDate");

        // Act
        var dateWhere = dateStrategy.BuildWhereClause(startDate, endDate);
        var intDateWhere = intDateStrategy.BuildWhereClause(startDate, endDate);
        var scd2Where = scd2Strategy.BuildWhereClause(startDate, endDate);

        // Assert - Each strategy has different WHERE clause logic
        Assert.Contains("TransactionDate", dateWhere);
        Assert.Contains(">=", dateWhere);
        Assert.Contains("<=", dateWhere);

        Assert.Contains("TransactionDateKey", intDateWhere);
        Assert.Contains("20240101", intDateWhere);
        Assert.Contains("20240331", intDateWhere);

        Assert.Contains("EffectiveDate", scd2Where);
        Assert.Contains("ExpirationDate", scd2Where);
    }

    [Fact]
    public void Should_Handle_Single_Day_Partition_Correctly()
    {
        // Arrange - Same start and end date (single day extract)
        var singleDate = new DateTime(2024, 6, 15);

        var dateStrategy = new DatePartitionStrategy("ProcessDate");
        var intDateStrategy = new IntDatePartitionStrategy("ProcessDateKey");

        // Act
        var datePath = dateStrategy.GetPartitionPath(singleDate);
        var intDatePath = intDateStrategy.GetPartitionPath(singleDate);
        var dateWhere = dateStrategy.BuildWhereClause(singleDate, singleDate);
        var intDateWhere = intDateStrategy.BuildWhereClause(singleDate, singleDate);

        // Assert - Single day should work identically to ranges
        Assert.Equal("year=2024/month=06/day=15", datePath);
        Assert.Equal("date_key=20240615", intDatePath);
        Assert.Contains("ProcessDate", dateWhere);
        Assert.Contains("ProcessDateKey", intDateWhere);
    }

    [Fact]
    public void Should_Demonstrate_SCD2_Effective_And_Expiration_Date_Logic()
    {
        // Arrange - SCD2 tables track history with effective/expiration dates
        var queryDate = new DateTime(2024, 11, 4);

        var scd2Strategy = new Scd2PartitionStrategy("ValidFrom", "ValidTo");

        // Act
        var partitionPath = scd2Strategy.GetPartitionPath(queryDate);
        var whereClause = scd2Strategy.BuildWhereClause(queryDate, queryDate);

        // Assert - SCD2 uses different partition naming
        Assert.Equal("effective_year=2024/effective_month=11/effective_day=04", partitionPath);
        Assert.Contains("ValidFrom", whereClause);
        Assert.Contains("ValidTo", whereClause);

        // SCD2 queries typically look for records "active" on a date
        // WHERE ValidFrom <= @date AND (ValidTo > @date OR ValidTo IS NULL)
    }

    [Fact]
    public void Should_Handle_Start_Of_Century_Date()
    {
        // Arrange - Edge case: Year 2000
        var centuryStart = new DateTime(2000, 1, 1);

        var dateStrategy = new DatePartitionStrategy("EventDate");
        var intDateStrategy = new IntDatePartitionStrategy("EventDateKey");

        // Act
        var datePath = dateStrategy.GetPartitionPath(centuryStart);
        var intDatePath = intDateStrategy.GetPartitionPath(centuryStart);

        // Assert - Should handle Y2K dates correctly
        Assert.Equal("year=2000/month=01/day=01", datePath);
        Assert.Equal("date_key=20000101", intDatePath);
    }

    [Fact]
    public void Should_Handle_Far_Future_Date()
    {
        // Arrange - Far future date (common in SCD2 "current" records)
        var futureDate = new DateTime(9999, 12, 31);

        var dateStrategy = new DatePartitionStrategy("ExpirationDate");
        var scd2Strategy = new Scd2PartitionStrategy("EffectiveDate", "ExpirationDate");

        // Act
        var datePath = dateStrategy.GetPartitionPath(futureDate);
        var scd2Path = scd2Strategy.GetPartitionPath(futureDate);

        // Assert - Should handle max dates (often used for "current" records)
        Assert.Equal("year=9999/month=12/day=31", datePath);
        Assert.Equal("effective_year=9999/effective_month=12/effective_day=31", scd2Path);
    }

    [Fact]
    public void Should_Demonstrate_Static_Table_Has_No_Partitioning()
    {
        // Arrange
        var anyDate = new DateTime(2024, 11, 4);
        var staticStrategy = new StaticTableStrategy();

        // Act
        var partitionPath = staticStrategy.GetPartitionPath(anyDate);
        var whereClause = staticStrategy.BuildWhereClause(anyDate, anyDate);

        // Assert - Static tables don't partition by date
        Assert.Equal(string.Empty, partitionPath);
        Assert.Equal(string.Empty, whereClause);

        // Static tables extract all data regardless of date parameters
    }

    [Theory]
    [InlineData(2024, 2, 29, "year=2024/month=02/day=29")] // Leap year
    [InlineData(2024, 12, 31, "year=2024/month=12/day=31")] // Year end
    [InlineData(2024, 1, 1, "year=2024/month=01/day=01")]    // Year start
    [InlineData(2024, 6, 15, "year=2024/month=06/day=15")]   // Mid-year
    public void Should_Generate_Consistent_Hive_Style_Paths(int year, int month, int day, string expected)
    {
        // Arrange
        var testDate = new DateTime(year, month, day);
        var strategy = new DatePartitionStrategy("PartitionDate");

        // Act
        var partitionPath = strategy.GetPartitionPath(testDate);

        // Assert - Should always use Hive-compatible format (key=value)
        Assert.Equal(expected, partitionPath);
        Assert.Matches(@"^year=\d{4}/month=\d{2}/day=\d{2}$", partitionPath);
    }

    [Theory]
    [InlineData(2024, 2, 29, "date_key=20240229")] // Leap year
    [InlineData(2024, 12, 31, "date_key=20241231")] // Year end
    [InlineData(2024, 1, 1, "date_key=20240101")]   // Year start
    [InlineData(2000, 1, 1, "date_key=20000101")]   // Y2K
    public void Should_Generate_Consistent_IntDate_Paths(int year, int month, int day, string expected)
    {
        // Arrange
        var testDate = new DateTime(year, month, day);
        var strategy = new IntDatePartitionStrategy("DateKey");

        // Act
        var partitionPath = strategy.GetPartitionPath(testDate);

        // Assert - Should always use date_key=YYYYMMDD format
        Assert.Equal(expected, partitionPath);
        Assert.Matches(@"^date_key=\d{8}$", partitionPath);
    }
}
