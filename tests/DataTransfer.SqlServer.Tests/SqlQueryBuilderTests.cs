using DataTransfer.Core.Models;
using DataTransfer.Core.Strategies;
using DataTransfer.SqlServer;
using Xunit;

namespace DataTransfer.SqlServer.Tests;

public class SqlQueryBuilderTests
{
    [Fact]
    public void QueryBuilder_Should_Generate_Basic_Select_Query()
    {
        var tableConfig = new TableConfiguration
        {
            Source = new TableIdentifier
            {
                Database = "TestDB",
                Schema = "dbo",
                Table = "TestTable"
            }
        };

        var builder = new SqlQueryBuilder();
        var query = builder.BuildSelectQuery(tableConfig, null);

        Assert.Contains("SELECT * FROM [TestDB].[dbo].[TestTable]", query);
    }

    [Fact]
    public void QueryBuilder_Should_Add_Where_Clause_From_Strategy()
    {
        var tableConfig = new TableConfiguration
        {
            Source = new TableIdentifier
            {
                Database = "TestDB",
                Schema = "dbo",
                Table = "TestTable"
            },
            Partitioning = new PartitioningConfiguration
            {
                Type = PartitionType.Date,
                Column = "CreatedDate"
            }
        };

        var strategy = new DatePartitionStrategy("CreatedDate");
        var startDate = new DateTime(2024, 1, 1);
        var endDate = new DateTime(2024, 1, 31);

        var builder = new SqlQueryBuilder();
        var query = builder.BuildSelectQuery(tableConfig, strategy, startDate, endDate);

        Assert.Contains("SELECT * FROM", query);
        Assert.Contains("WHERE", query);
        Assert.Contains("CreatedDate", query);
    }

    [Fact]
    public void QueryBuilder_Should_Escape_Identifiers_With_Brackets()
    {
        var tableConfig = new TableConfiguration
        {
            Source = new TableIdentifier
            {
                Database = "Test-DB",
                Schema = "dbo",
                Table = "Test Table"
            }
        };

        var builder = new SqlQueryBuilder();
        var query = builder.BuildSelectQuery(tableConfig, null);

        Assert.Contains("[Test-DB]", query);
        Assert.Contains("[dbo]", query);
        Assert.Contains("[Test Table]", query);
    }

    [Fact]
    public void QueryBuilder_Should_Generate_Count_Query()
    {
        var tableConfig = new TableConfiguration
        {
            Source = new TableIdentifier
            {
                Database = "TestDB",
                Schema = "dbo",
                Table = "TestTable"
            }
        };

        var builder = new SqlQueryBuilder();
        var query = builder.BuildCountQuery(tableConfig, null);

        Assert.Contains("SELECT COUNT(*)", query);
        Assert.Contains("FROM [TestDB].[dbo].[TestTable]", query);
    }

    [Fact]
    public void QueryBuilder_Should_Add_Where_Clause_To_Count_Query()
    {
        var tableConfig = new TableConfiguration
        {
            Source = new TableIdentifier
            {
                Database = "TestDB",
                Schema = "dbo",
                Table = "TestTable"
            }
        };

        var strategy = new DatePartitionStrategy("CreatedDate");
        var startDate = new DateTime(2024, 1, 1);
        var endDate = new DateTime(2024, 1, 31);

        var builder = new SqlQueryBuilder();
        var query = builder.BuildCountQuery(tableConfig, strategy, startDate, endDate);

        Assert.Contains("SELECT COUNT(*)", query);
        Assert.Contains("WHERE", query);
        Assert.Contains("CreatedDate", query);
    }

    [Fact]
    public void QueryBuilder_Should_Generate_Insert_Query()
    {
        var tableConfig = new TableConfiguration
        {
            Destination = new TableIdentifier
            {
                Database = "DestDB",
                Schema = "dbo",
                Table = "DestTable"
            }
        };

        var columns = new[] { "Id", "Name", "CreatedDate" };

        var builder = new SqlQueryBuilder();
        var query = builder.BuildInsertQuery(tableConfig, columns);

        Assert.Contains("INSERT INTO [DestDB].[dbo].[DestTable]", query);
        Assert.Contains("[Id]", query);
        Assert.Contains("[Name]", query);
        Assert.Contains("[CreatedDate]", query);
    }

    [Fact]
    public void QueryBuilder_Should_Generate_Truncate_Query()
    {
        var tableConfig = new TableConfiguration
        {
            Destination = new TableIdentifier
            {
                Database = "DestDB",
                Schema = "dbo",
                Table = "DestTable"
            }
        };

        var builder = new SqlQueryBuilder();
        var query = builder.BuildTruncateQuery(tableConfig);

        Assert.Contains("TRUNCATE TABLE [DestDB].[dbo].[DestTable]", query);
    }

    [Fact]
    public void QueryBuilder_Should_Handle_Static_Partition_Without_Where()
    {
        var tableConfig = new TableConfiguration
        {
            Source = new TableIdentifier
            {
                Database = "TestDB",
                Schema = "dbo",
                Table = "TestTable"
            }
        };

        var strategy = new StaticTableStrategy();

        var builder = new SqlQueryBuilder();
        var query = builder.BuildSelectQuery(tableConfig, strategy);

        Assert.Contains("SELECT * FROM", query);
        Assert.DoesNotContain("WHERE", query);
    }
}
