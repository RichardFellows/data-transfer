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

    [Fact]
    public void QueryBuilder_Should_Add_Custom_Where_Clause_From_ExtractSettings()
    {
        var tableConfig = new TableConfiguration
        {
            Source = new TableIdentifier
            {
                Database = "TestDB",
                Schema = "dbo",
                Table = "TestTable"
            },
            ExtractSettings = new ExtractSettings
            {
                WhereClause = "Status = 'Active' AND IsDeleted = 0"
            }
        };

        var builder = new SqlQueryBuilder();
        var query = builder.BuildSelectQuery(tableConfig, null);

        Assert.Contains("SELECT * FROM [TestDB].[dbo].[TestTable]", query);
        Assert.Contains("WHERE Status = 'Active' AND IsDeleted = 0", query);
    }

    [Fact]
    public void QueryBuilder_Should_Combine_Strategy_And_Custom_Where_Clauses()
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
            },
            ExtractSettings = new ExtractSettings
            {
                WhereClause = "Status = 'Active'"
            }
        };

        var strategy = new DatePartitionStrategy("CreatedDate");
        var startDate = new DateTime(2024, 1, 1);
        var endDate = new DateTime(2024, 1, 31);

        var builder = new SqlQueryBuilder();
        var query = builder.BuildSelectQuery(tableConfig, strategy, startDate, endDate);

        Assert.Contains("WHERE", query);
        Assert.Contains("CreatedDate", query);
        Assert.Contains("Status = 'Active'", query);
        Assert.Contains("AND", query);  // Should combine with AND
    }

    [Fact]
    public void QueryBuilder_Should_Add_Row_Limit_Using_TOP()
    {
        var tableConfig = new TableConfiguration
        {
            Source = new TableIdentifier
            {
                Database = "TestDB",
                Schema = "dbo",
                Table = "TestTable"
            },
            ExtractSettings = new ExtractSettings
            {
                RowLimit = 1000
            }
        };

        var builder = new SqlQueryBuilder();
        var query = builder.BuildSelectQuery(tableConfig, null);

        Assert.Contains("SELECT TOP (1000) * FROM [TestDB].[dbo].[TestTable]", query);
    }

    [Fact]
    public void QueryBuilder_Should_Support_Row_Limit_With_Where_Clause()
    {
        var tableConfig = new TableConfiguration
        {
            Source = new TableIdentifier
            {
                Database = "TestDB",
                Schema = "dbo",
                Table = "TestTable"
            },
            ExtractSettings = new ExtractSettings
            {
                RowLimit = 500,
                WhereClause = "IsActive = 1"
            }
        };

        var builder = new SqlQueryBuilder();
        var query = builder.BuildSelectQuery(tableConfig, null);

        Assert.Contains("SELECT TOP (500) * FROM", query);
        Assert.Contains("WHERE IsActive = 1", query);
    }

    [Fact]
    public void QueryBuilder_Should_Not_Add_TOP_When_RowLimit_Is_Null()
    {
        var tableConfig = new TableConfiguration
        {
            Source = new TableIdentifier
            {
                Database = "TestDB",
                Schema = "dbo",
                Table = "TestTable"
            },
            ExtractSettings = new ExtractSettings
            {
                RowLimit = null
            }
        };

        var builder = new SqlQueryBuilder();
        var query = builder.BuildSelectQuery(tableConfig, null);

        Assert.DoesNotContain("TOP", query);
        Assert.Contains("SELECT * FROM", query);
    }

    [Fact]
    public void QueryBuilder_Should_Support_All_Features_Combined()
    {
        // Test TOP + custom WHERE + partition WHERE all together
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
            },
            ExtractSettings = new ExtractSettings
            {
                RowLimit = 100,
                WhereClause = "Status = 'Active'"
            }
        };

        var strategy = new DatePartitionStrategy("CreatedDate");
        var startDate = new DateTime(2024, 1, 1);
        var endDate = new DateTime(2024, 1, 31);

        var builder = new SqlQueryBuilder();
        var query = builder.BuildSelectQuery(tableConfig, strategy, startDate, endDate);

        Assert.Contains("SELECT TOP (100) * FROM", query);
        Assert.Contains("WHERE", query);
        Assert.Contains("CreatedDate", query);
        Assert.Contains("Status = 'Active'", query);
    }
}
