using DataTransfer.SqlServer;
using DataTransfer.SqlServer.Models;
using Microsoft.Data.SqlClient;
using Xunit;

namespace DataTransfer.SqlServer.Tests;

public class SqlSchemaDiscoveryTests
{
    [Fact]
    public void Constructor_ShouldThrowArgumentNullException_WhenConnectionStringIsNull()
    {
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => new SqlSchemaDiscovery(null!));
    }

    [Fact]
    public void Constructor_ShouldThrowArgumentException_WhenConnectionStringIsEmpty()
    {
        // Act & Assert
        Assert.Throws<ArgumentException>(() => new SqlSchemaDiscovery(""));
    }

    [Fact]
    public void Constructor_ShouldThrowArgumentException_WhenConnectionStringIsWhitespace()
    {
        // Act & Assert
        Assert.Throws<ArgumentException>(() => new SqlSchemaDiscovery("   "));
    }

    [Fact]
    public async Task DiscoverDatabaseAsync_ShouldThrowInvalidOperationException_WhenConnectionFails()
    {
        // Arrange
        var invalidConnectionString = "Server=invalid;Database=test;User=sa;Password=test";
        var discovery = new SqlSchemaDiscovery(invalidConnectionString);

        // Act & Assert
        await Assert.ThrowsAsync<SqlException>(async () =>
            await discovery.DiscoverDatabaseAsync());
    }

    [Fact]
    public void TableInfo_ShouldHaveRequiredProperties()
    {
        // Arrange & Act
        var table = new TableInfo
        {
            Schema = "dbo",
            TableName = "Orders",
            RowCount = 1000,
            Columns = new List<ColumnInfo>()
        };

        // Assert
        Assert.Equal("dbo", table.Schema);
        Assert.Equal("Orders", table.TableName);
        Assert.Equal(1000, table.RowCount);
        Assert.NotNull(table.Columns);
    }

    [Fact]
    public void ColumnInfo_ShouldHaveRequiredProperties()
    {
        // Arrange & Act
        var column = new ColumnInfo
        {
            ColumnName = "OrderDate",
            DataType = "datetime2",
            IsNullable = false,
            MaxLength = -1,
            Precision = 0,
            Scale = 0
        };

        // Assert
        Assert.Equal("OrderDate", column.ColumnName);
        Assert.Equal("datetime2", column.DataType);
        Assert.False(column.IsNullable);
    }

    [Fact]
    public void ColumnInfo_ShouldSuggestDatePartitioning_ForDateColumns()
    {
        // Arrange
        var column = new ColumnInfo
        {
            ColumnName = "OrderDate",
            DataType = "datetime2",
            IsNullable = false
        };

        // Act
        var suggestion = column.GetPartitionSuggestion();

        // Assert
        Assert.NotNull(suggestion);
        Assert.Equal("date", suggestion.PartitionType);
        Assert.Equal("OrderDate", suggestion.ColumnName);
        Assert.Contains("DATE", suggestion.Reason);
    }

    [Fact]
    public void ColumnInfo_ShouldSuggestIntDatePartitioning_ForIntColumns()
    {
        // Arrange
        var column = new ColumnInfo
        {
            ColumnName = "DateKey",
            DataType = "int",
            IsNullable = false
        };

        // Act
        var suggestion = column.GetPartitionSuggestion();

        // Assert
        Assert.NotNull(suggestion);
        Assert.Equal("int_date", suggestion.PartitionType);
        Assert.Equal("DateKey", suggestion.ColumnName);
        Assert.Contains("integer", suggestion.Reason);
    }

    [Fact]
    public void ColumnInfo_ShouldNotSuggestPartitioning_ForNonDateColumns()
    {
        // Arrange
        var column = new ColumnInfo
        {
            ColumnName = "CustomerName",
            DataType = "nvarchar",
            IsNullable = true
        };

        // Act
        var suggestion = column.GetPartitionSuggestion();

        // Assert
        Assert.Null(suggestion);
    }

    [Fact]
    public void TableInfo_ShouldSuggestStaticPartitioning_ForSmallTables()
    {
        // Arrange
        var table = new TableInfo
        {
            Schema = "dbo",
            TableName = "Products",
            RowCount = 500,
            Columns = new List<ColumnInfo>
            {
                new() { ColumnName = "ProductID", DataType = "int", IsNullable = false },
                new() { ColumnName = "ProductName", DataType = "nvarchar", IsNullable = false }
            }
        };

        // Act
        var suggestion = table.GetBestPartitionSuggestion();

        // Assert
        Assert.NotNull(suggestion);
        Assert.Equal("static", suggestion.PartitionType);
        Assert.Contains("small", suggestion.Reason);
    }

    [Fact]
    public void TableInfo_ShouldSuggestDatePartitioning_ForTablesWithDateColumns()
    {
        // Arrange
        var table = new TableInfo
        {
            Schema = "dbo",
            TableName = "Orders",
            RowCount = 100000,
            Columns = new List<ColumnInfo>
            {
                new() { ColumnName = "OrderID", DataType = "int", IsNullable = false },
                new() { ColumnName = "OrderDate", DataType = "datetime2", IsNullable = false },
                new() { ColumnName = "CustomerID", DataType = "int", IsNullable = false }
            }
        };

        // Act
        var suggestion = table.GetBestPartitionSuggestion();

        // Assert
        Assert.NotNull(suggestion);
        Assert.Equal("date", suggestion.PartitionType);
        Assert.Equal("OrderDate", suggestion.ColumnName);
    }

    [Fact]
    public void TableInfo_ShouldSuggestScd2Partitioning_ForTablesWithEffectiveAndExpirationDates()
    {
        // Arrange
        var table = new TableInfo
        {
            Schema = "dbo",
            TableName = "CustomerDimension",
            RowCount = 5000,
            Columns = new List<ColumnInfo>
            {
                new() { ColumnName = "CustomerKey", DataType = "int", IsNullable = false },
                new() { ColumnName = "EffectiveDate", DataType = "datetime2", IsNullable = false },
                new() { ColumnName = "ExpirationDate", DataType = "datetime2", IsNullable = true }
            }
        };

        // Act
        var suggestion = table.GetBestPartitionSuggestion();

        // Assert
        Assert.NotNull(suggestion);
        Assert.Equal("scd2", suggestion.PartitionType);
        Assert.Equal("EffectiveDate", suggestion.EffectiveDateColumn);
        Assert.Equal("ExpirationDate", suggestion.ExpirationDateColumn);
    }

    [Fact]
    public void DatabaseInfo_ShouldAggregateTableInformation()
    {
        // Arrange
        var dbInfo = new DatabaseInfo
        {
            DatabaseName = "MyDB",
            ServerVersion = "Microsoft SQL Server 2022",
            Tables = new List<TableInfo>
            {
                new() { Schema = "dbo", TableName = "Orders", RowCount = 1000, Columns = new List<ColumnInfo>() },
                new() { Schema = "dbo", TableName = "Products", RowCount = 500, Columns = new List<ColumnInfo>() }
            }
        };

        // Act & Assert
        Assert.Equal("MyDB", dbInfo.DatabaseName);
        Assert.Equal(2, dbInfo.Tables.Count);
        Assert.Equal(2, dbInfo.TotalTables);
    }
}
