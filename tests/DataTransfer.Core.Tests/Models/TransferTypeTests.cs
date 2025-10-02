using DataTransfer.Core.Models;

namespace DataTransfer.Core.Tests.Models;

public class TransferTypeTests
{
    [Fact]
    public void TransferType_Should_HaveSqlToSqlValue()
    {
        // Act
        var value = TransferType.SqlToSql;

        // Assert
        Assert.Equal("SqlToSql", value.ToString());
    }

    [Fact]
    public void TransferType_Should_HaveSqlToParquetValue()
    {
        // Act
        var value = TransferType.SqlToParquet;

        // Assert
        Assert.Equal("SqlToParquet", value.ToString());
    }

    [Fact]
    public void TransferType_Should_HaveParquetToSqlValue()
    {
        // Act
        var value = TransferType.ParquetToSql;

        // Assert
        Assert.Equal("ParquetToSql", value.ToString());
    }

    [Fact]
    public void TransferType_Should_HaveExactlyThreeValues()
    {
        // Act
        var values = Enum.GetValues<TransferType>();

        // Assert
        Assert.Equal(3, values.Length);
    }
}

public class SourceTypeTests
{
    [Fact]
    public void SourceType_Should_HaveSqlServerValue()
    {
        // Act
        var value = SourceType.SqlServer;

        // Assert
        Assert.Equal("SqlServer", value.ToString());
    }

    [Fact]
    public void SourceType_Should_HaveParquetValue()
    {
        // Act
        var value = SourceType.Parquet;

        // Assert
        Assert.Equal("Parquet", value.ToString());
    }

    [Fact]
    public void SourceType_Should_HaveExactlyTwoValues()
    {
        // Act
        var values = Enum.GetValues<SourceType>();

        // Assert
        Assert.Equal(2, values.Length);
    }
}

public class DestinationTypeTests
{
    [Fact]
    public void DestinationType_Should_HaveSqlServerValue()
    {
        // Act
        var value = DestinationType.SqlServer;

        // Assert
        Assert.Equal("SqlServer", value.ToString());
    }

    [Fact]
    public void DestinationType_Should_HaveParquetValue()
    {
        // Act
        var value = DestinationType.Parquet;

        // Assert
        Assert.Equal("Parquet", value.ToString());
    }

    [Fact]
    public void DestinationType_Should_HaveExactlyTwoValues()
    {
        // Act
        var values = Enum.GetValues<DestinationType>();

        // Assert
        Assert.Equal(2, values.Length);
    }
}
