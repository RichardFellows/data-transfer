using DataTransfer.Configuration;
using DataTransfer.Core.Models;

namespace DataTransfer.Configuration.Tests;

public class TransferConfigurationValidatorTests
{
    private readonly ConfigurationValidator _validator = new();

    [Fact]
    public void ValidateTransfer_SqlToParquet_ValidConfig_ReturnsValid()
    {
        // Arrange
        var config = new TransferConfiguration
        {
            TransferType = TransferType.SqlToParquet,
            Source = new SourceConfiguration
            {
                Type = SourceType.SqlServer,
                ConnectionString = "Server=localhost;Database=Test;",
                Table = new TableIdentifier { Database = "Test", Schema = "dbo", Table = "Orders" }
            },
            Destination = new DestinationConfiguration
            {
                Type = DestinationType.Parquet,
                ParquetPath = "./output.parquet"
            }
        };

        // Act
        var result = _validator.ValidateTransfer(config);

        // Assert
        Assert.True(result.IsValid);
        Assert.Empty(result.Errors);
    }

    [Fact]
    public void ValidateTransfer_SqlToParquet_MissingConnectionString_ReturnsInvalid()
    {
        // Arrange
        var config = new TransferConfiguration
        {
            TransferType = TransferType.SqlToParquet,
            Source = new SourceConfiguration
            {
                Type = SourceType.SqlServer,
                Table = new TableIdentifier { Database = "Test", Schema = "dbo", Table = "Orders" }
            },
            Destination = new DestinationConfiguration
            {
                Type = DestinationType.Parquet,
                ParquetPath = "./output.parquet"
            }
        };

        // Act
        var result = _validator.ValidateTransfer(config);

        // Assert
        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.Contains("connection string"));
    }

    [Fact]
    public void ValidateTransfer_SqlToParquet_MissingTable_ReturnsInvalid()
    {
        // Arrange
        var config = new TransferConfiguration
        {
            TransferType = TransferType.SqlToParquet,
            Source = new SourceConfiguration
            {
                Type = SourceType.SqlServer,
                ConnectionString = "Server=localhost;Database=Test;"
            },
            Destination = new DestinationConfiguration
            {
                Type = DestinationType.Parquet,
                ParquetPath = "./output.parquet"
            }
        };

        // Act
        var result = _validator.ValidateTransfer(config);

        // Assert
        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.Contains("table"));
    }

    [Fact]
    public void ValidateTransfer_SqlToParquet_MissingParquetPath_ReturnsInvalid()
    {
        // Arrange
        var config = new TransferConfiguration
        {
            TransferType = TransferType.SqlToParquet,
            Source = new SourceConfiguration
            {
                Type = SourceType.SqlServer,
                ConnectionString = "Server=localhost;Database=Test;",
                Table = new TableIdentifier { Database = "Test", Schema = "dbo", Table = "Orders" }
            },
            Destination = new DestinationConfiguration
            {
                Type = DestinationType.Parquet
            }
        };

        // Act
        var result = _validator.ValidateTransfer(config);

        // Assert
        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.Contains("Parquet path"));
    }

    [Fact]
    public void ValidateTransfer_ParquetToSql_ValidConfig_ReturnsValid()
    {
        // Arrange
        var config = new TransferConfiguration
        {
            TransferType = TransferType.ParquetToSql,
            Source = new SourceConfiguration
            {
                Type = SourceType.Parquet,
                ParquetPath = "./input.parquet"
            },
            Destination = new DestinationConfiguration
            {
                Type = DestinationType.SqlServer,
                ConnectionString = "Server=localhost;Database=Test;",
                Table = new TableIdentifier { Database = "Test", Schema = "dbo", Table = "Orders" }
            }
        };

        // Act
        var result = _validator.ValidateTransfer(config);

        // Assert
        Assert.True(result.IsValid);
        Assert.Empty(result.Errors);
    }

    [Fact]
    public void ValidateTransfer_ParquetToSql_MissingParquetPath_ReturnsInvalid()
    {
        // Arrange
        var config = new TransferConfiguration
        {
            TransferType = TransferType.ParquetToSql,
            Source = new SourceConfiguration
            {
                Type = SourceType.Parquet
            },
            Destination = new DestinationConfiguration
            {
                Type = DestinationType.SqlServer,
                ConnectionString = "Server=localhost;Database=Test;",
                Table = new TableIdentifier { Database = "Test", Schema = "dbo", Table = "Orders" }
            }
        };

        // Act
        var result = _validator.ValidateTransfer(config);

        // Assert
        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.Contains("Parquet path"));
    }

    [Fact]
    public void ValidateTransfer_ParquetToSql_MissingDestinationTable_ReturnsInvalid()
    {
        // Arrange
        var config = new TransferConfiguration
        {
            TransferType = TransferType.ParquetToSql,
            Source = new SourceConfiguration
            {
                Type = SourceType.Parquet,
                ParquetPath = "./input.parquet"
            },
            Destination = new DestinationConfiguration
            {
                Type = DestinationType.SqlServer,
                ConnectionString = "Server=localhost;Database=Test;"
            }
        };

        // Act
        var result = _validator.ValidateTransfer(config);

        // Assert
        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.Contains("table"));
    }

    [Fact]
    public void ValidateTransfer_WrongSourceType_ReturnsInvalid()
    {
        // Arrange
        var config = new TransferConfiguration
        {
            TransferType = TransferType.SqlToParquet,
            Source = new SourceConfiguration
            {
                Type = SourceType.Parquet,  // Wrong! Should be SqlServer
                ParquetPath = "./input.parquet"
            },
            Destination = new DestinationConfiguration
            {
                Type = DestinationType.Parquet,
                ParquetPath = "./output.parquet"
            }
        };

        // Act
        var result = _validator.ValidateTransfer(config);

        // Assert
        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.Contains("Source must be SqlServer"));
    }

    [Fact]
    public void ValidateTransfer_WrongDestinationType_ReturnsInvalid()
    {
        // Arrange
        var config = new TransferConfiguration
        {
            TransferType = TransferType.ParquetToSql,
            Source = new SourceConfiguration
            {
                Type = SourceType.Parquet,
                ParquetPath = "./input.parquet"
            },
            Destination = new DestinationConfiguration
            {
                Type = DestinationType.Parquet,  // Wrong! Should be SqlServer
                ParquetPath = "./output.parquet"
            }
        };

        // Act
        var result = _validator.ValidateTransfer(config);

        // Assert
        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.Contains("Destination must be SqlServer"));
    }
}
