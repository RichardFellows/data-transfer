using DataTransfer.Configuration;
using DataTransfer.Core.Models;
using Xunit;

namespace DataTransfer.Configuration.Tests;

public class ConfigurationValidatorTests
{
    [Fact]
    public void Validator_Should_Pass_Valid_Configuration()
    {
        var config = CreateValidConfiguration();
        var validator = new ConfigurationValidator();

        var result = validator.Validate(config);

        Assert.True(result.IsValid);
        Assert.Empty(result.Errors);
    }

    [Fact]
    public void Validator_Should_Fail_When_Source_Connection_Missing()
    {
        var config = CreateValidConfiguration();
        config.Connections.Source = string.Empty;
        var validator = new ConfigurationValidator();

        var result = validator.Validate(config);

        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.Contains("Source connection"));
    }

    [Fact]
    public void Validator_Should_Fail_When_Destination_Connection_Missing()
    {
        var config = CreateValidConfiguration();
        config.Connections.Destination = string.Empty;
        var validator = new ConfigurationValidator();

        var result = validator.Validate(config);

        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.Contains("Destination connection"));
    }

    [Fact]
    public void Validator_Should_Fail_When_No_Tables_Configured()
    {
        var config = CreateValidConfiguration();
        config.Tables.Clear();
        var validator = new ConfigurationValidator();

        var result = validator.Validate(config);

        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.Contains("table"));
    }

    [Fact]
    public void Validator_Should_Fail_When_Storage_BasePath_Missing()
    {
        var config = CreateValidConfiguration();
        config.Storage.BasePath = string.Empty;
        var validator = new ConfigurationValidator();

        var result = validator.Validate(config);

        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.Contains("BasePath"));
    }

    [Fact]
    public void Validator_Should_Fail_When_Table_Source_Database_Missing()
    {
        var config = CreateValidConfiguration();
        config.Tables[0].Source.Database = string.Empty;
        var validator = new ConfigurationValidator();

        var result = validator.Validate(config);

        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.Contains("Source database"));
    }

    [Fact]
    public void Validator_Should_Fail_When_Table_Source_Table_Missing()
    {
        var config = CreateValidConfiguration();
        config.Tables[0].Source.Table = string.Empty;
        var validator = new ConfigurationValidator();

        var result = validator.Validate(config);

        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.Contains("Source table"));
    }

    [Fact]
    public void Validator_Should_Fail_When_Date_Partition_Missing_Column()
    {
        var config = CreateValidConfiguration();
        config.Tables[0].Partitioning.Type = PartitionType.Date;
        config.Tables[0].Partitioning.Column = null;
        var validator = new ConfigurationValidator();

        var result = validator.Validate(config);

        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.Contains("Partition column", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void Validator_Should_Fail_When_IntDate_Partition_Missing_Format()
    {
        var config = CreateValidConfiguration();
        config.Tables[0].Partitioning.Type = PartitionType.IntDate;
        config.Tables[0].Partitioning.Column = "DateKey";
        config.Tables[0].Partitioning.Format = null;
        var validator = new ConfigurationValidator();

        var result = validator.Validate(config);

        // Should still pass as format has a default value
        Assert.True(result.IsValid);
    }

    [Fact]
    public void Validator_Should_Pass_Static_Partition_Without_Column()
    {
        var config = CreateValidConfiguration();
        config.Tables[0].Partitioning.Type = PartitionType.Static;
        config.Tables[0].Partitioning.Column = null;
        var validator = new ConfigurationValidator();

        var result = validator.Validate(config);

        Assert.True(result.IsValid);
    }

    [Fact]
    public void Validator_Should_Accumulate_Multiple_Errors()
    {
        var config = CreateValidConfiguration();
        config.Connections.Source = string.Empty;
        config.Connections.Destination = string.Empty;
        config.Storage.BasePath = string.Empty;
        var validator = new ConfigurationValidator();

        var result = validator.Validate(config);

        Assert.False(result.IsValid);
        Assert.True(result.Errors.Count >= 3);
    }

    private static DataTransferConfiguration CreateValidConfiguration()
    {
        return new DataTransferConfiguration
        {
            Connections = new ConnectionConfiguration
            {
                Source = "Server=localhost;Database=Source;",
                Destination = "Server=localhost;Database=Dest;"
            },
            Tables = new List<TableConfiguration>
            {
                new TableConfiguration
                {
                    Source = new TableIdentifier
                    {
                        Database = "SourceDB",
                        Schema = "dbo",
                        Table = "SourceTable"
                    },
                    Destination = new TableIdentifier
                    {
                        Database = "DestDB",
                        Schema = "dbo",
                        Table = "DestTable"
                    },
                    Partitioning = new PartitioningConfiguration
                    {
                        Type = PartitionType.Date,
                        Column = "CreatedDate"
                    },
                    ExtractSettings = new ExtractSettings
                    {
                        BatchSize = 100000,
                        DateRange = new DateRange
                        {
                            StartDate = new DateTime(2024, 1, 1),
                            EndDate = new DateTime(2024, 12, 31)
                        }
                    }
                }
            },
            Storage = new StorageConfiguration
            {
                BasePath = "/data/extracts",
                Compression = "snappy"
            }
        };
    }
}
