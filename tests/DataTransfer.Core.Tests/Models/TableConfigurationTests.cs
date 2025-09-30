using DataTransfer.Core.Models;
using Xunit;

namespace DataTransfer.Core.Tests.Models;

public class TableConfigurationTests
{
    [Fact]
    public void TableConfiguration_Should_Have_Source_Database()
    {
        var config = new TableConfiguration
        {
            Source = new TableIdentifier
            {
                Database = "GFRM_STAR2",
                Schema = "dbo",
                Table = "Reporting_Client"
            }
        };

        Assert.Equal("GFRM_STAR2", config.Source.Database);
        Assert.Equal("dbo", config.Source.Schema);
        Assert.Equal("Reporting_Client", config.Source.Table);
    }

    [Fact]
    public void TableConfiguration_Should_Have_Destination_Database()
    {
        var config = new TableConfiguration
        {
            Destination = new TableIdentifier
            {
                Database = "GFRM_STAR2_COPY",
                Schema = "dbo",
                Table = "Reporting_Client"
            }
        };

        Assert.Equal("GFRM_STAR2_COPY", config.Destination.Database);
    }

    [Fact]
    public void TableConfiguration_Should_Have_Partitioning_Settings()
    {
        var config = new TableConfiguration
        {
            Partitioning = new PartitioningConfiguration
            {
                Type = PartitionType.Date,
                Column = "CreatedDate"
            }
        };

        Assert.Equal(PartitionType.Date, config.Partitioning.Type);
        Assert.Equal("CreatedDate", config.Partitioning.Column);
    }

    [Fact]
    public void TableConfiguration_Should_Have_Extract_Settings()
    {
        var config = new TableConfiguration
        {
            ExtractSettings = new ExtractSettings
            {
                BatchSize = 100000,
                DateRange = new DateRange
                {
                    StartDate = new DateTime(2024, 1, 1),
                    EndDate = new DateTime(2024, 12, 31)
                }
            }
        };

        Assert.Equal(100000, config.ExtractSettings.BatchSize);
        Assert.Equal(new DateTime(2024, 1, 1), config.ExtractSettings.DateRange.StartDate);
        Assert.Equal(new DateTime(2024, 12, 31), config.ExtractSettings.DateRange.EndDate);
    }

    [Fact]
    public void TableIdentifier_Should_Generate_FullyQualifiedName()
    {
        var identifier = new TableIdentifier
        {
            Database = "GFRM_STAR2",
            Schema = "dbo",
            Table = "Reporting_Client"
        };

        Assert.Equal("GFRM_STAR2.dbo.Reporting_Client", identifier.FullyQualifiedName);
    }

    [Fact]
    public void PartitioningConfiguration_Should_Support_IntDate_With_Format()
    {
        var config = new PartitioningConfiguration
        {
            Type = PartitionType.IntDate,
            Column = "DateKey",
            Format = "yyyyMMdd"
        };

        Assert.Equal(PartitionType.IntDate, config.Type);
        Assert.Equal("DateKey", config.Column);
        Assert.Equal("yyyyMMdd", config.Format);
    }

    [Fact]
    public void PartitioningConfiguration_Should_Support_Static_Type()
    {
        var config = new PartitioningConfiguration
        {
            Type = PartitionType.Static
        };

        Assert.Equal(PartitionType.Static, config.Type);
        Assert.Null(config.Column);
    }

    [Fact]
    public void PartitioningConfiguration_Should_Support_Scd2_Type()
    {
        var config = new PartitioningConfiguration
        {
            Type = PartitionType.Scd2,
            Column = "EffectiveDate"
        };

        Assert.Equal(PartitionType.Scd2, config.Type);
        Assert.Equal("EffectiveDate", config.Column);
    }
}
