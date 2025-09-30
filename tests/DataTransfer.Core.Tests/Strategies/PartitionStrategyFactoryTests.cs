using DataTransfer.Core.Models;
using DataTransfer.Core.Strategies;
using Xunit;

namespace DataTransfer.Core.Tests.Strategies;

public class PartitionStrategyFactoryTests
{
    [Fact]
    public void Factory_Should_Create_DatePartitionStrategy()
    {
        var config = new PartitioningConfiguration
        {
            Type = PartitionType.Date,
            Column = "CreatedDate"
        };

        var strategy = PartitionStrategyFactory.Create(config);

        Assert.IsType<DatePartitionStrategy>(strategy);
    }

    [Fact]
    public void Factory_Should_Create_IntDatePartitionStrategy()
    {
        var config = new PartitioningConfiguration
        {
            Type = PartitionType.IntDate,
            Column = "DateKey",
            Format = "yyyyMMdd"
        };

        var strategy = PartitionStrategyFactory.Create(config);

        Assert.IsType<IntDatePartitionStrategy>(strategy);
    }

    [Fact]
    public void Factory_Should_Create_Scd2PartitionStrategy()
    {
        var config = new PartitioningConfiguration
        {
            Type = PartitionType.Scd2,
            Column = "EffectiveDate",
            Format = "ExpirationDate"
        };

        var strategy = PartitionStrategyFactory.Create(config);

        Assert.IsType<Scd2PartitionStrategy>(strategy);
    }

    [Fact]
    public void Factory_Should_Create_StaticTableStrategy()
    {
        var config = new PartitioningConfiguration
        {
            Type = PartitionType.Static
        };

        var strategy = PartitionStrategyFactory.Create(config);

        Assert.IsType<StaticTableStrategy>(strategy);
    }

    [Fact]
    public void Factory_Should_Throw_For_Invalid_Configuration()
    {
        var config = new PartitioningConfiguration
        {
            Type = PartitionType.Date,
            Column = null
        };

        Assert.Throws<ArgumentException>(() => PartitionStrategyFactory.Create(config));
    }

    [Fact]
    public void Factory_Should_Default_IntDate_Format()
    {
        var config = new PartitioningConfiguration
        {
            Type = PartitionType.IntDate,
            Column = "DateKey"
        };

        var strategy = PartitionStrategyFactory.Create(config);

        Assert.IsType<IntDatePartitionStrategy>(strategy);
    }
}
