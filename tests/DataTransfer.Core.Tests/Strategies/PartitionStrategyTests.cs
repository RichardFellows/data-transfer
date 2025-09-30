using DataTransfer.Core.Models;
using DataTransfer.Core.Strategies;
using Xunit;

namespace DataTransfer.Core.Tests.Strategies;

public class PartitionStrategyTests
{
    [Fact]
    public void PartitionStrategy_Should_Be_Abstract()
    {
        var type = typeof(PartitionStrategy);
        Assert.True(type.IsAbstract);
    }

    [Fact]
    public void PartitionStrategy_Should_Have_GetPartitionPath_Method()
    {
        var type = typeof(PartitionStrategy);
        var method = type.GetMethod("GetPartitionPath");

        Assert.NotNull(method);
        Assert.Equal(typeof(string), method.ReturnType);
    }

    [Fact]
    public void PartitionStrategy_Should_Have_BuildWhereClause_Method()
    {
        var type = typeof(PartitionStrategy);
        var method = type.GetMethod("BuildWhereClause");

        Assert.NotNull(method);
        Assert.Equal(typeof(string), method.ReturnType);
    }
}
