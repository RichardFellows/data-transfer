using DataTransfer.Core.Interfaces;
using DataTransfer.Core.Models;
using Xunit;

namespace DataTransfer.Core.Tests.Interfaces;

public class IDataLoaderTests
{
    [Fact]
    public void IDataLoader_Should_Have_LoadAsync_Method()
    {
        var interfaceType = typeof(IDataLoader);
        var method = interfaceType.GetMethod("LoadAsync");

        Assert.NotNull(method);
        Assert.True(method.ReturnType.IsGenericType && method.ReturnType.GetGenericTypeDefinition() == typeof(Task<>));
    }

    [Fact]
    public void IDataLoader_LoadAsync_Should_Take_TableConfiguration()
    {
        var interfaceType = typeof(IDataLoader);
        var method = interfaceType.GetMethod("LoadAsync");

        Assert.NotNull(method);
        var parameters = method.GetParameters();
        Assert.Contains(parameters, p => p.ParameterType == typeof(TableConfiguration));
    }

    [Fact]
    public void IDataLoader_LoadAsync_Should_Take_CancellationToken()
    {
        var interfaceType = typeof(IDataLoader);
        var method = interfaceType.GetMethod("LoadAsync");

        Assert.NotNull(method);
        var parameters = method.GetParameters();
        Assert.Contains(parameters, p => p.ParameterType == typeof(CancellationToken));
    }
}
