using DataTransfer.Core.Interfaces;
using DataTransfer.Core.Models;
using Xunit;

namespace DataTransfer.Core.Tests.Interfaces;

public class ITableExtractorTests
{
    [Fact]
    public void ITableExtractor_Should_Have_ExtractAsync_Method()
    {
        // This test verifies the interface contract exists
        var interfaceType = typeof(ITableExtractor);
        var method = interfaceType.GetMethod("ExtractAsync");

        Assert.NotNull(method);
        Assert.True(method.ReturnType.IsGenericType && method.ReturnType.GetGenericTypeDefinition() == typeof(Task<>));
    }

    [Fact]
    public void ITableExtractor_ExtractAsync_Should_Take_TableConfiguration()
    {
        var interfaceType = typeof(ITableExtractor);
        var method = interfaceType.GetMethod("ExtractAsync");

        Assert.NotNull(method);
        var parameters = method.GetParameters();
        Assert.Contains(parameters, p => p.ParameterType == typeof(TableConfiguration));
    }

    [Fact]
    public void ITableExtractor_ExtractAsync_Should_Take_CancellationToken()
    {
        var interfaceType = typeof(ITableExtractor);
        var method = interfaceType.GetMethod("ExtractAsync");

        Assert.NotNull(method);
        var parameters = method.GetParameters();
        Assert.Contains(parameters, p => p.ParameterType == typeof(CancellationToken));
    }
}
