using DataTransfer.Core.Interfaces;
using Xunit;

namespace DataTransfer.Core.Tests.Interfaces;

public class IParquetStorageTests
{
    [Fact]
    public void IParquetStorage_Should_Have_WriteAsync_Method()
    {
        var interfaceType = typeof(IParquetStorage);
        var method = interfaceType.GetMethod("WriteAsync");

        Assert.NotNull(method);
        Assert.True(method.ReturnType == typeof(Task) || method.ReturnType.BaseType == typeof(Task));
    }

    [Fact]
    public void IParquetStorage_Should_Have_ReadAsync_Method()
    {
        var interfaceType = typeof(IParquetStorage);
        var method = interfaceType.GetMethod("ReadAsync");

        Assert.NotNull(method);
        Assert.True(method.ReturnType.IsGenericType && method.ReturnType.GetGenericTypeDefinition() == typeof(Task<>));
    }

    [Fact]
    public void IParquetStorage_WriteAsync_Should_Take_Stream()
    {
        var interfaceType = typeof(IParquetStorage);
        var method = interfaceType.GetMethod("WriteAsync");

        Assert.NotNull(method);
        var parameters = method.GetParameters();
        Assert.Contains(parameters, p => p.ParameterType == typeof(Stream));
    }

    [Fact]
    public void IParquetStorage_WriteAsync_Should_Take_CancellationToken()
    {
        var interfaceType = typeof(IParquetStorage);
        var method = interfaceType.GetMethod("WriteAsync");

        Assert.NotNull(method);
        var parameters = method.GetParameters();
        Assert.Contains(parameters, p => p.ParameterType == typeof(CancellationToken));
    }
}
