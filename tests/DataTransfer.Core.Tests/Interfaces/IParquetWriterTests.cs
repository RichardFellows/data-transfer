using DataTransfer.Core.Interfaces;

namespace DataTransfer.Core.Tests.Interfaces;

public class IParquetWriterTests
{
    [Fact]
    public void IParquetWriter_Interface_ShouldExist()
    {
        // Arrange
        var interfaceType = typeof(IParquetWriter);

        // Assert
        Assert.NotNull(interfaceType);
        Assert.True(interfaceType.IsInterface);
    }

    [Fact]
    public void IParquetWriter_Should_HaveWriteToParquetAsyncMethod()
    {
        // Arrange
        var interfaceType = typeof(IParquetWriter);

        // Act
        var method = interfaceType.GetMethod("WriteToParquetAsync");

        // Assert
        Assert.NotNull(method);
        Assert.Equal(typeof(Task<int>), method.ReturnType);

        var parameters = method.GetParameters();
        Assert.Equal(4, parameters.Length);
        Assert.Equal("dataStream", parameters[0].Name);
        Assert.Equal(typeof(Stream), parameters[0].ParameterType);
        Assert.Equal("outputPath", parameters[1].Name);
        Assert.Equal(typeof(string), parameters[1].ParameterType);
        Assert.Equal("partitionDate", parameters[2].Name);
        Assert.Equal(typeof(DateTime?), parameters[2].ParameterType);
        Assert.Equal("cancellationToken", parameters[3].Name);
        Assert.Equal(typeof(CancellationToken), parameters[3].ParameterType);
    }
}
