using DataTransfer.Core.Interfaces;
using DataTransfer.Core.Models;

namespace DataTransfer.Core.Tests.Interfaces;

public class IParquetExtractorTests
{
    [Fact]
    public void IParquetExtractor_Interface_ShouldExist()
    {
        // Arrange
        var interfaceType = typeof(IParquetExtractor);

        // Assert
        Assert.NotNull(interfaceType);
        Assert.True(interfaceType.IsInterface);
    }

    [Fact]
    public void IParquetExtractor_Should_HaveExtractFromParquetAsyncMethod()
    {
        // Arrange
        var interfaceType = typeof(IParquetExtractor);

        // Act
        var method = interfaceType.GetMethod("ExtractFromParquetAsync");

        // Assert
        Assert.NotNull(method);
        Assert.Equal(typeof(Task<ExtractionResult>), method.ReturnType);

        var parameters = method.GetParameters();
        Assert.Equal(3, parameters.Length);
        Assert.Equal("parquetPath", parameters[0].Name);
        Assert.Equal(typeof(string), parameters[0].ParameterType);
        Assert.Equal("outputStream", parameters[1].Name);
        Assert.Equal(typeof(Stream), parameters[1].ParameterType);
        Assert.Equal("cancellationToken", parameters[2].Name);
        Assert.Equal(typeof(CancellationToken), parameters[2].ParameterType);
    }
}
