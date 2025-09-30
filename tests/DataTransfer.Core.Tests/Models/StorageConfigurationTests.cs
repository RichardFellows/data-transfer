using DataTransfer.Core.Models;
using Xunit;

namespace DataTransfer.Core.Tests.Models;

public class StorageConfigurationTests
{
    [Fact]
    public void StorageConfiguration_Should_Have_BasePath()
    {
        var config = new StorageConfiguration
        {
            BasePath = "/data/extracts"
        };

        Assert.Equal("/data/extracts", config.BasePath);
    }

    [Fact]
    public void StorageConfiguration_Should_Have_Compression_Type()
    {
        var config = new StorageConfiguration
        {
            Compression = "snappy"
        };

        Assert.Equal("snappy", config.Compression);
    }

    [Fact]
    public void StorageConfiguration_Should_Default_To_Snappy_Compression()
    {
        var config = new StorageConfiguration
        {
            BasePath = "/data/extracts"
        };

        Assert.Equal("snappy", config.Compression);
    }
}
