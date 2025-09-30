using DataTransfer.Core.Models;
using Xunit;

namespace DataTransfer.Core.Tests.Models;

public class DataTransferConfigurationTests
{
    [Fact]
    public void DataTransferConfiguration_Should_Have_Connections()
    {
        var config = new DataTransferConfiguration
        {
            Connections = new ConnectionConfiguration
            {
                Source = "Server=localhost;Database=GFRM_STAR2;",
                Destination = "Server=localhost;Database=GFRM_STAR2_COPY;"
            }
        };

        Assert.NotNull(config.Connections);
        Assert.Equal("Server=localhost;Database=GFRM_STAR2;", config.Connections.Source);
        Assert.Equal("Server=localhost;Database=GFRM_STAR2_COPY;", config.Connections.Destination);
    }

    [Fact]
    public void DataTransferConfiguration_Should_Have_Tables_Collection()
    {
        var config = new DataTransferConfiguration
        {
            Tables = new List<TableConfiguration>
            {
                new TableConfiguration
                {
                    Source = new TableIdentifier { Database = "DB1", Schema = "dbo", Table = "Table1" }
                },
                new TableConfiguration
                {
                    Source = new TableIdentifier { Database = "DB1", Schema = "dbo", Table = "Table2" }
                }
            }
        };

        Assert.Equal(2, config.Tables.Count);
    }

    [Fact]
    public void DataTransferConfiguration_Should_Have_Storage_Settings()
    {
        var config = new DataTransferConfiguration
        {
            Storage = new StorageConfiguration
            {
                BasePath = "/data/extracts",
                Compression = "snappy"
            }
        };

        Assert.NotNull(config.Storage);
        Assert.Equal("/data/extracts", config.Storage.BasePath);
    }
}
