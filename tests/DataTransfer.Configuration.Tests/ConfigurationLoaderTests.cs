using DataTransfer.Configuration;
using DataTransfer.Core.Models;
using Xunit;

namespace DataTransfer.Configuration.Tests;

public class ConfigurationLoaderTests
{
    [Fact]
    public void ConfigurationLoader_Should_Load_Valid_Json_File()
    {
        var jsonContent = @"{
            ""connections"": {
                ""source"": ""Server=localhost;Database=Source;"",
                ""destination"": ""Server=localhost;Database=Dest;""
            },
            ""tables"": [
                {
                    ""source"": {
                        ""database"": ""GFRM_STAR2"",
                        ""schema"": ""dbo"",
                        ""table"": ""Reporting_Client""
                    },
                    ""destination"": {
                        ""database"": ""GFRM_STAR2_COPY"",
                        ""schema"": ""dbo"",
                        ""table"": ""Reporting_Client""
                    },
                    ""partitioning"": {
                        ""type"": ""date"",
                        ""column"": ""CreatedDate""
                    },
                    ""extractSettings"": {
                        ""batchSize"": 100000,
                        ""dateRange"": {
                            ""startDate"": ""2024-01-01"",
                            ""endDate"": ""2024-12-31""
                        }
                    }
                }
            ],
            ""storage"": {
                ""basePath"": ""/data/extracts"",
                ""compression"": ""snappy""
            }
        }";

        var tempFile = Path.GetTempFileName();
        File.WriteAllText(tempFile, jsonContent);

        try
        {
            var loader = new ConfigurationLoader();
            var config = loader.Load(tempFile);

            Assert.NotNull(config);
            Assert.NotNull(config.Connections);
            Assert.Equal("Server=localhost;Database=Source;", config.Connections.Source);
            Assert.Single(config.Tables);
            Assert.Equal("GFRM_STAR2", config.Tables[0].Source.Database);
            Assert.Equal("/data/extracts", config.Storage.BasePath);
        }
        finally
        {
            File.Delete(tempFile);
        }
    }

    [Fact]
    public void ConfigurationLoader_Should_Throw_When_File_Not_Found()
    {
        var loader = new ConfigurationLoader();

        Assert.Throws<FileNotFoundException>(() => loader.Load("nonexistent.json"));
    }

    [Fact]
    public void ConfigurationLoader_Should_Throw_On_Invalid_Json()
    {
        var tempFile = Path.GetTempFileName();
        File.WriteAllText(tempFile, "{ invalid json }");

        try
        {
            var loader = new ConfigurationLoader();

            Assert.Throws<System.Text.Json.JsonException>(() => loader.Load(tempFile));
        }
        finally
        {
            File.Delete(tempFile);
        }
    }

    [Fact]
    public void ConfigurationLoader_Should_Parse_PartitionType_Enum()
    {
        var jsonContent = @"{
            ""connections"": { ""source"": ""src"", ""destination"": ""dest"" },
            ""tables"": [{
                ""source"": { ""database"": ""db"", ""schema"": ""dbo"", ""table"": ""tbl"" },
                ""destination"": { ""database"": ""db"", ""schema"": ""dbo"", ""table"": ""tbl"" },
                ""partitioning"": { ""type"": ""intDate"", ""column"": ""DateKey"", ""format"": ""yyyyMMdd"" }
            }],
            ""storage"": { ""basePath"": ""/data"" }
        }";

        var tempFile = Path.GetTempFileName();
        File.WriteAllText(tempFile, jsonContent);

        try
        {
            var loader = new ConfigurationLoader();
            var config = loader.Load(tempFile);

            Assert.Equal(PartitionType.IntDate, config.Tables[0].Partitioning.Type);
        }
        finally
        {
            File.Delete(tempFile);
        }
    }

    [Fact]
    public void ConfigurationLoader_Should_Handle_Multiple_Tables()
    {
        var jsonContent = @"{
            ""connections"": { ""source"": ""src"", ""destination"": ""dest"" },
            ""tables"": [
                {
                    ""source"": { ""database"": ""db1"", ""schema"": ""dbo"", ""table"": ""tbl1"" },
                    ""destination"": { ""database"": ""db1"", ""schema"": ""dbo"", ""table"": ""tbl1"" },
                    ""partitioning"": { ""type"": ""date"", ""column"": ""Date1"" }
                },
                {
                    ""source"": { ""database"": ""db2"", ""schema"": ""dbo"", ""table"": ""tbl2"" },
                    ""destination"": { ""database"": ""db2"", ""schema"": ""dbo"", ""table"": ""tbl2"" },
                    ""partitioning"": { ""type"": ""static"" }
                }
            ],
            ""storage"": { ""basePath"": ""/data"" }
        }";

        var tempFile = Path.GetTempFileName();
        File.WriteAllText(tempFile, jsonContent);

        try
        {
            var loader = new ConfigurationLoader();
            var config = loader.Load(tempFile);

            Assert.Equal(2, config.Tables.Count);
            Assert.Equal("tbl1", config.Tables[0].Source.Table);
            Assert.Equal("tbl2", config.Tables[1].Source.Table);
        }
        finally
        {
            File.Delete(tempFile);
        }
    }
}
