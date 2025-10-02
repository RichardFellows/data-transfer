using System.Text.Json;
using DataTransfer.Core.Interfaces;
using DataTransfer.Core.Models;
using DataTransfer.Parquet;
using Xunit;

namespace DataTransfer.Parquet.Tests;

public class ParquetExtractorTests
{
    [Fact]
    public void ParquetExtractor_Should_Implement_IParquetExtractor()
    {
        var extractor = new ParquetExtractor("/tmp/test");

        Assert.IsAssignableFrom<IParquetExtractor>(extractor);
    }

    [Fact]
    public void ParquetExtractor_Should_Require_BasePath()
    {
        Assert.Throws<ArgumentException>(() => new ParquetExtractor(""));
    }

    [Fact]
    public async Task ExtractFromParquetAsync_Should_Throw_When_FilePath_Empty()
    {
        var extractor = new ParquetExtractor("/tmp/test");
        var outputStream = new MemoryStream();

        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await extractor.ExtractFromParquetAsync("", outputStream));
    }

    [Fact]
    public async Task ExtractFromParquetAsync_Should_Throw_When_OutputStream_Null()
    {
        var extractor = new ParquetExtractor("/tmp/test");

        await Assert.ThrowsAsync<ArgumentNullException>(async () =>
            await extractor.ExtractFromParquetAsync("test.parquet", null!));
    }

    [Fact]
    public async Task ExtractFromParquetAsync_Should_Throw_When_File_NotFound()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        Directory.CreateDirectory(tempDir);

        try
        {
            var extractor = new ParquetExtractor(tempDir);
            var outputStream = new MemoryStream();

            await Assert.ThrowsAsync<FileNotFoundException>(async () =>
                await extractor.ExtractFromParquetAsync("nonexistent.parquet", outputStream));
        }
        finally
        {
            if (Directory.Exists(tempDir))
            {
                Directory.Delete(tempDir, true);
            }
        }
    }

    [Fact]
    public async Task ExtractFromParquetAsync_Should_Extract_Data_From_Parquet_File()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        Directory.CreateDirectory(tempDir);

        try
        {
            // Arrange - First create a Parquet file
            var storage = new ParquetStorage(tempDir);
            var testData = CreateTestJsonStream();
            await storage.WriteAsync(testData, "test.parquet", new DateTime(2024, 3, 15));

            // Act - Extract from the created file
            var extractor = new ParquetExtractor(tempDir);
            var outputStream = new MemoryStream();
            var result = await extractor.ExtractFromParquetAsync(
                "year=2024/month=03/day=15/test.parquet",
                outputStream);

            // Assert
            Assert.True(result.Success);
            Assert.Equal(3, result.RowsExtracted);

            // Verify JSON output
            outputStream.Position = 0;
            var jsonDoc = await JsonDocument.ParseAsync(outputStream);
            Assert.Equal(JsonValueKind.Array, jsonDoc.RootElement.ValueKind);
            Assert.Equal(3, jsonDoc.RootElement.GetArrayLength());
        }
        finally
        {
            if (Directory.Exists(tempDir))
            {
                Directory.Delete(tempDir, true);
            }
        }
    }

    [Fact]
    public async Task ExtractFromParquetAsync_Should_Return_ExtractionResult_With_Timing()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        Directory.CreateDirectory(tempDir);

        try
        {
            // Arrange
            var storage = new ParquetStorage(tempDir);
            var testData = CreateTestJsonStream();
            await storage.WriteAsync(testData, "test.parquet", new DateTime(2024, 3, 15));

            // Act
            var extractor = new ParquetExtractor(tempDir);
            var outputStream = new MemoryStream();
            var result = await extractor.ExtractFromParquetAsync(
                "year=2024/month=03/day=15/test.parquet",
                outputStream);

            // Assert
            Assert.True(result.StartTime > DateTime.MinValue);
            Assert.True(result.EndTime > result.StartTime);
            Assert.True(result.Duration.TotalMilliseconds > 0);
        }
        finally
        {
            if (Directory.Exists(tempDir))
            {
                Directory.Delete(tempDir, true);
            }
        }
    }

    [Fact]
    public async Task ExtractFromParquetAsync_Should_Handle_Empty_Parquet_File()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        Directory.CreateDirectory(tempDir);

        try
        {
            // Arrange - Create empty Parquet file
            var storage = new ParquetStorage(tempDir);
            var emptyData = CreateEmptyJsonStream();
            await storage.WriteAsync(emptyData, "empty.parquet", new DateTime(2024, 3, 15));

            // Act
            var extractor = new ParquetExtractor(tempDir);
            var outputStream = new MemoryStream();
            var result = await extractor.ExtractFromParquetAsync(
                "year=2024/month=03/day=15/empty.parquet",
                outputStream);

            // Assert
            Assert.True(result.Success);
            Assert.Equal(0, result.RowsExtracted);
        }
        finally
        {
            if (Directory.Exists(tempDir))
            {
                Directory.Delete(tempDir, true);
            }
        }
    }

    private static MemoryStream CreateTestJsonStream()
    {
        var json = @"[
            {""Id"": 1, ""Name"": ""Test1"", ""Value"": 10.5},
            {""Id"": 2, ""Name"": ""Test2"", ""Value"": 20.5},
            {""Id"": 3, ""Name"": ""Test3"", ""Value"": 30.5}
        ]";

        var stream = new MemoryStream();
        var writer = new StreamWriter(stream);
        writer.Write(json);
        writer.Flush();
        stream.Position = 0;
        return stream;
    }

    private static MemoryStream CreateEmptyJsonStream()
    {
        var json = "[]";
        var stream = new MemoryStream();
        var writer = new StreamWriter(stream);
        writer.Write(json);
        writer.Flush();
        stream.Position = 0;
        return stream;
    }
}
