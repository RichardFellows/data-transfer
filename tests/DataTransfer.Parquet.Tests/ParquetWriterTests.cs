using System.Text.Json;
using DataTransfer.Core.Interfaces;
using DataTransfer.Parquet;
using Xunit;

namespace DataTransfer.Parquet.Tests;

public class ParquetWriterTests
{
    [Fact]
    public void ParquetWriter_Should_Implement_IParquetWriter()
    {
        var storage = new ParquetStorage("/tmp/test");
        var writer = new ParquetWriter(storage);

        Assert.IsAssignableFrom<IParquetWriter>(writer);
    }

    [Fact]
    public void ParquetWriter_Should_Require_Storage()
    {
        Assert.Throws<ArgumentNullException>(() => new ParquetWriter(null!));
    }

    [Fact]
    public async Task WriteToParquetAsync_Should_Write_Data_And_Return_RowCount()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        Directory.CreateDirectory(tempDir);

        try
        {
            // Arrange
            var storage = new ParquetStorage(tempDir);
            var writer = new ParquetWriter(storage);
            var testData = CreateTestJsonStream();

            // Act
            var rowCount = await writer.WriteToParquetAsync(
                testData,
                "test.parquet",
                new DateTime(2024, 3, 15));

            // Assert
            Assert.Equal(3, rowCount);

            // Verify file was created
            var expectedFile = Path.Combine(tempDir, "year=2024", "month=03", "day=15", "test.parquet");
            Assert.True(File.Exists(expectedFile));
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
    public async Task WriteToParquetAsync_Should_Handle_Null_PartitionDate()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        Directory.CreateDirectory(tempDir);

        try
        {
            // Arrange
            var storage = new ParquetStorage(tempDir);
            var writer = new ParquetWriter(storage);
            var testData = CreateTestJsonStream();

            // Act
            var rowCount = await writer.WriteToParquetAsync(
                testData,
                "test.parquet",
                null); // No partition date - should use current date

            // Assert
            Assert.Equal(3, rowCount);

            // Verify a file was created (with today's partition)
            var now = DateTime.UtcNow;
            var expectedPath = Path.Combine(tempDir, $"year={now.Year:D4}", $"month={now.Month:D2}", $"day={now.Day:D2}");
            Assert.True(Directory.Exists(expectedPath));
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
    public async Task WriteToParquetAsync_Should_Handle_Empty_Data()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        Directory.CreateDirectory(tempDir);

        try
        {
            // Arrange
            var storage = new ParquetStorage(tempDir);
            var writer = new ParquetWriter(storage);
            var emptyData = CreateEmptyJsonStream();

            // Act
            var rowCount = await writer.WriteToParquetAsync(
                emptyData,
                "empty.parquet",
                new DateTime(2024, 3, 15));

            // Assert
            Assert.Equal(0, rowCount);
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
    public async Task WriteToParquetAsync_Should_Throw_When_Stream_Null()
    {
        var storage = new ParquetStorage("/tmp/test");
        var writer = new ParquetWriter(storage);

        await Assert.ThrowsAsync<ArgumentNullException>(async () =>
            await writer.WriteToParquetAsync(null!, "test.parquet"));
    }

    [Fact]
    public async Task WriteToParquetAsync_Should_Throw_When_OutputPath_Empty()
    {
        var storage = new ParquetStorage("/tmp/test");
        var writer = new ParquetWriter(storage);
        var stream = new MemoryStream();

        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await writer.WriteToParquetAsync(stream, ""));
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
