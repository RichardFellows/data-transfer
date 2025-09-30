using DataTransfer.Core.Interfaces;
using DataTransfer.Parquet;
using Xunit;

namespace DataTransfer.Parquet.Tests;

public class ParquetStorageTests
{
    [Fact]
    public void ParquetStorage_Should_Implement_IParquetStorage()
    {
        var storage = new ParquetStorage("/tmp/test");

        Assert.IsAssignableFrom<IParquetStorage>(storage);
    }

    [Fact]
    public void ParquetStorage_Should_Require_BasePath()
    {
        Assert.Throws<ArgumentException>(() => new ParquetStorage(""));
    }

    [Fact]
    public async Task WriteAsync_Should_Throw_When_Stream_Null()
    {
        var storage = new ParquetStorage("/tmp/test");

        await Assert.ThrowsAsync<ArgumentNullException>(async () =>
            await storage.WriteAsync(null!, "file.parquet", DateTime.Now));
    }

    [Fact]
    public async Task WriteAsync_Should_Throw_When_FilePath_Empty()
    {
        var storage = new ParquetStorage("/tmp/test");
        var stream = new MemoryStream();

        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await storage.WriteAsync(stream, "", DateTime.Now));
    }

    [Fact]
    public async Task WriteAsync_Should_Create_Directory_If_Not_Exists()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        var storage = new ParquetStorage(tempDir);
        var stream = CreateTestJsonStream();

        try
        {
            await storage.WriteAsync(stream, "test.parquet", new DateTime(2024, 3, 15));

            Assert.True(Directory.Exists(tempDir));
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
    public async Task WriteAsync_Should_Create_Partition_Subdirectories()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        var storage = new ParquetStorage(tempDir);
        var stream = CreateTestJsonStream();

        try
        {
            await storage.WriteAsync(stream, "test.parquet", new DateTime(2024, 3, 15));

            var expectedPath = Path.Combine(tempDir, "year=2024", "month=03", "day=15");
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
    public async Task WriteAsync_Should_Create_Parquet_File()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        var storage = new ParquetStorage(tempDir);
        var stream = CreateTestJsonStream();

        try
        {
            await storage.WriteAsync(stream, "test.parquet", new DateTime(2024, 3, 15));

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
    public async Task ReadAsync_Should_Throw_When_FilePath_Empty()
    {
        var storage = new ParquetStorage("/tmp/test");

        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await storage.ReadAsync(""));
    }

    [Fact]
    public async Task ReadAsync_Should_Throw_When_File_Not_Found()
    {
        var storage = new ParquetStorage("/tmp/test");

        await Assert.ThrowsAsync<FileNotFoundException>(async () =>
            await storage.ReadAsync("nonexistent.parquet"));
    }

    [Fact]
    public async Task WriteAsync_And_ReadAsync_Should_RoundTrip_Data()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        var storage = new ParquetStorage(tempDir);
        var originalStream = CreateTestJsonStream();

        try
        {
            // Write
            await storage.WriteAsync(originalStream, "test.parquet", new DateTime(2024, 3, 15));

            // Read
            var filePath = "year=2024/month=03/day=15/test.parquet";
            var readStream = await storage.ReadAsync(filePath);

            Assert.NotNull(readStream);
            Assert.True(readStream.Length > 0);
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
    public async Task WriteAsync_Should_Handle_CancellationToken()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        var storage = new ParquetStorage(tempDir);
        var stream = CreateTestJsonStream();
        var cts = new CancellationTokenSource();
        cts.Cancel();

        try
        {
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
                await storage.WriteAsync(stream, "test.parquet", DateTime.Now, cts.Token));
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
            {""Id"": 1, ""Name"": ""Test1"", ""Value"": 100.5},
            {""Id"": 2, ""Name"": ""Test2"", ""Value"": 200.75}
        ]";
        var stream = new MemoryStream();
        var writer = new StreamWriter(stream);
        writer.Write(json);
        writer.Flush();
        stream.Position = 0;
        return stream;
    }
}
