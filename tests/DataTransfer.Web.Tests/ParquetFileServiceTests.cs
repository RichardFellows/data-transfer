using Microsoft.Extensions.Logging.Abstractions;
using DataTransfer.Web.Services;
using Xunit;

namespace DataTransfer.Web.Tests;

/// <summary>
/// Unit tests for ParquetFileService
/// </summary>
public class ParquetFileServiceTests : IDisposable
{
    private readonly string _testDirectory;
    private readonly ParquetFileService _service;

    public ParquetFileServiceTests()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), $"parquet_test_{Guid.NewGuid()}");
        Directory.CreateDirectory(_testDirectory);
        _service = new ParquetFileService(NullLogger<ParquetFileService>.Instance, _testDirectory);
    }

    public void Dispose()
    {
        if (Directory.Exists(_testDirectory))
        {
            Directory.Delete(_testDirectory, true);
        }
    }

    [Fact]
    public void GetAvailableParquetFiles_Should_Return_Empty_List_When_Directory_Empty()
    {
        // Act
        var files = _service.GetAvailableParquetFiles();

        // Assert
        Assert.Empty(files);
    }

    [Fact]
    public void GetAvailableParquetFiles_Should_Return_Parquet_Files()
    {
        // Arrange
        var file1 = Path.Combine(_testDirectory, "test1.parquet");
        var file2 = Path.Combine(_testDirectory, "test2.parquet");
        File.WriteAllText(file1, "dummy data");
        File.WriteAllText(file2, "dummy data");

        // Act
        var files = _service.GetAvailableParquetFiles();

        // Assert
        Assert.Equal(2, files.Count);
        Assert.Contains(files, f => f.FileName == "test1.parquet");
        Assert.Contains(files, f => f.FileName == "test2.parquet");
    }

    [Fact]
    public void GetAvailableParquetFiles_Should_Return_Files_From_Subdirectories()
    {
        // Arrange
        var subDir = Path.Combine(_testDirectory, "year=2024", "month=01");
        Directory.CreateDirectory(subDir);
        var file1 = Path.Combine(_testDirectory, "root.parquet");
        var file2 = Path.Combine(subDir, "partitioned.parquet");
        File.WriteAllText(file1, "dummy data");
        File.WriteAllText(file2, "dummy data");

        // Act
        var files = _service.GetAvailableParquetFiles();

        // Assert
        Assert.Equal(2, files.Count);
        Assert.Contains(files, f => f.RelativePath == "root.parquet");
        Assert.Contains(files, f => f.RelativePath.Contains("year=2024"));
    }

    [Fact]
    public void GetAvailableParquetFiles_Should_Exclude_Non_Parquet_Files()
    {
        // Arrange
        var parquetFile = Path.Combine(_testDirectory, "data.parquet");
        var txtFile = Path.Combine(_testDirectory, "readme.txt");
        var csvFile = Path.Combine(_testDirectory, "data.csv");
        File.WriteAllText(parquetFile, "dummy data");
        File.WriteAllText(txtFile, "readme");
        File.WriteAllText(csvFile, "csv data");

        // Act
        var files = _service.GetAvailableParquetFiles();

        // Assert
        Assert.Single(files);
        Assert.Equal("data.parquet", files[0].FileName);
    }

    [Fact]
    public void GetAvailableParquetFiles_Should_Include_File_Metadata()
    {
        // Arrange
        var file = Path.Combine(_testDirectory, "test.parquet");
        File.WriteAllText(file, "dummy data with some length");

        // Act
        var files = _service.GetAvailableParquetFiles();

        // Assert
        Assert.Single(files);
        var fileInfo = files[0];
        Assert.Equal("test.parquet", fileInfo.FileName);
        Assert.True(fileInfo.SizeBytes > 0);
        Assert.True(fileInfo.LastModified > DateTime.MinValue);
        Assert.Contains("test.parquet", fileInfo.DisplayName);
    }

    [Fact]
    public void GetFullPath_Should_Return_Absolute_Path()
    {
        // Act
        var fullPath = _service.GetFullPath("test.parquet");

        // Assert
        Assert.Equal(Path.Combine(_testDirectory, "test.parquet"), fullPath);
    }

    [Fact]
    public void GetFullPath_Should_Handle_Relative_Paths_With_Subdirectories()
    {
        // Act
        var fullPath = _service.GetFullPath("year=2024/month=01/data.parquet");

        // Assert
        Assert.Equal(Path.Combine(_testDirectory, "year=2024", "month=01", "data.parquet"), fullPath);
    }
}
