using DataTransfer.Web.Services;
using DataTransfer.Web.Models;
using Xunit;
using Microsoft.Extensions.Logging.Abstractions;
using Parquet;
using Parquet.Data;
using Parquet.Schema;

namespace DataTransfer.Web.Tests.Services;

/// <summary>
/// TDD tests for ParquetFileService preview functionality
/// Tests Parquet file schema reading and sample data extraction
/// </summary>
public class ParquetFileServicePreviewTests
{
    private readonly ParquetFileService _service;
    private const string TestParquetDirectory = "./test-parquet-files";

    public ParquetFileServicePreviewTests()
    {
        _service = new ParquetFileService(
            NullLogger<ParquetFileService>.Instance,
            TestParquetDirectory);

        // Ensure test directory exists
        Directory.CreateDirectory(TestParquetDirectory);
    }

    [Fact]
    public async Task GetParquetPreviewAsync_Should_Return_Schema_Information()
    {
        // Arrange
        var testFile = await CreateTestParquetFileAsync("schema_test.parquet");

        // Act
        var preview = await _service.GetParquetPreviewAsync("schema_test.parquet");

        // Assert
        Assert.NotNull(preview);
        Assert.NotEmpty(preview.Columns);

        // Our test file has: Id (INT), Name (STRING), Amount (DECIMAL)
        Assert.Equal(3, preview.Columns.Count);

        var idColumn = preview.Columns.FirstOrDefault(c => c.Name == "Id");
        Assert.NotNull(idColumn);
        Assert.Equal("INT", idColumn.DataType);

        var nameColumn = preview.Columns.FirstOrDefault(c => c.Name == "Name");
        Assert.NotNull(nameColumn);
        Assert.Equal("STRING", nameColumn.DataType);

        var amountColumn = preview.Columns.FirstOrDefault(c => c.Name == "Amount");
        Assert.NotNull(amountColumn);
        Assert.Equal("DECIMAL", amountColumn.DataType);
    }

    [Fact]
    public async Task GetParquetPreviewAsync_Should_Return_Sample_Rows()
    {
        // Arrange
        await CreateTestParquetFileAsync("sample_test.parquet");

        // Act
        var preview = await _service.GetParquetPreviewAsync("sample_test.parquet");

        // Assert
        Assert.NotEmpty(preview.Rows);
        Assert.Equal(5, preview.Rows.Count); // Test file has 5 rows

        // Check first row
        var firstRow = preview.Rows.First();
        Assert.True(firstRow.ContainsKey("Id"));
        Assert.True(firstRow.ContainsKey("Name"));
        Assert.True(firstRow.ContainsKey("Amount"));
    }

    [Fact]
    public async Task GetParquetPreviewAsync_Should_Limit_To_10_Rows()
    {
        // Arrange
        await CreateTestParquetFileWithManyRowsAsync("large_test.parquet", 25);

        // Act
        var preview = await _service.GetParquetPreviewAsync("large_test.parquet");

        // Assert
        Assert.Equal(10, preview.Rows.Count); // Should be limited to 10
    }

    [Fact]
    public async Task GetParquetPreviewAsync_Should_Return_Total_Row_Count()
    {
        // Arrange
        await CreateTestParquetFileWithManyRowsAsync("count_test.parquet", 15);

        // Act
        var preview = await _service.GetParquetPreviewAsync("count_test.parquet");

        // Assert
        Assert.NotNull(preview.TotalRowCount);
        Assert.Equal(15, preview.TotalRowCount.Value);
    }

    [Fact]
    public async Task GetParquetPreviewAsync_Should_Handle_Empty_File()
    {
        // Arrange
        await CreateTestParquetFileWithManyRowsAsync("empty_test.parquet", 0);

        // Act
        var preview = await _service.GetParquetPreviewAsync("empty_test.parquet");

        // Assert
        Assert.NotNull(preview);
        Assert.NotEmpty(preview.Columns); // Schema should still be present
        Assert.Empty(preview.Rows);
        Assert.Equal(0, preview.TotalRowCount.Value);
    }

    [Fact]
    public async Task GetParquetPreviewAsync_Should_Handle_NonExistent_File()
    {
        // Act
        var preview = await _service.GetParquetPreviewAsync("nonexistent.parquet");

        // Assert
        Assert.NotNull(preview);
        Assert.Empty(preview.Columns);
        Assert.Empty(preview.Rows);
    }

    #region Helper Methods

    private async Task<string> CreateTestParquetFileAsync(string fileName)
    {
        return await CreateTestParquetFileWithManyRowsAsync(fileName, 5);
    }

    private async Task<string> CreateTestParquetFileWithManyRowsAsync(string fileName, int rowCount)
    {
        var fullPath = Path.Combine(TestParquetDirectory, fileName);

        // Create sample data arrays
        var ids = new int[rowCount];
        var names = new string[rowCount];
        var amounts = new decimal[rowCount];

        for (int i = 0; i < rowCount; i++)
        {
            ids[i] = i + 1;
            names[i] = $"Item {i + 1}";
            amounts[i] = (i + 1) * 10.50m;
        }

        // Create schema
        var schema = new ParquetSchema(
            new DataField<int>("Id"),
            new DataField<string>("Name"),
            new DataField<decimal>("Amount")
        );

        // Write Parquet file
        await using var fileStream = File.Create(fullPath);
        using var writer = await ParquetWriter.CreateAsync(schema, fileStream);
        using var groupWriter = writer.CreateRowGroup();

        await groupWriter.WriteColumnAsync(new DataColumn(
            schema.GetDataFields()[0],
            ids));

        await groupWriter.WriteColumnAsync(new DataColumn(
            schema.GetDataFields()[1],
            names));

        await groupWriter.WriteColumnAsync(new DataColumn(
            schema.GetDataFields()[2],
            amounts));

        return fullPath;
    }

    private class TestDataRow
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public decimal Amount { get; set; }
    }

    #endregion
}
