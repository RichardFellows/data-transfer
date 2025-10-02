using DataTransfer.Core.Models;

namespace DataTransfer.Core.Tests.Models;

public class TransferConfigurationTests
{
    [Fact]
    public void TransferConfiguration_Should_HaveTransferTypeProperty()
    {
        // Arrange
        var config = new TransferConfiguration();

        // Act & Assert
        Assert.NotNull(config);
        var property = typeof(TransferConfiguration).GetProperty("TransferType");
        Assert.NotNull(property);
        Assert.Equal(typeof(TransferType), property.PropertyType);
    }

    [Fact]
    public void TransferConfiguration_Should_HaveSourceProperty()
    {
        // Arrange
        var config = new TransferConfiguration();

        // Act & Assert
        var property = typeof(TransferConfiguration).GetProperty("Source");
        Assert.NotNull(property);
        Assert.Equal(typeof(SourceConfiguration), property.PropertyType);
        Assert.NotNull(config.Source);
    }

    [Fact]
    public void TransferConfiguration_Should_HaveDestinationProperty()
    {
        // Arrange
        var config = new TransferConfiguration();

        // Act & Assert
        var property = typeof(TransferConfiguration).GetProperty("Destination");
        Assert.NotNull(property);
        Assert.Equal(typeof(DestinationConfiguration), property.PropertyType);
        Assert.NotNull(config.Destination);
    }

    [Fact]
    public void TransferConfiguration_Should_HavePartitioningProperty()
    {
        // Arrange
        var config = new TransferConfiguration();

        // Act & Assert
        var property = typeof(TransferConfiguration).GetProperty("Partitioning");
        Assert.NotNull(property);
        Assert.Equal(typeof(PartitioningConfiguration), property.PropertyType);
    }
}

public class SourceConfigurationTests
{
    [Fact]
    public void SourceConfiguration_Should_HaveTypeProperty()
    {
        // Arrange
        var config = new SourceConfiguration();

        // Act & Assert
        var property = typeof(SourceConfiguration).GetProperty("Type");
        Assert.NotNull(property);
        Assert.Equal(typeof(SourceType), property.PropertyType);
    }

    [Fact]
    public void SourceConfiguration_Should_HaveSqlServerProperties()
    {
        // Arrange
        var config = new SourceConfiguration();

        // Act & Assert
        Assert.NotNull(typeof(SourceConfiguration).GetProperty("ConnectionString"));
        Assert.NotNull(typeof(SourceConfiguration).GetProperty("Table"));
    }

    [Fact]
    public void SourceConfiguration_Should_HaveParquetPathProperty()
    {
        // Arrange
        var config = new SourceConfiguration();

        // Act & Assert
        var property = typeof(SourceConfiguration).GetProperty("ParquetPath");
        Assert.NotNull(property);
        Assert.Equal(typeof(string), property.PropertyType);
    }
}

public class DestinationConfigurationTests
{
    [Fact]
    public void DestinationConfiguration_Should_HaveTypeProperty()
    {
        // Arrange
        var config = new DestinationConfiguration();

        // Act & Assert
        var property = typeof(DestinationConfiguration).GetProperty("Type");
        Assert.NotNull(property);
        Assert.Equal(typeof(DestinationType), property.PropertyType);
    }

    [Fact]
    public void DestinationConfiguration_Should_HaveSqlServerProperties()
    {
        // Arrange
        var config = new DestinationConfiguration();

        // Act & Assert
        Assert.NotNull(typeof(DestinationConfiguration).GetProperty("ConnectionString"));
        Assert.NotNull(typeof(DestinationConfiguration).GetProperty("Table"));
    }

    [Fact]
    public void DestinationConfiguration_Should_HaveParquetProperties()
    {
        // Arrange
        var config = new DestinationConfiguration();

        // Act & Assert
        Assert.NotNull(typeof(DestinationConfiguration).GetProperty("ParquetPath"));
        Assert.NotNull(typeof(DestinationConfiguration).GetProperty("Compression"));
    }

    [Fact]
    public void DestinationConfiguration_Compression_Should_DefaultToSnappy()
    {
        // Arrange & Act
        var config = new DestinationConfiguration();

        // Assert
        Assert.Equal("Snappy", config.Compression);
    }
}
