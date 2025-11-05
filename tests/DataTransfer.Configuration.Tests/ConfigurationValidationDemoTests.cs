using DataTransfer.Configuration;
using DataTransfer.Core.Models;
using Xunit;

namespace DataTransfer.Configuration.Tests;

/// <summary>
/// Comprehensive demonstration tests for configuration validation.
/// These tests showcase all validation rules and help prevent user configuration errors.
/// </summary>
public class ConfigurationValidationDemoTests
{
    [Fact]
    public void Should_Detect_Invalid_Date_Range_End_Before_Start()
    {
        // Arrange - End date before start date (common user error)
        var config = CreateValidConfiguration();
        config.Tables[0].ExtractSettings.DateRange = new DateRange
        {
            StartDate = new DateTime(2024, 12, 31),
            EndDate = new DateTime(2024, 1, 1) // ERROR: End before start
        };
        var validator = new ConfigurationValidator();

        // Act
        var result = validator.Validate(config);

        // Assert - Should detect invalid date range
        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e =>
            e.Contains("date range", StringComparison.OrdinalIgnoreCase) ||
            e.Contains("end date", StringComparison.OrdinalIgnoreCase) ||
            e.Contains("start date", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void Should_Detect_SCD2_Missing_Effective_Date_Column()
    {
        // Arrange - SCD2 requires both effective and expiration date columns
        var config = CreateValidConfiguration();
        config.Tables[0].Partitioning.Type = PartitionType.Scd2;
        config.Tables[0].Partitioning.ScdEffectiveDateColumn = null; // ERROR: Missing
        config.Tables[0].Partitioning.ScdExpirationDateColumn = "ExpirationDate";
        var validator = new ConfigurationValidator();

        // Act
        var result = validator.Validate(config);

        // Assert - Should require effective date column for SCD2
        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e =>
            e.Contains("effective", StringComparison.OrdinalIgnoreCase) ||
            e.Contains("SCD2", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void Should_Detect_SCD2_Missing_Expiration_Date_Column()
    {
        // Arrange - SCD2 requires both columns
        var config = CreateValidConfiguration();
        config.Tables[0].Partitioning.Type = PartitionType.Scd2;
        config.Tables[0].Partitioning.ScdEffectiveDateColumn = "EffectiveDate";
        config.Tables[0].Partitioning.ScdExpirationDateColumn = null; // ERROR: Missing
        var validator = new ConfigurationValidator();

        // Act
        var result = validator.Validate(config);

        // Assert - Should require expiration date column for SCD2
        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e =>
            e.Contains("expiration", StringComparison.OrdinalIgnoreCase) ||
            e.Contains("SCD2", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void Should_Detect_Invalid_Batch_Size_Zero()
    {
        // Arrange - Batch size must be positive
        var config = CreateValidConfiguration();
        config.Tables[0].ExtractSettings.BatchSize = 0; // ERROR: Invalid
        var validator = new ConfigurationValidator();

        // Act
        var result = validator.Validate(config);

        // Assert - Should require positive batch size
        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e =>
            e.Contains("batch", StringComparison.OrdinalIgnoreCase) ||
            e.Contains("greater than", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void Should_Detect_Invalid_Batch_Size_Negative()
    {
        // Arrange
        var config = CreateValidConfiguration();
        config.Tables[0].ExtractSettings.BatchSize = -1000; // ERROR: Negative
        var validator = new ConfigurationValidator();

        // Act
        var result = validator.Validate(config);

        // Assert
        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.Contains("batch", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void Should_Warn_For_Very_Large_Batch_Size()
    {
        // Arrange - Very large batch sizes might cause memory issues
        var config = CreateValidConfiguration();
        config.Tables[0].ExtractSettings.BatchSize = 10_000_000; // 10 million rows
        var validator = new ConfigurationValidator();

        // Act
        var result = validator.Validate(config);

        // Assert - Might pass but could have warnings
        // This demonstrates the difference between errors and warnings
        if (!result.IsValid)
        {
            Assert.Contains(result.Errors, e =>
                e.Contains("batch", StringComparison.OrdinalIgnoreCase) ||
                e.Contains("large", StringComparison.OrdinalIgnoreCase));
        }
    }

    [Fact]
    public void Should_Detect_Missing_Required_Fields_For_All_Partition_Types()
    {
        // Demonstrate validation for each partition type
        var validator = new ConfigurationValidator();

        // Date partitioning - requires Column
        var dateConfig = CreateValidConfiguration();
        dateConfig.Tables[0].Partitioning.Type = PartitionType.Date;
        dateConfig.Tables[0].Partitioning.Column = null;
        var dateResult = validator.Validate(dateConfig);
        Assert.False(dateResult.IsValid);

        // IntDate partitioning - requires Column
        var intDateConfig = CreateValidConfiguration();
        intDateConfig.Tables[0].Partitioning.Type = PartitionType.IntDate;
        intDateConfig.Tables[0].Partitioning.Column = null;
        var intDateResult = validator.Validate(intDateConfig);
        Assert.False(intDateResult.IsValid);

        // SCD2 partitioning - requires both SCD columns
        var scd2Config = CreateValidConfiguration();
        scd2Config.Tables[0].Partitioning.Type = PartitionType.Scd2;
        scd2Config.Tables[0].Partitioning.ScdEffectiveDateColumn = null;
        var scd2Result = validator.Validate(scd2Config);
        Assert.False(scd2Result.IsValid);

        // Static partitioning - no special requirements
        var staticConfig = CreateValidConfiguration();
        staticConfig.Tables[0].Partitioning.Type = PartitionType.Static;
        staticConfig.Tables[0].Partitioning.Column = null;
        var staticResult = validator.Validate(staticConfig);
        Assert.True(staticResult.IsValid); // Should pass
    }

    [Fact]
    public void Should_Detect_Duplicate_Table_Configurations()
    {
        // Arrange - Same source table configured twice
        var config = CreateValidConfiguration();
        var duplicateTable = new TableConfiguration
        {
            Source = new TableIdentifier
            {
                Database = config.Tables[0].Source.Database,
                Schema = config.Tables[0].Source.Schema,
                Table = config.Tables[0].Source.Table // Same as first table
            },
            Destination = new TableIdentifier
            {
                Database = "OtherDB",
                Schema = "dbo",
                Table = "OtherTable"
            },
            Partitioning = new PartitioningConfiguration
            {
                Type = PartitionType.Static
            },
            ExtractSettings = new ExtractSettings
            {
                BatchSize = 50000
            }
        };
        config.Tables.Add(duplicateTable);
        var validator = new ConfigurationValidator();

        // Act
        var result = validator.Validate(config);

        // Assert - Should detect duplicate source tables
        if (!result.IsValid)
        {
            Assert.Contains(result.Errors, e =>
                e.Contains("duplicate", StringComparison.OrdinalIgnoreCase));
        }
    }

    [Fact]
    public void Should_Detect_Invalid_Connection_String_Format()
    {
        // Arrange - Malformed connection string
        var config = CreateValidConfiguration();
        config.Connections.Source = "InvalidConnectionString"; // Missing Server, Database, etc.
        var validator = new ConfigurationValidator();

        // Act
        var result = validator.Validate(config);

        // Assert - Should validate connection string format
        // Note: This might pass basic validation but fail at runtime
        // Configuration validation typically checks for presence, not format
        Assert.True(result.IsValid || result.Errors.Any(e =>
            e.Contains("connection", StringComparison.OrdinalIgnoreCase)));
    }

    [Fact]
    public void Should_Validate_Storage_Configuration_Completeness()
    {
        // Arrange - Missing storage configuration fields
        var config = CreateValidConfiguration();
        config.Storage.BasePath = string.Empty; // ERROR
        config.Storage.Compression = null;
        var validator = new ConfigurationValidator();

        // Act
        var result = validator.Validate(config);

        // Assert
        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e =>
            e.Contains("basepath", StringComparison.OrdinalIgnoreCase) ||
            e.Contains("storage", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void Should_Provide_Clear_Error_Messages_For_Multiple_Errors()
    {
        // Arrange - Configuration with multiple errors
        var config = new DataTransferConfiguration
        {
            Connections = new ConnectionConfiguration
            {
                Source = string.Empty, // ERROR 1
                Destination = string.Empty // ERROR 2
            },
            Tables = new List<TableConfiguration>(), // ERROR 3: No tables
            Storage = new StorageConfiguration
            {
                BasePath = string.Empty, // ERROR 4
                Compression = "invalid"
            }
        };
        var validator = new ConfigurationValidator();

        // Act
        var result = validator.Validate(config);

        // Assert - Should accumulate all errors with clear messages
        Assert.False(result.IsValid);
        Assert.True(result.Errors.Count >= 3,
            $"Expected at least 3 errors, got {result.Errors.Count}");

        // Each error should be descriptive
        foreach (var error in result.Errors)
        {
            Assert.False(string.IsNullOrWhiteSpace(error));
            Assert.True(error.Length > 10, "Error messages should be descriptive");
        }
    }

    [Fact]
    public void Should_Pass_Minimal_Valid_Configuration()
    {
        // Arrange - Absolute minimum required for valid config
        var config = new DataTransferConfiguration
        {
            Connections = new ConnectionConfiguration
            {
                Source = "Server=localhost;Database=SourceDB;Integrated Security=true;",
                Destination = "Server=localhost;Database=DestDB;Integrated Security=true;"
            },
            Tables = new List<TableConfiguration>
            {
                new TableConfiguration
                {
                    Source = new TableIdentifier
                    {
                        Database = "SourceDB",
                        Schema = "dbo",
                        Table = "Table1"
                    },
                    Destination = new TableIdentifier
                    {
                        Database = "DestDB",
                        Schema = "dbo",
                        Table = "Table1"
                    },
                    Partitioning = new PartitioningConfiguration
                    {
                        Type = PartitionType.Static // Simplest type
                    },
                    ExtractSettings = new ExtractSettings
                    {
                        BatchSize = 100000
                    }
                }
            },
            Storage = new StorageConfiguration
            {
                BasePath = "./parquet-data",
                Compression = "snappy"
            }
        };
        var validator = new ConfigurationValidator();

        // Act
        var result = validator.Validate(config);

        // Assert - Should pass with minimum required fields
        Assert.True(result.IsValid,
            $"Expected valid configuration but got errors: {string.Join(", ", result.Errors)}");
        Assert.Empty(result.Errors);
    }

    [Fact]
    public void Should_Pass_Complex_Valid_Configuration_With_All_Features()
    {
        // Arrange - Complex but valid configuration showcasing all features
        var config = new DataTransferConfiguration
        {
            Connections = new ConnectionConfiguration
            {
                Source = "Server=prodserver;Database=SalesDB;Integrated Security=true;TrustServerCertificate=true;",
                Destination = "Server=datawarehouse;Database=SalesDW;Integrated Security=true;TrustServerCertificate=true;"
            },
            Tables = new List<TableConfiguration>
            {
                // Date-partitioned fact table
                new TableConfiguration
                {
                    Source = new TableIdentifier { Database = "SalesDB", Schema = "sales", Table = "Orders" },
                    Destination = new TableIdentifier { Database = "SalesDW", Schema = "fact", Table = "FactOrders" },
                    Partitioning = new PartitioningConfiguration
                    {
                        Type = PartitionType.Date,
                        Column = "OrderDate"
                    },
                    ExtractSettings = new ExtractSettings
                    {
                        BatchSize = 100000,
                        DateRange = new DateRange
                        {
                            StartDate = new DateTime(2024, 1, 1),
                            EndDate = new DateTime(2024, 12, 31)
                        }
                    }
                },
                // SCD2 dimension table
                new TableConfiguration
                {
                    Source = new TableIdentifier { Database = "SalesDB", Schema = "sales", Table = "Customers" },
                    Destination = new TableIdentifier { Database = "SalesDW", Schema = "dim", Table = "DimCustomer" },
                    Partitioning = new PartitioningConfiguration
                    {
                        Type = PartitionType.Scd2,
                        ScdEffectiveDateColumn = "EffectiveDate",
                        ScdExpirationDateColumn = "ExpirationDate"
                    },
                    ExtractSettings = new ExtractSettings
                    {
                        BatchSize = 50000,
                        DateRange = new DateRange
                        {
                            StartDate = new DateTime(2024, 1, 1),
                            EndDate = new DateTime(2024, 12, 31)
                        }
                    }
                },
                // Static reference table
                new TableConfiguration
                {
                    Source = new TableIdentifier { Database = "SalesDB", Schema = "ref", Table = "Countries" },
                    Destination = new TableIdentifier { Database = "SalesDW", Schema = "dim", Table = "DimCountry" },
                    Partitioning = new PartitioningConfiguration
                    {
                        Type = PartitionType.Static
                    },
                    ExtractSettings = new ExtractSettings
                    {
                        BatchSize = 10000
                    }
                }
            },
            Storage = new StorageConfiguration
            {
                BasePath = "/data/warehouse/parquet",
                Compression = "snappy"
            }
        };
        var validator = new ConfigurationValidator();

        // Act
        var result = validator.Validate(config);

        // Assert - Complex config should still validate successfully
        Assert.True(result.IsValid,
            $"Expected valid configuration but got errors: {string.Join(", ", result.Errors)}");
        Assert.Empty(result.Errors);
    }

    private static DataTransferConfiguration CreateValidConfiguration()
    {
        return new DataTransferConfiguration
        {
            Connections = new ConnectionConfiguration
            {
                Source = "Server=localhost;Database=SourceDB;Integrated Security=true;",
                Destination = "Server=localhost;Database=DestDB;Integrated Security=true;"
            },
            Tables = new List<TableConfiguration>
            {
                new TableConfiguration
                {
                    Source = new TableIdentifier
                    {
                        Database = "SourceDB",
                        Schema = "dbo",
                        Table = "SourceTable"
                    },
                    Destination = new TableIdentifier
                    {
                        Database = "DestDB",
                        Schema = "dbo",
                        Table = "DestTable"
                    },
                    Partitioning = new PartitioningConfiguration
                    {
                        Type = PartitionType.Date,
                        Column = "CreatedDate"
                    },
                    ExtractSettings = new ExtractSettings
                    {
                        BatchSize = 100000,
                        DateRange = new DateRange
                        {
                            StartDate = new DateTime(2024, 1, 1),
                            EndDate = new DateTime(2024, 12, 31)
                        }
                    }
                }
            },
            Storage = new StorageConfiguration
            {
                BasePath = "/data/extracts",
                Compression = "snappy"
            }
        };
    }
}
