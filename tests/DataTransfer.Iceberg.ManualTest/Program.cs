using DataTransfer.Core.Models.Iceberg;
using DataTransfer.Iceberg.Catalog;
using DataTransfer.Iceberg.Integration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Console;

namespace DataTransfer.Iceberg.ManualTest;

/// <summary>
/// Manual test program for creating Iceberg tables for validation
/// Usage: dotnet run --project tests/DataTransfer.Iceberg.ManualTest /tmp/test-warehouse test_table
/// </summary>
class Program
{
    static async Task<int> Main(string[] args)
    {
        if (args.Length != 2)
        {
            Console.WriteLine("Usage: dotnet run --project tests/DataTransfer.Iceberg.ManualTest <warehouse_path> <table_name>");
            Console.WriteLine("Example: dotnet run --project tests/DataTransfer.Iceberg.ManualTest /tmp/test-warehouse test_table");
            return 1;
        }

        var warehousePath = args[0];
        var tableName = args[1];

        Console.WriteLine("========================================");
        Console.WriteLine("Creating Test Iceberg Table");
        Console.WriteLine("========================================");
        Console.WriteLine($"Warehouse: {warehousePath}");
        Console.WriteLine($"Table: {tableName}");
        Console.WriteLine("========================================");
        Console.WriteLine();

        // Create logger
        using var loggerFactory = LoggerFactory.Create(builder =>
        {
            builder.AddConsole();
            builder.SetMinimumLevel(LogLevel.Information);
        });

        var catalogLogger = loggerFactory.CreateLogger<FilesystemCatalog>();
        var writerLogger = loggerFactory.CreateLogger<IcebergTableWriter>();

        // Create catalog
        var catalog = new FilesystemCatalog(warehousePath, catalogLogger);

        // Define schema with various data types
        var schema = new IcebergSchema
        {
            SchemaId = 0,
            Type = "struct",
            Fields = new List<IcebergField>
            {
                new IcebergField { Id = 1, Name = "id", Required = true, Type = "int" },
                new IcebergField { Id = 2, Name = "name", Required = false, Type = "string" },
                new IcebergField { Id = 3, Name = "amount", Required = true, Type = "double" },
                new IcebergField { Id = 4, Name = "is_active", Required = false, Type = "boolean" },
                new IcebergField { Id = 5, Name = "created_at", Required = true, Type = "timestamp" },
                new IcebergField { Id = 6, Name = "count", Required = false, Type = "long" }
            }
        };

        // Create test data
        var data = new List<Dictionary<string, object>>
        {
            new() {
                ["id"] = 1,
                ["name"] = "Alice",
                ["amount"] = 100.50,
                ["is_active"] = true,
                ["created_at"] = new DateTime(2024, 1, 15, 10, 30, 0, DateTimeKind.Utc),
                ["count"] = 1000L
            },
            new() {
                ["id"] = 2,
                ["name"] = "Bob",
                ["amount"] = 250.75,
                ["is_active"] = true,
                ["created_at"] = new DateTime(2024, 2, 20, 14, 45, 0, DateTimeKind.Utc),
                ["count"] = 2500L
            },
            new() {
                ["id"] = 3,
                ["name"] = "Charlie",
                ["amount"] = 75.25,
                ["is_active"] = false,
                ["created_at"] = new DateTime(2024, 3, 10, 9, 15, 0, DateTimeKind.Utc),
                ["count"] = 500L
            },
            new() {
                ["id"] = 4,
                ["name"] = "Diana",
                ["amount"] = 500.00,
                ["is_active"] = true,
                ["created_at"] = new DateTime(2024, 4, 5, 16, 20, 0, DateTimeKind.Utc),
                ["count"] = 5000L
            },
            new() {
                ["id"] = 5,
                ["name"] = "Eve",
                ["amount"] = 150.00,
                ["is_active"] = true,
                ["created_at"] = new DateTime(2024, 5, 12, 11, 0, 0, DateTimeKind.Utc),
                ["count"] = 1500L
            }
        };

        // Write table
        var writer = new IcebergTableWriter(catalog, writerLogger);
        var result = await writer.WriteTableAsync(tableName, schema, data);

        Console.WriteLine();
        Console.WriteLine("========================================");
        Console.WriteLine("Result");
        Console.WriteLine("========================================");

        if (result.Success)
        {
            Console.WriteLine($"✓ Table created successfully!");
            Console.WriteLine($"  Table path: {result.TablePath}");
            Console.WriteLine($"  Snapshot ID: {result.SnapshotId}");
            Console.WriteLine($"  Records written: {result.RecordCount}");
            Console.WriteLine($"  Data files: {result.DataFileCount}");
            Console.WriteLine();
            Console.WriteLine("Validation:");
            Console.WriteLine($"  Metadata: {result.TablePath}/metadata/v1.metadata.json");
            Console.WriteLine($"  Data files: {result.TablePath}/data/");
            Console.WriteLine();
            Console.WriteLine("To validate this table, run:");
            Console.WriteLine($"  ./scripts/validate-iceberg-table.sh {warehousePath} {tableName}");
            Console.WriteLine();
            Console.WriteLine("Or with PyIceberg:");
            Console.WriteLine($"  python3 scripts/validate-with-pyiceberg.py {warehousePath} {tableName}");
            return 0;
        }
        else
        {
            Console.WriteLine($"✗ Table creation failed: {result.ErrorMessage}");
            return 1;
        }
    }
}
