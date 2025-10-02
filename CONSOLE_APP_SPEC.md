# Console Application Implementation Specification

## Overview
Complete the `src/DataTransfer.Console/` project to provide a CLI entry point for the data transfer solution.

## Project Setup

### 1. Update DataTransfer.Console.csproj

**Add these package references:**
```xml
<PackageReference Include="Microsoft.Extensions.Hosting" Version="8.0.1" />
<PackageReference Include="Microsoft.Extensions.Configuration" Version="8.0.0" />
<PackageReference Include="Microsoft.Extensions.Configuration.Json" Version="8.0.1" />
<PackageReference Include="Microsoft.Extensions.DependencyInjection" Version="8.0.1" />
<PackageReference Include="Serilog" Version="4.3.0" />
<PackageReference Include="Serilog.Extensions.Logging" Version="9.0.2" />
<PackageReference Include="Serilog.Sinks.Console" Version="6.0.0" />
<PackageReference Include="Serilog.Sinks.File" Version="6.0.0" />
```

**Add project references:**
```xml
<ProjectReference Include="..\DataTransfer.Core\DataTransfer.Core.csproj" />
<ProjectReference Include="..\DataTransfer.Configuration\DataTransfer.Configuration.csproj" />
<ProjectReference Include="..\DataTransfer.SqlServer\DataTransfer.SqlServer.csproj" />
<ProjectReference Include="..\DataTransfer.Parquet\DataTransfer.Parquet.csproj" />
<ProjectReference Include="..\DataTransfer.Pipeline\DataTransfer.Pipeline.csproj" />
```

## Program.cs Implementation

### Required Using Statements
```csharp
using DataTransfer.Configuration;
using DataTransfer.Core.Interfaces;
using DataTransfer.Parquet;
using DataTransfer.Pipeline;
using DataTransfer.SqlServer;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Serilog;
```

### Key Implementation Steps

#### 1. Serilog Configuration
```csharp
Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .WriteTo.Console()
    .WriteTo.File("logs/datatransfer-.txt", rollingInterval: RollingInterval.Day)
    .CreateLogger();
```

#### 2. Dependency Injection Setup
```csharp
builder.ConfigureServices((context, services) =>
{
    services.AddLogging(loggingBuilder =>
        loggingBuilder.AddSerilog(dispose: true));

    // Register implementations
    services.AddSingleton<ITableExtractor, SqlTableExtractor>();
    services.AddSingleton<IParquetStorage>(sp =>
        new ParquetStorage("parquet-output")); // Base path for Parquet files
    services.AddSingleton<IDataLoader, SqlDataLoader>();
    services.AddSingleton<DataTransferOrchestrator>();
    services.AddSingleton<ConfigurationLoader>();
    services.AddSingleton<ConfigurationValidator>();
});
```

#### 3. Configuration Loading & Validation
```csharp
// Load configuration
var config = await configLoader.LoadAsync("config/appsettings.json");

// Validate
var validationResult = validator.Validate(config);
if (!validationResult.IsValid)
{
    logger.LogError("Configuration validation failed:");
    foreach (var error in validationResult.Errors)
    {
        logger.LogError("  - {Error}", error);
    }
    return 1; // Exit code for failure
}
```

#### 4. Process All Tables
```csharp
int successCount = 0;
int failureCount = 0;
long totalRowsExtracted = 0;
long totalRowsLoaded = 0;

foreach (var tableConfig in config.Tables)
{
    try
    {
        logger.LogInformation("Starting transfer for {Table}",
            tableConfig.Source.FullyQualifiedName);

        var result = await orchestrator.TransferTableAsync(
            tableConfig,
            config.SourceConnection.ConnectionString,
            config.DestinationConnection.ConnectionString);

        if (result.Success)
        {
            successCount++;
            totalRowsExtracted += result.RowsExtracted;
            totalRowsLoaded += result.RowsLoaded;
            logger.LogInformation("✓ Completed {Table}: {Rows} rows in {Duration}ms",
                tableConfig.Source.FullyQualifiedName,
                result.RowsLoaded,
                result.Duration.TotalMilliseconds);
        }
        else
        {
            failureCount++;
            logger.LogError("✗ Failed {Table}: {Error}",
                tableConfig.Source.FullyQualifiedName,
                result.ErrorMessage);
        }
    }
    catch (Exception ex)
    {
        failureCount++;
        logger.LogError(ex, "✗ Exception processing {Table}: {Message}",
            tableConfig.Source.FullyQualifiedName,
            ex.Message);
    }
}
```

#### 5. Display Summary
```csharp
logger.LogInformation("=====================================");
logger.LogInformation("Transfer Summary:");
logger.LogInformation("  Success: {Success}", successCount);
logger.LogInformation("  Failed: {Failed}", failureCount);
logger.LogInformation("  Total Rows Extracted: {Extracted}", totalRowsExtracted);
logger.LogInformation("  Total Rows Loaded: {Loaded}", totalRowsLoaded);
logger.LogInformation("=====================================");

return failureCount == 0 ? 0 : 1;
```

#### 6. Cleanup
```csharp
finally
{
    Log.CloseAndFlush(); // Ensure all logs are written
}
```

## Error Handling Strategy

1. **Configuration Errors:** Exit immediately with code 1
2. **Individual Table Failures:** Log error, continue to next table
3. **Fatal Errors:** Catch at top level, log critical, exit with code 1
4. **Return Codes:**
   - `0` = All tables transferred successfully
   - `1` = One or more tables failed OR configuration invalid

## Expected Console Output Example

```
[2025-10-01 10:30:00 INF] DataTransfer Console Application Starting
[2025-10-01 10:30:00 INF] Configuration loaded and validated successfully
[2025-10-01 10:30:00 INF] Processing 2 tables
[2025-10-01 10:30:00 INF] Starting transfer for SourceDB.dbo.Orders
[2025-10-01 10:30:01 INF] Extracting data from source table SourceDB.dbo.Orders
[2025-10-01 10:30:02 INF] Extracted 5000 rows from SourceDB.dbo.Orders
[2025-10-01 10:30:02 INF] Writing data to Parquet file dbo_Orders_20251001103002.parquet
[2025-10-01 10:30:03 INF] Successfully wrote data to Parquet file
[2025-10-01 10:30:03 INF] Reading data from Parquet file year=2025/month=10/day=01/dbo_Orders_20251001103002.parquet
[2025-10-01 10:30:04 INF] Loading data to destination table DestDB.dbo.Orders
[2025-10-01 10:30:05 INF] Loaded 5000 rows to DestDB.dbo.Orders
[2025-10-01 10:30:05 INF] Transfer completed successfully in 5234ms
[2025-10-01 10:30:05 INF] ✓ Completed SourceDB.dbo.Orders: 5000 rows in 5234ms
[2025-10-01 10:30:05 INF] Starting transfer for SourceDB.dbo.Products
[2025-10-01 10:30:05 INF] Extracting data from source table SourceDB.dbo.Products
[2025-10-01 10:30:06 INF] Extracted 1000 rows from SourceDB.dbo.Products
[2025-10-01 10:30:06 INF] Writing data to Parquet file dbo_Products_20251001103006.parquet
[2025-10-01 10:30:07 INF] Successfully wrote data to Parquet file
[2025-10-01 10:30:07 INF] Reading data from Parquet file year=2025/month=10/day=01/dbo_Products_20251001103006.parquet
[2025-10-01 10:30:07 INF] Loading data to destination table DestDB.dbo.Products
[2025-10-01 10:30:08 INF] Loaded 1000 rows to DestDB.dbo.Products
[2025-10-01 10:30:08 INF] Transfer completed successfully in 2567ms
[2025-10-01 10:30:08 INF] ✓ Completed SourceDB.dbo.Products: 1000 rows in 2567ms
[2025-10-01 10:30:08 INF] =====================================
[2025-10-01 10:30:08 INF] Transfer Summary:
[2025-10-01 10:30:08 INF]   Success: 2
[2025-10-01 10:30:08 INF]   Failed: 0
[2025-10-01 10:30:08 INF]   Total Rows Extracted: 6000
[2025-10-01 10:30:08 INF]   Total Rows Loaded: 6000
[2025-10-01 10:30:08 INF] =====================================
```

## Testing Approach

### Manual Testing (Immediate)
```bash
# Build
dotnet build src/DataTransfer.Console

# Run (will fail without real SQL Server)
dotnet run --project src/DataTransfer.Console

# Expected: Should show configuration loading and validation
# Will fail on actual database connection (expected)
```

### With Test Database
```bash
# Requires SQL Server instance
# Update config/appsettings.json with valid connection strings
dotnet run --project src/DataTransfer.Console

# Should complete successfully if databases exist and are accessible
```

## Configuration File Structure

The `config/appsettings.json` should already exist. Ensure it follows this structure:

```json
{
  "sourceConnection": {
    "connectionString": "Server=localhost;Database=SourceDB;Integrated Security=true;TrustServerCertificate=true;"
  },
  "destinationConnection": {
    "connectionString": "Server=localhost;Database=DestDB;Integrated Security=true;TrustServerCertificate=true;"
  },
  "storage": {
    "type": "parquet",
    "basePath": "parquet-output",
    "compressionType": "snappy"
  },
  "tables": [
    {
      "source": {
        "database": "SourceDB",
        "schema": "dbo",
        "table": "Orders"
      },
      "destination": {
        "database": "DestDB",
        "schema": "dbo",
        "table": "Orders"
      },
      "partitioning": {
        "type": "date",
        "column": "OrderDate"
      },
      "extractSettings": {
        "batchSize": 100000
      }
    }
  ]
}
```

## Commit Message Format

After implementation and testing:

```
feat(console): add CLI application with DI and orchestration [GREEN]

Implemented console application entry point:
- Serilog logging to console and file (logs/datatransfer-.txt)
- Dependency injection using Microsoft.Extensions.Hosting
- Configuration loading and validation from config/appsettings.json
- Processes all tables from configuration via DataTransferOrchestrator
- Displays transfer summary with success/failure counts and row statistics
- Error handling: continues on individual table failures
- Exit codes: 0 for success, 1 for any failures

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude <noreply@anthropic.com>
```

## Key Classes to Use

All these are already implemented and tested:

1. **ConfigurationLoader** (`DataTransfer.Configuration`)
   - `LoadAsync(string filePath)` → returns `DataTransferConfiguration`

2. **ConfigurationValidator** (`DataTransfer.Configuration`)
   - `Validate(DataTransferConfiguration config)` → returns `ValidationResult`

3. **DataTransferOrchestrator** (`DataTransfer.Pipeline`)
   - `TransferTableAsync(TableConfiguration, string sourceConn, string destConn, CancellationToken)` → returns `TransferResult`

4. **SqlTableExtractor** (`DataTransfer.SqlServer`)
   - Implements `ITableExtractor`

5. **ParquetStorage** (`DataTransfer.Parquet`)
   - Implements `IParquetStorage`
   - Constructor: `new ParquetStorage(string basePath)`

6. **SqlDataLoader** (`DataTransfer.SqlServer`)
   - Implements `IDataLoader`

## Success Criteria

✅ Console app builds without errors
✅ Loads configuration from `config/appsettings.json`
✅ Validates configuration and displays errors if invalid
✅ Attempts to process all tables (will fail without real DB, that's OK)
✅ Displays summary statistics
✅ Exits with appropriate code (0 or 1)
✅ Logs to both console and file
✅ Follows existing code patterns and conventions
✅ All 107 existing tests still pass after changes
