# Test Coverage Summary

**As of:** 2025-10-02
**Total Tests Passing:** 111
**Coverage Report:** TestResults/CoverageReport/index.html

## Test Breakdown by Project

### 1. DataTransfer.Core.Tests (48 tests)
**Location:** `tests/DataTransfer.Core.Tests/`

#### Models Tests (18 tests)
- `TableIdentifierTests.cs` (3 tests)
  - Should create with required properties
  - Should generate FullyQualifiedName correctly
  - Should handle different schema names

- `TableConfigurationTests.cs` (6 tests)
  - Should create with required properties
  - Should deserialize from JSON
  - Should handle missing optional properties
  - Should validate partition configuration
  - Should support all partition types
  - Should handle extract settings

- `PartitioningConfigurationTests.cs` (5 tests)
  - Should create with Type property
  - Should support Date partition type
  - Should support IntDate partition type
  - Should support Scd2 partition type
  - Should support Static partition type

- `DataTransferConfigurationTests.cs` (4 tests)
  - Should deserialize complete configuration from JSON
  - Should handle source and destination connections
  - Should handle storage configuration
  - Should contain list of table configurations

#### Interfaces Tests (12 tests)
- `ITableExtractorTests.cs` (4 tests)
  - Should define ExtractAsync method
  - Should require TableConfiguration parameter
  - Should require connection string parameter
  - Should return Task<ExtractionResult>

- `IParquetStorageTests.cs` (4 tests)
  - Should define WriteAsync method
  - Should define ReadAsync method
  - Should accept Stream and file path
  - Should return Task for both methods

- `IDataLoaderTests.cs` (4 tests)
  - Should define LoadAsync method
  - Should require TableConfiguration parameter
  - Should require connection string parameter
  - Should return Task<LoadResult>

#### Strategy Tests (18 tests)
- `DatePartitionStrategyTests.cs` (5 tests)
  - Should generate correct partition path
  - Should build WHERE clause with date range
  - Should handle start and end dates
  - Should format dates correctly in SQL
  - Should inherit from PartitionStrategy

- `IntDatePartitionStrategyTests.cs` (5 tests)
  - Should generate correct partition path
  - Should build WHERE clause with integer date format
  - Should convert dates to YYYYMMDD format
  - Should handle start and end dates
  - Should inherit from PartitionStrategy

- `Scd2PartitionStrategyTests.cs` (4 tests)
  - Should generate correct partition path
  - Should build WHERE clause for SCD2 logic
  - Should use EffectiveDate and ExpirationDate columns
  - Should handle NULL ExpirationDate (current records)

- `StaticTableStrategyTests.cs` (4 tests)
  - Should return "static" as partition path
  - Should return empty WHERE clause
  - Should not require date parameters
  - Should inherit from PartitionStrategy

### 2. DataTransfer.Configuration.Tests (16 tests)
**Location:** `tests/DataTransfer.Configuration.Tests/`

#### ConfigurationLoaderTests.cs (8 tests)
- Should load valid JSON configuration file
- Should deserialize source connection correctly
- Should deserialize destination connection correctly
- Should deserialize storage configuration correctly
- Should deserialize table list correctly
- Should throw when file not found
- Should throw when JSON is invalid
- Should support async LoadAsync method

#### ConfigurationValidatorTests.cs (8 tests)
- Should validate successfully when config is complete
- Should fail when source connection missing
- Should fail when destination connection missing
- Should fail when connection string is empty
- Should fail when tables list is empty
- Should fail when partition type requires column but column is missing
- Should fail when table source is missing
- Should accumulate multiple validation errors

### 3. DataTransfer.SqlServer.Tests (21 tests)
**Location:** `tests/DataTransfer.SqlServer.Tests/`

#### SqlQueryBuilderTests.cs (12 tests)
- Should build SELECT query for table
- Should build SELECT with Date partition WHERE clause
- Should build SELECT with IntDate partition WHERE clause
- Should build SELECT with Scd2 partition WHERE clause
- Should build SELECT with Static partition (no WHERE clause)
- Should format table names with brackets
- Should build COUNT query
- Should build INSERT query with column list
- Should build TRUNCATE query
- Should handle schema names correctly
- Should escape special characters in table names
- Should handle NULL partition strategy

#### SqlTableExtractorTests.cs (5 tests)
- Should implement ITableExtractor interface
- Should extract data to JSON stream
- Should use SqlDataReader for extraction
- Should handle empty result sets
- Should propagate CancellationToken

#### SqlDataLoaderTests.cs (4 tests)
- Should implement IDataLoader interface
- Should load data from JSON stream
- Should use SqlBulkCopy for loading
- Should handle batch size configuration

### 4. DataTransfer.Parquet.Tests (11 tests)
**Location:** `tests/DataTransfer.Parquet.Tests/`

#### ParquetStorageTests.cs (11 tests)
- Should implement IParquetStorage interface
- Should require base path in constructor
- Should throw when stream is null (WriteAsync)
- Should throw when file path is empty (WriteAsync)
- Should throw when file path is empty (ReadAsync)
- Should create directory if not exists
- Should create partition subdirectories (year/month/day)
- Should create Parquet file with Snappy compression
- Should handle cancellation token
- Should read Parquet file and return JSON stream
- Should round-trip data (Write → Read maintains integrity)

### 5. DataTransfer.Pipeline.Tests (10 tests)
**Location:** `tests/DataTransfer.Pipeline.Tests/`

#### DataTransferOrchestratorTests.cs (10 tests)
- Should require all dependencies (extractor, storage, loader, logger)
- Should throw when TableConfiguration is null
- Should throw when source connection string is empty
- Should throw when destination connection string is empty
- Should extract data from source via ITableExtractor
- Should write extracted data to Parquet via IParquetStorage
- Should read from Parquet after writing
- Should load data to destination via IDataLoader
- Should return TransferResult with success status and row counts
- Should handle CancellationToken and propagate cancellation

### 6. DataTransfer.Integration.Tests (5 tests)
**Location:** `tests/DataTransfer.Integration.Tests/`

#### E2E Pipeline Tests (5 tests)
- **EndToEndPipelineTests.cs**
  - Should transfer date-partitioned table end-to-end
  - Should transfer int-date-partitioned table end-to-end
  - Should transfer SCD2 table end-to-end
  - Should transfer static table end-to-end
  - Should transfer empty table end-to-end

**Infrastructure:**
- Uses Testcontainers.MsSql for real SQL Server instances
- Shared container with Respawn for database cleanup (57% faster than individual containers)
- Tests full Extract → Parquet → Load pipeline
- Validates data integrity with real databases
- Execution time: ~19 seconds for all 5 tests

## Test Execution Commands

```bash
# Run all tests
dotnet test

# Run with detailed output
dotnet test --logger "console;verbosity=detailed"

# Run specific test project
dotnet test tests/DataTransfer.Core.Tests
dotnet test tests/DataTransfer.Configuration.Tests
dotnet test tests/DataTransfer.SqlServer.Tests
dotnet test tests/DataTransfer.Parquet.Tests
dotnet test tests/DataTransfer.Pipeline.Tests

# Run single test
dotnet test --filter "FullyQualifiedName~DataTransferOrchestrator_Should_Require_Dependencies"

# Run with coverage (requires coverlet)
dotnet test /p:CollectCoverage=true /p:CoverageMinimum=80
```

## Coverage Target

**Minimum Required:** 80% (as per CLAUDE.md)

### Actual Coverage (Measured 2025-10-02)

**Overall Coverage:**
- **Line Coverage: 90.7%** (649/715 lines) ✅ **Exceeds target by 10.7%**
- **Branch Coverage: 81.2%** (186/229 branches) ✅ Exceeds target
- **Method Coverage: 96.7%** (88/91 methods) ✅ Excellent

**Coverage by Assembly:**
- **DataTransfer.Configuration: 76.6%** ⚠️ (Slightly below target)
  - ConfigurationLoader: 56.6% (needs improvement)
  - ConfigurationValidator: 83.3% ✅
  - ValidationResult: 100% ✅

- **DataTransfer.Core: 94.1%** ✅ (Excellent)
  - Most classes at 100%
  - ExtractionResult, LoadResult, PartitionStrategyFactory: 83.3%

- **DataTransfer.Parquet: 94.5%** ✅ (Excellent)
  - ParquetStorage: 94.5%

- **DataTransfer.Pipeline: 100%** ✅ (Perfect)
  - DataTransferOrchestrator: 100%

- **DataTransfer.SqlServer: 90.3%** ✅ (Excellent)
  - SqlQueryBuilder: 100% ✅
  - SqlDataLoader: 94.6% ✅
  - SqlTableExtractor: 82.4% ✅

**Analysis:**
- ✅ Overall target exceeded: 90.7% > 80% requirement
- ✅ All core functionality well-covered
- ⚠️ ConfigurationLoader at 56.6% is an outlier (likely due to file I/O error paths)
- ✅ Critical business logic (strategies, orchestrator, query builder) at 100%

## Test Patterns Used

### 1. Unit Tests with Mocks
Most tests use Moq for dependency mocking:
```csharp
var extractorMock = new Mock<ITableExtractor>();
extractorMock.Setup(x => x.ExtractAsync(...))
    .ReturnsAsync(new ExtractionResult { RowsExtracted = 100 });
```

### 2. Property-Based Tests
Model tests verify properties and JSON deserialization:
```csharp
var config = JsonSerializer.Deserialize<TableConfiguration>(json);
Assert.Equal("TestDB", config.Source.Database);
```

### 3. Interface Contract Tests
Tests verify interface methods exist and have correct signatures:
```csharp
var method = typeof(ITableExtractor).GetMethod("ExtractAsync");
Assert.NotNull(method);
Assert.True(method.ReturnType.IsGenericType);
```

### 4. Integration Tests (Pending)
Will use Testcontainers for end-to-end testing with real SQL Server

## Test Organization

All test projects follow consistent structure:
```
tests/
└── DataTransfer.{Layer}.Tests/
    ├── {ClassName}Tests.cs      (one test class per production class)
    ├── GlobalUsings.cs          (shared usings)
    └── DataTransfer.{Layer}.Tests.csproj
```

## Common Test Packages

All test projects use:
- **xUnit 2.4.2** - Test framework
- **Moq 4.20.72** - Mocking library
- **coverlet.collector 6.0.4** - Code coverage

## Next Testing Steps

1. **Console App Tests:** May skip unit tests, rely on manual testing
2. **Integration Tests:**
   - Add Testcontainers.MsSql
   - Test full pipeline with real SQL Server
   - Cover all 4 partition types
   - Verify data integrity
3. **Performance Benchmarks:**
   - Use BenchmarkDotNet
   - Test with large datasets (1M+ rows)
   - Measure memory usage
   - Profile bottlenecks

## Test Execution Time

Current test suite execution time: ~600ms total
- Core: ~26ms
- Configuration: ~137ms
- SqlServer: ~81ms
- Parquet: ~219ms (slower due to file I/O)
- Pipeline: ~174ms
- Integration: <1ms (dummy test)

All tests are fast and suitable for CI/CD pipelines.
