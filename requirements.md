# SQL Server Data Transfer Solution with Parquet Intermediate Storage

Create a complete .NET 8 solution for transferring data between SQL Server database instances using Apache Parquet as an intermediate storage format.

## Requirements Overview

Build a data transfer system that:
- Extracts data from source SQL Server database tables
- Stores extracts as partitioned Parquet files (Arrow format)
- Loads data from Parquet files into destination SQL Server database
- Supports configurable table specifications (DbName.Schema.TableName)
- Handles different table types: partitioned, SCD2, and static tables
- Partitions extracts by date
- Achieves 80%+ test coverage with XUnit 2

## Technical Specifications

### Core Technologies
- **.NET 8** - Target framework
- **Parquet.NET** - For Parquet file operations
- **System.Data.SqlClient** or **Microsoft.Data.SqlClient** - SQL Server connectivity
- **XUnit 2** - Testing framework
- **Deployment**: UBI8-based Docker container
- **Storage**: File system initially, S3-compatible for future

### Architecture Components

1. **Configuration System**
   - Source/destination connection strings
   - Table specifications (DbName, Schema, TableName)
   - Partition column mappings
   - Extract/load settings
   - Support JSON/YAML configuration files

2. **Data Extraction Engine**
   - Query builder for different table types:
     - Partitioned tables (date-based partitioning)
     - SCD2 tables (slowly changing dimensions)
     - Static tables (no partitioning)
   - Support for both DATE and INT date formats
   - Configurable batch sizing
   - Date range extraction parameters

3. **Parquet Storage Layer**
   - Arrow-formatted Parquet file generation
   - Date-based partitioning (e.g., year=2024/month=01/day=15/)
   - Compression optimization
   - Schema inference and preservation
   - File system abstraction for future S3 migration

4. **Data Loading Engine**
   - Parquet file discovery and reading
   - Destination table preparation (truncate/append modes)
   - Bulk insert operations
   - Transaction management
   - Error handling and retry logic

5. **Orchestration & Monitoring**
   - Pipeline execution coordinator
   - Progress tracking and logging
   - Error reporting and recovery
   - Metrics collection

## Project Structure
```
DataTransferSolution/
├── src/
│   ├── DataTransfer.Core/           # Core domain models and interfaces
│   ├── DataTransfer.Configuration/  # Configuration management
│   ├── DataTransfer.SqlServer/      # SQL Server data access
│   ├── DataTransfer.Parquet/        # Parquet operations
│   ├── DataTransfer.Pipeline/       # Orchestration engine
│   └── DataTransfer.Console/        # CLI application
├── tests/
│   ├── DataTransfer.Core.Tests/
│   ├── DataTransfer.SqlServer.Tests/
│   ├── DataTransfer.Parquet.Tests/
│   ├── DataTransfer.Pipeline.Tests/
│   └── DataTransfer.Integration.Tests/
├── docker/
│   └── Dockerfile                   # UBI8-based container
└── config/
    └── appsettings.json            # Configuration template
```

## Specific Implementation Requirements

### Configuration Schema
```json
{
  "connections": {
    "source": "Server=source;Database=GFRM_STAR2;...",
    "destination": "Server=dest;Database=GFRM_STAR2_COPY;..."
  },
  "tables": [
    {
      "source": {
        "database": "GFRM_STAR2",
        "schema": "dbo", 
        "table": "Reporting_Client"
      },
      "destination": {
        "database": "GFRM_STAR2_COPY",
        "schema": "dbo",
        "table": "Reporting_Client"
      },
      "partitioning": {
        "type": "date", // "date", "int_date", "scd2", "static"
        "column": "CreatedDate",
        "format": "yyyyMMdd" // for int_date type
      },
      "extractSettings": {
        "batchSize": 100000,
        "dateRange": {
          "startDate": "2024-01-01",
          "endDate": "2024-12-31"
        }
      }
    }
  ],
  "storage": {
    "basePath": "/data/extracts",
    "compression": "snappy"
  }
}
```

### Key Classes to Implement

1. **ITableExtractor** - Interface for data extraction
2. **IParquetStorage** - Interface for Parquet operations  
3. **IDataLoader** - Interface for destination loading
4. **TableConfiguration** - Configuration model
5. **PartitionStrategy** - Abstract base for partition handling
6. **DatePartitionStrategy** - Date-based partitioning
7. **IntDatePartitionStrategy** - Integer date partitioning
8. **Scd2PartitionStrategy** - SCD2 table handling
9. **StaticTableStrategy** - Non-partitioned tables
10. **DataTransferPipeline** - Main orchestrator

### Testing Requirements

- **Unit Tests**: All service classes with mocked dependencies
- **Integration Tests**: End-to-end pipeline with test databases
- **Parquet Tests**: File format validation and schema preservation
- **Configuration Tests**: JSON deserialization and validation
- **Performance Tests**: Large dataset handling benchmarks
- **Error Handling Tests**: Network failures, disk space, corrupted data
- **Achieve 80%+ code coverage** using coverlet

### Docker Requirements

- Base on UBI8 (Red Hat Universal Base Image)
- Include .NET 8 runtime
- Support configuration via environment variables
- Health check endpoints
- Proper logging to stdout/stderr
- Non-root user execution

## Expected Deliverables

1. **Complete .NET 8 solution** with all projects and dependencies
2. **Comprehensive XUnit test suite** with 80%+ coverage
3. **Docker configuration** with UBI8 base image
4. **Configuration templates** and documentation
5. **README.md** with setup and usage instructions
6. **Sample data** and test database scripts
7. **Performance benchmarks** and optimization notes

## Additional Considerations

- Implement proper async/await patterns throughout
- Use structured logging (Serilog recommended)
- Include cancellation token support for long-running operations
- Implement circuit breaker pattern for database connections
- Add metrics collection (counters, timers, gauges)
- Consider memory-efficient streaming for large datasets
- Implement proper disposal patterns for resources
- Add configuration validation with helpful error messages

## Development Methodology & Git Workflow

### Test-Driven Development (TDD)
**MANDATORY: Follow strict TDD red-green-refactor cycle for ALL code:**

1. **RED Phase**: Write a failing test first
   - Create specific, focused unit tests before any implementation
   - Ensure tests fail for the right reasons (not compilation errors)
   - Write descriptive test names that explain expected behavior

2. **GREEN Phase**: Write minimal code to make tests pass
   - Implement only enough code to satisfy the failing test
   - Don't optimize or add extra features yet
   - Focus on making the test pass quickly

3. **REFACTOR Phase**: Improve code quality while keeping tests green
   - Clean up implementation without changing behavior
   - Extract methods, improve naming, reduce duplication
   - Ensure all tests still pass after refactoring

**TDD Implementation Order:**
1. Start with core interfaces and models (write interface tests first)
2. Implement configuration system (test JSON deserialization, validation)
3. Build data extraction components (test SQL query generation, data retrieval)
4. Create Parquet storage layer (test file operations, schema preservation)
5. Develop data loading engine (test bulk inserts, transaction handling)
6. Build orchestration pipeline (test end-to-end workflows)

### Git Commit & PR Workflow

**Commit Strategy:**
- **Frequent commits following TDD cycles**:
  ```
  feat(core): add failing test for TableConfiguration validation [RED]
  feat(core): implement basic TableConfiguration class [GREEN] 
  refactor(core): extract validation logic to separate method [REFACTOR]
  ```

- **Commit message format**:
  ```
  <type>(<scope>): <description> [TDD_PHASE]
  
  Examples:
  feat(extraction): add failing test for SQL query builder [RED]
  feat(extraction): implement basic query generation [GREEN]
  refactor(extraction): optimize query parameter handling [REFACTOR]
  test(parquet): add integration tests for file partitioning
  docs(readme): update setup instructions
  ```

**Pull Request Process:**
1. **Feature PRs** - One PR per major component/feature
   - Include full TDD cycle evidence in commit history
   - Ensure 80%+ test coverage before PR submission
   - Include both unit and integration tests

2. **PR Requirements**:
   - All tests must pass (red-green cycle completed)
   - Code coverage reports included
   - Performance benchmarks for data operations
   - Docker build verification
   - Configuration validation tests

3. **PR Structure**:
   ```
   Title: feat: Implement SQL Server data extraction engine
   
   ## TDD Evidence
   - ✅ 47 tests written first (RED phase)
   - ✅ All tests now passing (GREEN phase) 
   - ✅ Code refactored for clean architecture (REFACTOR phase)
   
   ## Coverage Report
   - Unit Tests: 85% coverage
   - Integration Tests: 12 scenarios covered
   
   ## Performance Benchmarks
   - 100K row extraction: 2.3 seconds
   - Parquet file generation: 1.1 seconds
   ```

**Branch Strategy:**
- `main` - Production ready code
- `feature/*` - Individual component development
- `integration/*` - Cross-component integration work
- `release/*` - Release preparation branches

### Development Execution Plan

**Phase 1: Setup & Core Foundation**
- Initialize solution structure
- TDD: Configuration system, interfaces, models
- Create core domain models and abstractions

**Phase 2: Component Development**
- Implement data extraction engine
- Build Parquet storage layer
- Develop data loading engine
- Follow TDD for all components

**Phase 3: Integration & Pipeline**
- Combine all components with orchestration layer
- Full integration tests with test databases
- Performance benchmarking and optimization

### Testing Strategy with TDD

**Test Categories (all following TDD):**
1. **Unit Tests** - Test individual classes/methods in isolation
2. **Component Tests** - Test complete components with real dependencies
3. **Integration Tests** - Test full pipeline with test databases
4. **Contract Tests** - Test interfaces between components
5. **Performance Tests** - Benchmark data transfer operations

**TDD Test Examples to Write First:**
```csharp
// RED: Write these tests first, watch them fail
[Fact] public void TableConfiguration_Should_Parse_Valid_Json()
[Fact] public void SqlExtractor_Should_Generate_Partitioned_Query()  
[Fact] public void ParquetStorage_Should_Create_Date_Partitioned_Files()
[Fact] public void DataLoader_Should_Handle_Transaction_Rollback()
[Fact] public void Pipeline_Should_Complete_End_To_End_Transfer()
```

## Code Generation Instructions

**Generate the solution using this exact workflow:**

1. **Initialize Git Repository Structure**
   - Create main repository with proper .gitignore for .NET
   - Create feature branches for each component as needed

2. **Follow Strict TDD for Each Component**
   - Write failing tests first for every class and method
   - Implement minimal code to pass tests
   - Refactor while maintaining green tests
   - Commit after each TDD cycle

3. **Generate Complete Working Solution**
   - All code must be production-ready with full error handling
   - Include comprehensive logging throughout
   - Implement proper async/await patterns
   - Add cancellation token support
   - Include performance optimizations

4. **Create Full Test Suite**
   - Unit tests for all public methods
   - Integration tests with real SQL Server (use LocalDB/TestContainers)
   - Parquet file validation tests
   - Configuration validation tests
   - Error scenario tests (network failures, disk space, etc.)
   - Performance benchmark tests

5. **Docker & Deployment**
   - UBI8-based Dockerfile with .NET 8 runtime
   - Environment variable configuration
   - Health check endpoints
   - Proper logging to stdout/stderr
   - Non-root user execution

6. **Documentation & Setup**
   - Complete README with setup instructions
   - API documentation for all public interfaces
   - Configuration examples for different table types
   - Troubleshooting guide
   - Performance tuning recommendations

Generate a production-ready solution that can handle enterprise-scale data transfers with reliability, performance, and maintainability as key principles, developed using strict TDD methodology with proper git workflow management.