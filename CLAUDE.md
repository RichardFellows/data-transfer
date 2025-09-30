# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

.NET 8 solution for transferring data between SQL Server instances using Apache Parquet as intermediate storage. Handles partitioned tables, SCD2 tables, and static tables with date-based partitioning.

## Architecture

The solution follows a layered architecture across these projects:

- **DataTransfer.Core** - Domain models, interfaces (ITableExtractor, IParquetStorage, IDataLoader)
- **DataTransfer.Configuration** - JSON/YAML config management with validation
- **DataTransfer.SqlServer** - SQL Server extraction/loading with query builders for different table types
- **DataTransfer.Parquet** - Arrow-formatted Parquet file operations with date-based partitioning (year=YYYY/month=MM/day=DD/)
- **DataTransfer.Pipeline** - Orchestration engine coordinating extract-store-load pipeline
- **DataTransfer.Console** - CLI application entry point

### Key Design Patterns

- **Strategy Pattern**: PartitionStrategy abstract base with implementations for DatePartitionStrategy, IntDatePartitionStrategy, Scd2PartitionStrategy, StaticTableStrategy
- **Pipeline Pattern**: DataTransferPipeline orchestrates extraction → Parquet storage → loading
- **Repository Pattern**: ITableExtractor/IDataLoader abstract SQL operations
- **Factory Pattern**: Strategy selection based on table configuration

## Development Methodology

### TDD Workflow (MANDATORY)

Follow strict red-green-refactor cycle for ALL code:

1. **RED**: Write failing test first
2. **GREEN**: Implement minimal code to pass
3. **REFACTOR**: Improve code while keeping tests green

Commit after each TDD phase with format:
```
<type>(<scope>): <description> [TDD_PHASE]

Examples:
feat(extraction): add failing test for SQL query builder [RED]
feat(extraction): implement basic query generation [GREEN]
refactor(extraction): optimize query parameter handling [REFACTOR]
```

### Testing Commands

```bash
# Run all tests
dotnet test

# Run tests with coverage (requires coverlet)
dotnet test /p:CollectCoverage=true /p:CoverageMinimum=80

# Run specific test project
dotnet test tests/DataTransfer.Core.Tests

# Run single test
dotnet test --filter "FullyQualifiedName~TableConfiguration_Should_Parse_Valid_Json"
```

### Build Commands

```bash
# Build entire solution
dotnet build

# Build specific project
dotnet build src/DataTransfer.Core

# Clean and rebuild
dotnet clean && dotnet build
```

### Branch Strategy

Branch naming: `feature/*`, `integration/*`, `release/*`

Use Claude Code's rollback/snapshot functionality to manage development iterations.

## Configuration Schema

Tables support 4 partitioning types in configuration:
- `"date"` - DATE column partitioning
- `"int_date"` - Integer date format (e.g., 20240115)
- `"scd2"` - Slowly changing dimension type 2
- `"static"` - No partitioning

Configuration path: `config/appsettings.json`

## Docker

UBI8-based container with .NET 8 runtime. Build from `docker/Dockerfile`.

```bash
# Build Docker image
docker build -f docker/Dockerfile -t datatransfer:latest .

# Run with configuration
docker run -v /path/to/config:/config datatransfer:latest
```

## Test Coverage Requirements

Minimum 80% code coverage enforced. Coverage categories:
- Unit tests (mocked dependencies)
- Component tests (real dependencies)
- Integration tests (test databases - use LocalDB/TestContainers)
- Performance benchmarks (large dataset handling)
- Error scenarios (network failures, disk space, corrupted data)

## Implementation Notes

- Use async/await throughout with CancellationToken support
- Structured logging with Serilog
- Circuit breaker pattern for database connections
- Memory-efficient streaming for large datasets
- Proper IDisposable patterns for resource cleanup
- Batch sizing configured per table (default: 100,000 rows)
- Parquet compression: Snappy (configurable)
