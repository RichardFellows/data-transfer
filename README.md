# DataTransfer

A high-performance .NET 8 solution for transferring data between SQL Server instances using Apache Parquet as intermediate storage. Supports multiple partitioning strategies, handles large datasets efficiently through streaming, and provides Docker deployment for production environments.

## Features

- **Multiple Partition Strategies**: Date, Integer Date, SCD2 (Slowly Changing Dimensions), and Static tables
- **Apache Parquet Storage**: Industry-standard columnar format with Snappy compression
- **Date-Based Partitioning**: Hive-compatible partitioning scheme (`year=YYYY/month=MM/day=DD/`)
- **Streaming Architecture**: Memory-efficient processing for large datasets (millions of rows)
- **Production Ready**:
  - Docker support with 365MB optimized image
  - Comprehensive logging with Serilog (console and file outputs)
  - Robust error handling and recovery
  - 111 tests including 5 E2E integration tests with real SQL Server
- **Configurable**: JSON-based configuration with validation
- **High Performance**: SqlBulkCopy for fast loading, async/await throughout

## Prerequisites

- **.NET 8 SDK** - [Download](https://dotnet.microsoft.com/download/dotnet/8.0)
- **SQL Server 2019+** - Source and destination databases
- **Docker** (optional) - For containerized deployment

## Installation

### Clone the repository
```bash
git clone <repository-url>
cd DataTransfer
```

### Restore dependencies
```bash
dotnet restore
```

### Build the solution
```bash
dotnet build
```

### Run tests
```bash
dotnet test
```

## Configuration

Create or modify `config/appsettings.json` with your transfer configuration:

```json
{
  "sourceConnection": {
    "connectionString": "Server=localhost;Database=SourceDB;Integrated Security=true;TrustServerCertificate=true"
  },
  "destinationConnection": {
    "connectionString": "Server=localhost;Database=DestinationDB;Integrated Security=true;TrustServerCertificate=true"
  },
  "storage": {
    "type": "parquet",
    "basePath": "./parquet-output",
    "compressionType": "snappy"
  },
  "tables": [
    {
      "source": {
        "database": "SourceDB",
        "schema": "dbo",
        "table": "SalesTransactions"
      },
      "destination": {
        "database": "DestinationDB",
        "schema": "dbo",
        "table": "SalesTransactions"
      },
      "partitioning": {
        "type": "date",
        "column": "TransactionDate"
      },
      "extractSettings": {
        "batchSize": 100000
      }
    }
  ]
}
```

### Partition Types

#### 1. Date Partitioning (`"type": "date"`)
For tables with standard DATE or DATETIME columns:
```json
"partitioning": {
  "type": "date",
  "column": "TransactionDate"
}
```

#### 2. Integer Date Partitioning (`"type": "int_date"`)
For tables using integer date format (YYYYMMDD, e.g., 20240115):
```json
"partitioning": {
  "type": "int_date",
  "column": "DateKey"
}
```

#### 3. SCD2 Partitioning (`"type": "scd2"`)
For Slowly Changing Dimension Type 2 tables with effective/expiration dates:
```json
"partitioning": {
  "type": "scd2",
  "scdEffectiveDateColumn": "EffectiveDate",
  "scdExpirationDateColumn": "ExpirationDate"
}
```

#### 4. Static Tables (`"type": "static"`)
For reference tables without date-based partitioning:
```json
"partitioning": {
  "type": "static"
}
```

## Usage

### Local Execution

Run the console application:
```bash
dotnet run --project src/DataTransfer.Console
```

The application will:
1. Load and validate configuration from `config/appsettings.json`
2. Process each table defined in the configuration
3. Extract data from source SQL Server
4. Write to Parquet files with date-based partitioning
5. Load data into destination SQL Server
6. Display summary statistics

### Docker Execution

#### Build the Docker image
```bash
docker build -f docker/Dockerfile -t datatransfer:latest .
```

#### Run the container
```bash
docker run \
  -v $(pwd)/config:/config \
  -v $(pwd)/output:/parquet-output \
  -v $(pwd)/logs:/logs \
  datatransfer:latest
```

**Volume Mounts:**
- `/config` - Configuration files (required)
- `/parquet-output` - Parquet storage location (required)
- `/logs` - Log files output (optional)

**Docker Image Details:**
- Base: `mcr.microsoft.com/dotnet/runtime:8.0`
- Size: 365MB
- User: `datatransfer` (non-root, UID 1001)
- Health check: Validates application binary exists

## Build and Test Commands

### Build entire solution
```bash
dotnet build
```

### Build specific project
```bash
dotnet build src/DataTransfer.Core
```

### Run all tests
```bash
dotnet test
```

### Run tests with coverage
```bash
dotnet test /p:CollectCoverage=true /p:CoverageMinimum=80
```

### Run specific test project
```bash
dotnet test tests/DataTransfer.Core.Tests
```

### Clean and rebuild
```bash
dotnet clean && dotnet build
```

## Architecture

### Layered Design

```
┌─────────────────────────────────────────────────────┐
│          DataTransfer.Console (CLI)                 │
│  - Entry point with dependency injection            │
│  - Configuration management                         │
│  - Logging setup (Serilog)                          │
└────────────────┬────────────────────────────────────┘
                 │
┌────────────────▼────────────────────────────────────┐
│       DataTransfer.Pipeline (Orchestration)         │
│  - DataTransferOrchestrator                         │
│  - Coordinates Extract → Store → Load workflow      │
└───┬─────────────────┬────────────────┬──────────────┘
    │                 │                │
┌───▼─────────┐  ┌────▼─────────┐  ┌──▼────────────┐
│ SqlServer   │  │   Parquet    │  │ Configuration │
│ - Extractor │  │   - Storage  │  │   - Loader    │
│ - Loader    │  │   - Read/    │  │   - Validator │
│ - Query     │  │     Write    │  └───────────────┘
│   Builder   │  └──────────────┘
└─────┬───────┘
      │
┌─────▼──────────────────────────────────────────────┐
│          DataTransfer.Core (Domain)                 │
│  - Interfaces (ITableExtractor, IParquetStorage,   │
│    IDataLoader)                                     │
│  - Models (TableConfiguration, TransferResult)      │
│  - Partition Strategies (Date, IntDate, SCD2,      │
│    Static)                                          │
└─────────────────────────────────────────────────────┘
```

### Data Flow

1. **Extract**: SqlTableExtractor reads from SQL Server using SqlDataReader → JSON stream
2. **Write**: ParquetStorage converts JSON → Apache Parquet with Snappy compression
3. **Read**: ParquetStorage reads Parquet files → JSON stream
4. **Load**: SqlDataLoader writes JSON → SQL Server using SqlBulkCopy

### Design Patterns

- **Strategy Pattern**: Multiple partition strategies (Date, IntDate, SCD2, Static)
- **Repository Pattern**: Abstract data access via interfaces
- **Pipeline Pattern**: Orchestrated multi-step data flow
- **Dependency Injection**: Loose coupling and testability

## Project Structure

```
DataTransfer/
├── src/
│   ├── DataTransfer.Core/              # Domain models, interfaces, strategies
│   ├── DataTransfer.Configuration/     # JSON configuration management
│   ├── DataTransfer.SqlServer/         # SQL Server extraction and loading
│   ├── DataTransfer.Parquet/           # Parquet file operations
│   ├── DataTransfer.Pipeline/          # Transfer orchestration
│   └── DataTransfer.Console/           # CLI application entry point
├── tests/
│   ├── DataTransfer.Core.Tests/        # 48 unit tests
│   ├── DataTransfer.Configuration.Tests/ # 16 unit tests
│   ├── DataTransfer.SqlServer.Tests/   # 21 unit tests
│   ├── DataTransfer.Parquet.Tests/     # 11 unit tests
│   ├── DataTransfer.Pipeline.Tests/    # 10 unit tests
│   └── DataTransfer.Integration.Tests/ # 5 E2E tests (Testcontainers)
├── config/
│   └── appsettings.json                # Configuration file
├── docker/
│   └── Dockerfile                      # Docker deployment
├── CLAUDE.md                           # Project instructions for LLMs
├── ARCHITECTURE.md                     # Technical architecture details
└── README.md                           # This file
```

## Testing

The solution includes comprehensive test coverage:

- **111 total tests passing**
- **Unit tests**: 106 tests with mocked dependencies across 5 layers
- **Integration tests**: 5 E2E tests with real SQL Server (Testcontainers)
- **Test execution time**: ~19 seconds (optimized with shared containers + Respawn)
- **Code coverage target**: 80%+ (enforced via coverlet)

### Test Categories

- All 4 partition strategies (Date, IntDate, SCD2, Static)
- Empty table edge cases
- Error scenarios and recovery
- Full Extract → Parquet → Load pipeline validation
- Data integrity verification

## Performance Characteristics

### Throughput (approximate rates)
- **Extraction**: 50,000-100,000 rows/second
- **Parquet Write**: 30,000-50,000 rows/second
- **Parquet Read**: 60,000-100,000 rows/second
- **Loading**: 80,000-150,000 rows/second (SqlBulkCopy)

### Memory Usage
- Streaming architecture minimizes memory footprint
- Bounded by batch size (default: 100,000 rows)
- Suitable for large datasets (millions of rows)

## Contributing

### TDD Workflow (Mandatory)

Follow strict red-green-refactor cycle for all code changes:

1. **RED**: Write failing test first
2. **GREEN**: Implement minimal code to pass
3. **REFACTOR**: Improve code while keeping tests green

### Commit Format

Use conventional commits with TDD phase tags:

```
<type>(<scope>): <description> [TDD_PHASE]

Detailed explanation of changes

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude <noreply@anthropic.com>
```

**Types**: `feat`, `fix`, `refactor`, `test`, `docs`, `perf`
**Phases**: `[RED]`, `[GREEN]`, `[REFACTOR]`, or combined `[GREEN+REFACTOR]`

**Examples:**
```
feat(extraction): add failing test for SQL query builder [RED]
feat(extraction): implement basic query generation [GREEN]
refactor(extraction): optimize query parameter handling [REFACTOR]
```

### Branch Strategy

- `feature/*` - New features
- `integration/*` - Integration work
- `release/*` - Release preparation

## Dependencies

### Core Libraries
- **.NET 8.0** - Framework
- **Microsoft.Data.SqlClient 5.2.2** - SQL Server connectivity
- **Parquet.Net 5.2.0** - Apache Parquet file format
- **System.Text.Json** - JSON serialization (built-in)

### Logging
- **Serilog 4.3.0** - Structured logging
- **Microsoft.Extensions.Logging 9.0.9** - Logging abstractions

### Testing
- **xUnit 2.4.2** - Test framework
- **Moq 4.20.72** - Mocking framework
- **coverlet.collector 6.0.4** - Code coverage
- **Testcontainers.MsSql 3.10.0** - SQL Server test containers
- **Respawn 6.2.1** - Database reset between tests

## Troubleshooting

### Connection Issues
- Verify SQL Server is accessible from your environment
- Check connection strings in `config/appsettings.json`
- Ensure firewall rules allow SQL Server connectivity (port 1433)
- For Docker: Use host network or expose SQL Server ports

### Configuration Validation Errors
- Run with verbose logging to see detailed validation messages
- Ensure all required fields are present in configuration
- Verify partition column names match your database schema

### Docker Volume Permissions
- Ensure the host directories exist before mounting
- Check permissions on mounted volumes (UID 1001 for container user)
- Use absolute paths for volume mounts

### Parquet File Issues
- Verify write permissions on `basePath` directory
- Ensure sufficient disk space for Parquet files
- Check logs for detailed error messages

## License

[Specify your license here, e.g., MIT, Apache 2.0, etc.]

## Support

For issues, questions, or contributions, please refer to the project documentation:
- `ARCHITECTURE.md` - Detailed technical architecture
- `CLAUDE.md` - Development guidelines and project instructions
- `IMPLEMENTATION_STATUS.md` - Current project status and roadmap
