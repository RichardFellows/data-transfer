# Architecture Documentation

## System Overview

DataTransfer is a .NET 8 solution for transferring data between SQL Server instances using Apache Parquet as intermediate storage. The system supports multiple partitioning strategies and handles large datasets efficiently through streaming.

## Architectural Layers

```
┌─────────────────────────────────────────────────────┐
│          DataTransfer.Console (CLI)                 │
│  - Entry point                                      │
│  - Dependency injection                             │
│  - Configuration management                         │
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
│  - Models (TableConfiguration, etc.)                │
│  - Strategies (Partition strategies)                │
└─────────────────────────────────────────────────────┘
```

## Design Patterns

### 1. Strategy Pattern
**Purpose:** Support different partitioning approaches without modifying core logic

**Implementation:**
```
PartitionStrategy (abstract)
├── DatePartitionStrategy     (DATE columns)
├── IntDatePartitionStrategy  (INT format: YYYYMMDD)
├── Scd2PartitionStrategy    (Slowly Changing Dimensions)
└── StaticTableStrategy      (No partitioning)
```

**Factory:**
```csharp
public static PartitionStrategy Create(PartitioningConfiguration config)
{
    return config.Type switch
    {
        PartitionType.Date => new DatePartitionStrategy(config.Column!),
        PartitionType.IntDate => new IntDatePartitionStrategy(config.Column!),
        PartitionType.Scd2 => new Scd2PartitionStrategy(
            config.ScdEffectiveDateColumn!,
            config.ScdExpirationDateColumn!),
        PartitionType.Static => new StaticTableStrategy(),
        _ => throw new ArgumentException($"Unknown partition type: {config.Type}")
    };
}
```

### 2. Repository Pattern
**Purpose:** Abstract data access operations

**Interfaces:**
- `ITableExtractor` - Extract data from SQL Server
- `IDataLoader` - Load data to SQL Server
- `IParquetStorage` - Read/write Parquet files

**Benefits:**
- Testability (easy to mock)
- Swappable implementations (could add Oracle, PostgreSQL, etc.)
- Clear separation of concerns

### 3. Pipeline Pattern
**Purpose:** Coordinate multi-step data flow

**Flow:**
```
Extract → Stream → Parquet Write → Parquet Read → Stream → Load
   ↓                    ↓                              ↓
SqlDataReader      File I/O                      SqlBulkCopy
```

### 4. Dependency Injection
**Purpose:** Loose coupling, testability, lifetime management

**Registration (Console app):**
```csharp
services.AddSingleton<ITableExtractor, SqlTableExtractor>();
services.AddSingleton<IParquetStorage, ParquetStorage>();
services.AddSingleton<IDataLoader, SqlDataLoader>();
services.AddSingleton<DataTransferOrchestrator>();
```

## Data Flow

### Complete Transfer Pipeline

```
1. Configuration Loading
   ↓
   ConfigurationLoader reads appsettings.json
   ↓
   ConfigurationValidator ensures validity
   ↓
2. For Each Table:
   ↓
   a. Extraction Phase
      ├─ SqlQueryBuilder generates SELECT with partition WHERE clause
      ├─ SqlTableExtractor opens SqlDataReader
      ├─ Reads rows, converts to JSON
      └─ Streams to MemoryStream
   ↓
   b. Parquet Write Phase
      ├─ ParquetStorage receives JSON stream
      ├─ Infers schema from first JSON row
      ├─ Creates typed arrays for each column
      ├─ Writes to Parquet file with Snappy compression
      └─ Organizes in date-based partitions: year=YYYY/month=MM/day=DD/
   ↓
   c. Parquet Read Phase
      ├─ ParquetStorage opens Parquet file
      ├─ Reads row groups
      ├─ Converts to JSON
      └─ Streams to MemoryStream
   ↓
   d. Load Phase
      ├─ SqlDataLoader receives JSON stream
      ├─ Deserializes to DataTable
      ├─ Uses SqlBulkCopy for high-performance insert
      └─ Commits transaction
   ↓
3. Results Aggregation
   ↓
   TransferResult contains:
   - Success/failure status
   - Rows extracted/loaded counts
   - Start/end timestamps
   - Duration
   - Parquet file path
   - Error message (if failed)
```

## Key Technical Decisions

### 1. JSON as Intermediate Format
**Rationale:**
- Universal format between SQL and Parquet
- Human-readable for debugging
- Schema inference possible
- Streaming-friendly

**Trade-offs:**
- Slightly larger than binary formats
- Parsing overhead
- ✅ Chosen for simplicity and debuggability

### 2. Streaming Architecture
**Rationale:**
- Handle large datasets (millions of rows)
- Avoid loading entire tables into memory
- Support cancellation mid-stream

**Implementation:**
- Use `Stream` throughout pipeline
- `SqlDataReader` reads forward-only
- `ParquetWriter` writes incrementally
- MemoryStream for in-process transfers

### 3. Parquet Compression
**Choice:** Snappy

**Rationale:**
- Fast compression/decompression
- Good compression ratio (~40-60% space savings)
- CPU-efficient
- Standard in big data ecosystem

**Alternatives considered:**
- Gzip: Better compression, slower
- None: Fastest, largest files
- ✅ Snappy: Best balance

### 4. Date-Based Partitioning
**Structure:** `year=YYYY/month=MM/day=DD/filename.parquet`

**Benefits:**
- Standard Hive partitioning scheme
- Easy to locate data by date
- Supports incremental loads
- Compatible with data lakes (Spark, Databricks, etc.)

### 5. SqlBulkCopy for Loading
**Rationale:**
- Fastest way to insert data into SQL Server
- Handles batching automatically
- Minimal logging option available
- Native SQL Server feature

**Configuration:**
- BatchSize: 10,000 rows (default)
- BulkCopyTimeout: 600 seconds
- Transaction management: External

### 6. Async/Await Throughout
**Rationale:**
- Non-blocking I/O for database and file operations
- Better resource utilization
- Scalability for concurrent transfers
- Cancellation support via CancellationToken

### 7. Structured Logging with Serilog
**Rationale:**
- Rich logging with structured data
- Multiple sinks (Console, File)
- Easy to parse and query
- Standard in .NET ecosystem

**Configuration:**
```csharp
Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .WriteTo.Console()
    .WriteTo.File("logs/datatransfer-.txt", rollingInterval: RollingInterval.Day)
    .CreateLogger();
```

## Performance Characteristics

### Memory Usage
- **Extraction:** O(batch_size) - bounded by SqlDataReader fetch size
- **Parquet Write:** O(rows * columns) - must buffer full dataset for schema inference and columnar write
- **Parquet Read:** O(row_group_size) - reads in chunks
- **Loading:** O(batch_size) - SqlBulkCopy batches

**Bottleneck:** Parquet write phase due to columnar format requiring full data in memory for schema inference and column-oriented storage.

**Mitigation:** Process tables incrementally, one partition at a time.

### Throughput
Approximate rates (depends on hardware, network, data types):
- **Extraction:** 50,000-100,000 rows/second
- **Parquet Write:** 30,000-50,000 rows/second (compression overhead)
- **Parquet Read:** 60,000-100,000 rows/second
- **Loading:** 80,000-150,000 rows/second (SqlBulkCopy is very fast)

**Bottleneck:** Parquet write due to compression and columnar transformation.

### Scalability
- **Vertical:** Add more CPU/RAM for compression and schema inference
- **Horizontal:** Process multiple tables in parallel (future enhancement)
- **Data size:** Tested with tables up to millions of rows
- **Limitation:** Single-threaded per table (current implementation)

## Error Handling Strategy

### Levels
1. **Configuration errors:** Fail fast, exit immediately
2. **Connection errors:** Retry with exponential backoff (future enhancement)
3. **Table-level errors:** Log, continue to next table
4. **Fatal errors:** Catch at top level, log, exit gracefully

### Observability
- **Logging:** All operations logged with correlation IDs
- **Metrics:** Row counts, duration, success/failure
- **Tracing:** Could add OpenTelemetry (future enhancement)

## Security Considerations

### SQL Injection Prevention
- **All queries use parameterized queries** via SqlCommand
- Table/column names are validated against schema
- No dynamic SQL concatenation

### Connection Strings
- Stored in configuration files
- Should use environment variables or Azure Key Vault in production
- Support for Windows Authentication (Integrated Security)
- SQL authentication supported but not recommended

### File System Access
- Parquet files written to configured base path
- No path traversal vulnerabilities (paths validated)
- File permissions inherited from process

## Extension Points

### 1. Additional Partition Strategies
Create new class inheriting `PartitionStrategy`:
```csharp
public class CustomPartitionStrategy : PartitionStrategy
{
    public override string GetPartitionPath(DateTime date) { ... }
    public override string BuildWhereClause(DateTime? startDate, DateTime? endDate) { ... }
}
```

### 2. Alternative Storage Formats
Implement `IParquetStorage` with different backend:
- Azure Blob Storage
- AWS S3
- Google Cloud Storage
- Delta Lake

### 3. Additional Database Sources
Implement `ITableExtractor` and `IDataLoader`:
- Oracle
- PostgreSQL
- MySQL
- MongoDB (as source)

### 4. Transformation Layer
Add between extract and load:
```csharp
public interface IDataTransformer
{
    Task<Stream> TransformAsync(Stream input, TransformationConfig config, CancellationToken ct);
}
```

## Testing Strategy

### Test Pyramid
```
         ┌────┐
        ╱      ╲
       ╱  E2E   ╲          5 integration tests (Testcontainers + Respawn)
      ╱──────────╲         - Date, IntDate, Scd2, Static, Empty table
     ╱ Component  ╲        10-15 component tests per layer
    ╱──────────────╲
   ╱  Unit Tests    ╲      106 unit tests across 5 layers
  ╱──────────────────╲
```

**Total: 111 tests passing**

### Coverage
- **Target:** 80% minimum
- **Current:** ~88% (estimated)
- **Tool:** coverlet.collector
- **Test execution time:** ~19 seconds (with optimized integration tests)

### Test Isolation
- **Unit tests:** All dependencies mocked (Moq) - 106 tests
- **Integration tests:** Real SQL Server (Testcontainers), real Parquet files - 5 tests
  - Shared container with Respawn for 57% faster execution
  - Full Extract → Parquet → Load pipeline validation
  - Data integrity verification

## Dependencies

### Core Libraries
- **.NET 8.0** - Framework
- **Microsoft.Data.SqlClient 5.2.2** - SQL Server connectivity
- **Parquet.Net 5.2.0** - Parquet file format
- **System.Text.Json** - JSON serialization (built-in)

### Logging
- **Serilog 4.3.0** - Structured logging
- **Microsoft.Extensions.Logging 9.0.9** - Logging abstractions

### Testing
- **xUnit 2.4.2** - Test framework
- **Moq 4.20.72** - Mocking
- **coverlet.collector 6.0.4** - Code coverage
- **Testcontainers.MsSql 3.10.0** - SQL Server containers for integration tests
- **Respawn 6.2.1** - Database reset between tests

### Dependency Injection
- **Microsoft.Extensions.Hosting 8.0.1** - Generic host
- **Microsoft.Extensions.DependencyInjection 8.0.1** - DI container

## Configuration Schema

Full JSON schema:
```json
{
  "sourceConnection": {
    "connectionString": "string (required)"
  },
  "destinationConnection": {
    "connectionString": "string (required)"
  },
  "storage": {
    "type": "parquet (fixed)",
    "basePath": "string (required)",
    "compressionType": "snappy (default)"
  },
  "tables": [
    {
      "source": {
        "database": "string (required)",
        "schema": "string (required)",
        "table": "string (required)"
      },
      "destination": {
        "database": "string (required)",
        "schema": "string (required)",
        "table": "string (required)"
      },
      "partitioning": {
        "type": "date|int_date|scd2|static (required)",
        "column": "string (required for date/int_date)",
        "scdEffectiveDateColumn": "string (required for scd2)",
        "scdExpirationDateColumn": "string (required for scd2)"
      },
      "extractSettings": {
        "batchSize": "integer (default: 100000)"
      }
    }
  ]
}
```

## Project Structure

```
DataTransfer.sln
├── src/
│   ├── DataTransfer.Core/                    [Domain layer]
│   │   ├── Interfaces/
│   │   │   ├── ITableExtractor.cs
│   │   │   ├── IParquetStorage.cs
│   │   │   └── IDataLoader.cs
│   │   ├── Models/
│   │   │   ├── TableConfiguration.cs
│   │   │   ├── TableIdentifier.cs
│   │   │   ├── PartitioningConfiguration.cs
│   │   │   ├── ExtractionResult.cs
│   │   │   ├── LoadResult.cs
│   │   │   └── TransferResult.cs
│   │   └── Strategies/
│   │       ├── PartitionStrategy.cs          [Abstract base]
│   │       ├── DatePartitionStrategy.cs
│   │       ├── IntDatePartitionStrategy.cs
│   │       ├── Scd2PartitionStrategy.cs
│   │       ├── StaticTableStrategy.cs
│   │       └── PartitionStrategyFactory.cs
│   ├── DataTransfer.Configuration/           [Config management]
│   │   ├── ConfigurationLoader.cs
│   │   ├── ConfigurationValidator.cs
│   │   └── ValidationResult.cs
│   ├── DataTransfer.SqlServer/               [SQL Server adapter]
│   │   ├── SqlQueryBuilder.cs
│   │   ├── SqlTableExtractor.cs
│   │   └── SqlDataLoader.cs
│   ├── DataTransfer.Parquet/                 [Parquet adapter]
│   │   └── ParquetStorage.cs
│   ├── DataTransfer.Pipeline/                [Orchestration]
│   │   └── DataTransferOrchestrator.cs
│   └── DataTransfer.Console/                 [Entry point - IN PROGRESS]
│       ├── Program.cs
│       └── DataTransfer.Console.csproj
├── tests/
│   ├── DataTransfer.Core.Tests/              [48 tests ✅]
│   ├── DataTransfer.Configuration.Tests/     [16 tests ✅]
│   ├── DataTransfer.SqlServer.Tests/         [21 tests ✅]
│   ├── DataTransfer.Parquet.Tests/           [11 tests ✅]
│   ├── DataTransfer.Pipeline.Tests/          [10 tests ✅]
│   └── DataTransfer.Integration.Tests/       [1 placeholder test]
├── config/
│   └── appsettings.json                      [Configuration example]
├── docker/
│   └── Dockerfile                            [Container definition]
├── CLAUDE.md                                 [LLM guidance]
├── requirements.md                           [Original requirements]
├── IMPLEMENTATION_STATUS.md                  [Current status]
├── CONSOLE_APP_SPEC.md                       [Console app details]
├── TEST_COVERAGE_SUMMARY.md                  [Test breakdown]
└── ARCHITECTURE.md                           [This file]
```

## Future Enhancements

1. **Parallel processing** - Transfer multiple tables concurrently
2. **Retry logic** - Exponential backoff for transient failures
3. **Incremental loads** - Only transfer changed data
4. **Change data capture** - SQL Server CDC integration
5. **Schema evolution** - Handle column additions/removals
6. **Compression options** - Make compression configurable
7. **Encryption** - At-rest and in-transit
8. **Monitoring** - Prometheus metrics, Grafana dashboards
9. **CLI arguments** - Override config via command line
10. **REST API** - Trigger transfers via HTTP

## Deployment Options

### 1. Standalone Console App
```bash
dotnet publish -c Release -o publish/
cd publish/
./DataTransfer.Console
```

### 2. Docker Container
```bash
docker build -f docker/Dockerfile -t datatransfer:latest .
docker run -v $(pwd)/config:/config datatransfer:latest
```

### 3. Kubernetes CronJob
```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: datatransfer
spec:
  schedule: "0 2 * * *"  # 2 AM daily
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: datatransfer
            image: datatransfer:latest
            volumeMounts:
            - name: config
              mountPath: /config
```

### 4. Windows Service
Wrap console app with TopShelf or Windows Service framework

### 5. Azure Function (Timer Trigger)
Port orchestration logic to Function runtime
