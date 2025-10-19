# Command Reference: Documentation to Implementation Mapping

This document maps all commands referenced in the project documentation to their actual implementations in the codebase.

**Last Updated:** 2025-10-18

---

## Table of Contents

1. [Console Application Commands](#console-application-commands)
2. [Core Services](#core-services)
3. [Web UI Features](#web-ui-features)
4. [Demo Scripts](#demo-scripts)
5. [Build & Test Commands](#build--test-commands)

---

## Console Application Commands

All console commands are implemented in **`src/DataTransfer.Console/Program.cs`**

### Interactive Mode

**Documentation Reference:** README.md:177-179, QUICK_START.md:326-342, GETTING_STARTED.md:326-342

**Command:**
```bash
dotnet run --project src/DataTransfer.Console
```

**Implementation:**
- **Entry Point:** `Program.cs:30-35` - Detects no arguments and calls `RunInteractiveModeAsync()`
- **Method:** `Program.cs:91-136` - `RunInteractiveModeAsync(IServiceProvider services)`
- **Menu Options:**
  - Option 1: Run from config file → `RunFromConfigFileAsync()` (line 138-222)
  - Option 2: Load saved profile → `RunFromProfileAsync()` (line 224-316)
  - Option 3: List all profiles → `ListProfilesAsync()` (line 318-347)
  - Option 4: Exit

---

### Command-Line Arguments

**Documentation Reference:** README.md:188-260, CONSOLE_APP_SPEC.md, GETTING_STARTED.md:344-348

#### `--profile <name>` - Run Saved Profile

**Command:**
```bash
dotnet run --project src/DataTransfer.Console -- --profile "Daily Orders Extract"
```

**Implementation:**
- **Entry Point:** `Program.cs:349-502` - `RunCommandLineModeAsync()`
- **Argument Parsing:** `Program.cs:366-371` - Parses `--profile` flag
- **Execution:** `Program.cs:414-440` - Loads profile and executes transfer
- **Service Used:** `TransferProfileService` (src/DataTransfer.Configuration/Services/TransferProfileService.cs)
- **Orchestrator Used:** `UnifiedTransferOrchestrator` (src/DataTransfer.Pipeline/UnifiedTransferOrchestrator.cs)

**Profile Storage Location:** `./profiles/profiles.json`

---

#### `--config <path>` - Run from Config File (Legacy)

**Command:**
```bash
dotnet run --project src/DataTransfer.Console -- --config config/appsettings.json
```

**Implementation:**
- **Entry Point:** `Program.cs:349-502` - `RunCommandLineModeAsync()`
- **Argument Parsing:** `Program.cs:373-379` - Parses `--config` flag
- **Execution:** `Program.cs:441-496` - Loads legacy config and transfers tables
- **Services Used:**
  - `ConfigurationLoader` (src/DataTransfer.Configuration/)
  - `ConfigurationValidator` (src/DataTransfer.Configuration/)
  - `DataTransferOrchestrator` (src/DataTransfer.Pipeline/DataTransferOrchestrator.cs)

**Also Available in Interactive Mode:**
- **Method:** `Program.cs:138-222` - `RunFromConfigFileAsync()`

---

#### `--list-profiles` - List All Profiles

**Command:**
```bash
dotnet run --project src/DataTransfer.Console -- --list-profiles
```

**Implementation:**
- **Entry Point:** `Program.cs:349-502` - `RunCommandLineModeAsync()`
- **Argument Parsing:** `Program.cs:394-396` - Parses `--list-profiles` flag
- **Execution:** `Program.cs:403-407` - Calls `ListProfilesAsync()`
- **Display Method:** `Program.cs:318-347` - `ListProfilesAsync(TransferProfileService)`
- **Service Used:** `TransferProfileService.GetAllProfilesAsync()`

**Also Available in Interactive Mode:** Menu Option 3

---

#### `--environment <name>` - Use Environment-Specific Settings

**Documentation Reference:** config/environments.example.json

**Command:**
```bash
# Run profile with production environment
dotnet run --project src/DataTransfer.Console -- \
  --profile "Daily Orders Extract" --environment prod

# Environment settings are loaded from config/environments.json
```

**Implementation:**
- **Entry Point:** `Program.cs:349-502` - `RunCommandLineModeAsync()`
- **Argument Parsing:** `Program.cs:423-429` - Parses `--environment` flag
- **Loading Method:** `Program.cs:1040-1075` - `LoadEnvironmentAsync()`
- **Application Method:** `Program.cs:1077-1100` - `ApplyEnvironmentToConfiguration()`
- **Usage:** `Program.cs:560-565` - Applied to profile configurations

**Environment File Location:** `config/environments.json`

**Environment Configuration Format:**
```json
{
  "environments": [
    {
      "name": "prod",
      "variables": {
        "SourceConnectionString": "Server=prod-server;Database=SourceDB;...",
        "DestinationConnectionString": "Server=prod-server;Database=DestDB;...",
        "ParquetPath": "/data/prod/parquet-files"
      }
    }
  ]
}
```

**Token Replacement:**
Supports `${env:VariableName}` token syntax in:
- Source connection strings
- Destination connection strings
- Parquet file paths

**Example Profile with Tokens:**
```json
{
  "configuration": {
    "source": {
      "connectionString": "${env:SourceConnectionString}"
    },
    "destination": {
      "parquetPath": "${env:ParquetPath}"
    }
  }
}
```

**Token Processing:**
- **Service:** `EnvironmentManager` (src/DataTransfer.Configuration/EnvironmentManager.cs)
- **Method:** `ReplaceTokens()` - Uses regex pattern `\$\{env:([^}]+)\}`
- **Error Handling:** Throws `InvalidOperationException` if variable not found

**Tests:** `tests/DataTransfer.Configuration.Tests/EnvironmentConfigurationTests.cs`

---

#### `--discover <connection-string>` - Schema Discovery

**Documentation Reference:** README.md:198-243, GETTING_STARTED.md:248-283

**Command:**
```bash
# Discover entire database
dotnet run --project src/DataTransfer.Console -- \
  --discover "Server=localhost;Database=MyDB;Integrated Security=true;TrustServerCertificate=true"

# Discover specific table
dotnet run --project src/DataTransfer.Console -- \
  --discover "Server=localhost;..." --table dbo.Orders
```

**Implementation:**
- **Entry Point:** `Program.cs:349-502` - `RunCommandLineModeAsync()`
- **Argument Parsing:**
  - `Program.cs:380-386` - Parses `--discover` flag
  - `Program.cs:387-393` - Parses optional `--table` flag
- **Execution:** `Program.cs:409-412` - Calls `RunSchemaDiscoveryAsync()`
- **Discovery Method:** `Program.cs:504-611` - `RunSchemaDiscoveryAsync()`
- **Service Used:** `SqlSchemaDiscovery` (src/DataTransfer.SqlServer/SqlSchemaDiscovery.cs)

**Key Features:**
- **Connection Test:** Line 518-525 - Tests database connectivity
- **Table Discovery:** Line 528-566 - Discovers specific table with suggestions for typos
- **Database Discovery:** Line 568-603 - Discovers all tables grouped by schema
- **Partition Suggestions:** Line 619-633, 675-688 - AI-powered partition strategy recommendations
- **Display Methods:**
  - `DisplayTableSummary()` - Line 613-634 - Shows table overview
  - `DisplayTableDetails()` - Line 636-689 - Shows detailed column information

**Core Discovery Service:**
- **Class:** `SqlSchemaDiscovery` (src/DataTransfer.SqlServer/SqlSchemaDiscovery.cs)
- **Methods:**

---

#### `--export-iceberg <connection> <table>` - Export to Iceberg

**Documentation Reference:** docs/ICEBERG_QUICKSTART.md

**Command:**
```bash
dotnet run --project src/DataTransfer.Console -- \
  --export-iceberg "Server=localhost;Database=MyDb;..." "dbo.Customers" \
  --iceberg-name customers_export
```

**Implementation:**
- **Entry Point:** `Program.cs:349-502` - `RunCommandLineModeAsync()`
- **Argument Parsing:**
  - `Program.cs:439-445` - Parses `--export-iceberg` with connection string and table
  - `Program.cs:446-452` - Parses optional `--iceberg-name` flag
- **Execution:** `Program.cs:515` - Calls `RunExportIcebergAsync()`
- **Export Method:** `Program.cs:814-876` - `RunExportIcebergAsync()`
- **Transfer Type:** `TransferType.SqlToIceberg`
- **Services Used:**
  - `UnifiedTransferOrchestrator.ExecuteTransferAsync()`
  - `SqlServerToIcebergExporter` (src/DataTransfer.Iceberg/Integration/)

**Result:** Creates Iceberg table in `./iceberg-warehouse/{table-name}/`

---

#### `--import-iceberg <table> <connection> <destination>` - Import from Iceberg

**Documentation Reference:** docs/ICEBERG_QUICKSTART.md

**Command:**
```bash
dotnet run --project src/DataTransfer.Console -- \
  --import-iceberg customers_export \
  "Server=localhost;Database=TargetDb;..." "dbo.Customers"
```

**Implementation:**
- **Entry Point:** `Program.cs:349-502` - `RunCommandLineModeAsync()`
- **Argument Parsing:**
  - `Program.cs:453-461` - Parses `--import-iceberg` with table, connection, and destination
- **Execution:** `Program.cs:520` - Calls `RunImportIcebergAsync()`
- **Import Method:** `Program.cs:878-940` - `RunImportIcebergAsync()`
- **Transfer Type:** `TransferType.IcebergToSql`
- **Services Used:**
  - `UnifiedTransferOrchestrator.ExecuteTransferAsync()`
  - `IcebergReader` (src/DataTransfer.Iceberg/Readers/)
  - `SqlServerImporter` (src/DataTransfer.Iceberg/Integration/)

**Result:** Imports Iceberg snapshot to SQL Server using UPSERT merge

---

#### `--sync-iceberg <args>` - Incremental Synchronization

**Documentation Reference:** docs/ICEBERG_QUICKSTART.md

**Command:**
```bash
dotnet run --project src/DataTransfer.Console -- \
  --sync-iceberg \
  "Server=source;Database=SourceDb;..." "dbo.Orders" \
  orders_sync \
  "Server=target;Database=TargetDb;..." "dbo.Orders" \
  --primary-key OrderId \
  --watermark UpdatedAt \
  --merge-strategy upsert
```

**Implementation:**
- **Entry Point:** `Program.cs:349-502` - `RunCommandLineModeAsync()`
- **Argument Parsing:**
  - `Program.cs:462-472` - Parses `--sync-iceberg` with 5 arguments (source connection, source table, iceberg table, target connection, target table)
  - `Program.cs:473-479` - Parses `--primary-key` (required)
  - `Program.cs:480-486` - Parses `--watermark` (required)
  - `Program.cs:487-494` - Parses optional `--merge-strategy` (default: upsert)
- **Validation:** `Program.cs:527-531` - Ensures primary-key and watermark are provided
- **Execution:** `Program.cs:532-534` - Calls `RunSyncIcebergAsync()`
- **Sync Method:** `Program.cs:942-1030` - `RunSyncIcebergAsync()`
- **Transfer Type:** `TransferType.SqlToIcebergIncremental`
- **Services Used:**
  - `UnifiedTransferOrchestrator.ExecuteTransferAsync()`
  - `IncrementalSyncCoordinator` (src/DataTransfer.Iceberg/Integration/)
  - `FileWatermarkStore` (src/DataTransfer.Iceberg/Watermarks/)
  - `TimestampChangeDetection` (src/DataTransfer.Iceberg/ChangeDetection/)

**Watermark Storage:** `.watermarks/{iceberg-table-name}.json`

**How It Works:**
1. **First Run:** Full export from source → Iceberg → Import to target
2. **Subsequent Runs:** Incremental export (filtered by watermark) → Iceberg → Merge to target
3. **Watermark Update:** Stores last synced watermark value for next run

**Merge Strategies:**
- `upsert` (default): INSERT new rows, UPDATE existing (based on primary key)
- `append`: INSERT only, skip existing rows

---

### Methods:
  - `DiscoverDatabaseAsync()` - Line 28-43 - Full database discovery
  - `DiscoverTableAsync()` - Line 49+ - Single table discovery
  - `TestConnectionAsync()` - Tests database connection

**Documentation:** See docs/SCHEMA_DISCOVERY_TEST_GUIDE.md for testing details

---

#### `--help` - Show Help

**Command:**
```bash
dotnet run --project src/DataTransfer.Console -- --help
```

**Implementation:**
- **Entry Point:** `Program.cs:349-502` - `RunCommandLineModeAsync()`
- **Argument Parsing:** `Program.cs:397-400` - Parses `--help` flag
- **Display Method:** `Program.cs:691-715` - `ShowHelp()`

---

### `--table <schema.name>` - Table Filter (with --discover)

**Command:**
```bash
dotnet run --project src/DataTransfer.Console -- \
  --discover "..." --table dbo.Orders
```

**Implementation:**
- **Argument Parsing:** `Program.cs:387-393`
- **Usage:** `Program.cs:528-566` - Used within `RunSchemaDiscoveryAsync()`
- **Table Name Validation:** Requires format `schema.tablename`
- **Suggestions:** Line 549-560 - Shows similar table names if not found

---

## Core Services

### TransferProfileService

**Documentation Reference:** README.md:7-11, PHASE1_IMPLEMENTATION_PLANS.md

**Implementation:** `src/DataTransfer.Configuration/Services/TransferProfileService.cs`

**Purpose:** Manages transfer profiles (templates) with file-based storage

**Key Methods:**
- `SaveProfileAsync()` - Line 41+ - Saves new profile
- `GetAllProfilesAsync()` - Retrieves all profiles
- `GetProfileByIdAsync()` - Gets specific profile
- `UpdateProfileAsync()` - Updates existing profile
- `DeleteProfileAsync()` - Deletes profile

**Storage:**
- **Directory:** `./profiles/` (configurable via constructor)
- **File:** `profiles.json`
- **Format:** JSON with indentation
- **Thread-Safety:** Uses `SemaphoreSlim` for file locking

**Used By:**
- Console: Interactive mode (Option 2) and `--profile` command
- Web UI: Profile management pages

**Tests:** `tests/DataTransfer.Web.Tests/Services/TransferProfileServiceTests.cs`

---

### UnifiedTransferOrchestrator

**Documentation Reference:** README.md:8, BIDIRECTIONAL_TRANSFER_PLAN.md

**Implementation:** `src/DataTransfer.Pipeline/UnifiedTransferOrchestrator.cs`

**Purpose:** Orchestrates all types of data transfers: SQL→Parquet, Parquet→SQL, SQL→SQL

**Key Method:**
- `ExecuteTransferAsync()` - Line 35+ - Main transfer execution

**Transfer Types Supported:**
- `TransferType.SqlToParquet` - SQL Server → Parquet files
- `TransferType.ParquetToSql` - Parquet files → SQL Server
- `TransferType.SqlToSql` - SQL Server → SQL Server (via Parquet)

**Dependencies:**
- `ITableExtractor` - SQL extraction
- `IParquetExtractor` - Parquet reading
- `IDataLoader` - SQL loading
- `IParquetWriter` - Parquet writing

**Used By:**
- Console: Profile-based execution (`--profile`)
- Web UI: New transfer feature

**Tests:** `tests/DataTransfer.Pipeline.Tests/UnifiedTransferOrchestratorTests.cs`

---

### DataTransferOrchestrator (Legacy)

**Documentation Reference:** CONSOLE_APP_SPEC.md:90-137, QUICK_START.md:101-106

**Implementation:** `src/DataTransfer.Pipeline/DataTransferOrchestrator.cs`

**Purpose:** Legacy orchestrator for SQL→Parquet→SQL transfers (config file mode)

**Key Method:**
- `TransferTableAsync()` - Line 27+ - Transfers single table

**Workflow:**
1. Extract from SQL Server
2. Write to Parquet
3. Read from Parquet
4. Load to SQL Server

**Used By:**
- Console: Config file mode (`--config`)
- Legacy automation scripts

**Tests:** `tests/DataTransfer.Pipeline.Tests/DataTransferOrchestratorTests.cs`

---

### SqlSchemaDiscovery

**Documentation Reference:** README.md:22-28, 198-243, docs/SCHEMA_DISCOVERY_TEST_GUIDE.md

**Implementation:** `src/DataTransfer.SqlServer/SqlSchemaDiscovery.cs`

**Purpose:** Discovers SQL Server database schema and suggests optimal partition strategies

**Key Methods:**
- `DiscoverDatabaseAsync()` - Line 28+ - Discovers all tables
- `DiscoverTableAsync()` - Line 49+ - Discovers specific table
- `TestConnectionAsync()` - Tests database connection

**Features:**
- Table and column metadata extraction
- Row count analysis
- Partition strategy suggestions with confidence scores (60%-90%)
- SCD2 pattern detection
- Date/DateTime column identification
- Integer date format detection (YYYYMMDD)
- Table name suggestions for typos

**Used By:**
- Console: `--discover` command

**Tests:**
- Unit: `tests/DataTransfer.SqlServer.Tests/SqlSchemaDiscoveryTests.cs`
- Integration: `tests/DataTransfer.SqlServer.Tests/SqlSchemaDiscoveryIntegrationTests.cs`

---

## Web UI Features

**Documentation Reference:** README.md:159-171, QUICK_START.md:58-59

### Run Web UI

**Command:**
```bash
dotnet run --project src/DataTransfer.Web
# Navigate to http://localhost:5000
```

**Implementation:** `src/DataTransfer.Web/Program.cs`

**Features:**
- **New Transfer Page:** `src/DataTransfer.Web/Components/Pages/NewTransfer.razor`
  - SQL→Parquet transfers
  - Parquet→SQL imports
  - Cascading dropdowns for database/schema/table navigation
- **Transfer History:** View past transfers with status
- **Profile Management:** Create and manage transfer profiles
- **Dynamic Table Selection:** Auto-populate from connected databases

**Service:** `src/DataTransfer.Web/Services/TransferExecutionService.cs`

**Tests:** `tests/DataTransfer.Web.Tests/` - 20+ Playwright E2E tests

---

## Demo Scripts

**Documentation Reference:** GETTING_STARTED.md:465-475, demo/README.md

### SQL Server Setup

**Script:** `demo/00-setup-sqlserver-docker.sh`

**Purpose:** Sets up SQL Server in Docker container

**Creates:**
- Container: `sqlserver-iceberg-demo`
- Server: `localhost,1433`
- User: `sa`
- Password: `IcebergDemo@2024`

**Usage:**
```bash
./demo/00-setup-sqlserver-docker.sh
```

---

### Database Setup

**Script:** `demo/01-setup-demo-databases.sql`

**Purpose:** Creates demo databases and sample data

**Creates:**
- Database: `IcebergDemo_Source`
- Database: `IcebergDemo_Target`
- Tables: Customers, Orders, Products (10 rows each)

**Usage:**
```bash
docker exec sqlserver-iceberg-demo /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P "IcebergDemo@2024" -C \
  -i /tmp/01-setup-demo-databases.sql
```

**Documentation:** GETTING_STARTED.md:220-246

---

### Full Demo Scripts

**Documentation Reference:** GETTING_STARTED.md:465-475

**Scripts:**
- `./demo/run-demo.sh` - Full end-to-end Parquet demo
- `./demo/run-iceberg-demo.sh` - Iceberg export demo (see demo/ICEBERG_DEMO_README.md)
- `./demo/run-bidirectional-demo.sh` - Bidirectional transfer demo (see demo/INCREMENTAL_SYNC_README.md)

---

## Build & Test Commands

**Documentation Reference:** README.md:69-76, 289-319, QUICK_START.md:43-66, CLAUDE.md:37-63

### Build Commands

```bash
# Build entire solution
dotnet build

# Build specific project
dotnet build src/DataTransfer.Core

# Clean and rebuild
dotnet clean && dotnet build
```

**Implementation:** Standard .NET CLI commands

**Solution File:** `DataTransfer.sln`

---

### Test Commands

```bash
# Run all tests
dotnet test

# Run tests with coverage
dotnet test /p:CollectCoverage=true /p:CoverageMinimum=80

# Run specific test project
dotnet test tests/DataTransfer.Core.Tests

# Run single test
dotnet test --filter "FullyQualifiedName~TableConfiguration_Should_Parse_Valid_Json"

# Run Playwright E2E tests
dotnet test tests/DataTransfer.Web.Tests
```

**Implementation:** Standard .NET CLI test runner

**Test Projects:**
- `tests/DataTransfer.Core.Tests/` - 48 unit tests
- `tests/DataTransfer.Configuration.Tests/` - 16 unit tests
- `tests/DataTransfer.SqlServer.Tests/` - 21 unit tests
- `tests/DataTransfer.Parquet.Tests/` - 11 unit tests
- `tests/DataTransfer.Pipeline.Tests/` - 10 unit tests
- `tests/DataTransfer.Console.Tests/` - Console integration tests
- `tests/DataTransfer.Integration.Tests/` - 5 E2E tests with Testcontainers
- `tests/DataTransfer.Web.Tests/` - 20+ Playwright E2E tests

**Coverage Target:** 80%+ (enforced via coverlet)

**Documentation:** README.md:406-425, TEST_COVERAGE_SUMMARY.md

---

### Benchmark Commands

**Documentation Reference:** README.md:428-438

```bash
# Run benchmarks (BenchmarkDotNet)
dotnet run --project tests/DataTransfer.Benchmarks --configuration Release
```

**Implementation:** `tests/DataTransfer.Benchmarks/`

**Benchmark Suites:**
- `QueryBuildingBenchmarks` - SQL query generation performance
- `EndToEndBenchmarks` - Full transfer pipeline performance

**Results Location:** `BenchmarkDotNet.Artifacts/results/`

---

### Docker Commands

**Documentation Reference:** README.md:263-287

```bash
# Build Docker image
docker build -f docker/Dockerfile -t datatransfer:latest .

# Run container
docker run \
  -v $(pwd)/config:/config \
  -v $(pwd)/output:/parquet-output \
  -v $(pwd)/logs:/logs \
  datatransfer:latest
```

**Implementation:** `docker/Dockerfile`

**Image Details:**
- Base: `mcr.microsoft.com/dotnet/runtime:8.0`
- Size: 365MB
- User: `datatransfer` (non-root, UID 1001)

---

## Command Summary Table

| Command | Implementation File | Line(s) | Purpose |
|---------|-------------------|---------|---------|
| `--profile` | Program.cs | 366-371, 414-440 | Run saved profile |
| `--config` | Program.cs | 373-379, 441-496 | Run from config file |
| `--environment` | Program.cs | 423-429, 560-565, 1040-1100 | Use environment-specific settings |
| `--list-profiles` | Program.cs | 394-396, 403-407 | List all profiles |
| `--discover` | Program.cs | 380-386, 409-412, 504-611 | Schema discovery |
| `--table` | Program.cs | 387-393, 528-566 | Table filter for discovery |
| `--help` | Program.cs | 397-400, 691-715 | Show help |
| Interactive Mode | Program.cs | 30-35, 91-136 | Menu-driven interface |

---

## Service Implementation Map

| Service | Implementation | Line(s) | Used By |
|---------|---------------|---------|---------|
| TransferProfileService | Configuration/Services/TransferProfileService.cs | 12+ | Console, Web UI |
| UnifiedTransferOrchestrator | Pipeline/UnifiedTransferOrchestrator.cs | 10+ | Profile execution |
| DataTransferOrchestrator | Pipeline/DataTransferOrchestrator.cs | 8+ | Legacy config mode |
| SqlSchemaDiscovery | SqlServer/SqlSchemaDiscovery.cs | 9+ | `--discover` command |
| EnvironmentManager | Configuration/EnvironmentManager.cs | 9+ | `--environment` flag |
| ConfigurationLoader | Configuration/ | - | Config file loading |
| ConfigurationValidator | Configuration/ | - | Config validation |

---

## Quick Reference

### Most Common Commands

```bash
# Interactive mode (recommended for new users)
dotnet run --project src/DataTransfer.Console

# Run saved profile (automation)
dotnet run --project src/DataTransfer.Console -- --profile "My Profile"

# Run profile with environment-specific settings
dotnet run --project src/DataTransfer.Console -- --profile "My Profile" --environment prod

# Discover database schema
dotnet run --project src/DataTransfer.Console -- \
  --discover "Server=localhost;Database=MyDB;Integrated Security=true;TrustServerCertificate=true"

# Web UI (interactive transfers)
dotnet run --project src/DataTransfer.Web

# Run tests
dotnet test
```

---

## Related Documentation

- **README.md** - Main project documentation
- **GETTING_STARTED.md** - Step-by-step setup guide
- **QUICK_START.md** - Quick reference for LLM context
- **CONSOLE_APP_SPEC.md** - Console application specification
- **ARCHITECTURE.md** - Technical architecture details
- **docs/SCHEMA_DISCOVERY_TEST_GUIDE.md** - Schema discovery testing guide
- **demo/README.md** - Demo scripts documentation

---

**Maintained by:** Claude Code
**Repository:** DataTransfer
**Version:** As of commit with schema discovery feature
