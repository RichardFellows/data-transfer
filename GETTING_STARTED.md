# Getting Started: Two Database Setup

This guide walks you through setting up DataTransfer to sync data between two SQL Server databases using Parquet/Iceberg as intermediate storage.

**Estimated Time:** 30-45 minutes (including SQL Server Docker setup)

---

## Table of Contents
1. [Prerequisites](#prerequisites)
2. [Verify Your Environment](#verify-your-environment)
3. [Setup SQL Server](#setup-sql-server)
4. [Configure DataTransfer](#configure-datatransfer)
5. [Run Your First Transfer](#run-your-first-transfer)
6. [Verify Results](#verify-results)
7. [Next Steps](#next-steps)

---

## Prerequisites

### Required Software

| Component | Version | Purpose | Installation |
|-----------|---------|---------|--------------|
| **.NET 8 SDK** | 8.0+ | Run DataTransfer | [Download](https://dotnet.microsoft.com/download/dotnet/8.0) |
| **SQL Server** | 2019+ | Source and destination databases | See options below |
| **Docker** | Latest | (Optional) Run SQL Server in container | [Get Docker](https://docs.docker.com/get-docker/) |

### System Requirements
- **Disk Space:** 10GB+ free for Parquet files and databases
- **RAM:** 4GB+ recommended (8GB+ for large transfers)
- **Network:** Access to both SQL Server instances (if remote)

---

## Verify Your Environment

### Step 1: Check .NET SDK

```bash
dotnet --version
```

**Expected Output:** `8.0.xxx` (any 8.0.x version)

**If not installed:**
- **Windows:** Download from [dotnet.microsoft.com](https://dotnet.microsoft.com/download/dotnet/8.0)
- **Linux:** `sudo apt-get install dotnet-sdk-8.0` (Ubuntu/Debian)
- **macOS:** `brew install dotnet-sdk`

### Step 2: Clone and Build DataTransfer

```bash
# Clone repository (if not already done)
git clone <repository-url>
cd DataTransfer

# Restore dependencies
dotnet restore

# Build solution
dotnet build

# Verify tests run (optional but recommended)
dotnet test --filter "FullyQualifiedName~Core" --no-build
```

**Expected Output for Build:**
```
Build succeeded.
DataTransfer.Console -> /path/to/bin/Debug/net8.0/DataTransfer.Console.dll
```

**Troubleshooting:**
- **Build Errors:** Ensure you're in the project root directory with `DataTransfer.sln`
- **Test Failures:** Some integration tests require SQL Server (skip with `--filter "Category!=Integration"`)

---

## Setup SQL Server

Choose ONE of these options based on your environment:

### Option A: Docker SQL Server (Recommended for Testing)

**Pros:** Quick setup, isolated, easy cleanup
**Cons:** Requires Docker

```bash
# Run automated setup script
./demo/00-setup-sqlserver-docker.sh

# This creates:
# - Container: sqlserver-iceberg-demo
# - Server: localhost,1433
# - User: sa
# - Password: IcebergDemo@2024
```

**Verify SQL Server is running:**
```bash
docker ps | grep sql
```

**Test connection:**
```bash
docker exec sqlserver-iceberg-demo /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P "IcebergDemo@2024" -Q "SELECT @@VERSION" -C
```

**Expected Output:**
```
Microsoft SQL Server 2022 (RTM) ...
```

---

### Option B: Existing SQL Server (Windows/Remote)

**For Windows with SQL Server installed:**

1. **Verify SQL Server is running:**
   ```powershell
   # Open Services (services.msc)
   # Look for "SQL Server (MSSQLSERVER)" - Status should be "Running"
   ```

2. **Enable SQL Server Authentication (if using SQL accounts):**
   - Open SQL Server Management Studio (SSMS)
   - Right-click server → Properties → Security
   - Select "SQL Server and Windows Authentication mode"
   - Restart SQL Server service

3. **Test connection:**
   ```cmd
   sqlcmd -S localhost -E -Q "SELECT @@VERSION"
   ```
   *(Use `-E` for Windows Auth, or `-U username -P password` for SQL Auth)*

---

### Option C: Azure SQL Database

1. **Create Azure SQL Database** via [Azure Portal](https://portal.azure.com)
2. **Configure firewall** to allow your IP address
3. **Note your connection details:**
   - Server: `yourserver.database.windows.net`
   - Database: `your-database`
   - User: `youradmin`
   - Password: `yourpassword`

---

## Configure DataTransfer

### Step 1: Create Configuration File

**Option A: Start from clean template (recommended)**
```bash
# Copy clean template to working config
cp config/appsettings.template.json config/appsettings.json
```

**Option B: Start from annotated example**
```bash
# Copy example config (contains helpful comments, but you'll need to remove them)
cp config/appsettings.EXAMPLE.json config/appsettings.json
# Then remove all lines starting with // as JSON doesn't support comments
```

**Note:** Use `appsettings.template.json` for a ready-to-use starting point, or `appsettings.EXAMPLE.json` if you want detailed explanations (but remove comment lines).

### Step 2: Edit Connection Strings

Edit `config/appsettings.json` with your preferred text editor:

**For Docker SQL Server (Option A):**
```json
{
  "connections": {
    "source": "Server=localhost,1433;Database=SalesSource;User Id=sa;Password=IcebergDemo@2024;TrustServerCertificate=true",
    "destination": "Server=localhost,1433;Database=SalesDestination;User Id=sa;Password=IcebergDemo@2024;TrustServerCertificate=true"
  },
  "storage": {
    "basePath": "./parquet-output"
  },
  "tables": []
}
```

**For Windows Authentication (Option B):**
```json
{
  "connections": {
    "source": "Server=localhost;Database=SourceDB;Integrated Security=true;TrustServerCertificate=true",
    "destination": "Server=localhost;Database=DestDB;Integrated Security=true;TrustServerCertificate=true"
  },
  "storage": {
    "basePath": "C:\\data\\parquet"
  },
  "tables": []
}
```

**For Azure SQL (Option C):**
```json
{
  "connections": {
    "source": "Server=tcp:yourserver.database.windows.net,1433;Database=SourceDB;User Id=youradmin;Password=yourpassword;Encrypt=True;",
    "destination": "Server=tcp:yourserver.database.windows.net,1433;Database=DestDB;User Id=youradmin;Password=yourpassword;Encrypt=True;"
  },
  "storage": {
    "basePath": "./parquet-output"
  },
  "tables": []
}
```

### Step 3: Create Databases and Sample Data

**Using Docker SQL Server:**
```bash
# Copy SQL setup script into container
docker cp demo/01-setup-demo-databases.sql sqlserver-iceberg-demo:/tmp/

# Execute script to create databases and tables
docker exec sqlserver-iceberg-demo /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P "IcebergDemo@2024" -C \
  -i /tmp/01-setup-demo-databases.sql
```

**Using Windows SQL Server:**
```powershell
sqlcmd -S localhost -E -i demo/01-setup-demo-databases.sql
```

**Expected Output:**
```
Demo Databases Setup Complete!
Source Database: IcebergDemo_Source
  - Customers: 10 records
  - Orders: 10 records
  - Products: 10 records
```

### Step 4: Configure Tables for Transfer

Update `config/appsettings.json` to define which tables to transfer. Here's a complete working example:

```json
{
  "connections": {
    "source": "Server=localhost,1433;Database=IcebergDemo_Source;User Id=sa;Password=IcebergDemo@2024;TrustServerCertificate=true",
    "destination": "Server=localhost,1433;Database=IcebergDemo_Target;User Id=sa;Password=IcebergDemo@2024;TrustServerCertificate=true"
  },
  "storage": {
    "basePath": "./parquet-output"
  },
  "tables": [
    {
      "source": {
        "database": "IcebergDemo_Source",
        "schema": "dbo",
        "table": "Customers"
      },
      "destination": {
        "database": "IcebergDemo_Target",
        "schema": "dbo",
        "table": "Customers"
      },
      "partitioning": {
        "type": "static"
      },
      "extractSettings": {
        "batchSize": 10000
      }
    }
  ]
}
```

---

## Run Your First Transfer

### Option 1: Interactive Mode (Recommended for Learning)

```bash
dotnet run --project src/DataTransfer.Console
```

**You'll see a menu:**
```
DataTransfer Console - Main Menu
================================
1. Load and run saved profile
2. Run from config file (legacy)
3. List all saved profiles
4. Exit
```

**Choose Option 2** and enter: `config/appsettings.json`

### Option 2: Command-Line Mode (For Automation)

```bash
dotnet run --project src/DataTransfer.Console -- --config config/appsettings.json
```

### Expected Output

```
[11:30:45 INF] Loading configuration from: config/appsettings.json
[11:30:45 INF] Starting transfer for table IcebergDemo_Source.dbo.Customers
[11:30:45 INF] Extracted 10 rows from IcebergDemo_Source.dbo.Customers
[11:30:45 INF] Writing data to Parquet file static/dbo_Customers_20241018113045.parquet
[11:30:46 INF] Successfully wrote data to Parquet file
[11:30:46 INF] Reading data from Parquet file static/dbo_Customers_20241018113045.parquet
[11:30:46 INF] Loading data to destination table IcebergDemo_Target.dbo.Customers
[11:30:46 INF] Loaded 10 rows to IcebergDemo_Target.dbo.Customers
[11:30:46 INF] Transfer completed successfully in 1234.56ms

=== Transfer Summary ===
Tables processed: 1
Total rows transferred: 10
Total duration: 1.23s
Status: Success
```

---

## Verify Results

### Step 1: Check Parquet Files Were Created

```bash
# List generated Parquet files
ls -lh parquet-output/static/

# Expected output:
# dbo_Customers_20241018113045.parquet  (size varies)
```

**On Windows:**
```cmd
dir parquet-output\static
```

### Step 2: Verify Data in Destination Database

**Docker SQL Server:**
```bash
docker exec -it sqlserver-iceberg-demo /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P "IcebergDemo@2024" -C \
  -d IcebergDemo_Target \
  -Q "SELECT COUNT(*) AS RowCount FROM dbo.Customers"
```

**Windows SQL Server:**
```cmd
sqlcmd -S localhost -E -d IcebergDemo_Target -Q "SELECT COUNT(*) FROM dbo.Customers"
```

**Expected Output:**
```
RowCount
-----------
10
```

### Step 3: Compare Source vs Destination

```sql
-- Run this query in both databases
SELECT
    CustomerID,
    FirstName,
    LastName,
    Email,
    Balance
FROM dbo.Customers
ORDER BY CustomerID;
```

**The data should match exactly between source and destination.**

---

## Next Steps

### Learn More Features

1. **Transfer Multiple Tables**
   Add more table configurations to `config/appsettings.json`

2. **Use Date Partitioning**
   For transactional tables with date columns - see `demo/config/demo-config.json` for examples

3. **Web UI Interface**
   Interactive transfers with visual feedback:
   ```bash
   dotnet run --project src/DataTransfer.Web
   # Navigate to http://localhost:5000
   ```

4. **Scheduled Transfers**
   Create transfer profiles and run via command line:
   ```bash
   dotnet run --project src/DataTransfer.Console -- --profile "Daily Export"
   ```

5. **Iceberg Integration**
   Export to Apache Iceberg format for data lake scenarios:
   ```bash
   ./demo/run-iceberg-demo.sh
   ```

### Explore Documentation

- **Full Configuration Options:** [README.md#configuration](README.md#configuration)
- **Partition Strategies:** [README.md#partition-types](README.md#partition-types)
- **Architecture Details:** [ARCHITECTURE.md](ARCHITECTURE.md)
- **Troubleshooting:** [README.md#troubleshooting](README.md#troubleshooting)

### Try the Demo Scripts

```bash
# Full end-to-end Parquet demo
./demo/run-demo.sh

# Iceberg export demo
./demo/run-iceberg-demo.sh

# Bidirectional transfer demo
./demo/run-bidirectional-demo.sh
```

---

## Troubleshooting

### Connection Issues

**Problem:** `Login failed for user 'sa'`

**Solutions:**
1. Verify password:
   ```bash
   docker exec sqlserver-iceberg-demo /opt/mssql-tools18/bin/sqlcmd \
     -S localhost -U sa -P "IcebergDemo@2024" -Q "SELECT 1" -C
   ```
2. Check container is running: `docker ps`
3. Review container logs: `docker logs sqlserver-iceberg-demo`

**Problem:** `Cannot open database 'XYZ' requested by the login`

**Solution:**
Database doesn't exist. Create it:
```sql
CREATE DATABASE XYZ;
```

### File Permission Issues

**Problem:** `Access to the path '/data/parquet' is denied`

**Solutions:**
- **Linux:** `sudo chown -R $USER:$USER ./parquet-output`
- **Windows:** Right-click folder → Properties → Security → Add your user with Full Control
- **Docker:** Use volume mounts: `-v $(pwd)/parquet-output:/parquet-output`

### Build Issues

**Problem:** `error MSB3644: The reference assemblies for framework '.NETCoreApp,Version=v8.0' were not found`

**Solution:**
Install .NET 8 SDK: https://dotnet.microsoft.com/download/dotnet/8.0

**Problem:** `Test failures in DataTransfer.Integration.Tests`

**Solution:**
These tests require Docker and SQL Server. Skip them:
```bash
dotnet test --filter "Category!=Integration"
```

### Need More Help?

1. **Check detailed logs:** Set `"logLevel": "Debug"` in `config/appsettings.json`
2. **Review documentation:** See [README.md](README.md) and [ARCHITECTURE.md](ARCHITECTURE.md)
3. **Run validation script:** `./scripts/validate-setup.sh` (after creating it)
4. **Check demo scripts:** The demo scripts in `./demo/` contain working examples

---

## Quick Command Reference

```bash
# Build
dotnet build

# Run tests
dotnet test

# Run console (interactive)
dotnet run --project src/DataTransfer.Console

# Run with config
dotnet run --project src/DataTransfer.Console -- --config config/appsettings.json

# Run web UI
dotnet run --project src/DataTransfer.Web
# Then navigate to http://localhost:5000

# Docker SQL Server commands
docker ps                                           # Check container status
docker logs sqlserver-iceberg-demo                 # View logs
docker stop sqlserver-iceberg-demo                 # Stop container
docker start sqlserver-iceberg-demo                # Start container
docker rm -f sqlserver-iceberg-demo                # Remove container

# SQL Server commands
sqlcmd -S localhost,1433 -U sa -P "password" -Q "SELECT @@VERSION"  # Test connection
```

---

**Congratulations!** You've successfully set up DataTransfer and completed your first data transfer. 🎉
