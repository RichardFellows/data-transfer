# SQL Server ↔ Iceberg Bidirectional Transfer Demo

This directory contains demo scripts that showcase the new **SqlServerToIcebergExporter** functionality for exporting SQL Server tables to Apache Iceberg format (Parquet-backed) and querying/importing them back.

## Overview

The demo demonstrates a complete bidirectional data transfer workflow:

```
┌─────────────────┐         ┌──────────────────┐         ┌─────────────────┐
│  SQL Server     │ Export  │  Apache Iceberg  │ Import  │  SQL Server     │
│  (Source DB)    │────────>│  (Parquet Files) │────────>│  (Target DB)    │
└─────────────────┘         └──────────────────┘         └─────────────────┘
                                     │
                                     │ Query
                                     ▼
                            ┌──────────────────┐
                            │  DuckDB/PyIceberg│
                            │  (Analysis)      │
                            └──────────────────┘
```

## Features Demonstrated

✅ **Export SQL Server → Iceberg**
- Direct export using `SqlServerToIcebergExporter`
- Automatic schema inference
- Type mapping (SQL Server → Iceberg → Parquet)
- Nullability preservation
- Support for various data types

✅ **Iceberg Validation**
- Validate Iceberg v2 format compliance
- Check field-ID preservation
- Verify metadata structure

✅ **Query with DuckDB**
- Query Iceberg tables using SQL
- Join multiple tables
- Export to CSV

✅ **Import Iceberg → SQL Server**
- Schema recreation
- Data verification

## Quick Start

### Automated Setup (Recommended)

```bash
# Run the full demo with Docker SQL Server
./demo/run-iceberg-demo.sh
# Select option 1 - Full Demo

# This automatically:
# 1. Creates SQL Server Docker container
# 2. Populates sample databases
# 3. Exports to Iceberg
# 4. Validates tables
# 5. Queries with DuckDB (if installed)
```

### Manual Setup

```bash
# Step 1: Setup SQL Server in Docker
./demo/00-setup-sqlserver-docker.sh

# Step 2: Create demo databases
# (Copy SQL into container and execute)
docker cp demo/01-setup-demo-databases.sql sqlserver-iceberg-demo:/tmp/
docker exec sqlserver-iceberg-demo /opt/mssql-tools/bin/sqlcmd \
  -S localhost -U sa -P IcebergDemo@2024 -i /tmp/01-setup-demo-databases.sql

# Step 3: Export to Iceberg
./demo/02-export-to-iceberg.sh

# Step 4: Validate
./scripts/validate-iceberg-table.sh /tmp/iceberg-demo-warehouse customers
```

## Demo Scripts

### 00-setup-sqlserver-docker.sh
**NEW!** Sets up SQL Server 2022 in Docker container

```bash
./demo/00-setup-sqlserver-docker.sh

# Creates container: sqlserver-iceberg-demo
# Port: 1433
# Username: sa
# Password: IcebergDemo@2024
```

Features:
- Checks if Docker is installed and running
- Creates or reuses existing container
- Waits for SQL Server to be ready
- Saves connection details for other scripts
- Idempotent (safe to run multiple times)

### 01-setup-demo-databases.sql
Creates source SQL Server database with sample data (Customers, Orders, Products tables)

```bash
# Automatically used by run-iceberg-demo.sh
# Or run manually in Docker:
docker cp demo/01-setup-demo-databases.sql sqlserver-iceberg-demo:/tmp/
docker exec sqlserver-iceberg-demo /opt/mssql-tools/bin/sqlcmd \
  -S localhost -U sa -P IcebergDemo@2024 -i /tmp/01-setup-demo-databases.sql
```

### 02-export-to-iceberg.sh
Exports SQL Server tables to Iceberg format

```bash
./demo/02-export-to-iceberg.sh
```

Output: Iceberg warehouse at `/tmp/iceberg-demo-warehouse`

### 05-query-iceberg-with-duckdb.sh
Demonstrates querying Iceberg tables with DuckDB

```bash
./demo/05-query-iceberg-with-duckdb.sh
```

Requires: DuckDB CLI installed

## Iceberg Warehouse Structure

```
/tmp/iceberg-demo-warehouse/
├── customers/
│   ├── data/
│   │   └── data-0001.parquet          # Parquet file with field-IDs
│   └── metadata/
│       ├── v1.metadata.json            # Table metadata (Iceberg v2)
│       ├── version-hint.txt            # Version pointer
│       ├── snap-{uuid}.avro            # Manifest list
│       └── manifest-{uuid}.avro        # Manifest file
└── orders/
    └── ...
```

## Example Queries (DuckDB)

```sql
-- Load extension
INSTALL iceberg;
LOAD iceberg;

-- Query table
SELECT * FROM iceberg_scan('/tmp/iceberg-demo-warehouse/customers/metadata/v1.metadata.json');

-- Count records
SELECT COUNT(*) FROM iceberg_scan('...');

-- Join tables
SELECT c.name, COUNT(o.order_id)
FROM iceberg_scan('.../customers/...') c
LEFT JOIN iceberg_scan('.../orders/...') o ON c.id = o.customer_id
GROUP BY c.name;
```

## Validation

```bash
# Automated validation
./scripts/validate-iceberg-table.sh /tmp/iceberg-demo-warehouse customers

# With PyIceberg (if installed)
python3 scripts/validate-with-pyiceberg.py /tmp/iceberg-demo-warehouse customers
```

## Prerequisites

### Required
- **.NET 8 SDK**
- **Docker** (for SQL Server container)

### Optional
- **DuckDB** - Query Iceberg tables with SQL
- **PyIceberg** - Advanced validation
- **jq** - Pretty-print JSON metadata

## Docker Setup

### Check Docker Installation

```bash
# Check if Docker is installed
docker --version

# Check if Docker daemon is running
docker ps

# If not running
sudo systemctl start docker
sudo usermod -aG docker $USER
newgrp docker
```

### Install Docker (if needed)

```bash
# Ubuntu/Debian
sudo apt-get update
sudo apt-get install docker.io

# Or use official Docker install
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh
```

## SQL Server Options

### Option 1: Docker (Recommended - DEFAULT)

```bash
# Automatically set up by run-iceberg-demo.sh
./demo/run-iceberg-demo.sh
# Select option 1

# Or manually
./demo/00-setup-sqlserver-docker.sh
```

Creates:
- Container: `sqlserver-iceberg-demo`
- Image: `mcr.microsoft.com/mssql/server:2022-latest`
- Port: `1433`
- User: `sa`
- Password: `IcebergDemo@2024`

### Option 2: Existing SQL Server

If you have SQL Server running elsewhere:

```bash
# Set connection string
export SQL_CONNECTION_STRING="Server=myserver;User=sa;Password=pass"

# Run demo
./demo/02-export-to-iceberg.sh
```

### Option 3: Windows LocalDB

For Windows users (not recommended for demo):

```powershell
# Requires SQL Server LocalDB installed
sqlcmd -S "(localdb)\mssqllocaldb" -i demo\01-setup-demo-databases.sql
```

## Environment Variables

- `ICEBERG_WAREHOUSE` - Warehouse path (default: `/tmp/iceberg-demo-warehouse`)
- `SQL_CONNECTION_STRING` - SQL Server connection string

## Cleanup

```bash
./demo/run-iceberg-demo.sh
# Select option 5 (Cleanup)
```

## References

- Main validation guide: `docs/iceberg-validation-guide.md`
- Implementation details: `docs/ICEBERG_INTEGRATION_PLAN.md`
- Tests: `tests/DataTransfer.Iceberg.Tests/`
