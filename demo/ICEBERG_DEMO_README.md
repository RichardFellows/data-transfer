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

```bash
# Run the interactive demo menu
./demo/run-iceberg-demo.sh

# Or run export only
./demo/02-export-to-iceberg.sh

# Then validate
./scripts/validate-iceberg-table.sh /tmp/iceberg-demo-warehouse customers
```

## Demo Scripts

### 01-setup-demo-databases.sql
Creates source SQL Server database with sample data (Customers, Orders, Products tables)

```sql
sqlcmd -S "(localdb)\mssqllocaldb" -i demo/01-setup-demo-databases.sql
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

- **.NET 8 SDK** (required)
- **SQL Server LocalDB** (optional - Windows only)
- **DuckDB** (optional - for querying)
- **PyIceberg** (optional - for validation)

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
