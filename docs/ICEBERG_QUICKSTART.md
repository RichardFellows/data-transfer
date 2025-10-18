# Iceberg Integration Quick Start

This guide shows you how to use Apache Iceberg tables with DataTransfer for incremental data synchronization.

## What is Iceberg?

Apache Iceberg is a high-performance table format for large analytic tables. DataTransfer uses Iceberg as:
- **Intermediate storage** for incremental sync operations
- **Metadata layer** to track data changes with watermarks
- **Version control** for data with snapshot isolation

## Prerequisites

- .NET 8 SDK
- SQL Server (source and/or destination)
- Configured connection strings in `appsettings.json`

## Configuration

Add Iceberg warehouse path to your `config/appsettings.json`:

```json
{
  "Iceberg": {
    "WarehousePath": "./iceberg-warehouse",
    "DefaultCompression": "snappy"
  }
}
```

The warehouse directory will be created automatically and will contain:
```
iceberg-warehouse/
├── my_table/              # Each table is a directory
│   ├── metadata/          # Table metadata (JSON)
│   └── data/              # Parquet data files
└── .watermarks/           # Sync watermarks
```

## Transfer Types

### 1. SQL Server → Iceberg (Export)

Export a SQL Server table to an Iceberg table:

**Console:**
```bash
dotnet run --project src/DataTransfer.Console -- \
  --export-iceberg "Server=localhost;Database=MyDb;..." "dbo.Customers" \
  --iceberg-name customers_iceberg
```

**Web UI:**
1. Select "SQL Server → Iceberg (Export)" transfer type
2. Configure SQL Server source (connection, database, schema, table)
3. Enter Iceberg table name
4. Click "Execute Transfer"

**Result:** Creates an Iceberg table with a snapshot of the source data.

### 2. Iceberg → SQL Server (Import)

Import an Iceberg table to SQL Server:

**Console:**
```bash
dotnet run --project src/DataTransfer.Console -- \
  --import-iceberg customers_iceberg \
  "Server=localhost;Database=TargetDb;..." "dbo.Customers"
```

**Web UI:**
1. Select "Iceberg → SQL Server (Import)" transfer type
2. Select Iceberg table from dropdown (auto-populated)
3. Configure SQL Server destination
4. Click "Execute Transfer"

**Result:** Imports the current snapshot of the Iceberg table to SQL Server using UPSERT strategy.

### 3. SQL Server → Iceberg → SQL Server (Incremental Sync)

Incremental synchronization with watermark tracking:

**Console:**
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

**Web UI:**
1. Select "SQL Server → Iceberg → SQL Server (Incremental Sync)" transfer type
2. Configure SQL Server source
3. Enter Iceberg table name (used as intermediate storage)
4. Configure SQL Server destination (target)
5. **Incremental Sync Options:**
   - **Primary Key Column**: Unique identifier (e.g., `Id`, `OrderId`)
   - **Watermark Column**: Timestamp column (e.g., `UpdatedAt`, `ModifiedDate`)
   - **Merge Strategy**:
     - `upsert` (default): Insert new rows, update existing
     - `append`: Insert only, skip existing
6. Click "Execute Transfer"

**How it works:**
1. **First run**: Exports all rows from source to Iceberg, then imports to target
2. **Subsequent runs**:
   - Checks last watermark value
   - Exports only changed rows (WHERE watermark > last_value)
   - Merges changes to target using primary key

**Watermark tracking:**
- Stored in `.watermarks/{table-name}.json`
- Contains last synced watermark value and timestamp
- Persists between runs for true incremental sync

## Example Workflows

### Initial Data Migration

```bash
# Export production data to Iceberg
dotnet run --project src/DataTransfer.Console -- \
  --export-iceberg "Server=prod;..." "dbo.Products" \
  --iceberg-name products_export

# Import to UAT environment
dotnet run --project src/DataTransfer.Console -- \
  --import-iceberg products_export \
  "Server=uat;..." "dbo.Products"
```

### Continuous Incremental Sync

```bash
# Run periodically (e.g., via cron/scheduled task)
dotnet run --project src/DataTransfer.Console -- \
  --sync-iceberg \
  "Server=prod;Database=Sales;..." "dbo.Orders" \
  orders_sync \
  "Server=analytics;Database=Warehouse;..." "dbo.Orders" \
  --primary-key OrderId \
  --watermark UpdatedAt \
  --merge-strategy upsert
```

**First run output:**
```
Extracted 10,000 rows from source
Imported 10,000 rows to target
Watermark: 2025-01-15 10:30:00
```

**Second run output (5 minutes later):**
```
Extracted 15 rows from source (watermark filter applied)
Imported 15 rows to target
Watermark: 2025-01-15 10:35:00
```

## Schema Requirements

For incremental sync, your source table MUST have:

1. **Primary Key Column**:
   - Unique identifier for each row
   - Used to match rows during upsert
   - Examples: `Id`, `OrderId`, `CustomerGuid`

2. **Watermark Column**:
   - Timestamp that updates when row changes
   - Supports: `DATETIME`, `DATETIME2`, `DATETIMEOFFSET`
   - Examples: `UpdatedAt`, `ModifiedDate`, `LastUpdated`

**Example table:**
```sql
CREATE TABLE Orders (
    OrderId INT PRIMARY KEY,
    CustomerName NVARCHAR(100),
    OrderDate DATETIME,
    UpdatedAt DATETIME2 DEFAULT GETDATE(),  -- Watermark column
    ...
)

-- Update trigger to maintain watermark
CREATE TRIGGER trg_Orders_UpdatedAt ON Orders
AFTER UPDATE AS
BEGIN
    UPDATE Orders
    SET UpdatedAt = GETDATE()
    FROM Orders o
    INNER JOIN inserted i ON o.OrderId = i.OrderId
END
```

## Troubleshooting

### Watermark Not Advancing

**Problem:** Incremental sync always extracts 0 rows

**Solution:**
- Verify watermark column is updating on row changes
- Check watermark file: `.watermarks/{table-name}.json`
- Delete watermark file to force full re-sync

### Primary Key Violations

**Problem:** Import fails with unique constraint error

**Solution:**
- Verify primary key column name is correct
- Ensure primary key exists on target table
- Use `append` merge strategy if target is append-only

### Iceberg Table Not Found

**Problem:** "No Iceberg tables found" in Web UI

**Solution:**
- Check `Iceberg:WarehousePath` in configuration
- Verify warehouse directory exists and contains table directories
- Run an export to create initial table

## Advanced Configuration

### Custom Warehouse Location

```json
{
  "Iceberg": {
    "WarehousePath": "/mnt/data/iceberg",  // Shared storage
    "DefaultCompression": "snappy"
  }
}
```

### Profile-Based Sync

Save frequently used sync configurations as profiles in the Web UI:

1. Configure your incremental sync
2. Click "Save as Profile"
3. Enter profile name and tags
4. Reuse profile from dropdown in future transfers

## Performance Tips

1. **Index watermark column** on source table for faster incremental extraction
2. **Use appropriate batch size** (default: 100,000 rows)
3. **Schedule during off-peak hours** for large initial sync
4. **Monitor watermark lag** to ensure sync keeps up with changes
5. **Use `append` strategy** for append-only workloads (faster than upsert)

## See Also

- [ARCHITECTURE.md](ARCHITECTURE.md) - System design and components
- [COMMAND_REFERENCE.md](COMMAND_REFERENCE.md) - Complete command listing
- [Iceberg Specification](https://iceberg.apache.org/spec/) - Official Apache Iceberg docs
