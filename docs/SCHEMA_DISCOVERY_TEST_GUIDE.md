# Schema Discovery - Manual Test Guide

This guide provides step-by-step instructions for testing the schema discovery feature.

## Prerequisites

- SQL Server 2019+ (Docker, LocalDB, or full instance)
- .NET 8 SDK
- DataTransfer built successfully

## Quick Test Setup (Docker)

```bash
# 1. Start SQL Server in Docker
./demo/00-setup-sqlserver-docker.sh

# 2. Create demo databases with sample data
docker cp demo/01-setup-demo-databases.sql sqlserver-iceberg-demo:/tmp/
docker exec sqlserver-iceberg-demo /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P "IcebergDemo@2024" -C \
  -i /tmp/01-setup-demo-databases.sql

# 3. Test schema discovery
dotnet run --project src/DataTransfer.Console -- \
  --discover "Server=localhost,1433;Database=IcebergDemo_Source;User Id=sa;Password=IcebergDemo@2024;TrustServerCertificate=true"
```

## Expected Output

### Test 1: Discover Entire Database

**Command:**
```bash
dotnet run --project src/DataTransfer.Console -- \
  --discover "Server=localhost,1433;Database=IcebergDemo_Source;User Id=sa;Password=IcebergDemo@2024;TrustServerCertificate=true"
```

**Expected Output:**
```
╔════════════════════════════════════════════╗
║   🔍 Database Schema Discovery            ║
╚════════════════════════════════════════════╝

Testing connection... ✓ Connected

Database: IcebergDemo_Source
Server: Microsoft SQL Server 16.0.xxxx
Tables: 3
Total Rows: 20

═══════════════════════════════════════════════════════════════

Schema: dbo
───────────────────────────────────────────────────────────────

📊 dbo.Customers
   Rows: 10
   Columns: 10
   Suggested Partition: static
   Confidence: 85%

📊 dbo.Orders
   Rows: 10
   Columns: 7
   Suggested Partition: date
   Column: OrderDate
   Confidence: 80%

📊 dbo.Products
   Rows: 10
   Columns: 7
   Suggested Partition: static
   Confidence: 85%

═══════════════════════════════════════════════════════════════

💡 Tip: Use --table schema.tablename to see detailed information
   Example: --discover "..." --table dbo.Orders
```

### Test 2: Discover Specific Table (Orders)

**Command:**
```bash
dotnet run --project src/DataTransfer.Console -- \
  --discover "Server=localhost,1433;Database=IcebergDemo_Source;User Id=sa;Password=IcebergDemo@2024;TrustServerCertificate=true" \
  --table dbo.Orders
```

**Expected Output:**
```
╔════════════════════════════════════════════╗
║   🔍 Database Schema Discovery            ║
╚════════════════════════════════════════════╝

Testing connection... ✓ Connected

Discovering table: dbo.Orders

Table: dbo.Orders
Row Count: 10

Columns:
───────────────────────────────────────────────────────────────
  OrderID                        bigint               NOT NULL
  CustomerID                     int                  NOT NULL
  OrderDate                      datetime2            NOT NULL
     💡 Can be used for date partitioning
  ShippedDate                    datetime2            NULL
     💡 Can be used for date partitioning
  TotalAmount                    decimal(10,2)        NOT NULL
  Status                         varchar(20)          NOT NULL
  OrderNumber                    varchar(50)          NOT NULL

═══════════════════════════════════════════════════════════════

Recommended Partition Strategy:
───────────────────────────────────────────────────────────────

Type: date
Reason: Table has 10 rows and date column 'OrderDate' for time-based partitioning
Confidence: 80%

Sample Configuration:
{
  "type": "date",
  "column": "OrderDate"
}
```

### Test 3: Discover Non-Existent Table

**Command:**
```bash
dotnet run --project src/DataTransfer.Console -- \
  --discover "Server=localhost,1433;..." \
  --table dbo.NotExists
```

**Expected Output:**
```
❌ Table dbo.NotExists not found

Did you mean one of these?
  - dbo.Customers
  - dbo.Orders
  - dbo.Products
```

### Test 4: Invalid Connection String

**Command:**
```bash
dotnet run --project src/DataTransfer.Console -- \
  --discover "Server=invalid;Database=test;User=sa;Password=wrong"
```

**Expected Output:**
```
Testing connection... ❌ Failed
[ERROR] Failed to connect to database
```

## Validation Checklist

Use this checklist to verify the feature works correctly:

### Connection Handling
- [ ] ✓ Successfully connects to SQL Server
- [ ] ✓ Shows error for invalid connection strings
- [ ] ✓ Shows error for wrong credentials
- [ ] ✓ Handles timeout gracefully

### Database Discovery
- [ ] ✓ Shows database name
- [ ] ✓ Shows SQL Server version
- [ ] ✓ Shows correct table count
- [ ] ✓ Shows correct total row count
- [ ] ✓ Groups tables by schema

### Table Discovery
- [ ] ✓ Shows all tables in database
- [ ] ✓ Shows accurate row counts
- [ ] ✓ Shows column count per table
- [ ] ✓ Suggests appropriate partition strategies

### Detailed Table View
- [ ] ✓ Shows all columns with correct data types
- [ ] ✓ Shows nullability (NULL vs NOT NULL)
- [ ] ✓ Shows length for varchar/nvarchar columns
- [ ] ✓ Shows precision/scale for decimal columns
- [ ] ✓ Indicates partition-capable columns
- [ ] ✓ Provides partition strategy recommendation
- [ ] ✓ Generates valid configuration JSON

### Partition Strategy Detection
- [ ] ✓ Small tables (<10K rows) → suggests "static"
- [ ] ✓ Tables with DATE/DATETIME columns → suggests "date"
- [ ] ✓ Tables with integer date columns → suggests "int_date"
- [ ] ✓ Tables with SCD2 pattern → suggests "scd2"
- [ ] ✓ Confidence scores are reasonable (60%-90%)

### Error Handling
- [ ] ✓ Handles table not found
- [ ] ✓ Suggests similar table names for typos
- [ ] ✓ Handles invalid table format (missing schema prefix)
- [ ] ✓ Shows meaningful error messages

### Output Formatting
- [ ] ✓ Unicode box drawing works correctly
- [ ] ✓ Tables aligned properly
- [ ] ✓ Numbers formatted with thousands separators
- [ ] ✓ Percentages displayed correctly
- [ ] ✓ JSON output is valid and properly indented

## Test Results

### Environment
- **Date:** _____________
- **SQL Server:** Docker / LocalDB / Full Instance / Azure SQL
- **Version:** _____________
- **OS:** Windows / Linux / macOS
- **Tester:** _____________

### Results
- **All tests passed:** Yes / No
- **Issues found:** _____________
- **Notes:** _____________

## Integration Test Execution

To run the integration tests (requires SQL Server):

```bash
# Remove Skip attribute from tests in SqlSchemaDiscoveryIntegrationTests.cs
# Then run:
dotnet test tests/DataTransfer.SqlServer.Tests --filter "FullyQualifiedName~IntegrationTests"
```

## Performance Notes

Expected performance characteristics:

| Operation | Typical Duration | Notes |
|-----------|-----------------|-------|
| Connection test | < 1 second | Depends on network latency |
| Discover database (10 tables) | 1-3 seconds | Queries sys tables |
| Discover database (100 tables) | 5-10 seconds | Scales with table count |
| Discover single table | < 1 second | Single table query |

## Troubleshooting

### "TrustServerCertificate" Error
If you see SSL/TLS errors, add `TrustServerCertificate=true` to the connection string.

### "Login failed" Error
Check SQL Server authentication:
- SQL Server must be configured for SQL Server and Windows Authentication mode
- User account must have db_datareader permissions

### Slow Discovery
If discovery takes > 30 seconds:
- Check network latency to SQL Server
- Check SQL Server is not under heavy load
- Consider using --table to discover specific tables

### Missing Tables
If tables don't appear:
- Ensure you have SELECT permission on sys.tables and sys.columns
- Check you're connected to the correct database
- Verify tables exist: `SELECT * FROM sys.tables`

## Success Criteria

The feature is considered working correctly if:
1. All validation checklist items pass
2. Integration tests pass (when SQL Server available)
3. Output is readable and properly formatted
4. Partition suggestions are logical and helpful
5. Generated configuration JSON is valid and usable
6. Error messages are clear and actionable

## Next Steps

After successful testing:
1. Update GETTING_STARTED.md with schema discovery examples
2. Add to README.md feature list
3. Consider adding to Web UI as future enhancement
