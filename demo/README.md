# DataTransfer Demo

This demo shows how to use the DataTransfer tool to migrate data between SQL Server databases using Apache Parquet as intermediate storage.

## Demo Scenario

**Sales Database Migration** - Transfer sales data from a source database to a destination database with 4 different table types:

| Table | Type | Partition Strategy | Description |
|-------|------|-------------------|-------------|
| **Orders** | Transactional | DATE | Orders from Oct-Dec 2024 (~460 records) |
| **SalesTransactions** | Transactional | INT_DATE | Sales with YYYYMMDD format (~730 records) |
| **Products** | Reference | STATIC | Product catalog (20 products) |
| **CustomerDimension** | SCD2 | Slowly Changing | Customer tier history (9 records) |

## Prerequisites

- Docker (for SQL Server container)
- .NET 8 SDK
- Built DataTransfer.Console application

## Quick Start

### Option 1: Automated Setup (Recommended)

```bash
# Run the automated demo script
./demo/run-demo.sh
```

This script will:
1. Start SQL Server in Docker
2. Create source and destination databases
3. Populate source with sample data
4. Run the data transfer
5. Verify the results
6. Clean up (optional)

### Option 2: Manual Step-by-Step

#### Step 1: Start SQL Server Container

```bash
docker run -d \
  --name datatransfer-demo \
  -e "ACCEPT_EULA=Y" \
  -e "SA_PASSWORD=YourStrong@Passw0rd" \
  -p 1433:1433 \
  mcr.microsoft.com/mssql/server:2022-latest
```

Wait ~30 seconds for SQL Server to start, then verify:

```bash
docker exec -it datatransfer-demo /opt/mssql-tools18/bin/sqlcmd \
  -C -S localhost -U sa -P "YourStrong@Passw0rd" \
  -Q "SELECT @@VERSION"
```

#### Step 2: Create and Populate Source Database

```bash
# Create source database and tables
docker exec -i datatransfer-demo /opt/mssql-tools18/bin/sqlcmd \
  -C -S localhost -U sa -P "YourStrong@Passw0rd" \
  < demo/sql/01_create_source_database.sql

# Populate with sample data
docker exec -i datatransfer-demo /opt/mssql-tools18/bin/sqlcmd \
  -C -S localhost -U sa -P "YourStrong@Passw0rd" \
  < demo/sql/02_populate_source_data.sql
```

Expected output:
```
=== Data Population Summary ===
Products: 20
Customer Dimension Records: 9
Orders: ~460
Sales Transactions: ~730
```

#### Step 3: Create Destination Database

```bash
docker exec -i datatransfer-demo /opt/mssql-tools18/bin/sqlcmd \
  -C -S localhost -U sa -P "YourStrong@Passw0rd" \
  < demo/sql/03_create_destination_database.sql
```

#### Step 4: Run Data Transfer

```bash
# Build the console app (if not already built)
dotnet build src/DataTransfer.Console

# Run the data transfer
dotnet run --project src/DataTransfer.Console -- \
  --config demo/config/demo-config.json
```

You'll see output like:
```
[INFO] Loading configuration from: demo/config/demo-config.json
[INFO] Starting transfer for table SalesSource.dbo.Orders
[INFO] Extracted 460 rows from SalesSource.dbo.Orders
[INFO] Writing data to Parquet file dbo_Orders_20241002153045.parquet
[INFO] Successfully wrote data to Parquet file
[INFO] Reading data from Parquet file year=2024/month=10/day=01/dbo_Orders_20241002153045.parquet
[INFO] Loading data to destination table SalesDestination.dbo.Orders
[INFO] Loaded 460 rows to SalesDestination.dbo.Orders
[INFO] Transfer completed successfully in 1234.56ms
...
```

#### Step 5: Verify Results

Check the Parquet files were created:
```bash
tree demo/output/parquet
```

Expected structure:
```
demo/output/parquet/
├── year=2024/
│   ├── month=10/
│   │   ├── day=01/
│   │   │   ├── dbo_Orders_*.parquet
│   │   │   └── dbo_SalesTransactions_*.parquet
│   │   ├── day=02/
│   │   └── ...
│   ├── month=11/
│   └── month=12/
└── static/
    ├── dbo_Products_*.parquet
    └── dbo_CustomerDimension_*.parquet
```

Verify data in destination database:
```bash
docker exec -it datatransfer-demo /opt/mssql-tools18/bin/sqlcmd \
  -C -S localhost -U sa -P "YourStrong@Passw0rd" \
  -d SalesDestination \
  -Q "
    SELECT 'Orders' AS TableName, COUNT(*) AS RowCount FROM dbo.Orders
    UNION ALL
    SELECT 'SalesTransactions', COUNT(*) FROM dbo.SalesTransactions
    UNION ALL
    SELECT 'Products', COUNT(*) FROM dbo.Products
    UNION ALL
    SELECT 'CustomerDimension', COUNT(*) FROM dbo.CustomerDimension
  "
```

Expected output:
```
TableName              RowCount
--------------------- -----------
Orders                        460
SalesTransactions             730
Products                       20
CustomerDimension               9
```

#### Step 6: Cleanup

```bash
# Stop and remove container
docker stop datatransfer-demo
docker rm datatransfer-demo

# Remove Parquet files (optional)
rm -rf demo/output/parquet
```

## Understanding the Configuration

The demo uses `demo/config/demo-config.json` which demonstrates all partition strategies:

### 1. DATE Partition (Orders table)
```json
{
  "Partitioning": {
    "Type": "date",
    "PartitionColumn": "OrderDate"
  }
}
```
- Partitions by `OrderDate` column
- Creates folder structure: `year=2024/month=10/day=01/`
- Best for: Time-series data with DATE columns

### 2. INT_DATE Partition (SalesTransactions table)
```json
{
  "Partitioning": {
    "Type": "int_date",
    "PartitionColumn": "SaleDate"
  }
}
```
- Partitions by integer date (YYYYMMDD format)
- Converts 20241001 → `year=2024/month=10/day=01/`
- Best for: Legacy systems using integer dates

### 3. STATIC Partition (Products table)
```json
{
  "Partitioning": {
    "Type": "static"
  }
}
```
- No partitioning, single extraction
- Stores in `static/` folder
- Best for: Reference tables, lookup data

### 4. SCD2 Partition (CustomerDimension table)
```json
{
  "Partitioning": {
    "Type": "scd2",
    "EffectiveDateColumn": "EffectiveDate",
    "ExpirationDateColumn": "ExpirationDate"
  }
}
```
- Extracts active records (ExpirationDate IS NULL or within range)
- Best for: Slowly Changing Dimensions

## Data Volumes

- **Orders**: ~460 rows (3-7 orders/day × 92 days)
- **SalesTransactions**: ~730 rows (5-12 transactions/day × 92 days)
- **Products**: 20 rows (static reference data)
- **CustomerDimension**: 9 rows (SCD2 history)

Total: ~1,219 rows transferred

## Next Steps

1. **Modify date ranges**: Edit `demo-config.json` to transfer different time periods
2. **Add more tables**: Create new tables in SQL scripts and add to config
3. **Test incremental loads**: Run multiple times with different date ranges
4. **Explore Parquet files**: Use tools like `parquet-tools` or Python pandas to inspect

## Troubleshooting

### Connection Errors
- Ensure Docker container is running: `docker ps`
- Check SQL Server logs: `docker logs datatransfer-demo`
- Verify port 1433 is not in use

### Empty Results
- Confirm source data exists: Check Step 2 summary output
- Verify date ranges in config match source data dates
- Check SQL Server user has appropriate permissions

### File Not Found
- Ensure you're in the repository root directory
- Verify paths: `ls -la demo/sql/` and `ls -la demo/config/`

## Learn More

- [Architecture Documentation](../ARCHITECTURE.md)
- [Configuration Guide](../README.md#configuration)
- [Partition Strategies](../README.md#partition-strategies)
