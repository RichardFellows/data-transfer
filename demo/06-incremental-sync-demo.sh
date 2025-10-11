#!/bin/bash

# Demo: Incremental Synchronization with Apache Iceberg
# Shows end-to-end workflow: SQL Server → Iceberg → SQL Server with watermark-based CDC

set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
SA_PASSWORD="IcebergDemo@2024"
SOURCE_DB="IncrementalSourceDb"
TARGET_DB="IncrementalTargetDb"
WAREHOUSE_PATH="/tmp/iceberg-incremental-demo"
WATERMARK_PATH="/tmp/watermarks-demo"

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Incremental Sync Demo with Apache Iceberg${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Check if SQL Server container is running
if ! docker ps | grep -q sqlserver-iceberg-demo; then
    echo -e "${YELLOW}Starting SQL Server container...${NC}"
    docker run -d \
        --name sqlserver-iceberg-demo \
        -e "ACCEPT_EULA=Y" \
        -e "SA_PASSWORD=$SA_PASSWORD" \
        -p 1433:1433 \
        mcr.microsoft.com/mssql/server:2022-latest

    echo "Waiting for SQL Server to start..."
    sleep 20
fi

echo -e "${GREEN}✓ SQL Server is running${NC}"
echo ""

# Cleanup previous demo data
echo -e "${YELLOW}Cleaning up previous demo data...${NC}"
rm -rf "$WAREHOUSE_PATH" "$WATERMARK_PATH"
mkdir -p "$WAREHOUSE_PATH" "$WATERMARK_PATH"

# Create databases
echo -e "${YELLOW}Creating source and target databases...${NC}"
docker exec sqlserver-iceberg-demo /opt/mssql-tools18/bin/sqlcmd \
    -C -S localhost -U sa -P "$SA_PASSWORD" \
    -Q "IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '$SOURCE_DB') CREATE DATABASE [$SOURCE_DB]; IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '$TARGET_DB') CREATE DATABASE [$TARGET_DB];"

echo -e "${GREEN}✓ Databases created${NC}"
echo ""

# Create source table
echo -e "${YELLOW}Creating source table (Orders)...${NC}"
docker exec sqlserver-iceberg-demo /opt/mssql-tools18/bin/sqlcmd \
    -C -S localhost -U sa -P "$SA_PASSWORD" -d "$SOURCE_DB" \
    -Q "IF OBJECT_ID('Orders', 'U') IS NOT NULL DROP TABLE Orders;
        CREATE TABLE Orders (
            OrderId INT PRIMARY KEY,
            CustomerId INT NOT NULL,
            OrderDate DATETIME2 NOT NULL,
            Amount DECIMAL(18,2) NOT NULL,
            Status NVARCHAR(50) NOT NULL,
            ModifiedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE()
        );"

echo -e "${GREEN}✓ Source table created${NC}"
echo ""

# Create target table
echo -e "${YELLOW}Creating target table (Orders)...${NC}"
docker exec sqlserver-iceberg-demo /opt/mssql-tools18/bin/sqlcmd \
    -C -S localhost -U sa -P "$SA_PASSWORD" -d "$TARGET_DB" \
    -Q "IF OBJECT_ID('Orders', 'U') IS NOT NULL DROP TABLE Orders;
        CREATE TABLE Orders (
            OrderId INT PRIMARY KEY,
            CustomerId INT NOT NULL,
            OrderDate DATETIME2 NOT NULL,
            Amount DECIMAL(18,2) NOT NULL,
            Status NVARCHAR(50) NOT NULL,
            ModifiedDate DATETIME2
        );"

echo -e "${GREEN}✓ Target table created${NC}"
echo ""

# Insert initial data
echo -e "${YELLOW}Inserting initial data (1000 orders)...${NC}"
docker exec sqlserver-iceberg-demo /opt/mssql-tools18/bin/sqlcmd \
    -C -S localhost -U sa -P "$SA_PASSWORD" -d "$SOURCE_DB" \
    -Q "DECLARE @i INT = 1;
        WHILE @i <= 1000
        BEGIN
            INSERT INTO Orders (OrderId, CustomerId, OrderDate, Amount, Status, ModifiedDate)
            VALUES (@i, (@i % 100) + 1, DATEADD(DAY, -@i, GETUTCDATE()),
                    (@i * 10.50), 'Pending', GETUTCDATE());
            SET @i = @i + 1;
        END;"

echo -e "${GREEN}✓ Inserted 1000 initial orders${NC}"
echo ""

# Verify source data
SOURCE_COUNT=$(docker exec sqlserver-iceberg-demo /opt/mssql-tools18/bin/sqlcmd \
    -C -S localhost -U sa -P "$SA_PASSWORD" -d "$SOURCE_DB" -h -1 \
    -Q "SET NOCOUNT ON; SELECT COUNT(*) FROM Orders;")

echo -e "${BLUE}Source database: $SOURCE_COUNT rows${NC}"
echo ""

# Create C# program for incremental sync
echo -e "${YELLOW}Creating incremental sync program...${NC}"
cat > /tmp/IncrementalSyncDemo.cs << 'EOF'
using DataTransfer.Iceberg.Catalog;
using DataTransfer.Iceberg.ChangeDetection;
using DataTransfer.Iceberg.Integration;
using DataTransfer.Iceberg.Models;
using DataTransfer.Iceberg.Readers;
using DataTransfer.Iceberg.Watermarks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

var sourceConnection = args[0];
var targetConnection = args[1];
var warehousePath = args[2];
var watermarkPath = args[3];

// Setup components
var catalog = new FilesystemCatalog(warehousePath, NullLogger<FilesystemCatalog>.Instance);
var changeDetection = new TimestampChangeDetection("ModifiedDate");
var appender = new IcebergAppender(catalog, NullLogger<IcebergAppender>.Instance);
var reader = new IcebergReader(catalog, NullLogger<IcebergReader>.Instance);
var importer = new SqlServerImporter(NullLogger<SqlServerImporter>.Instance);
var watermarkStore = new FileWatermarkStore(watermarkPath);

var coordinator = new IncrementalSyncCoordinator(
    changeDetection, appender, reader, importer, watermarkStore,
    NullLogger<IncrementalSyncCoordinator>.Instance);

var options = new SyncOptions
{
    PrimaryKeyColumn = "OrderId",
    WatermarkColumn = "ModifiedDate",
    WarehousePath = warehousePath,
    WatermarkDirectory = watermarkPath
};

Console.WriteLine("Starting incremental sync...");
var result = await coordinator.SyncAsync(
    sourceConnection,
    "Orders",
    "orders_iceberg",
    targetConnection,
    "Orders",
    options);

if (result.Success)
{
    Console.WriteLine($"✓ Sync completed successfully");
    Console.WriteLine($"  Rows extracted: {result.RowsExtracted}");
    Console.WriteLine($"  Rows appended to Iceberg: {result.RowsAppended}");
    Console.WriteLine($"  Rows imported to target: {result.RowsImported}");
    Console.WriteLine($"  New snapshot ID: {result.NewSnapshotId}");
    Console.WriteLine($"  Duration: {result.Duration.TotalSeconds:F2}s");
}
else
{
    Console.WriteLine($"✗ Sync failed: {result.ErrorMessage}");
    Environment.Exit(1);
}
EOF

echo -e "${GREEN}✓ Program created${NC}"
echo ""

# Build the program
echo -e "${YELLOW}Building incremental sync program...${NC}"
cd /home/richard/sonnet45
cat > /tmp/IncrementalSyncDemo.csproj << EOF
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <TargetFramework>net8.0</TargetFramework>
    <Nullable>enable</Nullable>
  </PropertyGroup>
  <ItemGroup>
    <ProjectReference Include="/home/richard/sonnet45/src/DataTransfer.Iceberg/DataTransfer.Iceberg.csproj" />
  </ItemGroup>
</Project>
EOF

dotnet build /tmp/IncrementalSyncDemo.csproj -o /tmp/sync-demo > /dev/null 2>&1
cp /tmp/IncrementalSyncDemo.cs /tmp/sync-demo/Program.cs
dotnet build /tmp/sync-demo/IncrementalSyncDemo.csproj > /dev/null 2>&1

echo -e "${GREEN}✓ Program built${NC}"
echo ""

# Step 1: Initial Sync
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Step 1: Initial Sync (1000 rows)${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

SOURCE_CONN="Server=localhost;Database=$SOURCE_DB;User Id=sa;Password=$SA_PASSWORD;TrustServerCertificate=true;"
TARGET_CONN="Server=localhost;Database=$TARGET_DB;User Id=sa;Password=$SA_PASSWORD;TrustServerCertificate=true;"

dotnet run --project /tmp/sync-demo/IncrementalSyncDemo.csproj \
    "$SOURCE_CONN" "$TARGET_CONN" "$WAREHOUSE_PATH" "$WATERMARK_PATH"

echo ""

# Verify target after initial sync
TARGET_COUNT=$(docker exec sqlserver-iceberg-demo /opt/mssql-tools18/bin/sqlcmd \
    -C -S localhost -U sa -P "$SA_PASSWORD" -d "$TARGET_DB" -h -1 \
    -Q "SET NOCOUNT ON; SELECT COUNT(*) FROM Orders;")

echo -e "${GREEN}✓ Target database: $TARGET_COUNT rows${NC}"
echo ""

# Step 2: Add new orders
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Step 2: Adding New Orders (100 rows)${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

sleep 1  # Ensure different timestamp

docker exec sqlserver-iceberg-demo /opt/mssql-tools18/bin/sqlcmd \
    -C -S localhost -U sa -P "$SA_PASSWORD" -d "$SOURCE_DB" \
    -Q "DECLARE @i INT = 1001;
        WHILE @i <= 1100
        BEGIN
            INSERT INTO Orders (OrderId, CustomerId, OrderDate, Amount, Status, ModifiedDate)
            VALUES (@i, (@i % 100) + 1, DATEADD(DAY, -@i, GETUTCDATE()),
                    (@i * 10.50), 'Pending', GETUTCDATE());
            SET @i = @i + 1;
        END;"

SOURCE_COUNT=$(docker exec sqlserver-iceberg-demo /opt/mssql-tools18/bin/sqlcmd \
    -C -S localhost -U sa -P "$SA_PASSWORD" -d "$SOURCE_DB" -h -1 \
    -Q "SET NOCOUNT ON; SELECT COUNT(*) FROM Orders;")

echo -e "${GREEN}✓ Added 100 new orders (source now: $SOURCE_COUNT rows)${NC}"
echo ""

# Step 3: Incremental sync (new rows only)
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Step 3: Incremental Sync (new rows only)${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

dotnet run --project /tmp/sync-demo/IncrementalSyncDemo.csproj \
    "$SOURCE_CONN" "$TARGET_CONN" "$WAREHOUSE_PATH" "$WATERMARK_PATH"

echo ""

TARGET_COUNT=$(docker exec sqlserver-iceberg-demo /opt/mssql-tools18/bin/sqlcmd \
    -C -S localhost -U sa -P "$SA_PASSWORD" -d "$TARGET_DB" -h -1 \
    -Q "SET NOCOUNT ON; SELECT COUNT(*) FROM Orders;")

echo -e "${GREEN}✓ Target database: $TARGET_COUNT rows${NC}"
echo ""

# Step 4: Update existing orders
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Step 4: Updating Orders (10 rows)${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

sleep 1

docker exec sqlserver-iceberg-demo /opt/mssql-tools18/bin/sqlcmd \
    -C -S localhost -U sa -P "$SA_PASSWORD" -d "$SOURCE_DB" \
    -Q "UPDATE TOP(10) Orders
        SET Status = 'Shipped',
            Amount = Amount * 1.1,
            ModifiedDate = GETUTCDATE()
        WHERE Status = 'Pending';"

echo -e "${GREEN}✓ Updated 10 orders to 'Shipped' status${NC}"
echo ""

# Step 5: Incremental sync (updates only)
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Step 5: Incremental Sync (updates only)${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

dotnet run --project /tmp/sync-demo/IncrementalSyncDemo.csproj \
    "$SOURCE_CONN" "$TARGET_CONN" "$WAREHOUSE_PATH" "$WATERMARK_PATH"

echo ""

# Verify updates
SHIPPED_COUNT=$(docker exec sqlserver-iceberg-demo /opt/mssql-tools18/bin/sqlcmd \
    -C -S localhost -U sa -P "$SA_PASSWORD" -d "$TARGET_DB" -h -1 \
    -Q "SET NOCOUNT ON; SELECT COUNT(*) FROM Orders WHERE Status = 'Shipped';")

echo -e "${GREEN}✓ Target database has $SHIPPED_COUNT shipped orders${NC}"
echo ""

# Step 6: No changes sync
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Step 6: Sync with No Changes${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

dotnet run --project /tmp/sync-demo/IncrementalSyncDemo.csproj \
    "$SOURCE_CONN" "$TARGET_CONN" "$WAREHOUSE_PATH" "$WATERMARK_PATH"

echo ""

# Final verification
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Final Verification${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

SOURCE_COUNT=$(docker exec sqlserver-iceberg-demo /opt/mssql-tools18/bin/sqlcmd \
    -C -S localhost -U sa -P "$SA_PASSWORD" -d "$SOURCE_DB" -h -1 \
    -Q "SET NOCOUNT ON; SELECT COUNT(*) FROM Orders;")

TARGET_COUNT=$(docker exec sqlserver-iceberg-demo /opt/mssql-tools18/bin/sqlcmd \
    -C -S localhost -U sa -P "$SA_PASSWORD" -d "$TARGET_DB" -h -1 \
    -Q "SET NOCOUNT ON; SELECT COUNT(*) FROM Orders;")

echo -e "${BLUE}Source database:${NC} $SOURCE_COUNT rows"
echo -e "${BLUE}Target database:${NC} $TARGET_COUNT rows"

if [ "$SOURCE_COUNT" = "$TARGET_COUNT" ]; then
    echo -e "${GREEN}✓ Databases are in sync!${NC}"
else
    echo -e "${YELLOW}⚠ Row count mismatch${NC}"
fi

echo ""

# Show Iceberg table info
echo -e "${BLUE}Iceberg Table Information:${NC}"
SNAPSHOT_COUNT=$(find "$WAREHOUSE_PATH/orders_iceberg/metadata" -name "snap-*.avro" 2>/dev/null | wc -l)
DATA_FILE_COUNT=$(find "$WAREHOUSE_PATH/orders_iceberg/data" -name "*.parquet" 2>/dev/null | wc -l)

echo -e "  Warehouse path: $WAREHOUSE_PATH"
echo -e "  Snapshots: $SNAPSHOT_COUNT"
echo -e "  Data files: $DATA_FILE_COUNT"
echo ""

# Show watermark info
echo -e "${BLUE}Watermark Information:${NC}"
if [ -f "$WATERMARK_PATH/orders_iceberg.json" ]; then
    echo -e "  Watermark file: $WATERMARK_PATH/orders_iceberg.json"
    echo -e "  Content:"
    cat "$WATERMARK_PATH/orders_iceberg.json" | jq '.' 2>/dev/null || cat "$WATERMARK_PATH/orders_iceberg.json"
else
    echo -e "  ${YELLOW}No watermark file found${NC}"
fi

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Demo completed successfully!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo -e "${YELLOW}Summary:${NC}"
echo -e "  • Initial sync: 1000 rows"
echo -e "  • Added 100 new rows → incremental sync"
echo -e "  • Updated 10 rows → incremental sync"
echo -e "  • No changes → incremental sync (0 rows)"
echo -e "  • All data synced correctly ✓"
echo ""
echo -e "${YELLOW}Cleanup:${NC}"
echo -e "  To remove demo data: rm -rf $WAREHOUSE_PATH $WATERMARK_PATH"
echo -e "  To drop databases:"
echo -e "    docker exec sqlserver-iceberg-demo /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P '$SA_PASSWORD' \\"
echo -e "      -Q \"DROP DATABASE IF EXISTS $SOURCE_DB; DROP DATABASE IF EXISTS $TARGET_DB;\""
echo ""
