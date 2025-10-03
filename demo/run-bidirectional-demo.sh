#!/bin/bash
set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
NC='\033[0m' # No Color

# Configuration
CONTAINER_NAME="datatransfer-demo"
SA_PASSWORD="YourStrong@Passw0rd"
SQL_PORT="1433"
SQL_IMAGE="mcr.microsoft.com/mssql/server:2022-latest"

echo -e "${MAGENTA}╔═══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${MAGENTA}║    DataTransfer - Bi-Directional Transfer Demo Suite         ║${NC}"
echo -e "${MAGENTA}║    SQL Server ↔ Parquet File Transfers                       ║${NC}"
echo -e "${MAGENTA}╚═══════════════════════════════════════════════════════════════╝${NC}"
echo ""

# Function to wait for SQL Server to be ready
wait_for_sql() {
    echo -e "${YELLOW}⏳ Waiting for SQL Server to be ready...${NC}"
    local max_attempts=30
    local attempt=0

    while [ $attempt -lt $max_attempts ]; do
        if docker exec $CONTAINER_NAME /opt/mssql-tools18/bin/sqlcmd \
            -C -S localhost -U sa -P "$SA_PASSWORD" \
            -Q "SELECT 1" &> /dev/null; then
            echo -e "${GREEN}✓ SQL Server is ready!${NC}"
            return 0
        fi
        attempt=$((attempt + 1))
        echo -n "."
        sleep 2
    done

    echo -e "${RED}✗ SQL Server failed to start${NC}"
    return 1
}

# Step 1: Check if container exists
echo -e "${CYAN}════════════════════════════════════════════════════════════${NC}"
echo -e "${CYAN}  Phase 1: Environment Setup${NC}"
echo -e "${CYAN}════════════════════════════════════════════════════════════${NC}"
echo ""

echo -e "${YELLOW}📋 Checking Docker environment...${NC}"
if docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo -e "${YELLOW}⚠️  Container '${CONTAINER_NAME}' already exists${NC}"
    read -p "   Remove and recreate? (y/n): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo -e "${YELLOW}🗑️  Removing existing container...${NC}"
        docker stop $CONTAINER_NAME 2>/dev/null || true
        docker rm $CONTAINER_NAME 2>/dev/null || true
    else
        echo -e "${YELLOW}Using existing container...${NC}"
    fi
fi

# Step 2: Start SQL Server container
if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo -e "${YELLOW}🐳 Starting SQL Server container...${NC}"
    docker run -d \
        --name $CONTAINER_NAME \
        -e "ACCEPT_EULA=Y" \
        -e "SA_PASSWORD=$SA_PASSWORD" \
        -p $SQL_PORT:1433 \
        $SQL_IMAGE > /dev/null

    echo -e "${GREEN}✓ Container started${NC}"
    wait_for_sql
else
    echo -e "${GREEN}✓ Container already running${NC}"
fi

# Step 3: Create and populate source database
echo ""
echo -e "${YELLOW}📊 Creating source database and tables...${NC}"
docker exec -i $CONTAINER_NAME /opt/mssql-tools18/bin/sqlcmd \
    -C -S localhost -U sa -P "$SA_PASSWORD" \
    < demo/sql/01_create_source_database.sql > /dev/null
echo -e "${GREEN}✓ Source database created${NC}"

echo -e "${YELLOW}📝 Populating source database with sample data...${NC}"
docker exec -i $CONTAINER_NAME /opt/mssql-tools18/bin/sqlcmd \
    -C -S localhost -U sa -P "$SA_PASSWORD" \
    < demo/sql/02_populate_source_data.sql | grep -E "(Products:|Customer Dimension|Orders:|Sales Transactions:|Sample date)"
echo -e "${GREEN}✓ Source data populated${NC}"

# Create destination database
echo -e "${YELLOW}🎯 Creating destination database...${NC}"
docker exec -i $CONTAINER_NAME /opt/mssql-tools18/bin/sqlcmd \
    -C -S localhost -U sa -P "$SA_PASSWORD" \
    < demo/sql/03_create_destination_database.sql > /dev/null
echo -e "${GREEN}✓ Destination database created${NC}"

# Build console app
echo ""
echo -e "${YELLOW}🔨 Building DataTransfer applications...${NC}"
dotnet build --configuration Release -v quiet
echo -e "${GREEN}✓ Build complete${NC}"

# Demo 1: SQL → Parquet (Export Only)
echo ""
echo -e "${CYAN}════════════════════════════════════════════════════════════${NC}"
echo -e "${CYAN}  Demo 1: SQL Server → Parquet (Export)${NC}"
echo -e "${CYAN}════════════════════════════════════════════════════════════${NC}"
echo ""
echo -e "${BLUE}This demo exports SQL Server data to Parquet files for:${NC}"
echo -e "  • Archival and long-term storage"
echo -e "  • Analytics with tools like Spark, Pandas, DuckDB"
echo -e "  • Cloud data lake integration"
echo ""
read -p "Press Enter to start SQL → Parquet export..."
echo ""

dotnet run --project src/DataTransfer.Console --configuration Release -- \
    --config demo/config/export-to-parquet.json

echo ""
echo -e "${GREEN}✓ Export complete!${NC}"
echo -e "${YELLOW}📁 Parquet files created:${NC}"
if [ -d "demo/output/exports" ]; then
    echo ""
    tree demo/output/exports -L 4 2>/dev/null || find demo/output/exports -type f -name "*.parquet" | while read file; do
        size=$(ls -lh "$file" | awk '{print $5}')
        echo "  📄 $(basename $file) ($size)"
    done
    echo ""
    echo -e "${CYAN}💡 Tip: These Parquet files can now be:${NC}"
    echo "  • Loaded into data lakes (S3, Azure Blob, GCS)"
    echo "  • Analyzed with Python/Pandas: pd.read_parquet('file.parquet')"
    echo "  • Queried with DuckDB: SELECT * FROM 'file.parquet'"
else
    echo -e "${RED}No Parquet files found${NC}"
fi

# Demo 2: Parquet → SQL (Import Only)
echo ""
echo -e "${CYAN}════════════════════════════════════════════════════════════${NC}"
echo -e "${CYAN}  Demo 2: Parquet → SQL Server (Import)${NC}"
echo -e "${CYAN}════════════════════════════════════════════════════════════${NC}"
echo ""
echo -e "${BLUE}This demo imports Parquet files into SQL Server for:${NC}"
echo -e "  • Restoring archived data"
echo -e "  • Loading analytics results back to operational systems"
echo -e "  • Data warehouse population"
echo ""
read -p "Press Enter to start Parquet → SQL import..."
echo ""

# Create import destination table
docker exec -i $CONTAINER_NAME /opt/mssql-tools18/bin/sqlcmd \
    -C -S localhost -U sa -P "$SA_PASSWORD" \
    -Q "USE SalesDestination;
        IF OBJECT_ID('dbo.ImportedOrders', 'U') IS NOT NULL DROP TABLE dbo.ImportedOrders;
        CREATE TABLE dbo.ImportedOrders (
            OrderID INT PRIMARY KEY,
            CustomerID INT NOT NULL,
            OrderDate DATE NOT NULL,
            TotalAmount DECIMAL(10,2) NOT NULL,
            Status NVARCHAR(20) NOT NULL
        );" > /dev/null

dotnet run --project src/DataTransfer.Console --configuration Release -- \
    --config demo/config/import-from-parquet.json

echo ""
echo -e "${GREEN}✓ Import complete!${NC}"
echo -e "${YELLOW}📊 Verifying imported data:${NC}"
docker exec -i $CONTAINER_NAME /opt/mssql-tools18/bin/sqlcmd \
    -C -S localhost -U sa -P "$SA_PASSWORD" \
    -d SalesDestination \
    -h -1 \
    -Q "SET NOCOUNT ON;
        SELECT 'Total rows imported: ' + CAST(COUNT(*) AS VARCHAR(10)) FROM dbo.ImportedOrders
        UNION ALL
        SELECT 'Date range: ' + CAST(MIN(OrderDate) AS VARCHAR(20)) + ' to ' + CAST(MAX(OrderDate) AS VARCHAR(20)) FROM dbo.ImportedOrders
        UNION ALL
        SELECT 'Total amount: $' + FORMAT(SUM(TotalAmount), 'N2') FROM dbo.ImportedOrders"

# Demo 3: Traditional SQL → SQL (via Parquet intermediate)
echo ""
echo -e "${CYAN}════════════════════════════════════════════════════════════${NC}"
echo -e "${CYAN}  Demo 3: SQL Server → SQL Server (Migration)${NC}"
echo -e "${CYAN}════════════════════════════════════════════════════════════${NC}"
echo ""
echo -e "${BLUE}This demo migrates data between SQL Servers using Parquet:${NC}"
echo -e "  • Cross-server data migration"
echo -e "  • Database replication with transformation"
echo -e "  • Dev/Test environment population"
echo ""
read -p "Press Enter to start SQL → SQL migration..."
echo ""

dotnet run --project src/DataTransfer.Console --configuration Release -- \
    --config demo/config/demo-config.json

# Final verification
echo ""
echo -e "${CYAN}════════════════════════════════════════════════════════════${NC}"
echo -e "${CYAN}  Demo Results Summary${NC}"
echo -e "${CYAN}════════════════════════════════════════════════════════════${NC}"
echo ""

echo -e "${YELLOW}📊 Source Database (SalesSource):${NC}"
docker exec -i $CONTAINER_NAME /opt/mssql-tools18/bin/sqlcmd \
    -C -S localhost -U sa -P "$SA_PASSWORD" \
    -d SalesSource \
    -h -1 \
    -Q "SET NOCOUNT ON;
        SELECT '  Orders: ' + CAST(COUNT(*) AS VARCHAR(10)) + ' rows' FROM dbo.Orders
        UNION ALL
        SELECT '  Products: ' + CAST(COUNT(*) AS VARCHAR(10)) + ' rows' FROM dbo.Products"

echo ""
echo -e "${YELLOW}📁 Exported Parquet Files:${NC}"
if [ -d "demo/output" ]; then
    parquet_count=$(find demo/output -name "*.parquet" 2>/dev/null | wc -l)
    echo -e "  ${GREEN}$parquet_count Parquet files created${NC}"
fi

echo ""
echo -e "${YELLOW}📊 Destination Database (SalesDestination):${NC}"
docker exec -i $CONTAINER_NAME /opt/mssql-tools18/bin/sqlcmd \
    -C -S localhost -U sa -P "$SA_PASSWORD" \
    -d SalesDestination \
    -h -1 \
    -Q "SET NOCOUNT ON;
        SELECT '  Orders: ' + CAST(COUNT(*) AS VARCHAR(10)) + ' rows (migrated)' FROM dbo.Orders
        UNION ALL
        SELECT '  Products: ' + CAST(COUNT(*) AS VARCHAR(10)) + ' rows (migrated)' FROM dbo.Products
        UNION ALL
        SELECT '  ImportedOrders: ' + CAST(COUNT(*) AS VARCHAR(10)) + ' rows (from Parquet)' FROM dbo.ImportedOrders"

# Summary
echo ""
echo -e "${MAGENTA}╔═══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${MAGENTA}║                    ✓ All Demos Complete!                     ║${NC}"
echo -e "${MAGENTA}╚═══════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "${GREEN}Successfully demonstrated:${NC}"
echo -e "  ${GREEN}✓${NC} SQL Server → Parquet export"
echo -e "  ${GREEN}✓${NC} Parquet → SQL Server import"
echo -e "  ${GREEN}✓${NC} SQL Server → SQL Server migration"
echo ""
echo -e "${CYAN}Next: Try the Web UI!${NC}"
echo -e "  Run: ${YELLOW}./demo/run-web-ui-demo.sh${NC}"
echo ""
echo -e "${YELLOW}Explore the data:${NC}"
echo "  • Query source: docker exec -it $CONTAINER_NAME /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P '$SA_PASSWORD' -d SalesSource"
echo "  • Query destination: docker exec -it $CONTAINER_NAME /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P '$SA_PASSWORD' -d SalesDestination"
echo "  • View Parquet files: tree demo/output"
echo ""
echo -e "${YELLOW}Cleanup:${NC}"
echo "  • Stop: docker stop $CONTAINER_NAME"
echo "  • Remove: docker rm $CONTAINER_NAME"
echo "  • Clean files: rm -rf demo/output"
echo ""
