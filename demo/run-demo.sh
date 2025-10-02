#!/bin/bash
set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
CONTAINER_NAME="datatransfer-demo"
SA_PASSWORD="YourStrong@Passw0rd"
SQL_PORT="1433"
SQL_IMAGE="mcr.microsoft.com/mssql/server:2022-latest"

echo -e "${BLUE}═══════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}    DataTransfer Demo - Automated Setup & Execution    ${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════${NC}"
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
echo -e "${YELLOW}📋 Step 1: Checking Docker environment...${NC}"
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
    echo -e "${YELLOW}🐳 Step 2: Starting SQL Server container...${NC}"
    docker run -d \
        --name $CONTAINER_NAME \
        -e "ACCEPT_EULA=Y" \
        -e "SA_PASSWORD=$SA_PASSWORD" \
        -p $SQL_PORT:1433 \
        $SQL_IMAGE

    echo -e "${GREEN}✓ Container started${NC}"
    wait_for_sql
else
    echo -e "${GREEN}✓ Container already running${NC}"
fi

# Step 3: Create and populate source database
echo ""
echo -e "${YELLOW}📊 Step 3: Creating source database and tables...${NC}"
docker exec -i $CONTAINER_NAME /opt/mssql-tools18/bin/sqlcmd \
    -C -S localhost -U sa -P "$SA_PASSWORD" \
    < demo/sql/01_create_source_database.sql
echo -e "${GREEN}✓ Source database created${NC}"

echo -e "${YELLOW}📝 Step 4: Populating source database with sample data...${NC}"
docker exec -i $CONTAINER_NAME /opt/mssql-tools18/bin/sqlcmd \
    -C -S localhost -U sa -P "$SA_PASSWORD" \
    < demo/sql/02_populate_source_data.sql | grep -E "(Products:|Customer Dimension|Orders:|Sales Transactions:|Sample date)"
echo -e "${GREEN}✓ Source data populated${NC}"

# Step 4: Create destination database
echo ""
echo -e "${YELLOW}🎯 Step 5: Creating destination database...${NC}"
docker exec -i $CONTAINER_NAME /opt/mssql-tools18/bin/sqlcmd \
    -C -S localhost -U sa -P "$SA_PASSWORD" \
    < demo/sql/03_create_destination_database.sql
echo -e "${GREEN}✓ Destination database created${NC}"

# Step 5: Build console app
echo ""
echo -e "${YELLOW}🔨 Step 6: Building DataTransfer.Console...${NC}"
dotnet build src/DataTransfer.Console --configuration Release -v quiet
echo -e "${GREEN}✓ Build complete${NC}"

# Step 6: Run data transfer
echo ""
echo -e "${BLUE}═══════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}            Running Data Transfer Pipeline             ${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════${NC}"
echo ""

dotnet run --project src/DataTransfer.Console --configuration Release -- \
    --config demo/config/demo-config.json

# Step 7: Verify results
echo ""
echo -e "${BLUE}═══════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}               Verifying Transfer Results              ${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════${NC}"
echo ""

echo -e "${YELLOW}📁 Parquet files created:${NC}"
if [ -d "demo/output/parquet" ]; then
    tree demo/output/parquet -L 3 2>/dev/null || find demo/output/parquet -type f -name "*.parquet" | head -10
else
    echo -e "${RED}No Parquet files found${NC}"
fi

echo ""
echo -e "${YELLOW}📊 Destination database row counts:${NC}"
docker exec -i $CONTAINER_NAME /opt/mssql-tools18/bin/sqlcmd \
    -C -S localhost -U sa -P "$SA_PASSWORD" \
    -d SalesDestination \
    -h -1 \
    -Q "SET NOCOUNT ON;
        SELECT 'Orders: ' + CAST(COUNT(*) AS VARCHAR(10)) FROM dbo.Orders
        UNION ALL
        SELECT 'SalesTransactions: ' + CAST(COUNT(*) AS VARCHAR(10)) FROM dbo.SalesTransactions
        UNION ALL
        SELECT 'Products: ' + CAST(COUNT(*) AS VARCHAR(10)) FROM dbo.Products
        UNION ALL
        SELECT 'CustomerDimension: ' + CAST(COUNT(*) AS VARCHAR(10)) FROM dbo.CustomerDimension"

# Step 8: Summary
echo ""
echo -e "${BLUE}═══════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}                   ✓ Demo Complete!                    ${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════${NC}"
echo ""
echo -e "${YELLOW}Next Steps:${NC}"
echo "  • View Parquet files: tree demo/output/parquet"
echo "  • Query destination DB: docker exec -it $CONTAINER_NAME /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P '$SA_PASSWORD' -d SalesDestination"
echo "  • Modify config: edit demo/config/demo-config.json"
echo "  • Run again: ./demo/run-demo.sh"
echo ""
echo -e "${YELLOW}Cleanup:${NC}"
echo "  • Stop container: docker stop $CONTAINER_NAME"
echo "  • Remove container: docker rm $CONTAINER_NAME"
echo "  • Remove Parquet files: rm -rf demo/output/parquet"
echo ""
