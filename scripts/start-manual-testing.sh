#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}DataTransfer Manual Testing Setup${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Configuration
SQL_PASSWORD="YourStrong@Passw0rd"
SQL_PORT=1433
WEB_PORT=5000
CONTAINER_NAME="datatransfer-manual-test-sql"
PROJECT_ROOT="/home/richard/sonnet45"
WEB_PROJECT="src/DataTransfer.Web"

# Step 1: Check Docker
echo -e "${YELLOW}[1/6] Checking Docker...${NC}"
if ! docker ps > /dev/null 2>&1; then
    echo -e "${RED}ERROR: Docker is not running. Please start Docker Desktop.${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Docker is running${NC}"
echo ""

# Step 2: Start SQL Server container
echo -e "${YELLOW}[2/6] Starting SQL Server container...${NC}"

# Check if container already exists
if docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo "Container ${CONTAINER_NAME} already exists"

    # Check if it's running
    if docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
        echo -e "${GREEN}✓ SQL Server container already running${NC}"
    else
        echo "Starting existing container..."
        docker start ${CONTAINER_NAME}
        echo -e "${GREEN}✓ SQL Server container started${NC}"
    fi
else
    echo "Creating new SQL Server container..."
    docker run -d \
        --name ${CONTAINER_NAME} \
        -e "ACCEPT_EULA=Y" \
        -e "SA_PASSWORD=${SQL_PASSWORD}" \
        -p ${SQL_PORT}:1433 \
        mcr.microsoft.com/mssql/server:2022-latest

    echo -e "${GREEN}✓ SQL Server container created and started${NC}"
fi
echo ""

# Step 3: Wait for SQL Server to be ready
echo -e "${YELLOW}[3/6] Waiting for SQL Server to be ready...${NC}"
MAX_ATTEMPTS=30
ATTEMPT=0

while [ $ATTEMPT -lt $MAX_ATTEMPTS ]; do
    if docker exec ${CONTAINER_NAME} /opt/mssql-tools18/bin/sqlcmd \
        -C -S localhost -U sa -P "${SQL_PASSWORD}" \
        -Q "SELECT 1" > /dev/null 2>&1; then
        echo -e "${GREEN}✓ SQL Server is ready${NC}"
        break
    fi

    ATTEMPT=$((ATTEMPT + 1))
    echo -n "."
    sleep 1
done

if [ $ATTEMPT -eq $MAX_ATTEMPTS ]; then
    echo -e "${RED}ERROR: SQL Server failed to start within 30 seconds${NC}"
    exit 1
fi
echo ""

# Step 4: Seed databases
echo -e "${YELLOW}[4/6] Seeding test databases...${NC}"

# Create seed script
cat > /tmp/seed-databases.sql << 'EOF'
-- Create databases
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'TestSource')
    CREATE DATABASE TestSource;
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'TestDestination')
    CREATE DATABASE TestDestination;
GO

USE TestSource;
GO

-- Create schemas
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'sales')
    EXEC('CREATE SCHEMA sales');
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'hr')
    EXEC('CREATE SCHEMA hr');
GO

-- Create tables
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Customers' AND schema_id = SCHEMA_ID('dbo'))
    CREATE TABLE dbo.Customers (Id INT PRIMARY KEY, Name NVARCHAR(100));

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Orders' AND schema_id = SCHEMA_ID('sales'))
    CREATE TABLE sales.Orders (Id INT PRIMARY KEY, CustomerId INT, OrderDate DATE);

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Products' AND schema_id = SCHEMA_ID('sales'))
    CREATE TABLE sales.Products (Id INT PRIMARY KEY, ProductName NVARCHAR(100), Price DECIMAL(10,2));

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Employees' AND schema_id = SCHEMA_ID('hr'))
    CREATE TABLE hr.Employees (Id INT PRIMARY KEY, Name NVARCHAR(100), Department NVARCHAR(50));
GO

-- Insert sample data
IF NOT EXISTS (SELECT * FROM dbo.Customers)
BEGIN
    INSERT INTO dbo.Customers VALUES (1, 'Acme Corp'), (2, 'TechStart Inc'), (3, 'Global Solutions');
    INSERT INTO sales.Orders VALUES (1, 1, '2025-01-15'), (2, 2, '2025-01-16');
    INSERT INTO sales.Products VALUES (1, 'Widget', 99.99), (2, 'Gadget', 149.99);
    INSERT INTO hr.Employees VALUES (1, 'Alice', 'Engineering'), (2, 'Bob', 'Sales');
END
GO

PRINT 'Database seeding complete';
EOF

# Copy script to container and execute
docker cp /tmp/seed-databases.sql ${CONTAINER_NAME}:/tmp/seed.sql
docker exec ${CONTAINER_NAME} /opt/mssql-tools18/bin/sqlcmd \
    -C -S localhost -U sa -P "${SQL_PASSWORD}" \
    -i /tmp/seed.sql

echo -e "${GREEN}✓ Databases seeded:${NC}"
echo "  - TestSource (with dbo.Customers, sales.Orders, sales.Products, hr.Employees)"
echo "  - TestDestination (empty)"
rm /tmp/seed-databases.sql
echo ""

# Step 5: Configure connection string (via environment variable)
echo -e "${YELLOW}[5/6] Configuring web application...${NC}"

CONNECTION_STRING="Server=localhost,${SQL_PORT};Database=master;User Id=sa;Password=${SQL_PASSWORD};TrustServerCertificate=True"

# ASP.NET Core reads env vars with __ separator for nested config
export ConnectionStrings__LocalDemo="${CONNECTION_STRING}"

echo -e "${GREEN}✓ Connection string configured (via environment variable)${NC}"
echo ""

# Step 6: Start web server
echo -e "${YELLOW}[6/6] Starting web server...${NC}"

# Kill any existing process on port 5000
if lsof -Pi :${WEB_PORT} -sTCP:LISTEN -t > /dev/null 2>&1; then
    echo "Killing existing process on port ${WEB_PORT}..."
    kill $(lsof -t -i:${WEB_PORT}) 2>/dev/null || true
    sleep 2
fi

cd "${PROJECT_ROOT}"

# Start web server in background with environment variable
nohup env ConnectionStrings__LocalDemo="${CONNECTION_STRING}" \
    dotnet run --project ${WEB_PROJECT} --urls http://localhost:${WEB_PORT} \
    > /tmp/datatransfer-web.log 2>&1 &

WEB_PID=$!
echo "Web server started (PID: ${WEB_PID})"

# Wait for web server to be ready
echo -n "Waiting for web server to be ready"
MAX_ATTEMPTS=30
ATTEMPT=0

while [ $ATTEMPT -lt $MAX_ATTEMPTS ]; do
    if curl -s http://localhost:${WEB_PORT} > /dev/null 2>&1; then
        echo ""
        echo -e "${GREEN}✓ Web server is ready${NC}"
        break
    fi

    ATTEMPT=$((ATTEMPT + 1))
    echo -n "."
    sleep 1
done

if [ $ATTEMPT -eq $MAX_ATTEMPTS ]; then
    echo ""
    echo -e "${RED}ERROR: Web server failed to start within 30 seconds${NC}"
    echo "Check logs: tail -f /tmp/datatransfer-web.log"
    exit 1
fi
echo ""

# Final summary
echo -e "${BLUE}========================================${NC}"
echo -e "${GREEN}✓ Setup Complete!${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
echo -e "${GREEN}Services Running:${NC}"
echo "  • SQL Server:  localhost:${SQL_PORT} (user: sa, password: ${SQL_PASSWORD})"
echo "  • Web Server:  http://localhost:${WEB_PORT}"
echo ""
echo -e "${GREEN}Test Databases:${NC}"
echo "  • TestSource (with sample data)"
echo "  • TestDestination (empty)"
echo ""
echo -e "${GREEN}Available Tables in TestSource:${NC}"
echo "  • dbo.Customers (3 rows)"
echo "  • sales.Orders (2 rows)"
echo "  • sales.Products (2 rows)"
echo "  • hr.Employees (2 rows)"
echo ""
echo -e "${YELLOW}You can now start manual testing!${NC}"
echo "  Open browser: http://localhost:${WEB_PORT}"
echo ""
echo -e "${BLUE}To view web server logs:${NC}"
echo "  tail -f /tmp/datatransfer-web.log"
echo ""
echo -e "${BLUE}To stop services:${NC}"
echo "  ./scripts/stop-manual-testing.sh"
echo ""
echo -e "${GREEN}Happy Testing! 🚀${NC}"
