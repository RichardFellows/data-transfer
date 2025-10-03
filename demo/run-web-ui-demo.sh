#!/bin/bash
set -e

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

echo -e "${MAGENTA}╔═══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${MAGENTA}║          DataTransfer - Blazor Web UI Demo                   ║${NC}"
echo -e "${MAGENTA}║          Interactive Transfer Configuration & Execution      ║${NC}"
echo -e "${MAGENTA}╚═══════════════════════════════════════════════════════════════╝${NC}"
echo ""

# Check if SQL Server container is running
echo -e "${CYAN}════════════════════════════════════════════════════════════${NC}"
echo -e "${CYAN}  Phase 1: Environment Check${NC}"
echo -e "${CYAN}════════════════════════════════════════════════════════════${NC}"
echo ""

echo -e "${YELLOW}📋 Checking for SQL Server container...${NC}"
if docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo -e "${GREEN}✓ SQL Server container is running${NC}"
else
    echo -e "${YELLOW}⚠️  SQL Server container not found${NC}"
    echo -e "${YELLOW}   Run ./demo/run-bidirectional-demo.sh first to set up test data${NC}"
    echo ""
    read -p "   Do you want to start it now? (y/n): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo -e "${YELLOW}🐳 Starting SQL Server container...${NC}"
        docker run -d \
            --name $CONTAINER_NAME \
            -e "ACCEPT_EULA=Y" \
            -e "SA_PASSWORD=$SA_PASSWORD" \
            -p $SQL_PORT:1433 \
            mcr.microsoft.com/mssql/server:2022-latest > /dev/null

        echo -e "${GREEN}✓ Container started${NC}"

        # Wait for SQL Server
        echo -e "${YELLOW}⏳ Waiting for SQL Server to be ready...${NC}"
        local max_attempts=30
        local attempt=0
        while [ $attempt -lt $max_attempts ]; do
            if docker exec $CONTAINER_NAME /opt/mssql-tools18/bin/sqlcmd \
                -C -S localhost -U sa -P "$SA_PASSWORD" \
                -Q "SELECT 1" &> /dev/null; then
                echo -e "${GREEN}✓ SQL Server is ready!${NC}"
                break
            fi
            attempt=$((attempt + 1))
            echo -n "."
            sleep 2
        done

        echo ""
        echo -e "${YELLOW}💡 Note: For best experience, run ./demo/run-bidirectional-demo.sh${NC}"
        echo -e "${YELLOW}   first to create sample databases and data.${NC}"
    else
        echo -e "${YELLOW}Continuing without SQL Server (some features will be limited)${NC}"
    fi
fi

# Build web project
echo ""
echo -e "${CYAN}════════════════════════════════════════════════════════════${NC}"
echo -e "${CYAN}  Phase 2: Build Web Application${NC}"
echo -e "${CYAN}════════════════════════════════════════════════════════════${NC}"
echo ""

echo -e "${YELLOW}🔨 Building DataTransfer.Web...${NC}"
dotnet build src/DataTransfer.Web --configuration Release -v quiet
echo -e "${GREEN}✓ Build complete${NC}"

# Start web UI
echo ""
echo -e "${CYAN}════════════════════════════════════════════════════════════${NC}"
echo -e "${CYAN}  Phase 3: Launch Web UI${NC}"
echo -e "${CYAN}════════════════════════════════════════════════════════════${NC}"
echo ""

echo -e "${GREEN}🌐 Starting Blazor Server Web UI...${NC}"
echo ""
echo -e "${MAGENTA}╔═══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${MAGENTA}║                     Web UI Features                           ║${NC}"
echo -e "${MAGENTA}╠═══════════════════════════════════════════════════════════════╣${NC}"
echo -e "${MAGENTA}║  📊 Dashboard - View transfer statistics and history          ║${NC}"
echo -e "${MAGENTA}║  ➕ New Transfer - Interactive configuration builder          ║${NC}"
echo -e "${MAGENTA}║  📜 History - View past transfers with details                ║${NC}"
echo -e "${MAGENTA}║  🔍 Smart Dropdowns - Browse databases, schemas, tables       ║${NC}"
echo -e "${MAGENTA}║  🔌 Connection Presets - Quick access to saved connections    ║${NC}"
echo -e "${MAGENTA}╚═══════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "${CYAN}Available Transfer Types:${NC}"
echo -e "  ${GREEN}1. SQL Server → Parquet${NC}"
echo -e "     Export SQL data to Parquet files for archival/analytics"
echo ""
echo -e "  ${GREEN}2. Parquet → SQL Server${NC}"
echo -e "     Import Parquet files into SQL Server tables"
echo ""
echo -e "${CYAN}New in this version:${NC}"
echo -e "  ${GREEN}✓ Dynamic table discovery${NC} - No more manual typing!"
echo -e "  ${GREEN}✓ Cascading dropdowns${NC} - Database → Schema → Table"
echo -e "  ${GREEN}✓ Table search${NC} - Filter tables by name"
echo -e "  ${GREEN}✓ Connection presets${NC} - LocalDemo and LocalIntegrated"
echo -e "  ${GREEN}✓ Recent connections${NC} - Quick reconnect to last 3 servers"
echo ""
echo -e "${YELLOW}Connection String Presets:${NC}"
echo -e "  ${GREEN}LocalDemo:${NC}       Server=localhost,1433;User Id=sa;Password=YourStrong@Passw0rd;TrustServerCertificate=true"
echo -e "  ${GREEN}LocalIntegrated:${NC} Server=localhost;Integrated Security=true;TrustServerCertificate=true"
echo ""
echo -e "${YELLOW}Sample Databases (if you ran run-bidirectional-demo.sh):${NC}"
echo -e "  • SalesSource - Contains Orders, Products, SalesTransactions, CustomerDimension"
echo -e "  • SalesDestination - Target for transfers"
echo ""
echo -e "${CYAN}How to use:${NC}"
echo -e "  1. Select '${GREEN}LocalDemo${NC}' from Connection Preset dropdown"
echo -e "  2. Click '${GREEN}Test Connection${NC}' button"
echo -e "  3. Choose database from dropdown (e.g., '${GREEN}SalesSource${NC}')"
echo -e "  4. Select schema (defaults to '${GREEN}dbo${NC}')"
echo -e "  5. Pick a table from dropdown (use search to filter)"
echo ""
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}The web UI will be available at: ${YELLOW}http://localhost:5000${NC}"
echo -e "${GREEN}Press ${RED}Ctrl+C${GREEN} to stop the server${NC}"
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

# Run the web UI
dotnet run --project src/DataTransfer.Web --configuration Release --urls http://localhost:5000
