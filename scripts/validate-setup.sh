#!/bin/bash
# =====================================================
# DataTransfer Setup Validation Script
# =====================================================
# This script validates that your environment is ready for DataTransfer

set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}DataTransfer Setup Validation${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

ISSUES_FOUND=0

# =====================================================
# Check .NET SDK
# =====================================================
echo -e "${BLUE}[1/6] Checking .NET SDK...${NC}"
if command -v dotnet &> /dev/null; then
    DOTNET_VERSION=$(dotnet --version)
    MAJOR_VERSION=$(echo "$DOTNET_VERSION" | cut -d. -f1)

    if [ "$MAJOR_VERSION" -ge 8 ]; then
        echo -e "${GREEN}✓ .NET SDK $DOTNET_VERSION installed${NC}"
    else
        echo -e "${RED}✗ .NET SDK version is $DOTNET_VERSION, but version 8.0+ is required${NC}"
        echo -e "  Install from: https://dotnet.microsoft.com/download/dotnet/8.0"
        ISSUES_FOUND=$((ISSUES_FOUND + 1))
    fi
else
    echo -e "${RED}✗ .NET SDK not found${NC}"
    echo -e "  Install .NET 8 SDK from: https://dotnet.microsoft.com/download/dotnet/8.0"
    ISSUES_FOUND=$((ISSUES_FOUND + 1))
fi
echo ""

# =====================================================
# Check Project Structure
# =====================================================
echo -e "${BLUE}[2/6] Checking project structure...${NC}"
if [ -f "DataTransfer.sln" ]; then
    echo -e "${GREEN}✓ Solution file found${NC}"

    # Check key projects
    REQUIRED_PROJECTS=(
        "src/DataTransfer.Console"
        "src/DataTransfer.Core"
        "src/DataTransfer.Pipeline"
        "config"
    )

    for project in "${REQUIRED_PROJECTS[@]}"; do
        if [ -d "$project" ]; then
            echo -e "${GREEN}  ✓ $project exists${NC}"
        else
            echo -e "${RED}  ✗ $project not found${NC}"
            ISSUES_FOUND=$((ISSUES_FOUND + 1))
        fi
    done
else
    echo -e "${RED}✗ DataTransfer.sln not found${NC}"
    echo -e "  Ensure you're in the project root directory"
    ISSUES_FOUND=$((ISSUES_FOUND + 1))
fi
echo ""

# =====================================================
# Check Build Status
# =====================================================
echo -e "${BLUE}[3/6] Testing build...${NC}"
if command -v dotnet &> /dev/null && [ -f "DataTransfer.sln" ]; then
    echo -e "  Building solution (this may take a minute)..."
    if dotnet build --verbosity quiet > /tmp/datatransfer-build.log 2>&1; then
        echo -e "${GREEN}✓ Solution builds successfully${NC}"
    else
        echo -e "${RED}✗ Build failed${NC}"
        echo -e "  Run 'dotnet build' to see detailed errors"
        echo -e "  Or check: /tmp/datatransfer-build.log"
        ISSUES_FOUND=$((ISSUES_FOUND + 1))
    fi
else
    echo -e "${YELLOW}⊘ Skipped (prerequisites missing)${NC}"
fi
echo ""

# =====================================================
# Check Configuration
# =====================================================
echo -e "${BLUE}[4/6] Checking configuration...${NC}"
if [ -f "config/appsettings.json" ]; then
    echo -e "${GREEN}✓ config/appsettings.json exists${NC}"

    # Basic JSON validation
    if command -v python3 &> /dev/null; then
        if python3 -m json.tool config/appsettings.json > /dev/null 2>&1; then
            echo -e "${GREEN}  ✓ Configuration file is valid JSON${NC}"
        else
            echo -e "${RED}  ✗ Configuration file contains invalid JSON${NC}"
            echo -e "    Run: python3 -m json.tool config/appsettings.json"
            ISSUES_FOUND=$((ISSUES_FOUND + 1))
        fi
    else
        echo -e "${YELLOW}  ⊘ JSON validation skipped (python3 not found)${NC}"
    fi

    # Check for placeholder values
    if grep -q "YOUR_" config/appsettings.json; then
        echo -e "${YELLOW}  ⚠ Configuration contains placeholder values (YOUR_*)${NC}"
        echo -e "    Update connection strings and database names"
    fi
else
    echo -e "${YELLOW}⚠ config/appsettings.json not found${NC}"
    echo -e "  Copy config/appsettings.EXAMPLE.json to config/appsettings.json"
    echo -e "  Then customize for your environment"
fi

if [ -f "config/appsettings.EXAMPLE.json" ]; then
    echo -e "${GREEN}✓ config/appsettings.EXAMPLE.json exists (template)${NC}"
fi
echo ""

# =====================================================
# Check SQL Server Connectivity
# =====================================================
echo -e "${BLUE}[5/6] Checking SQL Server connectivity...${NC}"

SQL_AVAILABLE=false

# Check Docker SQL Server
if command -v docker &> /dev/null; then
    if docker ps --format '{{.Names}}' | grep -q "sqlserver"; then
        CONTAINER_NAME=$(docker ps --format '{{.Names}}' | grep "sqlserver" | head -1)
        echo -e "${GREEN}✓ SQL Server Docker container found: $CONTAINER_NAME${NC}"

        # Try to connect
        if docker exec "$CONTAINER_NAME" /opt/mssql-tools18/bin/sqlcmd \
            -S localhost -U sa -P "IcebergDemo@2024" -Q "SELECT @@VERSION" -C -b &> /dev/null 2>&1 || \
           docker exec "$CONTAINER_NAME" /opt/mssql-tools/bin/sqlcmd \
            -S localhost -U sa -P "IcebergDemo@2024" -Q "SELECT @@VERSION" -b &> /dev/null 2>&1; then
            echo -e "${GREEN}  ✓ Successfully connected to SQL Server${NC}"
            SQL_AVAILABLE=true
        else
            echo -e "${YELLOW}  ⚠ Container exists but connection failed${NC}"
            echo -e "    Check container logs: docker logs $CONTAINER_NAME"
        fi
    fi
fi

# Check local SQL Server (Windows/Linux)
if [ "$SQL_AVAILABLE" = false ] && command -v sqlcmd &> /dev/null; then
    if sqlcmd -S localhost -E -Q "SELECT @@VERSION" -b &> /dev/null 2>&1; then
        echo -e "${GREEN}✓ Local SQL Server found (Windows Authentication)${NC}"
        SQL_AVAILABLE=true
    elif sqlcmd -S "(localdb)\\mssqllocaldb" -E -Q "SELECT @@VERSION" -b &> /dev/null 2>&1; then
        echo -e "${GREEN}✓ SQL Server LocalDB found${NC}"
        SQL_AVAILABLE=true
    fi
fi

if [ "$SQL_AVAILABLE" = false ]; then
    echo -e "${YELLOW}⊘ No SQL Server detected${NC}"
    echo -e "  Options:"
    echo -e "  1. Run: ./demo/00-setup-sqlserver-docker.sh (requires Docker)"
    echo -e "  2. Install SQL Server Express/Developer"
    echo -e "  3. Use remote SQL Server (configure connection string)"
    echo ""
    echo -e "  Note: This is OK if you plan to use a remote SQL Server"
fi
echo ""

# =====================================================
# Check Storage Path
# =====================================================
echo -e "${BLUE}[6/6] Checking storage configuration...${NC}"

# Get storage path from config if it exists
if [ -f "config/appsettings.json" ] && command -v python3 &> /dev/null; then
    STORAGE_PATH=$(python3 -c "
import json
try:
    with open('config/appsettings.json') as f:
        config = json.load(f)
        print(config.get('storage', {}).get('basePath', './parquet-output'))
except:
    print('./parquet-output')
" 2>/dev/null || echo "./parquet-output")
else
    STORAGE_PATH="./parquet-output"
fi

echo -e "  Storage path: ${YELLOW}$STORAGE_PATH${NC}"

# Create directory if it doesn't exist and is relative
if [[ "$STORAGE_PATH" == ./* ]] || [[ "$STORAGE_PATH" == /* ]]; then
    if [ -d "$STORAGE_PATH" ]; then
        echo -e "${GREEN}✓ Storage directory exists${NC}"
    else
        echo -e "${YELLOW}⊘ Storage directory does not exist${NC}"
        echo -e "  Will be created automatically on first transfer"
    fi

    # Check write permissions
    if [ -w "$(dirname "$STORAGE_PATH")" ] || [ -w "$STORAGE_PATH" 2>/dev/null ]; then
        echo -e "${GREEN}✓ Write permissions OK${NC}"
    else
        echo -e "${RED}✗ No write permissions for storage path${NC}"
        echo -e "  Run: sudo chown -R $USER:$USER $STORAGE_PATH"
        ISSUES_FOUND=$((ISSUES_FOUND + 1))
    fi
else
    echo -e "${YELLOW}  ⊘ Custom path - verify manually${NC}"
fi
echo ""

# =====================================================
# Check Optional Tools
# =====================================================
echo -e "${BLUE}Optional Tools (recommended):${NC}"

if command -v docker &> /dev/null; then
    echo -e "${GREEN}✓ Docker installed: $(docker --version | cut -d' ' -f3 | tr -d ',')${NC}"
else
    echo -e "${YELLOW}⊘ Docker not found (needed for demo SQL Server)${NC}"
    echo -e "  Install: https://docs.docker.com/get-docker/"
fi

if command -v jq &> /dev/null; then
    echo -e "${GREEN}✓ jq installed (JSON processing)${NC}"
else
    echo -e "${YELLOW}⊘ jq not found (useful for config validation)${NC}"
    echo -e "  Install: sudo apt-get install jq"
fi

if command -v sqlcmd &> /dev/null; then
    echo -e "${GREEN}✓ sqlcmd installed${NC}"
else
    echo -e "${YELLOW}⊘ sqlcmd not found (needed for SQL Server management)${NC}"
    echo -e "  Install: ./demo/install-sqlcmd-linux.sh"
fi
echo ""

# =====================================================
# Summary
# =====================================================
echo -e "${BLUE}========================================${NC}"
if [ $ISSUES_FOUND -eq 0 ]; then
    echo -e "${GREEN}✓ Setup Validation Complete - No Issues Found!${NC}"
    echo -e "${BLUE}========================================${NC}"
    echo ""
    echo -e "${GREEN}Your environment is ready for DataTransfer!${NC}"
    echo ""
    echo -e "Next steps:"
    echo -e "  1. Configure: Edit config/appsettings.json"
    echo -e "  2. Setup databases: ./demo/00-setup-sqlserver-docker.sh"
    echo -e "  3. Run demo: ./demo/run-demo.sh"
    echo -e "  4. Or read: GETTING_STARTED.md"
    echo ""
else
    echo -e "${YELLOW}⚠ Setup Validation Complete - $ISSUES_FOUND Issue(s) Found${NC}"
    echo -e "${BLUE}========================================${NC}"
    echo ""
    echo -e "${YELLOW}Please resolve the issues above before proceeding.${NC}"
    echo ""
    echo -e "For help, see:"
    echo -e "  - GETTING_STARTED.md (step-by-step guide)"
    echo -e "  - README.md (full documentation)"
    echo -e "  - README.md#troubleshooting (common issues)"
    echo ""
    exit 1
fi
