#!/bin/bash
# =====================================================
# Main Demo Runner: SQL Server ↔ Iceberg Bidirectional Transfer
# =====================================================

set -e

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
CYAN='\033[0;36m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo ""
echo -e "${CYAN}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${CYAN}║                                                            ║${NC}"
echo -e "${CYAN}║  SQL Server ↔ Iceberg Bidirectional Transfer Demo         ║${NC}"
echo -e "${CYAN}║                                                            ║${NC}"
echo -e "${CYAN}║  Demonstrates:                                             ║${NC}"
echo -e "${CYAN}║  • Export SQL Server tables to Parquet-backed Iceberg      ║${NC}"
echo -e "${CYAN}║  • Validate Iceberg table structure and compliance         ║${NC}"
echo -e "${CYAN}║  • Query Iceberg data with DuckDB                          ║${NC}"
echo -e "${CYAN}║  • Import Iceberg data back to SQL Server                  ║${NC}"
echo -e "${CYAN}║                                                            ║${NC}"
echo -e "${CYAN}╚════════════════════════════════════════════════════════════╝${NC}"
echo ""

# Display menu
echo -e "${BLUE}Select demo mode:${NC}"
echo ""
echo "  1. Full Demo (all steps)"
echo "  2. Export Only (SQL Server → Iceberg)"
echo "  3. Query Iceberg with DuckDB"
echo "  4. Validate Iceberg Tables"
echo "  5. Cleanup (remove demo data)"
echo "  6. Exit"
echo ""
read -p "Enter choice [1-6]: " choice

case $choice in
    1)
        echo ""
        echo -e "${BLUE}========================================${NC}"
        echo -e "${BLUE}Running Full Demo${NC}"
        echo -e "${BLUE}========================================${NC}"
        echo ""

        # Step 0: Setup SQL Server in Docker
        echo -e "${BLUE}Step 0: Setting up SQL Server in Docker...${NC}"
        "$SCRIPT_DIR/00-setup-sqlserver-docker.sh"
        echo ""

        # Load connection details
        if [ -f /tmp/sqlserver-demo-connection.env ]; then
            source /tmp/sqlserver-demo-connection.env
        fi

        # Step 1: Setup databases
        echo -e "${BLUE}Step 1: Setting up demo databases...${NC}"
        if [ -n "$SQL_CONTAINER_NAME" ] && docker ps --format '{{.Names}}' | grep -q "^${SQL_CONTAINER_NAME}$"; then
            # Use Docker container
            docker exec "$SQL_CONTAINER_NAME" /opt/mssql-tools/bin/sqlcmd \
                -S localhost -U sa -P "$SQL_SERVER_PASSWORD" \
                -i /host/demo/01-setup-demo-databases.sql -b 2>&1 || \
            docker cp "$SCRIPT_DIR/01-setup-demo-databases.sql" "$SQL_CONTAINER_NAME:/tmp/setup.sql" && \
            docker exec "$SQL_CONTAINER_NAME" /opt/mssql-tools/bin/sqlcmd \
                -S localhost -U sa -P "$SQL_SERVER_PASSWORD" \
                -i /tmp/setup.sql -b
            echo -e "${GREEN}✓ Demo databases created${NC}"
        elif command -v sqlcmd &> /dev/null; then
            # Use local sqlcmd
            if sqlcmd -S "(localdb)\\mssqllocaldb" -i "$SCRIPT_DIR/01-setup-demo-databases.sql" -b; then
                echo -e "${GREEN}✓ Demo databases created${NC}"
            else
                echo -e "${YELLOW}⚠️  SQL Server not available - will use sample data${NC}"
            fi
        else
            echo -e "${YELLOW}⚠️  SQL Server not available - will use sample data${NC}"
        fi
        echo ""

        # Step 2: Export to Iceberg
        echo -e "${BLUE}Step 2: Exporting to Iceberg...${NC}"
        "$SCRIPT_DIR/02-export-to-iceberg.sh"
        echo ""

        # Step 3: Validate Iceberg tables
        echo -e "${BLUE}Step 3: Validating Iceberg tables...${NC}"
        WAREHOUSE_PATH="${ICEBERG_WAREHOUSE:-/tmp/iceberg-demo-warehouse}"
        if [ -d "$WAREHOUSE_PATH" ]; then
            for table_dir in "$WAREHOUSE_PATH"/*; do
                if [ -d "$table_dir" ]; then
                    table_name=$(basename "$table_dir")
                    echo ""
                    echo -e "${CYAN}Validating table: $table_name${NC}"
                    "$SCRIPT_DIR/../scripts/validate-iceberg-table.sh" "$WAREHOUSE_PATH" "$table_name" | head -30
                fi
            done
        fi
        echo ""

        # Step 4: Query with DuckDB (if available)
        if command -v duckdb &> /dev/null; then
            echo -e "${BLUE}Step 4: Querying with DuckDB...${NC}"
            "$SCRIPT_DIR/05-query-iceberg-with-duckdb.sh"
        else
            echo -e "${YELLOW}⚠️  DuckDB not installed - skipping query demo${NC}"
            echo "   Install: https://duckdb.org/docs/installation/"
        fi
        echo ""

        # Step 5: Import verification (requires SQL Server)
        if command -v sqlcmd &> /dev/null; then
            echo -e "${BLUE}Step 5: Setting up target database...${NC}"
            sqlcmd -S "(localdb)\\mssqllocaldb" -i "$SCRIPT_DIR/03-import-from-iceberg.sql" -b || true
        fi
        echo ""

        echo -e "${GREEN}========================================${NC}"
        echo -e "${GREEN}Full Demo Complete!${NC}"
        echo -e "${GREEN}========================================${NC}"
        ;;

    2)
        echo ""
        echo -e "${BLUE}Running Export Demo...${NC}"
        "$SCRIPT_DIR/02-export-to-iceberg.sh"
        ;;

    3)
        echo ""
        echo -e "${BLUE}Running DuckDB Query Demo...${NC}"
        "$SCRIPT_DIR/05-query-iceberg-with-duckdb.sh"
        ;;

    4)
        echo ""
        echo -e "${BLUE}Running Validation...${NC}"
        WAREHOUSE_PATH="${ICEBERG_WAREHOUSE:-/tmp/iceberg-demo-warehouse}"

        if [ ! -d "$WAREHOUSE_PATH" ]; then
            echo -e "${RED}✗ Warehouse not found: $WAREHOUSE_PATH${NC}"
            echo "Run export demo first"
            exit 1
        fi

        for table_dir in "$WAREHOUSE_PATH"/*; do
            if [ -d "$table_dir" ]; then
                table_name=$(basename "$table_dir")
                echo ""
                "$SCRIPT_DIR/../scripts/validate-iceberg-table.sh" "$WAREHOUSE_PATH" "$table_name"
            fi
        done
        ;;

    5)
        echo ""
        echo -e "${YELLOW}Cleaning up demo data...${NC}"

        # Remove warehouse
        WAREHOUSE_PATH="${ICEBERG_WAREHOUSE:-/tmp/iceberg-demo-warehouse}"
        if [ -d "$WAREHOUSE_PATH" ]; then
            rm -rf "$WAREHOUSE_PATH"
            echo -e "${GREEN}✓ Removed warehouse: $WAREHOUSE_PATH${NC}"
        fi

        # Remove temp files
        rm -f /tmp/iceberg-customers-export.csv
        rm -f /tmp/iceberg-export-demo.cs
        rm -f /tmp/create-orders-table.cs

        # Remove Docker container (if exists)
        if [ -f /tmp/sqlserver-demo-connection.env ]; then
            source /tmp/sqlserver-demo-connection.env
            if docker ps -a --format '{{.Names}}' | grep -q "^${SQL_CONTAINER_NAME}$"; then
                echo -e "${YELLOW}Removing SQL Server Docker container...${NC}"
                docker rm -f "$SQL_CONTAINER_NAME" 2>/dev/null || true
                echo -e "${GREEN}✓ Removed container: $SQL_CONTAINER_NAME${NC}"
            fi
            rm -f /tmp/sqlserver-demo-connection.env
        fi

        # Drop SQL Server databases (if available via sqlcmd)
        if command -v sqlcmd &> /dev/null; then
            sqlcmd -S "(localdb)\\mssqllocaldb" -Q "
                IF EXISTS (SELECT name FROM sys.databases WHERE name = 'IcebergDemo_Source')
                BEGIN
                    ALTER DATABASE IcebergDemo_Source SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                    DROP DATABASE IcebergDemo_Source;
                END
                IF EXISTS (SELECT name FROM sys.databases WHERE name = 'IcebergDemo_Target')
                BEGIN
                    ALTER DATABASE IcebergDemo_Target SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                    DROP DATABASE IcebergDemo_Target;
                END
            " -b 2>/dev/null || true
            echo -e "${GREEN}✓ Dropped demo databases${NC}"
        fi

        echo -e "${GREEN}✓ Cleanup complete${NC}"
        ;;

    6)
        echo "Exiting..."
        exit 0
        ;;

    *)
        echo -e "${RED}Invalid choice${NC}"
        exit 1
        ;;
esac

echo ""
echo -e "${CYAN}========================================${NC}"
echo -e "${CYAN}Demo Complete${NC}"
echo -e "${CYAN}========================================${NC}"
echo ""
echo "For more information:"
echo "  - Validation guide: docs/iceberg-validation-guide.md"
echo "  - Integration plan: docs/ICEBERG_INTEGRATION_PLAN.md"
echo ""
