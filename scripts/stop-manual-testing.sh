#!/bin/bash

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Stopping Manual Testing Environment${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

WEB_PORT=5000
CONTAINER_NAME="datatransfer-manual-test-sql"
PROJECT_ROOT="/home/richard/sonnet45"
WEB_PROJECT="src/DataTransfer.Web"

# Stop web server
echo -e "${YELLOW}[1/3] Stopping web server...${NC}"
if lsof -Pi :${WEB_PORT} -sTCP:LISTEN -t > /dev/null 2>&1; then
    kill $(lsof -t -i:${WEB_PORT}) 2>/dev/null || true
    echo -e "${GREEN}✓ Web server stopped${NC}"
else
    echo "Web server not running"
fi
echo ""

# Stop SQL Server container
echo -e "${YELLOW}[2/3] Stopping SQL Server container...${NC}"
if docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    docker stop ${CONTAINER_NAME}
    echo -e "${GREEN}✓ SQL Server container stopped${NC}"
else
    echo "SQL Server container not running"
fi
echo ""

# Clean up
echo -e "${YELLOW}[3/3] Cleaning up...${NC}"
# Remove old backups if they exist (no longer needed with env var approach)
APPSETTINGS_BACKUP="${PROJECT_ROOT}/${WEB_PROJECT}/appsettings.json.backup"
if [ -f "${APPSETTINGS_BACKUP}" ]; then
    rm "${APPSETTINGS_BACKUP}"
    echo "Removed old appsettings backup"
fi
echo -e "${GREEN}✓ Cleanup complete${NC}"
echo ""

echo -e "${GREEN}✓ Environment stopped${NC}"
echo ""
echo -e "${BLUE}To completely remove the SQL Server container:${NC}"
echo "  docker rm ${CONTAINER_NAME}"
echo ""
echo -e "${BLUE}To start testing again:${NC}"
echo "  ./scripts/start-manual-testing.sh"
