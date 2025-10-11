#!/bin/bash
# =====================================================
# Setup SQL Server in Docker for Demo
# =====================================================
# This script creates a SQL Server container for the Iceberg demo

set -e

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

CONTAINER_NAME="${SQL_CONTAINER_NAME:-sqlserver-iceberg-demo}"
SA_PASSWORD="${SA_PASSWORD:-IcebergDemo@2024}"
SQL_PORT="${SQL_PORT:-1433}"

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}SQL Server Docker Setup${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo -e "${RED}✗ Docker is not installed${NC}"
    echo ""
    echo "Install Docker:"
    echo "  Ubuntu/Debian: sudo apt-get install docker.io"
    echo "  Or follow: https://docs.docker.com/engine/install/"
    echo ""
    exit 1
fi

echo -e "${GREEN}✓ Docker is installed${NC}"

# Check if Docker daemon is running
if ! docker ps &> /dev/null; then
    echo -e "${RED}✗ Docker daemon is not running${NC}"
    echo ""
    echo "Start Docker:"
    echo "  sudo systemctl start docker"
    echo "  sudo usermod -aG docker $USER  # Add yourself to docker group"
    echo "  newgrp docker                   # Reload group membership"
    echo ""
    exit 1
fi

echo -e "${GREEN}✓ Docker daemon is running${NC}"
echo ""

# Check if container already exists
if docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo -e "${YELLOW}Container '$CONTAINER_NAME' already exists${NC}"

    # Check if it's running
    if docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
        echo -e "${GREEN}✓ Container is running${NC}"

        # Test connection
        echo ""
        echo -e "${BLUE}Testing SQL Server connection...${NC}"
        sleep 2

        if docker exec "$CONTAINER_NAME" /opt/mssql-tools18/bin/sqlcmd \
            -S localhost -U sa -P "$SA_PASSWORD" -Q "SELECT @@VERSION" -C -b &> /dev/null || \
           docker exec "$CONTAINER_NAME" /opt/mssql-tools/bin/sqlcmd \
            -S localhost -U sa -P "$SA_PASSWORD" -Q "SELECT @@VERSION" -b &> /dev/null; then
            echo -e "${GREEN}✓ SQL Server is ready${NC}"
            echo ""
            echo -e "${GREEN}========================================${NC}"
            echo -e "${GREEN}SQL Server Container Ready${NC}"
            echo -e "${GREEN}========================================${NC}"
            echo ""
            echo -e "Container: ${GREEN}$CONTAINER_NAME${NC}"
            echo -e "Server: ${GREEN}localhost,$SQL_PORT${NC}"
            echo -e "Username: ${GREEN}sa${NC}"
            echo -e "Password: ${GREEN}$SA_PASSWORD${NC}"
            echo ""
            echo "Connection string:"
            echo -e "${YELLOW}Server=localhost,$SQL_PORT;User Id=sa;Password=$SA_PASSWORD;TrustServerCertificate=true${NC}"
            echo ""
            exit 0
        else
            echo -e "${YELLOW}⚠️  SQL Server not ready yet, waiting...${NC}"
        fi
    else
        echo -e "${YELLOW}Starting existing container...${NC}"
        docker start "$CONTAINER_NAME"
    fi
else
    # Create new container
    echo -e "${BLUE}Creating SQL Server container...${NC}"
    echo ""
    echo "Container name: $CONTAINER_NAME"
    echo "Port: $SQL_PORT"
    echo "SA Password: $SA_PASSWORD"
    echo ""

    docker run \
        --name "$CONTAINER_NAME" \
        -e "ACCEPT_EULA=Y" \
        -e "SA_PASSWORD=$SA_PASSWORD" \
        -e "MSSQL_PID=Developer" \
        -p "$SQL_PORT:1433" \
        -d mcr.microsoft.com/mssql/server:2022-latest

    echo -e "${GREEN}✓ Container created${NC}"
fi

# Wait for SQL Server to be ready
echo ""
echo -e "${BLUE}Waiting for SQL Server to start...${NC}"
RETRY_COUNT=0
MAX_RETRIES=30

while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    # Try both mssql-tools and mssql-tools18 paths
    if docker exec "$CONTAINER_NAME" /opt/mssql-tools18/bin/sqlcmd \
        -S localhost -U sa -P "$SA_PASSWORD" -Q "SELECT 1" -C -b &> /dev/null || \
       docker exec "$CONTAINER_NAME" /opt/mssql-tools/bin/sqlcmd \
        -S localhost -U sa -P "$SA_PASSWORD" -Q "SELECT 1" -b &> /dev/null; then
        echo -e "${GREEN}✓ SQL Server is ready${NC}"
        break
    fi

    RETRY_COUNT=$((RETRY_COUNT + 1))
    echo -n "."
    sleep 2
done
echo ""

if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
    echo -e "${RED}✗ SQL Server failed to start within 60 seconds${NC}"
    echo ""
    echo "Check logs with: docker logs $CONTAINER_NAME"
    exit 1
fi

# Export connection details for other scripts
export SQL_SERVER_HOST="localhost,$SQL_PORT"
export SQL_SERVER_USER="sa"
export SQL_SERVER_PASSWORD="$SA_PASSWORD"
export SQL_CONNECTION_STRING="Server=localhost,$SQL_PORT;User Id=sa;Password=$SA_PASSWORD;TrustServerCertificate=true"

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}SQL Server Container Ready${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo -e "Container: ${GREEN}$CONTAINER_NAME${NC}"
echo -e "Server: ${GREEN}localhost,$SQL_PORT${NC}"
echo -e "Username: ${GREEN}sa${NC}"
echo -e "Password: ${GREEN}$SA_PASSWORD${NC}"
echo ""
echo "Connection string:"
echo -e "${YELLOW}Server=localhost,$SQL_PORT;User Id=sa;Password=$SA_PASSWORD;TrustServerCertificate=true${NC}"
echo ""
echo "Useful commands:"
echo -e "  ${BLUE}docker logs $CONTAINER_NAME${NC}      # View logs"
echo -e "  ${BLUE}docker stop $CONTAINER_NAME${NC}      # Stop container"
echo -e "  ${BLUE}docker start $CONTAINER_NAME${NC}     # Start container"
echo -e "  ${BLUE}docker rm -f $CONTAINER_NAME${NC}     # Remove container"
echo ""
echo "Next step:"
echo -e "  ${YELLOW}./demo/01-setup-demo-databases.sql${NC}"
echo ""

# Save connection details to a file that other scripts can source
cat > /tmp/sqlserver-demo-connection.env << EOF
export SQL_SERVER_HOST="localhost,$SQL_PORT"
export SQL_SERVER_USER="sa"
export SQL_SERVER_PASSWORD="$SA_PASSWORD"
export SQL_CONNECTION_STRING="Server=localhost,$SQL_PORT;User Id=sa;Password=$SA_PASSWORD;TrustServerCertificate=true"
export SQL_CONTAINER_NAME="$CONTAINER_NAME"
EOF

echo -e "${GREEN}✓ Connection details saved to /tmp/sqlserver-demo-connection.env${NC}"
echo ""
