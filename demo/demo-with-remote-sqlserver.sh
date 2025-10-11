#!/bin/bash
# Demo using a remote SQL Server instance
# Usage: ./demo-with-remote-sqlserver.sh <server> <username> <password>

set -e

if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <server> <username> <password>"
    echo ""
    echo "Examples:"
    echo "  # Windows host from WSL"
    echo "  ./demo-with-remote-sqlserver.sh localhost,1433 sa 'YourPassword'"
    echo ""
    echo "  # Remote SQL Server"
    echo "  ./demo-with-remote-sqlserver.sh myserver.database.windows.net sa 'Password123!'"
    echo ""
    exit 1
fi

SERVER="$1"
USERNAME="$2"
PASSWORD="$3"

# Build connection string
export SQL_CONNECTION_STRING="Server=$SERVER;User Id=$USERNAME;Password=$PASSWORD;TrustServerCertificate=true"

echo "Testing SQL Server connection..."
if command -v sqlcmd &> /dev/null; then
    if sqlcmd -S "$SERVER" -U "$USERNAME" -P "$PASSWORD" -Q "SELECT @@VERSION" -C; then
        echo "✓ Connected to SQL Server"
        echo ""

        # Run setup script
        echo "Setting up demo databases..."
        sqlcmd -S "$SERVER" -U "$USERNAME" -P "$PASSWORD" -i demo/01-setup-demo-databases.sql -C

        # Export to Iceberg
        echo ""
        echo "Exporting to Iceberg..."
        ./demo/02-export-to-iceberg.sh
    else
        echo "✗ Failed to connect to SQL Server"
        exit 1
    fi
else
    echo "✗ sqlcmd not found. Install with:"
    echo "  ./demo/install-sqlcmd-linux.sh"
    exit 1
fi
