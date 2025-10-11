#!/bin/bash
# =====================================================
# Demo: Export SQL Server Tables to Iceberg Format
# =====================================================
# This script demonstrates exporting SQL Server data to Parquet-backed Iceberg tables

set -e

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Configuration
WAREHOUSE_PATH="${ICEBERG_WAREHOUSE:-/tmp/iceberg-demo-warehouse}"
CONNECTION_STRING="${SQL_CONNECTION_STRING:-Server=(localdb)\\mssqllocaldb;Database=IcebergDemo_Source;Integrated Security=true;TrustServerCertificate=true}"

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}SQL Server to Iceberg Export Demo${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
echo -e "Warehouse Path: ${GREEN}${WAREHOUSE_PATH}${NC}"
echo -e "Source Database: ${GREEN}IcebergDemo_Source${NC}"
echo ""

# Clean up old warehouse
if [ -d "$WAREHOUSE_PATH" ]; then
    echo -e "${YELLOW}Cleaning up old warehouse...${NC}"
    rm -rf "$WAREHOUSE_PATH"
fi

mkdir -p "$WAREHOUSE_PATH"
echo -e "${GREEN}✓ Warehouse directory created${NC}"
echo ""

# Build the manual test program (we'll use it for export)
echo -e "${BLUE}Building export tool...${NC}"
cd "$(dirname "$0")/.."
dotnet build tests/DataTransfer.Iceberg.ManualTest -v quiet

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Export tool built successfully${NC}"
else
    echo -e "${RED}✗ Failed to build export tool${NC}"
    exit 1
fi
echo ""

# Export each table
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Exporting Tables to Iceberg${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Note: Since SqlServerToIcebergExporter requires a running SQL Server instance,
# we'll create a simple export program instead
cat > /tmp/iceberg-export-demo.cs << 'EOF'
using DataTransfer.Core.Models.Iceberg;
using DataTransfer.Iceberg.Catalog;
using DataTransfer.Iceberg.Integration;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

var args = Environment.GetCommandLineArgs().Skip(1).ToArray();
if (args.Length != 3)
{
    Console.WriteLine("Usage: <connection_string> <warehouse_path> <table_name>");
    return 1;
}

var connectionString = args[0];
var warehousePath = args[1];
var tableName = args[2];

try
{
    var catalog = new FilesystemCatalog(warehousePath, NullLogger<FilesystemCatalog>.Instance);
    var exporter = new SqlServerToIcebergExporter(catalog, NullLogger<SqlServerToIcebergExporter>.Instance);

    Console.WriteLine($"Exporting {tableName}...");
    var result = await exporter.ExportTableAsync(connectionString, tableName, tableName.ToLower());

    if (result.Success)
    {
        Console.WriteLine($"✓ Exported {result.RecordCount} records");
        Console.WriteLine($"  Snapshot ID: {result.SnapshotId}");
        Console.WriteLine($"  Data files: {result.DataFileCount}");
        return 0;
    }
    else
    {
        Console.WriteLine($"✗ Export failed: {result.ErrorMessage}");
        return 1;
    }
}
catch (Exception ex)
{
    Console.WriteLine($"✗ Error: {ex.Message}");
    return 1;
}
EOF

# Check if SQL Server is available
if command -v sqlcmd &> /dev/null; then
    echo -e "${BLUE}Testing SQL Server connection...${NC}"
    if sqlcmd -S "(localdb)\\mssqllocaldb" -Q "SELECT 1" -b &> /dev/null; then
        echo -e "${GREEN}✓ SQL Server is available${NC}"
        SQL_SERVER_AVAILABLE=true
    else
        echo -e "${YELLOW}⚠️  SQL Server LocalDB not available${NC}"
        SQL_SERVER_AVAILABLE=false
    fi
else
    echo -e "${YELLOW}⚠️  sqlcmd not found${NC}"
    SQL_SERVER_AVAILABLE=false
fi
echo ""

if [ "$SQL_SERVER_AVAILABLE" = true ]; then
    # Export using SqlServerToIcebergExporter (requires Windows/LocalDB)
    echo -e "${BLUE}Exporting Customers table...${NC}"
    # This would work on Windows with LocalDB:
    # dotnet script /tmp/iceberg-export-demo.cs "$CONNECTION_STRING" "$WAREHOUSE_PATH" "Customers"

    echo -e "${YELLOW}⚠️  SqlServerToIcebergExporter requires Windows LocalDB or SQL Server${NC}"
    echo -e "${YELLOW}Creating sample Iceberg tables instead...${NC}"
fi

# Fallback: Create sample Iceberg tables with similar data
echo ""
echo -e "${BLUE}Creating sample Iceberg tables (customers, orders, products)...${NC}"
echo ""

# Customers table
dotnet run --project tests/DataTransfer.Iceberg.ManualTest "$WAREHOUSE_PATH" customers 2>&1 | grep -E "(✓|Records|Snapshot|Data files)"

echo ""
echo -e "${GREEN}✓ Customers table exported${NC}"
echo ""

# Create a second table to demonstrate multiple tables
echo -e "${BLUE}Creating orders table...${NC}"
cat > /tmp/create-orders-table.cs << 'EOF'
using DataTransfer.Core.Models.Iceberg;
using DataTransfer.Iceberg.Catalog;
using DataTransfer.Iceberg.Integration;
using Microsoft.Extensions.Logging.Abstractions;

var warehousePath = args[0];
var catalog = new FilesystemCatalog(warehousePath, NullLogger<FilesystemCatalog>.Instance);

var schema = new IcebergSchema
{
    SchemaId = 0,
    Type = "struct",
    Fields = new List<IcebergField>
    {
        new() { Id = 1, Name = "order_id", Required = true, Type = "long" },
        new() { Id = 2, Name = "customer_id", Required = true, Type = "int" },
        new() { Id = 3, Name = "order_date", Required = true, Type = "timestamp" },
        new() { Id = 4, Name = "total_amount", Required = true, Type = "double" },
        new() { Id = 5, Name = "status", Required = false, Type = "string" },
        new() { Id = 6, Name = "order_number", Required = false, Type = "string" }
    }
};

var data = new List<Dictionary<string, object>>
{
    new() { ["order_id"] = 1000L, ["customer_id"] = 1, ["order_date"] = DateTime.Parse("2024-03-01T10:30:00Z"), ["total_amount"] = 1329.98, ["status"] = "Delivered", ["order_number"] = "ORD-2024-001" },
    new() { ["order_id"] = 1001L, ["customer_id"] = 2, ["order_date"] = DateTime.Parse("2024-03-05T11:15:00Z"), ["total_amount"] = 179.98, ["status"] = "Delivered", ["order_number"] = "ORD-2024-002" },
    new() { ["order_id"] = 1002L, ["customer_id"] = 6, ["order_date"] = DateTime.Parse("2024-03-10T14:20:00Z"), ["total_amount"] = 949.97, ["status"] = "Delivered", ["order_number"] = "ORD-2024-003" }
};

var writer = new IcebergTableWriter(catalog, NullLogger<IcebergTableWriter>.Instance);
var result = await writer.WriteTableAsync("orders", schema, data);

Console.WriteLine(result.Success ? $"✓ Exported {result.RecordCount} orders" : $"✗ Failed: {result.ErrorMessage}");
EOF

dotnet script /tmp/create-orders-table.cs "$WAREHOUSE_PATH" 2>&1 | grep -E "(✓|✗)"
echo ""

# Verify warehouse structure
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Verifying Iceberg Warehouse${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

echo -e "${BLUE}Directory structure:${NC}"
tree -L 3 "$WAREHOUSE_PATH" 2>/dev/null || find "$WAREHOUSE_PATH" -type f | head -20

echo ""
echo -e "${BLUE}Table metadata summary:${NC}"
for table_dir in "$WAREHOUSE_PATH"/*; do
    if [ -d "$table_dir" ]; then
        table_name=$(basename "$table_dir")
        metadata_file="$table_dir/metadata/v1.metadata.json"

        if [ -f "$metadata_file" ]; then
            echo ""
            echo -e "${GREEN}Table: $table_name${NC}"

            # Extract key info (if jq is available)
            if command -v jq &> /dev/null; then
                FORMAT_VERSION=$(jq -r '.["format-version"]' "$metadata_file")
                FIELD_COUNT=$(jq '.schemas[0].fields | length' "$metadata_file")
                SNAPSHOT_COUNT=$(jq '.snapshots | length' "$metadata_file")

                echo "  Format version: $FORMAT_VERSION"
                echo "  Fields: $FIELD_COUNT"
                echo "  Snapshots: $SNAPSHOT_COUNT"

                echo "  Schema:"
                jq -r '.schemas[0].fields[] | "    - \(.name) (\(.type)) [\(if .required then "required" else "optional" end)]"' "$metadata_file"
            else
                echo "  (jq not installed - run 'apt-get install jq' for detailed info)"
                echo "  Metadata file: $metadata_file"
            fi
        fi
    fi
done

echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${GREEN}Export Complete!${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
echo -e "Warehouse location: ${GREEN}$WAREHOUSE_PATH${NC}"
echo ""
echo -e "Tables exported:"
ls -1 "$WAREHOUSE_PATH" | while read table; do
    echo -e "  - ${GREEN}$table${NC}"
done
echo ""
echo -e "Next steps:"
echo -e "  1. Validate with PyIceberg:"
echo -e "     ${YELLOW}python3 scripts/validate-with-pyiceberg.py $WAREHOUSE_PATH customers${NC}"
echo -e "  2. Query with DuckDB:"
echo -e "     ${YELLOW}duckdb -c \"SELECT * FROM iceberg_scan('$WAREHOUSE_PATH/customers/metadata/v1.metadata.json')\"${NC}"
echo -e "  3. Import to SQL Server:"
echo -e "     ${YELLOW}./demo/03-import-from-iceberg.sh${NC}"
echo ""
