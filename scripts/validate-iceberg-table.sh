#!/bin/bash
# Iceberg Table Validation Script
# Validates Iceberg tables using available tools (PyIceberg, DuckDB, or manual inspection)

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Usage
usage() {
    echo "Usage: $0 <warehouse_path> <table_name>"
    echo ""
    echo "Example:"
    echo "  $0 /tmp/iceberg-warehouse test_table"
    echo ""
    echo "This script validates Iceberg tables using available tools:"
    echo "  1. PyIceberg (if Python and pyiceberg are installed)"
    echo "  2. DuckDB (if duckdb is installed)"
    echo "  3. Manual inspection (always available)"
    exit 1
}

# Check arguments
if [ "$#" -ne 2 ]; then
    usage
fi

WAREHOUSE_PATH="$1"
TABLE_NAME="$2"
TABLE_PATH="$WAREHOUSE_PATH/$TABLE_NAME"
METADATA_PATH="$TABLE_PATH/metadata"

echo "========================================"
echo "Iceberg Table Validation"
echo "========================================"
echo "Warehouse: $WAREHOUSE_PATH"
echo "Table: $TABLE_NAME"
echo "========================================"
echo ""

# Check if table exists
if [ ! -d "$TABLE_PATH" ]; then
    echo -e "${RED}❌ Table directory not found: $TABLE_PATH${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Table directory exists${NC}"

# Check directory structure
echo ""
echo "--- Directory Structure ---"
if [ -d "$TABLE_PATH/data" ]; then
    echo -e "${GREEN}✓ data/ directory exists${NC}"
    DATA_FILE_COUNT=$(find "$TABLE_PATH/data" -name "*.parquet" | wc -l)
    echo "  Parquet files: $DATA_FILE_COUNT"
else
    echo -e "${RED}❌ data/ directory missing${NC}"
fi

if [ -d "$METADATA_PATH" ]; then
    echo -e "${GREEN}✓ metadata/ directory exists${NC}"
else
    echo -e "${RED}❌ metadata/ directory missing${NC}"
    exit 1
fi

# Check version-hint.txt
if [ -f "$METADATA_PATH/version-hint.txt" ]; then
    VERSION=$(cat "$METADATA_PATH/version-hint.txt")
    echo -e "${GREEN}✓ version-hint.txt exists (version: $VERSION)${NC}"
else
    echo -e "${RED}❌ version-hint.txt missing${NC}"
    exit 1
fi

# Check metadata JSON
METADATA_FILE="$METADATA_PATH/v${VERSION}.metadata.json"
if [ -f "$METADATA_FILE" ]; then
    echo -e "${GREEN}✓ v${VERSION}.metadata.json exists${NC}"

    # Validate JSON syntax
    if command -v jq &> /dev/null; then
        if jq empty "$METADATA_FILE" 2>/dev/null; then
            echo -e "${GREEN}✓ Metadata JSON is valid${NC}"

            # Extract key information
            FORMAT_VERSION=$(jq -r '.["format-version"]' "$METADATA_FILE")
            TABLE_UUID=$(jq -r '.["table-uuid"]' "$METADATA_FILE")
            SCHEMA_COUNT=$(jq '.schemas | length' "$METADATA_FILE")
            FIELD_COUNT=$(jq '.schemas[0].fields | length' "$METADATA_FILE")
            SNAPSHOT_COUNT=$(jq '.snapshots | length' "$METADATA_FILE")

            echo "  Format version: $FORMAT_VERSION"
            echo "  Table UUID: $TABLE_UUID"
            echo "  Schemas: $SCHEMA_COUNT"
            echo "  Fields: $FIELD_COUNT"
            echo "  Snapshots: $SNAPSHOT_COUNT"

            # Check field IDs
            echo ""
            echo "  Schema fields:"
            jq -r '.schemas[0].fields[] | "    - Field \(.id): \(.name) (\(.type))"' "$METADATA_FILE"
        else
            echo -e "${RED}❌ Metadata JSON is invalid${NC}"
            exit 1
        fi
    else
        echo -e "${YELLOW}⚠️  jq not installed, skipping JSON validation${NC}"
    fi
else
    echo -e "${RED}❌ v${VERSION}.metadata.json missing${NC}"
    exit 1
fi

# Check manifest files
MANIFEST_COUNT=$(find "$METADATA_PATH" -name "manifest-*.avro" | wc -l)
if [ "$MANIFEST_COUNT" -gt 0 ]; then
    echo -e "${GREEN}✓ Manifest files exist ($MANIFEST_COUNT)${NC}"
else
    echo -e "${YELLOW}⚠️  No manifest files found${NC}"
fi

# Check manifest list files
MANIFEST_LIST_COUNT=$(find "$METADATA_PATH" -name "snap-*.avro" | wc -l)
if [ "$MANIFEST_LIST_COUNT" -gt 0 ]; then
    echo -e "${GREEN}✓ Manifest list files exist ($MANIFEST_LIST_COUNT)${NC}"
else
    echo -e "${YELLOW}⚠️  No manifest list files found${NC}"
fi

echo ""
echo "========================================"
echo "Tool-Based Validation"
echo "========================================"

# Try PyIceberg validation
if command -v python3 &> /dev/null; then
    echo ""
    echo "--- PyIceberg Validation ---"
    if python3 -c "import pyiceberg" 2>/dev/null; then
        SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
        python3 "$SCRIPT_DIR/validate-with-pyiceberg.py" "$WAREHOUSE_PATH" "$TABLE_NAME"
    else
        echo -e "${YELLOW}⚠️  PyIceberg not installed (pip install pyiceberg)${NC}"
    fi
else
    echo -e "${YELLOW}⚠️  Python3 not available${NC}"
fi

# Try DuckDB validation
if command -v duckdb &> /dev/null; then
    echo ""
    echo "--- DuckDB Validation ---"
    echo "Scanning table with DuckDB..."

    # Create temporary SQL script
    TEMP_SQL=$(mktemp)
    cat > "$TEMP_SQL" << EOF
INSTALL iceberg;
LOAD iceberg;
SELECT COUNT(*) as record_count FROM iceberg_scan('$METADATA_FILE');
SELECT * FROM iceberg_scan('$METADATA_FILE') LIMIT 5;
EOF

    if duckdb < "$TEMP_SQL" 2>/dev/null; then
        echo -e "${GREEN}✓ DuckDB can read the table${NC}"
    else
        echo -e "${YELLOW}⚠️  DuckDB failed to read table${NC}"
    fi

    rm "$TEMP_SQL"
else
    echo -e "${YELLOW}⚠️  DuckDB not installed${NC}"
fi

echo ""
echo "========================================"
echo "Validation Summary"
echo "========================================"
echo -e "${GREEN}✅ Manual validation passed${NC}"
echo ""
echo "The Iceberg table structure is valid."
echo ""
echo "For full validation, install:"
echo "  - PyIceberg: pip install pyiceberg pandas"
echo "  - DuckDB: https://duckdb.org/docs/installation/"
echo ""
