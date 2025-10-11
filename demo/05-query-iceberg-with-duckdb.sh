#!/bin/bash
# =====================================================
# Demo: Query Iceberg Tables with DuckDB
# =====================================================
# This demonstrates querying Iceberg tables using DuckDB
# as an intermediate step or for ad-hoc analysis

set -e

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

WAREHOUSE_PATH="${ICEBERG_WAREHOUSE:-/tmp/iceberg-demo-warehouse}"

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Query Iceberg with DuckDB${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Check if DuckDB is installed
if ! command -v duckdb &> /dev/null; then
    echo -e "${YELLOW}DuckDB not installed${NC}"
    echo ""
    echo "To install DuckDB:"
    echo "  wget https://github.com/duckdb/duckdb/releases/download/v0.10.0/duckdb_cli-linux-amd64.zip"
    echo "  unzip duckdb_cli-linux-amd64.zip"
    echo "  sudo mv duckdb /usr/local/bin/"
    echo ""
    echo "Or use Docker:"
    echo "  docker run -v $WAREHOUSE_PATH:/data -it --rm duckdb/duckdb"
    echo ""
    exit 1
fi

echo -e "${GREEN}✓ DuckDB is installed${NC}"
echo ""

# Check if warehouse exists
if [ ! -d "$WAREHOUSE_PATH" ]; then
    echo -e "${YELLOW}⚠️  Warehouse not found: $WAREHOUSE_PATH${NC}"
    echo "Run ./demo/02-export-to-iceberg.sh first"
    exit 1
fi

echo -e "Warehouse: ${GREEN}$WAREHOUSE_PATH${NC}"
echo ""

# List available tables
echo -e "${BLUE}Available Iceberg Tables:${NC}"
ls -1 "$WAREHOUSE_PATH" | while read table; do
    if [ -d "$WAREHOUSE_PATH/$table" ]; then
        echo -e "  - ${GREEN}$table${NC}"
    fi
done
echo ""

# Query customers table
CUSTOMERS_METADATA="$WAREHOUSE_PATH/customers/metadata/v1.metadata.json"

if [ -f "$CUSTOMERS_METADATA" ]; then
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}Query 1: Count all customers${NC}"
    echo -e "${BLUE}========================================${NC}"
    echo ""

    duckdb << EOF
INSTALL iceberg;
LOAD iceberg;

SELECT COUNT(*) as total_customers
FROM iceberg_scan('$CUSTOMERS_METADATA');
EOF

    echo ""
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}Query 2: Show all customers${NC}"
    echo -e "${BLUE}========================================${NC}"
    echo ""

    duckdb << EOF
INSTALL iceberg;
LOAD iceberg;

SELECT *
FROM iceberg_scan('$CUSTOMERS_METADATA')
ORDER BY id;
EOF

    echo ""
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}Query 3: Active customers with balance > 1000${NC}"
    echo -e "${BLUE}========================================${NC}"
    echo ""

    duckdb << EOF
INSTALL iceberg;
LOAD iceberg;

SELECT
    id,
    name,
    amount as balance,
    is_active
FROM iceberg_scan('$CUSTOMERS_METADATA')
WHERE amount > 1000 AND is_active = true
ORDER BY amount DESC;
EOF

    echo ""
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}Query 4: Aggregate statistics${NC}"
    echo -e "${BLUE}========================================${NC}"
    echo ""

    duckdb << EOF
INSTALL iceberg;
LOAD iceberg;

SELECT
    COUNT(*) as total_customers,
    SUM(CASE WHEN is_active THEN 1 ELSE 0 END) as active_customers,
    SUM(CASE WHEN NOT is_active THEN 1 ELSE 0 END) as inactive_customers,
    ROUND(AVG(amount), 2) as avg_balance,
    ROUND(SUM(amount), 2) as total_balance,
    MIN(amount) as min_balance,
    MAX(amount) as max_balance
FROM iceberg_scan('$CUSTOMERS_METADATA');
EOF

    echo ""
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}Query 5: Export to CSV${NC}"
    echo -e "${BLUE}========================================${NC}"
    echo ""

    OUTPUT_CSV="/tmp/iceberg-customers-export.csv"

    duckdb << EOF
INSTALL iceberg;
LOAD iceberg;

COPY (
    SELECT *
    FROM iceberg_scan('$CUSTOMERS_METADATA')
    ORDER BY id
) TO '$OUTPUT_CSV' (HEADER, DELIMITER ',');
EOF

    echo -e "${GREEN}✓ Exported to: $OUTPUT_CSV${NC}"
    echo ""
    echo "First few lines:"
    head -n 6 "$OUTPUT_CSV" || true

    echo ""
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}Query 6: Schema information${NC}"
    echo -e "${BLUE}========================================${NC}"
    echo ""

    duckdb << EOF
INSTALL iceberg;
LOAD iceberg;

DESCRIBE SELECT * FROM iceberg_scan('$CUSTOMERS_METADATA');
EOF

else
    echo -e "${YELLOW}⚠️  Customers table metadata not found${NC}"
    echo "Expected: $CUSTOMERS_METADATA"
fi

# Query orders table if it exists
ORDERS_METADATA="$WAREHOUSE_PATH/orders/metadata/v1.metadata.json"

if [ -f "$ORDERS_METADATA" ]; then
    echo ""
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}Query 7: Orders summary${NC}"
    echo -e "${BLUE}========================================${NC}"
    echo ""

    duckdb << EOF
INSTALL iceberg;
LOAD iceberg;

SELECT
    COUNT(*) as total_orders,
    SUM(total_amount) as total_revenue,
    AVG(total_amount) as avg_order_value,
    MIN(order_date) as earliest_order,
    MAX(order_date) as latest_order
FROM iceberg_scan('$ORDERS_METADATA');
EOF

    echo ""
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}Query 8: Join customers and orders${NC}"
    echo -e "${BLUE}========================================${NC}"
    echo ""

    duckdb << EOF
INSTALL iceberg;
LOAD iceberg;

WITH customers AS (
    SELECT id as customer_id, name as customer_name, amount as balance
    FROM iceberg_scan('$CUSTOMERS_METADATA')
),
orders AS (
    SELECT customer_id, order_id, total_amount, status, order_date
    FROM iceberg_scan('$ORDERS_METADATA')
)
SELECT
    c.customer_name,
    c.balance,
    COUNT(o.order_id) as order_count,
    COALESCE(SUM(o.total_amount), 0) as total_spent
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_name, c.balance
ORDER BY total_spent DESC;
EOF

fi

echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${GREEN}DuckDB Queries Complete!${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
echo "You can run custom queries with:"
echo -e "  ${YELLOW}duckdb${NC}"
echo -e "  ${YELLOW}INSTALL iceberg; LOAD iceberg;${NC}"
echo -e "  ${YELLOW}SELECT * FROM iceberg_scan('$CUSTOMERS_METADATA');${NC}"
echo ""
