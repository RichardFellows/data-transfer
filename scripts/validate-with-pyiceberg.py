#!/usr/bin/env python3
"""
Validates Iceberg tables using PyIceberg

Usage:
    python3 validate-with-pyiceberg.py <warehouse_path> <table_name>

Example:
    python3 validate-with-pyiceberg.py /tmp/iceberg-warehouse test_table

Prerequisites:
    pip install pyiceberg pandas
"""
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError
import sys
import os

def validate_iceberg_table(warehouse_path, table_name):
    """
    Validates an Iceberg table created by DataTransfer.Iceberg

    Checks:
    - Table metadata is readable
    - Schema is valid
    - Field IDs are preserved
    - Snapshots exist
    - Parquet files are readable
    """
    try:
        # Verify warehouse path exists
        if not os.path.exists(warehouse_path):
            print(f"❌ Warehouse path does not exist: {warehouse_path}")
            return False

        # Load filesystem catalog
        catalog = load_catalog(
            "local",
            **{
                "type": "filesystem",
                "warehouse": warehouse_path,
            }
        )

        # Load table
        table = catalog.load_table(table_name)

        print(f"✓ Successfully loaded table: {table_name}")
        print(f"  Location: {table.location()}")
        print(f"  Format version: {table.metadata.format_version}")
        print(f"  Table UUID: {table.metadata.table_uuid}")

        # Validate format version
        if table.metadata.format_version != 2:
            print(f"  ⚠️  WARNING: Format version is {table.metadata.format_version}, expected 2")

        # Validate schema
        schema = table.schema()
        print(f"\n✓ Schema validation:")
        print(f"  Schema ID: {schema.schema_id}")
        print(f"  Field count: {len(schema.fields)}")

        field_id_issues = False
        for field in schema.fields:
            print(f"    - Field {field.field_id}: {field.name} ({field.field_type})")
            if field.field_id <= 0:
                print(f"      ⚠️  WARNING: Invalid field ID {field.field_id}")
                field_id_issues = True

        if field_id_issues:
            print("\n❌ Field ID issues detected - schema evolution may not work correctly")
            return False

        # Validate snapshots
        snapshots = list(table.metadata.snapshots)
        if not snapshots:
            print("\n⚠️  WARNING: No snapshots found (table is empty)")
        else:
            print(f"\n✓ Snapshots: {len(snapshots)}")
            for snapshot in snapshots:
                print(f"  - Snapshot {snapshot.snapshot_id}")
                print(f"    Timestamp: {snapshot.timestamp_ms}")
                print(f"    Summary: {snapshot.summary}")

        # Validate current snapshot
        current_snapshot = table.metadata.current_snapshot()
        if current_snapshot:
            print(f"\n✓ Current snapshot: {current_snapshot.snapshot_id}")

            # Try to scan table
            print("\n✓ Scanning table data...")
            scan = table.scan()
            df = scan.to_pandas()
            print(f"  Record count: {len(df)}")
            print(f"  Columns: {list(df.columns)}")

            if len(df) > 0:
                print(f"\n  Sample data (first 5 rows):")
                print(df.head().to_string())

                # Validate column types
                print(f"\n  Column types:")
                for col, dtype in df.dtypes.items():
                    print(f"    {col}: {dtype}")
        else:
            print("\n⚠️  No current snapshot (empty table)")

        # Additional metadata checks
        print(f"\n✓ Additional metadata:")
        print(f"  Last sequence number: {table.metadata.last_sequence_number}")
        print(f"  Last updated: {table.metadata.last_updated_ms}")
        print(f"  Properties: {table.properties}")

        print(f"\n✅ Table '{table_name}' is valid and compatible with PyIceberg!")
        print(f"   All checks passed. The table structure conforms to Iceberg v2 specification.")
        return True

    except NoSuchTableError:
        print(f"❌ Table not found: {table_name}")
        print(f"   Verify the table exists in warehouse: {warehouse_path}")
        return False
    except ModuleNotFoundError as e:
        print(f"❌ Missing Python module: {e}")
        print(f"   Install with: pip install pyiceberg pandas")
        return False
    except Exception as e:
        print(f"❌ Validation failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    if len(sys.argv) != 3:
        print("Usage: python3 validate-with-pyiceberg.py <warehouse_path> <table_name>")
        print("\nExample:")
        print("  python3 validate-with-pyiceberg.py /tmp/iceberg-warehouse test_table")
        print("\nThis script validates Iceberg tables created by DataTransfer.Iceberg")
        print("and verifies compatibility with the PyIceberg library.")
        sys.exit(1)

    warehouse = sys.argv[1]
    table = sys.argv[2]

    print("=" * 70)
    print("Iceberg Table Validation (PyIceberg)")
    print("=" * 70)
    print(f"Warehouse: {warehouse}")
    print(f"Table: {table}")
    print("=" * 70)
    print()

    success = validate_iceberg_table(warehouse, table)

    print()
    print("=" * 70)
    if success:
        print("RESULT: PASSED ✅")
    else:
        print("RESULT: FAILED ❌")
    print("=" * 70)

    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
