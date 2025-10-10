-- Iceberg Table Validation using DuckDB
--
-- Usage:
--   duckdb < validate-with-duckdb.sql
--
-- Or interactively:
--   duckdb
--   .read validate-with-duckdb.sql
--
-- Prerequisites:
--   DuckDB 0.10.0+ with Iceberg extension

-- Load Iceberg extension
INSTALL iceberg;
LOAD iceberg;

-- Configuration
-- REPLACE THESE PATHS WITH YOUR ACTUAL TABLE PATHS
.print ''
.print '========================================='
.print 'Iceberg Table Validation (DuckDB)'
.print '========================================='
.print ''

-- Set variables (modify these for your environment)
SET VARIABLE table_metadata_path = '/tmp/iceberg-warehouse/test_table/metadata/v1.metadata.json';

.print 'Validating Iceberg table...'
.print 'Metadata path: ', getvariable('table_metadata_path')
.print ''

-- 1. Validate schema
.print '--- Schema Validation ---'
DESCRIBE iceberg_scan(getvariable('table_metadata_path'));
.print ''

-- 2. Count records
.print '--- Record Count ---'
SELECT COUNT(*) as total_records
FROM iceberg_scan(getvariable('table_metadata_path'));
.print ''

-- 3. Sample data (first 10 rows)
.print '--- Sample Data (First 10 Rows) ---'
SELECT *
FROM iceberg_scan(getvariable('table_metadata_path'))
LIMIT 10;
.print ''

-- 4. Column statistics
.print '--- Column Statistics ---'
SELECT
    column_name,
    data_type,
    COUNT(*) as non_null_count
FROM (
    SELECT *
    FROM iceberg_scan(getvariable('table_metadata_path'))
) t
CROSS JOIN (
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = 'iceberg_scan'
) s
GROUP BY column_name, data_type;
.print ''

.print '========================================='
.print 'Validation Complete'
.print '========================================='
.print ''
.print 'If you see data above, the Iceberg table'
.print 'is valid and compatible with DuckDB!'
.print ''
