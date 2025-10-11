-- =====================================================
-- Demo: Verify Roundtrip Data Integrity
-- =====================================================
-- This script compares source and target databases to verify
-- that data was correctly exported to Iceberg and imported back

-- =====================================================
-- Compare Record Counts
-- =====================================================
PRINT '========================================';
PRINT 'Record Count Comparison';
PRINT '========================================';
PRINT '';

SELECT
    'Customers' AS TableName,
    (SELECT COUNT(*) FROM IcebergDemo_Source.dbo.Customers) AS SourceCount,
    (SELECT COUNT(*) FROM IcebergDemo_Target.dbo.Customers) AS TargetCount,
    CASE
        WHEN (SELECT COUNT(*) FROM IcebergDemo_Source.dbo.Customers) =
             (SELECT COUNT(*) FROM IcebergDemo_Target.dbo.Customers)
        THEN 'MATCH ✓'
        ELSE 'MISMATCH ✗'
    END AS Status
UNION ALL
SELECT
    'Orders',
    (SELECT COUNT(*) FROM IcebergDemo_Source.dbo.Orders),
    (SELECT COUNT(*) FROM IcebergDemo_Target.dbo.Orders),
    CASE
        WHEN (SELECT COUNT(*) FROM IcebergDemo_Source.dbo.Orders) =
             (SELECT COUNT(*) FROM IcebergDemo_Target.dbo.Orders)
        THEN 'MATCH ✓'
        ELSE 'MISMATCH ✗'
    END
UNION ALL
SELECT
    'Products',
    (SELECT COUNT(*) FROM IcebergDemo_Source.dbo.Products),
    (SELECT COUNT(*) FROM IcebergDemo_Target.dbo.Products),
    CASE
        WHEN (SELECT COUNT(*) FROM IcebergDemo_Source.dbo.Products) =
             (SELECT COUNT(*) FROM IcebergDemo_Target.dbo.Products)
        THEN 'MATCH ✓'
        ELSE 'MISMATCH ✗'
    END;

PRINT '';
PRINT '========================================';
PRINT 'Sample Data Comparison - Customers';
PRINT '========================================';
PRINT '';

-- Compare sample customer records
SELECT
    s.CustomerID,
    s.FirstName,
    s.LastName,
    s.Email,
    s.Balance AS SourceBalance,
    t.Balance AS TargetBalance,
    CASE
        WHEN s.FirstName = t.FirstName AND
             s.LastName = t.LastName AND
             s.Email = t.Email AND
             s.Balance = t.Balance
        THEN 'MATCH ✓'
        ELSE 'MISMATCH ✗'
    END AS Status
FROM IcebergDemo_Source.dbo.Customers s
FULL OUTER JOIN IcebergDemo_Target.dbo.Customers t ON s.CustomerID = t.CustomerID
ORDER BY s.CustomerID;

PRINT '';
PRINT '========================================';
PRINT 'Data Type Verification';
PRINT '========================================';
PRINT '';

-- Verify data types match
SELECT
    'Source' AS Database,
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE,
    CHARACTER_MAXIMUM_LENGTH,
    NUMERIC_PRECISION,
    NUMERIC_SCALE
FROM IcebergDemo_Source.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'Customers'
UNION ALL
SELECT
    'Target',
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE,
    CHARACTER_MAXIMUM_LENGTH,
    NUMERIC_PRECISION,
    NUMERIC_SCALE
FROM IcebergDemo_Target.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'Customers'
ORDER BY COLUMN_NAME, Database;

PRINT '';
PRINT '========================================';
PRINT 'Null Value Handling Check';
PRINT '========================================';
PRINT '';

-- Check that NULL values were preserved
SELECT
    'Source NULL counts' AS Check_Type,
    SUM(CASE WHEN Email IS NULL THEN 1 ELSE 0 END) AS Email_Nulls,
    SUM(CASE WHEN DateOfBirth IS NULL THEN 1 ELSE 0 END) AS DOB_Nulls,
    SUM(CASE WHEN Notes IS NULL THEN 1 ELSE 0 END) AS Notes_Nulls
FROM IcebergDemo_Source.dbo.Customers
UNION ALL
SELECT
    'Target NULL counts',
    SUM(CASE WHEN Email IS NULL THEN 1 ELSE 0 END),
    SUM(CASE WHEN DateOfBirth IS NULL THEN 1 ELSE 0 END),
    SUM(CASE WHEN Notes IS NULL THEN 1 ELSE 0 END)
FROM IcebergDemo_Target.dbo.Customers;

PRINT '';
PRINT '========================================';
PRINT 'Special Value Tests';
PRINT '========================================';
PRINT '';

-- Test edge cases
SELECT
    'Source' AS Database,
    MIN(Balance) AS Min_Balance,
    MAX(Balance) AS Max_Balance,
    AVG(Balance) AS Avg_Balance,
    MIN(CreatedAt) AS Earliest_Date,
    MAX(CreatedAt) AS Latest_Date
FROM IcebergDemo_Source.dbo.Customers
UNION ALL
SELECT
    'Target',
    MIN(Balance),
    MAX(Balance),
    AVG(Balance),
    MIN(CreatedAt),
    MAX(CreatedAt)
FROM IcebergDemo_Target.dbo.Customers;

-- Test GUID preservation
SELECT
    s.CustomerID,
    s.UniqueIdentifier AS Source_GUID,
    t.UniqueIdentifier AS Target_GUID,
    CASE
        WHEN s.UniqueIdentifier = t.UniqueIdentifier
        THEN 'MATCH ✓'
        ELSE 'MISMATCH ✗'
    END AS GUID_Status
FROM IcebergDemo_Source.dbo.Customers s
JOIN IcebergDemo_Target.dbo.Customers t ON s.CustomerID = t.CustomerID
ORDER BY s.CustomerID;

PRINT '';
PRINT '========================================';
PRINT 'Verification Complete';
PRINT '========================================';
PRINT '';
PRINT 'Check Status column for any mismatches';
PRINT 'All MATCH ✓ = Successful roundtrip!';
PRINT '';
