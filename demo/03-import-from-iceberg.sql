-- =====================================================
-- Demo: Import from Iceberg to SQL Server (Simulated)
-- =====================================================
-- This script demonstrates importing Iceberg data back to SQL Server
-- Note: Direct Iceberg → SQL Server import requires custom tooling or
-- intermediate steps (e.g., Parquet → SQL Server bulk insert)

-- For this demo, we'll create the target schema and show verification queries

USE IcebergDemo_Target;
GO

-- =====================================================
-- Create Target Tables (matching source schema)
-- =====================================================

-- Table 1: Customers
CREATE TABLE Customers (
    CustomerID INT PRIMARY KEY,
    FirstName NVARCHAR(50) NOT NULL,
    LastName NVARCHAR(50) NOT NULL,
    Email VARCHAR(100) NULL,
    IsActive BIT NOT NULL,
    DateOfBirth DATE NULL,
    CreatedAt DATETIME2 NOT NULL,
    Balance DECIMAL(18, 2) NOT NULL,
    UniqueIdentifier UNIQUEIDENTIFIER NOT NULL,
    Notes NVARCHAR(MAX) NULL
);

-- Table 2: Orders
CREATE TABLE Orders (
    OrderID BIGINT PRIMARY KEY,
    CustomerID INT NOT NULL,
    OrderDate DATETIME2 NOT NULL,
    ShippedDate DATETIME2 NULL,
    TotalAmount DECIMAL(10, 2) NOT NULL,
    Status VARCHAR(20) NOT NULL,
    OrderNumber VARCHAR(50) NOT NULL UNIQUE
);

-- Table 3: Products
CREATE TABLE Products (
    ProductID INT PRIMARY KEY,
    ProductName NVARCHAR(100) NOT NULL,
    Category VARCHAR(50) NOT NULL,
    Price DECIMAL(10, 2) NOT NULL,
    StockQuantity INT NOT NULL,
    Weight FLOAT NULL,
    IsDiscontinued BIT NOT NULL,
    LastRestocked DATETIMEOFFSET NULL
);

GO

PRINT '';
PRINT '========================================';
PRINT 'Target Schema Created';
PRINT '========================================';
PRINT 'Tables: Customers, Orders, Products';
PRINT '';
PRINT 'Ready for data import from Iceberg/Parquet files';
PRINT '';
PRINT 'Import methods:';
PRINT '  1. SQL Server BULK INSERT from Parquet (requires native Parquet support)';
PRINT '  2. Custom .NET tool to read Parquet and insert to SQL Server';
PRINT '  3. Use intermediate CSV export from DuckDB';
PRINT '';
PRINT 'Next: Run verification script after import';
PRINT '========================================';
GO
