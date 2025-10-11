-- =====================================================
-- Demo Setup: Create Source and Target SQL Server Databases
-- =====================================================
-- This script creates two databases to demonstrate bidirectional data transfer
-- Run this on SQL Server (LocalDB or full SQL Server instance)

USE master;
GO

-- Drop databases if they exist (for clean demo)
IF EXISTS (SELECT name FROM sys.databases WHERE name = 'IcebergDemo_Source')
BEGIN
    ALTER DATABASE IcebergDemo_Source SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE IcebergDemo_Source;
END
GO

IF EXISTS (SELECT name FROM sys.databases WHERE name = 'IcebergDemo_Target')
BEGIN
    ALTER DATABASE IcebergDemo_Target SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE IcebergDemo_Target;
END
GO

-- Create source database
CREATE DATABASE IcebergDemo_Source;
GO

-- Create target database
CREATE DATABASE IcebergDemo_Target;
GO

-- Switch to source database
USE IcebergDemo_Source;
GO

-- =====================================================
-- Create Sample Tables with Various Data Types
-- =====================================================

-- Table 1: Customers (demonstrates various data types)
CREATE TABLE Customers (
    CustomerID INT PRIMARY KEY IDENTITY(1,1),
    FirstName NVARCHAR(50) NOT NULL,
    LastName NVARCHAR(50) NOT NULL,
    Email VARCHAR(100) NULL,
    IsActive BIT NOT NULL DEFAULT 1,
    DateOfBirth DATE NULL,
    CreatedAt DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    Balance DECIMAL(18, 2) NOT NULL DEFAULT 0.00,
    UniqueIdentifier UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID(),
    Notes NVARCHAR(MAX) NULL
);

-- Table 2: Orders (demonstrates foreign keys and timestamps)
CREATE TABLE Orders (
    OrderID BIGINT PRIMARY KEY IDENTITY(1000,1),
    CustomerID INT NOT NULL,
    OrderDate DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    ShippedDate DATETIME2 NULL,
    TotalAmount DECIMAL(10, 2) NOT NULL,
    Status VARCHAR(20) NOT NULL CHECK (Status IN ('Pending', 'Shipped', 'Delivered', 'Cancelled')),
    OrderNumber VARCHAR(50) NOT NULL UNIQUE,
    FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID)
);

-- Table 3: Products (demonstrates various numeric types)
CREATE TABLE Products (
    ProductID INT PRIMARY KEY IDENTITY(1,1),
    ProductName NVARCHAR(100) NOT NULL,
    Category VARCHAR(50) NOT NULL,
    Price DECIMAL(10, 2) NOT NULL,
    StockQuantity INT NOT NULL DEFAULT 0,
    Weight FLOAT NULL,
    IsDiscontinued BIT NOT NULL DEFAULT 0,
    LastRestocked DATETIMEOFFSET NULL
);

-- =====================================================
-- Insert Sample Data
-- =====================================================

-- Insert Customers
INSERT INTO Customers (FirstName, LastName, Email, IsActive, DateOfBirth, Balance, Notes) VALUES
('John', 'Doe', 'john.doe@example.com', 1, '1985-06-15', 1250.50, 'VIP customer'),
('Jane', 'Smith', 'jane.smith@example.com', 1, '1990-03-22', 3500.00, NULL),
('Bob', 'Johnson', 'bob.j@example.com', 1, '1978-11-08', 750.25, 'Prefers email contact'),
('Alice', 'Williams', 'alice.w@example.com', 0, '1995-01-30', 0.00, 'Account suspended'),
('Charlie', 'Brown', 'charlie.b@example.com', 1, '1982-07-19', 2100.75, NULL),
('Diana', 'Garcia', 'diana.g@example.com', 1, '1988-09-12', 4200.00, 'Premium member'),
('Eve', 'Martinez', 'eve.m@example.com', 1, '1992-04-25', 1850.30, NULL),
('Frank', 'Lopez', 'frank.l@example.com', 1, '1975-12-03', 920.00, 'Loyal customer since 2010'),
('Grace', 'Lee', 'grace.l@example.com', 0, '1998-02-17', 0.00, 'Inactive account'),
('Henry', 'Taylor', 'henry.t@example.com', 1, '1980-08-28', 3350.45, NULL);

-- Insert Products
INSERT INTO Products (ProductName, Category, Price, StockQuantity, Weight, IsDiscontinued, LastRestocked) VALUES
('Laptop Pro 15"', 'Electronics', 1299.99, 45, 2.1, 0, '2024-01-15T10:30:00+00:00'),
('Wireless Mouse', 'Electronics', 29.99, 230, 0.15, 0, '2024-02-20T14:00:00+00:00'),
('Office Chair Deluxe', 'Furniture', 349.99, 15, 18.5, 0, '2024-01-10T09:00:00+00:00'),
('Notebook A4', 'Stationery', 4.99, 500, 0.3, 0, '2024-03-01T11:00:00+00:00'),
('Coffee Maker Pro', 'Appliances', 89.99, 32, 2.8, 0, '2024-02-15T13:30:00+00:00'),
('USB-C Cable', 'Electronics', 12.99, 180, 0.05, 0, '2024-03-05T10:00:00+00:00'),
('Standing Desk', 'Furniture', 599.99, 8, 35.0, 0, '2024-01-20T15:00:00+00:00'),
('Mechanical Keyboard', 'Electronics', 149.99, 67, 1.2, 0, '2024-02-28T12:00:00+00:00'),
('Desk Lamp LED', 'Furniture', 39.99, 95, 0.8, 0, '2024-03-10T09:30:00+00:00'),
('Webcam HD', 'Electronics', 79.99, 54, 0.3, 0, '2024-02-25T16:00:00+00:00');

-- Insert Orders
DECLARE @Customer1 INT = (SELECT CustomerID FROM Customers WHERE Email = 'john.doe@example.com');
DECLARE @Customer2 INT = (SELECT CustomerID FROM Customers WHERE Email = 'jane.smith@example.com');
DECLARE @Customer3 INT = (SELECT CustomerID FROM Customers WHERE Email = 'diana.g@example.com');
DECLARE @Customer4 INT = (SELECT CustomerID FROM Customers WHERE Email = 'eve.m@example.com');
DECLARE @Customer5 INT = (SELECT CustomerID FROM Customers WHERE Email = 'henry.t@example.com');

INSERT INTO Orders (CustomerID, OrderDate, ShippedDate, TotalAmount, Status, OrderNumber) VALUES
(@Customer1, '2024-03-01T10:30:00', '2024-03-02T14:00:00', 1329.98, 'Delivered', 'ORD-2024-001'),
(@Customer2, '2024-03-05T11:15:00', '2024-03-06T09:30:00', 179.98, 'Delivered', 'ORD-2024-002'),
(@Customer3, '2024-03-10T14:20:00', '2024-03-11T10:00:00', 949.97, 'Delivered', 'ORD-2024-003'),
(@Customer1, '2024-03-15T09:45:00', NULL, 89.99, 'Pending', 'ORD-2024-004'),
(@Customer4, '2024-03-18T16:30:00', '2024-03-19T11:00:00', 354.98, 'Shipped', 'ORD-2024-005'),
(@Customer5, '2024-03-20T13:00:00', '2024-03-21T15:30:00', 1449.97, 'Delivered', 'ORD-2024-006'),
(@Customer2, '2024-03-22T10:00:00', NULL, 52.98, 'Pending', 'ORD-2024-007'),
(@Customer3, '2024-03-25T15:45:00', NULL, 599.99, 'Pending', 'ORD-2024-008'),
(@Customer1, '2024-03-28T11:30:00', '2024-03-28T16:00:00', 12.99, 'Delivered', 'ORD-2024-009'),
(@Customer4, '2024-03-30T14:15:00', NULL, 229.97, 'Pending', 'ORD-2024-010');

GO

-- =====================================================
-- Verification Queries
-- =====================================================

-- Show record counts
SELECT 'Customers' AS TableName, COUNT(*) AS RecordCount FROM Customers
UNION ALL
SELECT 'Orders', COUNT(*) FROM Orders
UNION ALL
SELECT 'Products', COUNT(*) FROM Products;

-- Show sample data
SELECT TOP 5 CustomerID, FirstName, LastName, Email, IsActive, Balance, CreatedAt
FROM Customers
ORDER BY CustomerID;

SELECT TOP 5 OrderID, CustomerID, OrderDate, TotalAmount, Status, OrderNumber
FROM Orders
ORDER BY OrderID;

SELECT TOP 5 ProductID, ProductName, Category, Price, StockQuantity, Weight
FROM Products
ORDER BY ProductID;

GO

PRINT '';
PRINT '========================================';
PRINT 'Demo Databases Setup Complete!';
PRINT '========================================';
PRINT 'Source Database: IcebergDemo_Source';
PRINT '  - Customers: 10 records';
PRINT '  - Orders: 10 records';
PRINT '  - Products: 10 records';
PRINT '';
PRINT 'Target Database: IcebergDemo_Target (empty)';
PRINT '';
PRINT 'Next step: Run demo export script';
PRINT '========================================';
