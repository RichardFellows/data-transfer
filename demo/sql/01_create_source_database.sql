-- Create Source Database and Tables
-- This script sets up a sample sales database with different table types

USE master;
GO

-- Create source database
IF EXISTS (SELECT name FROM sys.databases WHERE name = 'SalesSource')
    DROP DATABASE SalesSource;
GO

CREATE DATABASE SalesSource;
GO

USE SalesSource;
GO

-- 1. Orders Table (DATE partitioned)
-- Typical transactional table partitioned by order date
CREATE TABLE dbo.Orders (
    OrderId INT PRIMARY KEY,
    CustomerId INT NOT NULL,
    OrderDate DATE NOT NULL,
    TotalAmount DECIMAL(18,2) NOT NULL,
    Status VARCHAR(20) NOT NULL,
    ShipCountry VARCHAR(50)
);
GO

-- 2. SalesTransactions Table (INT DATE partitioned)
-- Sales data with integer date format (YYYYMMDD)
CREATE TABLE dbo.SalesTransactions (
    TransactionId INT PRIMARY KEY,
    ProductId INT NOT NULL,
    SaleDate INT NOT NULL,  -- Format: YYYYMMDD (e.g., 20240115)
    Quantity INT NOT NULL,
    UnitPrice DECIMAL(18,2) NOT NULL,
    Revenue DECIMAL(18,2) NOT NULL
);
GO

-- 3. Products Table (STATIC)
-- Reference data that changes infrequently
CREATE TABLE dbo.Products (
    ProductId INT PRIMARY KEY,
    ProductName VARCHAR(100) NOT NULL,
    Category VARCHAR(50) NOT NULL,
    ListPrice DECIMAL(18,2) NOT NULL,
    IsActive BIT NOT NULL DEFAULT 1
);
GO

-- 4. CustomerDimension Table (SCD Type 2)
-- Slowly Changing Dimension with historical tracking
CREATE TABLE dbo.CustomerDimension (
    CustomerKey INT PRIMARY KEY,
    CustomerId INT NOT NULL,
    CustomerName VARCHAR(100) NOT NULL,
    Tier VARCHAR(20) NOT NULL,
    EffectiveDate DATE NOT NULL,
    ExpirationDate DATE NULL,  -- NULL means current record
    IsActive BIT NOT NULL DEFAULT 1
);
GO

PRINT 'Source database and tables created successfully!';
