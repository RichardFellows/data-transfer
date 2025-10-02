-- Create Destination Database
-- Mirror structure of source database for data transfer

USE master;
GO

-- Create destination database
IF EXISTS (SELECT name FROM sys.databases WHERE name = 'SalesDestination')
    DROP DATABASE SalesDestination;
GO

CREATE DATABASE SalesDestination;
GO

USE SalesDestination;
GO

-- Create identical table structures (without data)

CREATE TABLE dbo.Orders (
    OrderId INT PRIMARY KEY,
    CustomerId INT NOT NULL,
    OrderDate DATE NOT NULL,
    TotalAmount DECIMAL(18,2) NOT NULL,
    Status VARCHAR(20) NOT NULL,
    ShipCountry VARCHAR(50)
);
GO

CREATE TABLE dbo.SalesTransactions (
    TransactionId INT PRIMARY KEY,
    ProductId INT NOT NULL,
    SaleDate INT NOT NULL,
    Quantity INT NOT NULL,
    UnitPrice DECIMAL(18,2) NOT NULL,
    Revenue DECIMAL(18,2) NOT NULL
);
GO

CREATE TABLE dbo.Products (
    ProductId INT PRIMARY KEY,
    ProductName VARCHAR(100) NOT NULL,
    Category VARCHAR(50) NOT NULL,
    ListPrice DECIMAL(18,2) NOT NULL,
    IsActive BIT NOT NULL DEFAULT 1
);
GO

CREATE TABLE dbo.CustomerDimension (
    CustomerKey INT PRIMARY KEY,
    CustomerId INT NOT NULL,
    CustomerName VARCHAR(100) NOT NULL,
    Tier VARCHAR(20) NOT NULL,
    EffectiveDate DATE NOT NULL,
    ExpirationDate DATE NULL,
    IsActive BIT NOT NULL DEFAULT 1
);
GO

PRINT 'Destination database and tables created successfully!';
PRINT 'Ready to receive transferred data.';
