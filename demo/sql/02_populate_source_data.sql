-- Populate Source Database with Sample Data
USE SalesSource;
GO

-- 1. Insert Products (Static Reference Data)
-- 20 products across different categories
INSERT INTO dbo.Products (ProductId, ProductName, Category, ListPrice, IsActive) VALUES
(1, 'Laptop Pro 15"', 'Electronics', 1299.99, 1),
(2, 'Wireless Mouse', 'Electronics', 29.99, 1),
(3, 'USB-C Cable', 'Accessories', 19.99, 1),
(4, 'Office Chair Deluxe', 'Furniture', 399.99, 1),
(5, 'Standing Desk', 'Furniture', 599.99, 1),
(6, 'Monitor 27" 4K', 'Electronics', 449.99, 1),
(7, 'Keyboard Mechanical', 'Electronics', 129.99, 1),
(8, 'Desk Lamp LED', 'Accessories', 45.99, 1),
(9, 'Notebook Set', 'Stationery', 12.99, 1),
(10, 'Pen Box Premium', 'Stationery', 24.99, 1),
(11, 'Webcam HD', 'Electronics', 89.99, 1),
(12, 'Headphones Wireless', 'Electronics', 199.99, 1),
(13, 'Phone Stand', 'Accessories', 15.99, 1),
(14, 'Cable Organizer', 'Accessories', 8.99, 1),
(15, 'Laptop Bag', 'Accessories', 59.99, 1),
(16, 'External SSD 1TB', 'Electronics', 149.99, 1),
(17, 'Docking Station', 'Electronics', 279.99, 1),
(18, 'Whiteboard Portable', 'Furniture', 129.99, 1),
(19, 'Filing Cabinet', 'Furniture', 189.99, 1),
(20, 'Desk Mat Large', 'Accessories', 34.99, 1);
GO

-- 2. Insert Customer Dimension (SCD2 - Historical Data)
-- Some customers with tier changes over time
INSERT INTO dbo.CustomerDimension (CustomerKey, CustomerId, CustomerName, Tier, EffectiveDate, ExpirationDate, IsActive) VALUES
-- Customer 101: Bronze -> Silver -> Gold progression
(1, 101, 'Acme Corp', 'Bronze', '2024-01-01', '2024-03-31', 0),
(2, 101, 'Acme Corp', 'Silver', '2024-04-01', '2024-06-30', 0),
(3, 101, 'Acme Corp', 'Gold', '2024-07-01', NULL, 1),

-- Customer 102: Silver -> Gold
(4, 102, 'TechStart Inc', 'Silver', '2024-01-01', '2024-05-15', 0),
(5, 102, 'TechStart Inc', 'Gold', '2024-05-16', NULL, 1),

-- Customer 103: Always Gold
(6, 103, 'Global Solutions Ltd', 'Gold', '2024-01-01', NULL, 1),

-- Customer 104: Bronze only
(7, 104, 'Small Business Co', 'Bronze', '2024-01-01', NULL, 1),

-- Customer 105: Silver -> Platinum
(8, 105, 'Enterprise Group', 'Silver', '2024-01-01', '2024-08-01', 0),
(9, 105, 'Enterprise Group', 'Platinum', '2024-08-01', NULL, 1);
GO

-- 3. Insert Orders (DATE partitioned - last 3 months)
-- Generate orders from October to December 2024
DECLARE @OrderId INT = 1;
DECLARE @CurrentDate DATE = '2024-10-01';
DECLARE @EndDate DATE = '2024-12-31';

WHILE @CurrentDate <= @EndDate
BEGIN
    -- 3-7 orders per day
    DECLARE @OrdersToday INT = 3 + (ABS(CHECKSUM(NEWID())) % 5);
    DECLARE @DailyOrder INT = 0;

    WHILE @DailyOrder < @OrdersToday
    BEGIN
        INSERT INTO dbo.Orders (OrderId, CustomerId, OrderDate, TotalAmount, Status, ShipCountry)
        VALUES (
            @OrderId,
            101 + (ABS(CHECKSUM(NEWID())) % 5),  -- Customer 101-105
            @CurrentDate,
            ROUND(50 + (ABS(CHECKSUM(NEWID())) % 1000), 2),
            CASE (ABS(CHECKSUM(NEWID())) % 4)
                WHEN 0 THEN 'Pending'
                WHEN 1 THEN 'Shipped'
                WHEN 2 THEN 'Delivered'
                ELSE 'Completed'
            END,
            CASE (ABS(CHECKSUM(NEWID())) % 5)
                WHEN 0 THEN 'USA'
                WHEN 1 THEN 'Canada'
                WHEN 2 THEN 'UK'
                WHEN 3 THEN 'Germany'
                ELSE 'France'
            END
        );

        SET @OrderId = @OrderId + 1;
        SET @DailyOrder = @DailyOrder + 1;
    END

    SET @CurrentDate = DATEADD(DAY, 1, @CurrentDate);
END
GO

-- 4. Insert Sales Transactions (INT DATE partitioned - last 3 months)
-- Generate sales with integer date format (YYYYMMDD)
DECLARE @TransactionId INT = 1;
DECLARE @CurrentDate DATE = '2024-10-01';
DECLARE @EndDate DATE = '2024-12-31';

WHILE @CurrentDate <= @EndDate
BEGIN
    -- 5-12 transactions per day
    DECLARE @TransToday INT = 5 + (ABS(CHECKSUM(NEWID())) % 8);
    DECLARE @DailyTrans INT = 0;

    WHILE @DailyTrans < @TransToday
    BEGIN
        DECLARE @Quantity INT = 1 + (ABS(CHECKSUM(NEWID())) % 5);
        DECLARE @UnitPrice DECIMAL(18,2) = 10 + (ABS(CHECKSUM(NEWID())) % 500);

        INSERT INTO dbo.SalesTransactions (TransactionId, ProductId, SaleDate, Quantity, UnitPrice, Revenue)
        VALUES (
            @TransactionId,
            1 + (ABS(CHECKSUM(NEWID())) % 20),  -- Product 1-20
            CONVERT(INT, CONVERT(VARCHAR(8), @CurrentDate, 112)),  -- YYYYMMDD format
            @Quantity,
            @UnitPrice,
            @Quantity * @UnitPrice
        );

        SET @TransactionId = @TransactionId + 1;
        SET @DailyTrans = @DailyTrans + 1;
    END

    SET @CurrentDate = DATEADD(DAY, 1, @CurrentDate);
END
GO

-- Display summary
PRINT '=== Data Population Summary ===';
PRINT 'Products: ' + CAST((SELECT COUNT(*) FROM dbo.Products) AS VARCHAR(10));
PRINT 'Customer Dimension Records: ' + CAST((SELECT COUNT(*) FROM dbo.CustomerDimension) AS VARCHAR(10));
PRINT 'Orders: ' + CAST((SELECT COUNT(*) FROM dbo.Orders) AS VARCHAR(10));
PRINT 'Sales Transactions: ' + CAST((SELECT COUNT(*) FROM dbo.SalesTransactions) AS VARCHAR(10));
PRINT '';
PRINT 'Sample date ranges:';
PRINT 'Orders: ' + CONVERT(VARCHAR(10), (SELECT MIN(OrderDate) FROM dbo.Orders), 120) + ' to ' + CONVERT(VARCHAR(10), (SELECT MAX(OrderDate) FROM dbo.Orders), 120);
PRINT 'Sales Transactions: ' + CAST((SELECT MIN(SaleDate) FROM dbo.SalesTransactions) AS VARCHAR(10)) + ' to ' + CAST((SELECT MAX(SaleDate) FROM dbo.SalesTransactions) AS VARCHAR(10));
GO
