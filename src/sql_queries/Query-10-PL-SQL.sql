-- Step 1: Create a temporary table for intermediate results
CREATE TEMPORARY TABLE TEMP_SALES_DATA (
    SalesID INT,
    CustomerID INT,
    ProductID INT,
    SalesAmount NUMBER(10, 2),
    SalesDate DATE,
    Region STRING,
    ParentSalesID INT
);

-- Step 2: Insert sample data into the temporary table
INSERT INTO TEMP_SALES_DATA (SalesID, CustomerID, ProductID, SalesAmount, SalesDate, Region, ParentSalesID) VALUES
    (1, 101, 1001, 500.00, CURRENT_DATE - 10, 'North', NULL),
    (2, 102, 1002, 300.00, CURRENT_DATE - 8, 'South', 1),
    (3, 103, 1003, 700.00, CURRENT_DATE - 6, 'East', 1),
    (4, 104, 1004, 200.00, CURRENT_DATE - 4, 'West', 2),
    (5, 105, 1005, 400.00, CURRENT_DATE - 2, 'North', 3);