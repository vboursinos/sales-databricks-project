BEGIN
    -- Create a temporary table to hold intermediate results
    EXECUTE IMMEDIATE 'CREATE GLOBAL TEMPORARY TABLE TEMP_SALES_DATA (
        SalesID INT,
        CustomerID INT,
        ProductID INT,
        SalesAmount NUMBER(10, 2),
        SalesDate DATE,
        Region VARCHAR2(50),
        ParentSalesID INT
    ) ON COMMIT PRESERVE ROWS';

    -- Insert sample data into the temporary table
    EXECUTE IMMEDIATE 'INSERT INTO TEMP_SALES_DATA (SalesID, CustomerID, ProductID, SalesAmount, SalesDate, Region, ParentSalesID) VALUES 
        (1, 101, 1001, 500.00, SYSDATE - 10, ''North'', NULL),
        (2, 102, 1002, 300.00, SYSDATE - 8, ''South'', 1),
        (3, 103, 1003, 700.00, SYSDATE - 6, ''East'', 1),
        (4, 104, 1004, 200.00, SYSDATE - 4, ''West'', 2),
        (5, 105, 1005, 400.00, SYSDATE - 2, ''North'', 3)';

    -- Perform complex calculations and insert results into the target table
    EXECUTE IMMEDIATE 'INSERT INTO SALES_SUMMARY (CustomerID, TotalSales, AverageSales, MaxSales, MinSales, SalesHierarchy, RegionSummary)
    SELECT 
        CustomerID,
        SUM(SalesAmount) AS TotalSales,
        AVG(SalesAmount) AS AverageSales,
        MAX(SalesAmount) AS MaxSales,
        MIN(SalesAmount) AS MinSales,
        SYS_CONNECT_BY_PATH(SalesID, ''->'') AS SalesHierarchy,
        (SELECT LISTAGG(Region, '','') WITHIN GROUP (ORDER BY Region) FROM TEMP_SALES_DATA WHERE TEMP_SALES_DATA.CustomerID = SD.CustomerID) AS RegionSummary
    FROM TEMP_SALES_DATA SD
    START WITH ParentSalesID IS NULL
    CONNECT BY PRIOR SalesID = ParentSalesID
    GROUP BY CustomerID';

    -- Use window functions to calculate running totals, ranks, and cumulative percentages
    EXECUTE IMMEDIATE 'INSERT INTO SALES_ANALYSIS (SalesID, CustomerID, SalesAmount, RunningTotal, SalesRank, CumulativePercentage)
    SELECT 
        SalesID,
        CustomerID,
        SalesAmount,
        SUM(SalesAmount) OVER (PARTITION BY CustomerID ORDER BY SalesDate) AS RunningTotal,
        RANK() OVER (PARTITION BY CustomerID ORDER BY SalesAmount DESC) AS SalesRank,
        ROUND(SUM(SalesAmount) OVER (PARTITION BY CustomerID ORDER BY SalesDate) / SUM(SalesAmount) OVER (PARTITION BY CustomerID) * 100, 2) AS CumulativePercentage
    FROM TEMP_SALES_DATA';

    -- Use analytical functions to calculate moving averages and standard deviations
    EXECUTE IMMEDIATE 'INSERT INTO SALES_TRENDS (SalesID, CustomerID, SalesAmount, MovingAverage, StdDev)
    SELECT 
        SalesID,
        CustomerID,
        SalesAmount,
        AVG(SalesAmount) OVER (PARTITION BY CustomerID ORDER BY SalesDate ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS MovingAverage,
        STDDEV(SalesAmount) OVER (PARTITION BY CustomerID ORDER BY SalesDate ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS StdDev
    FROM TEMP_SALES_DATA';

    -- Use subqueries and joins to calculate additional metrics
    EXECUTE IMMEDIATE 'INSERT INTO SALES_METRICS (CustomerID, TotalProducts, UniqueRegions, MaxSalesDate)
    SELECT 
        CustomerID,
        (SELECT COUNT(DISTINCT ProductID) FROM TEMP_SALES_DATA WHERE TEMP_SALES_DATA.CustomerID = SD.CustomerID) AS TotalProducts,
        (SELECT COUNT(DISTINCT Region) FROM TEMP_SALES_DATA WHERE TEMP_SALES_DATA.CustomerID = SD.CustomerID) AS UniqueRegions,
        (SELECT MAX(SalesDate) FROM TEMP_SALES_DATA WHERE TEMP_SALES_DATA.CustomerID = SD.CustomerID) AS MaxSalesDate
    FROM TEMP_SALES_DATA SD
    GROUP BY CustomerID';

    -- Clean up temporary table
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TEMP_SALES_DATA';
END;

