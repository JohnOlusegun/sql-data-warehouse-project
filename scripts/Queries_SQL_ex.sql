=======================================================================
-- WORKING WITH NULLS
=======================================================================
-- Use Case 1: Find average scores of the customers and replacing nulls

SELECT  
    CustomerID,
    Score,
    COALESCE(Score, 0) AS Score2,
    AVG(Score) OVER () AS AvgScores,
    AVG(COALESCE(Score, 0)) OVER () AS AvgScores_Nl
FROM Sales.Customers;

-----------------------------------------------------------------------------------------------
-- Use Case 2: Display the full name of the customers in a single field by merging their first and last names, and add 10 bonus points to each customer's Score)

SELECT 
    CustomerID,
    FirstName,
    LastName,
    FirstName + ' ' + COALESCE(LastName, ' ') AS FullName,
    Score,
    COALESCE(Score, 0) + 10 AS ScoreWithBonus
FROM Sales.Customers;

-----------------------------------------------------------------------------------------------
-- Sort the customers from lowest to highest scores with the NULLs at the end
SELECT 
    CustomerID,
    Score
FROM Sales.Customers 
ORDER BY 
    CASE WHEN Score IS NULL THEN 1 ELSE 0 END, 
    Score;

-- NULLIF: Will check two values; if equal, will return NULL
-- Find the sales price for each order by dividing Sales by Quantity
SELECT
    OrdersID,
    Sales,
    Quantity,
    Sales / NULLIF(Quantity, 0) AS Price 
FROM Sales.Orders;

-- IS NULL and IS NOT NULL
-- Identify the customers who have no scores
SELECT 
    *
FROM Sales.Customers
WHERE Score IS NULL;

-- Handling NULL, BLANK Space, Empty String
WITH Orders AS (
    SELECT 1 AS Id, 'A' AS Category UNION
    SELECT 2, NULL UNION
    SELECT 3, '' UNION
    SELECT 4, '  '
)
SELECT
    *,
    TRIM(Category) AS Policy1,
    NULLIF(TRIM(Category), '') AS Policy2,
    COALESCE(NULLIF(TRIM(Category), ''), 'unknown') AS Policy3
FROM Orders;

=======================================================================
-- CASE STATEMENT
=======================================================================
-- Generate a report showing the total sales for each category (High, Medium, Low)
-- Sort the Category from highest to lowest total sales
SELECT 
    Category,
    SUM(Sales) AS TotalSales
FROM (
    SELECT 
        OrderID,
        Sales,
        CASE
            WHEN Sales > 50 THEN 'High'
            WHEN Sales > 20 THEN 'Medium'
            ELSE 'Low'
        END AS Category 
    FROM Sales.Orders
) AS t
GROUP BY Category 
ORDER BY TotalSales DESC;

USE SalesDB;
-- Using CASE statement to map Values 
-- Retrive employee details with gender displayed as full text 
SELECT 
    EmployeeID,
    FirstName,
    LastName,
    Gender,
    CASE 
        WHEN Gender = 'F' THEN 'Female'
        WHEN Gender = 'M' THEN 'Male'
        ELSE 'Not Avaliable'
    END AS GenderFulLText
FROM Sales.Employees

-- Retrive employee details with abbreviated country code
SELECT 
    CustomerID,
    FirstName,
    LastName,
    Country,
    CASE 
        WHEN Country = 'Germany' THEN 'DE'
        WHEN Country = 'USA' THEN 'US'
        ELSE 'N/A'
    END AS CountrySHRT
FROM Sales.Customers;

-- OR using the Quick Form
SELECT 
    CustomerID,
    FirstName,
    LastName,
    Country,
    CASE Country
        WHEN  'Germany' THEN 'DE'
        WHEN  'USA' THEN 'US'
        ELSE 'N/A'
    END AS CountrySHRT
FROM Sales.Customers;

-- Using CASE to handle NULL
-- find the average score of customers and treat Nulls as 0
-- Additionally provide details such CustomerID and LastName
SELECT
    CustomerID,
    LastName,
    Score,
    AVG(CASE 
            WHEN Score IS NULL THEN 0
            ELSE Score 
            END ) OVER() AVG_ScoreClean,
    AVG(Score) OVER() as AvgCustomer
FROM 
    Sales.Customers;

-- Customers whose scores are greater than the Average score
SELECT 
    CustomerID,
    LastName,
    Score,
    AVG_ScoreClean
FROM(
    SELECT
        CustomerID,
        LastName,
        Score,
        AVG(CASE 
                WHEN Score IS NULL THEN 0
                ELSE Score 
            END ) OVER() AVG_ScoreClean,
        AVG(Score) OVER() as AvgCustomer
    FROM Sales.Customers) T 
WHERE Score > AVG_ScoreClean;

====================================================================================
-- Conditional Aggregation 
====================================================================================
-- USE CASE: Count how many times each customer has made an order with sales greater than 30
SELECT
        CustomerID,
        SUM(CASE 
            WHEN Sales > 30 THEN 1
            ELSE 0
        END) AS TotalOrdersHighSales,
        COUNT(*) AS TotalOrders
FROM Sales.Orders
GROUP BY CustomerID;

/*
SELECT
    CustomerID,
    SUM(HighSalesFlag) AS TotalOrdersHighSales,
    COUNT(*) AS TotalOrders
FROM (
    SELECT 
        CustomerID,
        CASE WHEN Sales > 30 THEN 1 ELSE 0 END AS HighSalesFlag
    FROM Sales.Orders
) AS Sub
GROUP BY CustomerID;
--------------------------------------------------------------------
WITH OrderFlags AS (
    SELECT 
        CustomerID,
        CASE WHEN Sales > 30 THEN 1 ELSE 0 END AS HighSalesFlag
    FROM Sales.Orders
)
SELECT
    CustomerID,
    SUM(HighSalesFlag) AS TotalOrdersHighSales,
    COUNT(*) AS TotalOrders
FROM OrderFlags
GROUP BY CustomerID;
*/

-- USE CASE: Count how many times each customer has made an order with sales greater than 30 include Customer's name
WITH CustomerHighSales AS (
    SELECT
        CustomerID,
        SUM(CASE 
            WHEN Sales > 30 THEN 1
            ELSE 0
        END) AS TotalOrdersHighSales,
        COUNT(*) AS TotalOrders
    FROM Sales.Orders
    GROUP BY CustomerID
)
SELECT 
    B.FirstName,
    C.CustomerID,
    C.TotalOrdersHighSales,
    C.TotalOrders
FROM CustomerHighSales AS C 
INNER JOIN Sales.Customers AS B 
    ON C.CustomerID = B.CustomerID;

-- with Subquerry
SELECT 
    B.FirstName,
    A.CustomerID,
    A.TotalOrdersHighSales,
    A.TotalOrders
FROM (
    SELECT
        CustomerID,
        SUM(CASE 
            WHEN Sales > 30 THEN 1
            ELSE 0
        END) AS TotalOrdersHighSales,
        COUNT(*) AS TotalOrders
    FROM Sales.Orders
    GROUP BY CustomerID
) AS A
INNER JOIN Sales.Customers AS B 
    ON A.CustomerID = B.CustomerID;

------------------------------------------------------------------------
-- Windows Function Syntax
-- Expression OVER (PARTITION BY ... ORDER BY ... FRAME)

-- 1. Find the total sales across all orders (Include OrderID and OrderDate)
SELECT 
    OrderID,
    OrderDate,
    SUM(Sales) OVER () AS TotalSales
FROM Sales.Orders;


-- 2. Find the total sales for each product (Include OrderID and OrderDate)
SELECT 
    OrderID,
    OrderDate,
    ProductID,
    SUM(Sales) OVER (PARTITION BY ProductID) AS TotalSales
FROM Sales.Orders;


-- 3. Find total sales overall and by product
SELECT 
    OrderID,
    OrderDate,
    ProductID,
    Sales,
    SUM(Sales) OVER () AS TotalSales,
    SUM(Sales) OVER (PARTITION BY ProductID) AS TotalSalesByProducts
FROM Sales.Orders;


-- 4. Find the total sales for each combination of product and order status
SELECT 
    OrderID,
    OrderDate,
    ProductID,
    Sales,
    SUM(Sales) OVER (PARTITION BY ProductID, OrderStatus) AS TotalSalesByProductAndStatus
FROM Sales.Orders;


-- 5. Rank each order based on their Sales from highest to lowest
SELECT 
    OrderID,
    OrderDate,
    Sales,
    RANK() OVER (ORDER BY Sales DESC) AS RankSales
FROM Sales.Orders;


-- 6. Windows Frame: Sum of Sales over a sliding window (current row and next 2 rows)
SELECT 
    OrderID,
    OrderDate,
    OrderStatus,
    Sales,
    SUM(Sales) OVER (
        PARTITION BY OrderStatus 
        ORDER BY OrderDate 
        ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING
    ) AS TotalSales
FROM Sales.Orders;


-- 7. Rank customers based on total sales using GROUP BY with Window Function
SELECT 
    CustomerID,
    SUM(Sales) AS TotalSales,
    RANK() OVER (ORDER BY SUM(Sales) DESC) AS RankCustomers
FROM Sales.Orders 
GROUP BY CustomerID;

--=============================================================================
-- WINDOW FUNCTIONS (Integer-Based Ranking)
--=============================================================================

-- ROW_NUMBER(): Rank the orders based on their sales from highest to lowest
SELECT
    OrderID,
    ProductID,
    Sales,
    ROW_NUMBER() OVER (ORDER BY Sales DESC) AS SalesRankROW
FROM Sales.Orders;


-- RANK(): Assigns same rank to ties, skipping subsequent numbers
SELECT
    OrderID,
    ProductID,
    Sales,
    RANK() OVER (ORDER BY Sales DESC) AS SalesRank
FROM Sales.Orders;


-- DENSE_RANK(): Assigns same rank to ties, does not skip numbers
SELECT
    OrderID,
    ProductID,
    Sales,
    DENSE_RANK() OVER (ORDER BY Sales DESC) AS SalesRank_Dense
FROM Sales.Orders;


--=============================================================================
-- USE CASES
--=============================================================================

-- 1. Find the top highest sale per product (Top-N analysis)
SELECT 
    OrderID,
    ProductID,
    Sales
FROM (
    SELECT
        OrderID,
        ProductID,
        Sales,
        ROW_NUMBER() OVER (PARTITION BY ProductID ORDER BY Sales DESC) AS RankByProduct
    FROM Sales.Orders
) AS T
WHERE RankByProduct = 1;


-- 2. Find the bottom 2 customers based on their total sales (Bottom-N analysis)
SELECT 
    CustomerID,
    TotalSales
FROM (
    SELECT
        CustomerID,
        SUM(Sales) AS TotalSales,
        ROW_NUMBER() OVER (ORDER BY SUM(Sales)) AS RankCustomers
    FROM Sales.Orders
    GROUP BY CustomerID
) AS T
WHERE RankCustomers <= 2;


-- Assign unique IDs to the rows of the 'OrdersArchive' table
SELECT
    ROW_NUMBER() OVER (ORDER BY OrderID, OrderDate) AS UniqueID,
    *
FROM Sales.OrdersArchive;


-- Identify and return clean result by removing duplicates (latest record kept)
SELECT *
FROM (
    SELECT
        ROW_NUMBER() OVER (PARTITION BY OrderID ORDER BY CreationTime DESC) AS rn,
        *
    FROM Sales.OrdersArchive
) AS T
WHERE rn = 1;


-- Return the duplicate (bad) data
SELECT *
FROM (
    SELECT
        ROW_NUMBER() OVER (PARTITION BY OrderID ORDER BY CreationTime DESC) AS rn,
        *
    FROM Sales.OrdersArchive
) AS T
WHERE rn > 1;


-- NTILE(n): Divide rows into specified number of approximately equal groups
SELECT
    OrderID,
    Sales,
    NTILE(3) OVER (ORDER BY Sales DESC) AS ThreeBucket,
    NTILE(2) OVER (ORDER BY Sales DESC) AS TwoBucket,
    NTILE(1) OVER (ORDER BY Sales DESC) AS OneBucket
FROM Sales.Orders;


-- Segment orders into three sales categories: High, Medium, Low
SELECT
    OrderID,
    Sales,
    CASE 
        WHEN Buckets = 1 THEN 'High'
        WHEN Buckets = 2 THEN 'Medium'
        WHEN Buckets = 3 THEN 'Low'
    END AS SalesSegment
FROM (
    SELECT
        OrderID,
        Sales,
        NTILE(3) OVER (ORDER BY Sales DESC) AS Buckets
    FROM Sales.Orders
) AS T;


-- Divide orders into 2 groups for parallel export (ETL load balancing)
SELECT 
    NTILE(2) OVER (ORDER BY OrderID) AS Bucket,
    *
FROM Sales.Orders;


--=============================================================================
-- WINDOW FUNCTIONS (Percentage-Based Ranking)
--=============================================================================

-- CUME_DIST(): Cumulative distribution including ties
-- Find products within the highest 40% of prices
SELECT *,
       CONCAT(DistRank * 100, '%') AS DistRankPerc
FROM (
    SELECT
        Product,
        Price,
        CUME_DIST() OVER (ORDER BY Price DESC) AS DistRank
    FROM Sales.Products
) AS T
WHERE DistRank <= 0.4;


-- PERCENT_RANK(): Relative rank excluding ties
-- Find products within the highest 40% of prices
SELECT *,
       CONCAT(DistRank * 100, '%') AS DistRankPerc
FROM (
    SELECT
        Product,
        Price,
        PERCENT_RANK() OVER (ORDER BY Price DESC) AS DistRank
    FROM Sales.Products
) AS T
WHERE DistRank <= 0.4;

--==============================================================
-- Window Value Functions (LEAD, LAG, LAST_VALUE, FIRST_VALUE)
--==============================================================
-- LEAD Function : Accesses the *next* row
-- LAG Function : Accesses the *previous* row

--==============================================================
-- USE CASE 1: Analyze month-over-month performance by finding 
--             the % change between the current and previous months
--==============================================================

WITH MonthlySales AS (
    SELECT 
        FORMAT(OrderDate, 'yyyy-MM') AS OrderYRMonth,
        DATENAME(MONTH, OrderDate)     AS MonthName,
        SUM(Sales)                     AS CurrentMthSales
    FROM Sales.Orders
    GROUP BY 
        FORMAT(OrderDate, 'yyyy-MM'), 
        DATENAME(MONTH, OrderDate)
)

SELECT 
    OrderYRMonth,
    MonthName,
    CurrentMthSales,
    LAG(CurrentMthSales) OVER (ORDER BY OrderYRMonth) AS PreviousSales,
    CurrentMthSales - LAG(CurrentMthSales) OVER (ORDER BY OrderYRMonth) AS MoM_Change,
    ROUND(
        CAST(CurrentMthSales - LAG(CurrentMthSales) OVER (ORDER BY OrderYRMonth) AS FLOAT) 
        / NULLIF(LAG(CurrentMthSales) OVER (ORDER BY OrderYRMonth), 0) * 100, 
        2
    ) AS MoMPercent_Change
FROM MonthlySales;


--==============================================================
-- USE CASE 2: Analyze customer loyalty by ranking customers 
--             based on the average number of days between orders
--==============================================================

SELECT
    CustomerID,
    AVG(DaysUntilNextOrder) AS AvgOrderGap,
    RANK() OVER (ORDER BY COALESCE(AVG(DaysUntilNextOrder), 99999)) AS CustLoyaltyRank
FROM (
    SELECT 
        OrderID,
        CustomerID,
        OrderDate                                                  AS CurrentDate,
        LEAD(OrderDate) OVER (PARTITION BY CustomerID ORDER BY OrderDate) AS NextOrder,
        DATEDIFF(DAY, OrderDate, LEAD(OrderDate) OVER (PARTITION BY CustomerID ORDER BY OrderDate)) AS DaysUntilNextOrder
    FROM Sales.Orders
) AS T
GROUP BY CustomerID;


--==============================================================
-- USE CASE 3: Find the lowest and highest sales for each product
--==============================================================

SELECT 
    OrderID,
    ProductID,
    Sales,
    FIRST_VALUE(Sales) OVER (PARTITION BY ProductID ORDER BY Sales)        AS LowestSales,
    -- LAST_VALUE requires a ROWS clause for correct result
    -- LAST_VALUE(Sales) OVER (PARTITION BY ProductID ORDER BY Sales 
    --     ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)              AS HighestSales,
    
    FIRST_VALUE(Sales) OVER (PARTITION BY ProductID ORDER BY Sales DESC)   AS HighestSalesAlt
FROM Sales.Orders;


