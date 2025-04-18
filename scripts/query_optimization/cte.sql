
-- CASE 1: Find the Total Sales Per Customer (Non-Recursive: Stand alone cte)
WITH CTE_Total_Sales AS 
(
    SELECT 
        CustomerID,
        SUM(sales) AS TotalSales
    FROM Sales.Orders
    GROUP BY CustomerID
),
-- CASE 2: Find the last order date for each customer (Non-Recursive: Stand alone cte)
CTE_Last_Order AS 
(
    SELECT
        CustomerID,
        MAX(OrderDate) AS Last_Order
    FROM Sales.Orders
    GROUP BY CustomerID
),
-- CASE 3: Rank the customers based on total sales per customer (Non-Recursive: Nested cte)
CTE_Customer_rank AS
(
    SELECT
        CustomerID,
        TotalSales,
        RANK() OVER (ORDER BY TotalSales DESC) as Customer_rank
    FROM CTE_Total_Sales 
),
-- CASE 4: Segment the customer based on their total sales (Non-Recursive: Nested cte)
CTE_customer_segment AS
(
    SELECT
        CustomerID,
        CASE WHEN TotalSales > 100 THEN 'High'
            WHEN TotalSales > 80 THEN 'Medium'
            ELSE 'Low'
        END AS CustomerSegments
    FROM CTE_Total_Sales
)
-- Main Query
SELECT
    c.CustomerID,
    c.Firstname,
    c.Lastname,
    cts.TotalSales,
    clo.Last_Order,
    ccr.Customer_rank,
    csg.CustomerSegments
FROM Sales.Customers AS c
LEFT JOIN CTE_Total_Sales AS cts
    ON cts.CustomerID = c.CustomerID
LEFT JOIN CTE_Last_Order AS clo
    ON clo.CustomerID = c.CustomerID
LEFT JOIN CTE_Customer_rank AS ccr 
    ON ccr.CustomerID = c.CustomerID
LEFT JOIN CTE_customer_segment AS csg 
    ON csg.CustomerID = c.CustomerID


--=================================================
-- Recursive CTE: Runs until the conditions are met
--=================================================
-- Generate a Sequence of numbers from  1 to 20

WITH Series AS 
(
    -- Anchor Query
    SELECT
    1 AS MyNumber
    UNION ALL
    -- Recursive Query
    SELECT 
    MyNumber + 1
    FROM Series
    WHERE MyNumber < 20
)
-- Main Query
SELECT 
*
FROM Series;
-- OPTION (MAXRECURSION 5)

-- USE CASE: Show the employee hierarchy by displaying each employee's level within the organization
WITH CTE_Emp_Hierarchy AS 
(
    -- Anchor Query
    SELECT
        EmployeeID,
        FirstName,
        ManagerID,
        1 AS Level
    FROM Sales.Employees
    WHERE ManagerID IS NULL
    UNION ALL
    -- Recursive Query
    SELECT 
        e.EmployeeID,
        e.FirstName,
        e.ManagerID,
        Level + 1
    FROM Sales.Employees AS e
    INNER JOIN CTE_Emp_Hierarchy AS ceh 
        ON e.ManagerID = ceh.EmployeeID
)
-- Main Query
SELECT 
* 
FROM CTE_Emp_Hierarchy
