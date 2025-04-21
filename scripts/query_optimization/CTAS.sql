/*==============================================
    CTAS - CREATE TABLE AS SELECT
================================================*/

-- USE CASES: OPTIMIZATION
-- will allow to get updated data if written alongside the T-SQL
IF OBJECT_ID ('Sales.MonthlyOrders', 'U') IS NOT NULL
	DROP TABLE Sales.MonthlyOrders;
GO
SELECT 
	DATENAME(month, OrderDate) as OrderMonth,
	COUNT(OrderID) as TotalOrders
INTO Sales.MonthlyOrders
FROM Sales.Orders
GROUP BY DATENAME(month, OrderDate);


SELECT * FROM Sales.MonthlyOrders
