/*
=========================================================
Set Operators in SQL
=========================================================
Scripts Purpose:
  UNION: combine rows by removing the duplicates. Returns all distinct
  UNION ALL: Combine all rows from the Table A and Table B together without removing Duplicates
  Except: Returns all distinct rows from the first query that are not found in the second query. Order of query is important (Data Discrepancy checks, Integration Checks, Updates)
  Intersect: Returns only the rows that are common in each queries. Doesnt mind the order of query

SET RULES:
 1. Set operators can be used almost with all clause
 2. Order BY is allowed only once at the end of the query
 3. The number of columns in each query must be the same
 4. Data types of column in each query must match
 5. The order of columns in each query must be the same
 6. Columns names is determine by the First Table
======================================================
*/


-- UNION
SELECT 
	Firstname,
	LastName
FROM Sales.Employees
UNION
SELECT 
	FirstName,
	LastName
FROM Sales.Customers

go

-- UNION ALL
SELECT 
	Firstname,
	LastName
FROM Sales.Employees
UNION ALL
SELECT 
	FirstName,
	LastName
FROM Sales.Customers

go

-- EXCEPT (employees who are not customers)
SELECT 
	Firstname,
	LastName
FROM Sales.Employees
EXCEPT
SELECT 
	FirstName,
	LastName
FROM Sales.Customers

go

-- INTERSECT (employees who are also customer)
SELECT 
	Firstname,
	LastName
FROM Sales.Employees
INTERSECT
SELECT 
	FirstName,
	LastName
FROM Sales.Customers


-- UNION best Practice
SELECT 
      'Orders' AS SourceTable
      ,[OrderID]
      ,[ProductID]
      ,[CustomerID]
      ,[SalesPersonID]
      ,[OrderDate]
      ,[ShipDate]
      ,[OrderStatus]
      ,[ShipAddress]
      ,[BillAddress]
      ,[Quantity]
      ,[Sales]
      ,[CreationTime]
  FROM [SalesDB].[Sales].[Orders]
UNION
SELECT 
      'OrdersArchive' AS SourceTable
      ,[OrderID]
      ,[ProductID]
      ,[CustomerID]
      ,[SalesPersonID]
      ,[OrderDate]
      ,[ShipDate]
      ,[OrderStatus]
      ,[ShipAddress]
      ,[BillAddress]
      ,[Quantity]
      ,[Sales]
      ,[CreationTime]
  FROM [SalesDB].[Sales].[OrdersArchive]
ORDER BY OrderID


