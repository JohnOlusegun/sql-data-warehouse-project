/*================================================================
    VIEW - Doesn't store any data (Also available on data Update)
==================================================================*/

-- Create a view
IF OBJECT_ID ('Sales.V_Monthly_Summary', 'V') IS NOT NULL
	DROP VIEW Sales.V_Monthly_Summary;
GO
CREATE VIEW Sales.V_Monthly_Summary AS 
(
	SELECT 
		DATETRUNC(month, OrderDate) AS OrderMonth,
		SUM(Sales) AS TotalSales,
		COUNT(OrderDate) AS TotalOrders,
		SUM(Quantity) AS TotalQuantities
	FROM Sales.Orders
	GROUP BY DATETRUNC(month, OrderDate)
);

-- Query the view
SELECT 
	OrderMonth,
	TotalSales,
	SUM(TotalSales) OVER (ORDER BY OrderMonth) AS RunningTotal
FROM Sales.V_Monthly_Summary;

-- USE Case: Provide view that combines details from orders, products, customers, and employees
IF OBJECT_ID ('Sales.V_Orders_Details', 'V') IS NOT NULL
	DROP VIEW Sales.V_Orders_Details;
GO
CREATE VIEW Sales.V_Orders_Details AS
(
	SELECT 
		o.OrderID,
		o.OrderDate,
		p.Product,
		p.Category,
		COALESCE(c.FirstName,'') + ' ' + COALESCE(c.LastName,'') as CustomerName,
		Country as CustomerCountry,
		COALESCE(e.FirstName,'') + ' ' + COALESCE(e.LastName,'') as EmployeeName,
		e.Department,
		o.Sales,
		o.Quantity
	FROM Sales.Orders as o
	LEFT JOIN Sales.Products as p
		ON p.ProductID = o.ProductID
	LEFT JOIN Sales.Customers as c
		ON c.CustomerID = o.CustomerID
	LEFT JOIN Sales.Employees as e
		 ON e.EmployeeID = o.SalesPersonID
);

SELECT * FROM Sales.V_Orders_Details

-- VIEWS FOR DATA SECURITY - Providing details for EU sales team and exclude data related to USA
IF OBJECT_ID ('Sales.V_Orders_Details_EU', 'V') IS NOT NULL
	DROP VIEW Sales.V_Orders_Details_EU;
GO
CREATE VIEW Sales.V_Orders_Details_EU AS
(
	SELECT 
		o.OrderID,
		o.OrderDate,
		p.Product,
		p.Category,
		COALESCE(c.FirstName,'') + ' ' + COALESCE(c.LastName,'') as CustomerName,
		Country as CustomerCountry,
		COALESCE(e.FirstName,'') + ' ' + COALESCE(e.LastName,'') as EmployeeName,
		e.Department,
		o.Sales,
		o.Quantity
	FROM Sales.Orders as o
	LEFT JOIN Sales.Products as p
		ON p.ProductID = o.ProductID
	LEFT JOIN Sales.Customers as c
		ON c.CustomerID = o.CustomerID
	LEFT JOIN Sales.Employees as e
		 ON e.EmployeeID = o.SalesPersonID
	WHERE c.Country !='USA'
);

SELECT * FROM Sales.V_Orders_Details_EU;
