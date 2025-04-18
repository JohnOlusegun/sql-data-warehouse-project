/*
=====================================================================
SQL JOINS

	- We explore all kinds of join in this code, from the regular to the advanced type of joins
=====================================================================
*/

-- NO Join
SELECT * FROM customers;
SELECT * FROM orders;

--Inner join (Only the matching rows from both tables) Get customers along with their orders, but only customers who have placed an order. Doesnt mind the order of Table)
SELECT 
	customers.id,
	customers.first_name,
	orders.order_id,
	orders.sales
FROM customers 
INNER JOIN orders 
ON id = customer_id;

SELECT 
	c.id,
	c.first_name,
	o.order_id,
	o.sales
FROM customers  AS c
	INNER JOIN orders AS o
	ON c.id = o.customer_id;

-- LEFT JOIN (all from the left and matching from the right), Order of the table is very important
SELECT 
	c.id,
	c.first_name,
	o.order_id,
	o.sales
FROM customers  AS c
	LEFT JOIN orders AS o
	ON c.id = o.customer_id;

-- RIGHT JOIN (all from the right and matching from the left) Order of the table is very important
SELECT 
	c.id,
	c.first_name,
	o.order_id,
	o.sales
FROM customers  AS c
	RIGHT JOIN orders AS o
	ON c.id = o.customer_id

-- LEFT JOIN
SELECT 
	c.id,
	c.first_name,
	o.order_id,
	o.sales
FROM orders  AS o
	LEFT JOIN customers AS c
	ON c.id = o.customer_id;

-- Full Join
SELECT 
	c.id,
	c.first_name,
	o.order_id,
	o.sales
FROM orders  AS o
	Full Join customers AS c
	ON c.id = o.customer_id;

-- ADVANCED JOIN, LEFT ANTI JOIN ( I prefer swapping the tables than using a right Join)
SELECT *
FROM customers c
left JOIN orders o
	on c.id = o.customer_id
where o.customer_id is null;

-- Orders that didnt have a recorded customer
SELECT *
FROM orders o
left JOIN customers c 
on c.id = o.customer_id
where c.id is null;

-- Full Anti Join
SELECT *
FROM orders o
FULL JOIN customers c 
on c.id = o.customer_id
where c.id is null or o.customer_id is null;


-- Multiple Join of tables
SELECT 
    o.OrderID,
    o.Sales,
    c.FirstName AS CustomerFirstName,
    c.LastName AS CustomerLastName,
    p.Product AS ProductName,
    p.Price,
    e.FirstName AS EmployeeFirstName,
    e.LastName AS EmployeeLastName
FROM Sales.Orders AS o
LEFT JOIN Sales.Customers AS c
    ON o.CustomerID = c.CustomerID
LEFT JOIN Sales.Products AS p
    ON o.ProductID = p.ProductID
LEFT JOIN Sales.Employees AS e
    ON o.SalesPersonID = e.EmployeeID;

