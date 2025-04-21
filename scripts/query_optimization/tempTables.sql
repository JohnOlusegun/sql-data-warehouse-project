/*==================================================================
    TEMP - #name will be automatically deleted once the session ends
===================================================================*/

-- Creating Temp table
SELECT
*
INTO #Orders
FROM Sales.Orders

DELETE FROM #Orders
WHERE OrderStatus = 'Delivered'

SELECT
*
INTO Sales.OrdersTest
FROM #Orders
