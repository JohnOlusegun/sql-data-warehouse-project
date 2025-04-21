/*
========================================
            SQL DATE FUNCTIONS
========================================

- DATEPART(part, date) returns INT values.
- DATENAME(part, date) returns STRING values.
- DATETRUNC(part, date) truncates the date to a specified part.
- EOMONTH(date) returns the last day of the month.
*/

--========================================
--           DATE EXTRACTION EXAMPLES
--========================================

SELECT
    OrderID,
    CreationTime,

    -- DATETRUNC Examples
    DATETRUNC(year, CreationTime)   AS Year_dt,
    DATETRUNC(minute, CreationTime) AS Minute_dt,
    DATETRUNC(day, CreationTime)    AS Day_dt,

    -- DATENAME Examples
    DATENAME(month, CreationTime)   AS Month_dn,
    DATENAME(weekday, CreationTime) AS Weekday_dn,
    DATENAME(day, CreationTime)     AS Day_dn,
    DATENAME(year, CreationTime)    AS Year_dn,

    -- DATEPART Examples
    DATEPART(year, CreationTime)    AS Year_dp,
    DATEPART(month, CreationTime)   AS Month_dp,
    DATEPART(day, CreationTime)     AS Day_dp,
    DATEPART(hour, CreationTime)    AS Hour_dp,
    DATEPART(quarter, CreationTime) AS Quarter_dp,
    DATEPART(week, CreationTime)    AS Week_dp,
    DATEPART(minute, CreationTime)  AS Minute_dp,

    -- Random Extraction Functions
    YEAR(CreationTime)              AS Year,
    MONTH(CreationTime)             AS Month,
    DAY(CreationTime)               AS Day,

    -- EOMONTH Example
    EOMONTH(CreationTime)           AS EndOfMonth

FROM Sales.Orders;


--========================================
-- COUNT ORDERS BY MONTH (DATETRUNC)
--========================================

SELECT 
    DATETRUNC(month, CreationTime) AS CreationMonth,
    COUNT(*)                       AS OrderCount
FROM Sales.Orders 
GROUP BY DATETRUNC(month, CreationTime);


--========================================
-- FIRST & LAST DAY OF THE MONTH
--========================================

SELECT 
    OrderID,
    CreationTime,
    CAST(DATETRUNC(month, CreationTime) AS DATE) AS StartOfMonth,
    EOMONTH(CreationTime)                      AS EndOfMonth
FROM Sales.Orders;


--========================================
-- NUMBER OF ORDERS PLACED EACH YEAR
--========================================

SELECT 
    YEAR(OrderDate) AS OrderYear,
    COUNT(*)        AS NrOfOrders
FROM Sales.Orders 
GROUP BY YEAR(OrderDate);


--========================================
-- NUMBER OF ORDERS PLACED EACH MONTH
--========================================

SELECT 
    DATENAME(month, OrderDate) AS MonthName,
    COUNT(*)                   AS NrOfOrders
FROM Sales.Orders 
GROUP BY DATENAME(month, OrderDate);


--========================================
-- ORDERS PLACED IN FEBRUARY
--========================================

SELECT 
    *
FROM Sales.Orders 
WHERE MONTH(OrderDate) = 2;

--========================================
  FORMAT
--========================================
SELECT
    OrderID,
    CreationTime,
    FORMAT(CreationTime, 'MM-dd-yyyy') USA_Format,
    FORMAT(CreationTime, 'dd-MM-yyyy') EURO_Format,
    FORMAT(CreationTime, 'dd') dd,
    FORMAT(CreationTime, 'ddd') ddd,
    FORMAT(CreationTime, 'dddd') dddd,
    FORMAT(CreationTime, 'MM') MM,
    FORMAT(CreationTime, 'MMM') MMM,
    FORMAT(CreationTime, 'MMMM') MMMM
FROM 
    Sales.Orders;

-- Show CreationTime using the following format:
-- Day Wed Jan Q1 2025 12:34:56 PM
SELECT
    OrderID,
    CreationTime,
    'Day ' + FORMAT(CreationTime, 'ddd, MMM') + 
    ' Q' + DATENAME(quarter, CreationTime) + ' ' + 
    FORMAT(CreationTime, 'yyyy hh:mm:ss tt') AS CustomFormat
FROM 
    Sales.Orders;

/*========================================
  CONVERT
========================================*/
SELECT
    CONVERT(INT, '123') AS [String to Int CONVERT],
    CONVERT(DATE, '2025-08-20') AS [String to Date CONVERT],
    CreationTime,
    CONVERT(DATE, CreationTime) AS [Datetime to Date CONVERT],
    CONVERT(DATE, CreationTime) AS [Datetime to Date CONVERT],
    CONVERT(VARCHAR, CreationTime, 32) AS [USA Std. Style:32],
    CONVERT(VARCHAR, CreationTime, 34) AS [EURO Std. Style:34]
FROM 
    Sales.Orders;


/*========================================
  CAST
========================================*/
SELECT
	CAST('123' AS INT) AS [String to Int],
	CAST(123 AS VARCHAR) AS [Int to String],
	CAST('2025-08-20' AS DATE) AS [String to Date],
	CAST('2025-08-20' AS DATETIME2) AS [String to Datetime],
	CreationTime,
	CAST(CreationTime AS DATE) AS [Datetime to Date]
FROM 
	Sales.Orders;

-----------------------------------------------------------------------------------

 -- ========================================
 -- Date Calculations (Use Cases)
 -- ======================================== 

SELECT
	OrderID,
	OrderDate,
	DATEADD(day, -10, OrderDate) AS TenDaysBefore,
	DATEADD(month, 3, OrderDate) AS ThreeMonthsLater,
	DATEADD(year, 2, OrderDate) AS TwoYearsLater
FROM 
	Sales.Orders;

-- Calculate the age of employees
SELECT
	EmployeeID,
	BirthDate,
	DATEDIFF(year, BirthDate, GETDATE()) Age
FROM 
	Sales.Employees;

-- Find the average shipping duration in days for each month
SELECT
	MONTH(OrderDate) AS OrderDate,
	AVG(DATEDIFF(day, OrderDate, ShipDate)) AvgShip
FROM 
	Sales.Orders
GROUP BY 
	MONTH(OrderDate);

-- Time Gap Analysis
-- Find the number of days between each order and the previous order
SELECT
	OrderID,
	OrderDate CurrentOrderDate,
	LAG(OrderDate) OVER (ORDER BY OrderDate) PreviousOrderDate,
	DATEDIFF(day, LAG(OrderDate) OVER (ORDER BY OrderDate), OrderDate)NrOfDays
FROM 
	Sales.Orders;

-----------------------------------------------------------------------------------

/*========================================
  ISDATE
========================================*/
SELECT
	ISDATE('2025-08-20'),
	ISDATE('2025'),
	ISDATE(20250820),
	ISDATE(8),
	ISDATE(OrderDate)
FROM 
	Sales.Orders;


SELECT
	--CAST(OrderDate AS DATE) OrderDate,
	OrderDate,
	ISDATE(OrderDate),
	CASE WHEN ISDATE(OrderDate) = 1 THEN CAST(OrderDate AS DATE)
		 ELSE '9999-01-01'
	END NewOrderDate
FROM
(
	SELECT '2025-08-20' AS OrderDate UNION
	SELECT '2025-08-21' UNION
	SELECT '2025-08-23' UNION
	SELECT '2025-08' 
)t
--WHERE ISDATE(OrderDate) = 0

------------------------------------------------------------------------

-- All possible parts can be used in DATE  Extraction
SELECT 
    'Year' AS DatePart, 
    DATEPART(year, GETDATE()) AS DatePart_Output,
    DATENAME(year, GETDATE()) AS DateName_Output,
    DATETRUNC(year, GETDATE()) AS DateTrunc_Output
UNION ALL
SELECT 
    'YY', 
    DATEPART(yy, GETDATE()) AS DatePart_Output,
    DATENAME(yy, GETDATE()) AS DateName_Output, 
    DATETRUNC(yy, GETDATE()) AS DateTrunc_Output
UNION ALL
SELECT 
    'YYYY', 
    DATEPART(yyyy, GETDATE()) AS DatePart_Output,
    DATENAME(yyyy, GETDATE()) AS DateName_Output, 
    DATETRUNC(yyyy, GETDATE()) AS DateTrunc_Output
UNION ALL
SELECT 
    'Quarter', 
    DATEPART(quarter, GETDATE()) AS DatePart_Output,
    DATENAME(quarter, GETDATE()) AS DateName_Output, 
    DATETRUNC(quarter, GETDATE()) AS DateTrunc_Output
UNION ALL
SELECT 
    'QQ', 
    DATEPART(qq, GETDATE()) AS DatePart_Output,
    DATENAME(qq, GETDATE()) AS DateName_Output, 
    DATETRUNC(qq, GETDATE()) AS DateTrunc_Output
UNION ALL
SELECT 
    'Q', 
    DATEPART(q, GETDATE()) AS DatePart_Output,
    DATENAME(q, GETDATE()) AS DateName_Output, 
    DATETRUNC(q, GETDATE()) AS DateTrunc_Output
UNION ALL
SELECT 
    'Month', 
    DATEPART(month, GETDATE()) AS DatePart_Output,
    DATENAME(month, GETDATE()) AS DateName_Output, 
    DATETRUNC(month, GETDATE()) AS DateTrunc_Output
UNION ALL
SELECT 
    'MM', 
    DATEPART(mm, GETDATE()) AS DatePart_Output,
    DATENAME(mm, GETDATE()) AS DateName_Output, 
    DATETRUNC(mm, GETDATE()) AS DateTrunc_Output
UNION ALL
SELECT 
    'M', 
    DATEPART(m, GETDATE()) AS DatePart_Output,
    DATENAME(m, GETDATE()) AS DateName_Output, 
    DATETRUNC(m, GETDATE()) AS DateTrunc_Output
UNION ALL
SELECT 
    'DayOfYear', 
    DATEPART(dayofyear, GETDATE()) AS DatePart_Output,
    DATENAME(dayofyear, GETDATE()) AS DateName_Output, 
    DATETRUNC(dayofyear, GETDATE()) AS DateTrunc_Output
UNION ALL
SELECT 
    'DY', 
    DATEPART(dy, GETDATE()) AS DatePart_Output,
    DATENAME(dy, GETDATE()) AS DateName_Output, 
    DATETRUNC(dy, GETDATE()) AS DateTrunc_Output
UNION ALL
SELECT 
    'Y', 
    DATEPART(y, GETDATE()) AS DatePart_Output,
    DATENAME(y, GETDATE()) AS DateName_Output, 
    DATETRUNC(y, GETDATE()) AS DateTrunc_Output
UNION ALL
SELECT 
    'Day', 
    DATEPART(day, GETDATE()) AS DatePart_Output,
    DATENAME(day, GETDATE()) AS DateName_Output, 
    DATETRUNC(day, GETDATE()) AS DateTrunc_Output
UNION ALL
SELECT 
    'DD', 
    DATEPART(dd, GETDATE()) AS DatePart_Output,
    DATENAME(dd, GETDATE()) AS DateName_Output, 
    DATETRUNC(dd, GETDATE()) AS DateTrunc_Output
UNION ALL
SELECT 
    'D', 
    DATEPART(d, GETDATE()) AS DatePart_Output,
    DATENAME(d, GETDATE()) AS DateName_Output, 
    DATETRUNC(d, GETDATE()) AS DateTrunc_Output
UNION ALL
SELECT 
    'Week', 
    DATEPART(week, GETDATE()) AS DatePart_Output,
    DATENAME(week, GETDATE()) AS DateName_Output, 
    DATETRUNC(week, GETDATE()) AS DateTrunc_Output
UNION ALL
SELECT 
    'WK', 
    DATEPART(wk, GETDATE()) AS DatePart_Output,
    DATENAME(wk, GETDATE()) AS DateName_Output, 
    DATETRUNC(wk, GETDATE()) AS DateTrunc_Output
UNION ALL
SELECT 
    'WW', 
    DATEPART(ww, GETDATE()) AS DatePart_Output,
    DATENAME(ww, GETDATE()) AS DateName_Output, 
    DATETRUNC(ww, GETDATE()) AS DateTrunc_Output
UNION ALL
SELECT 
    'Weekday', 
    DATEPART(weekday, GETDATE()) AS DatePart_Output,
    DATENAME(weekday, GETDATE()) AS DateName_Output, 
    NULL AS DateTrunc_Output
UNION ALL
SELECT 
    'DW', 
    DATEPART(dw, GETDATE()) AS DatePart_Output,
    DATENAME(dw, GETDATE()) AS DateName_Output, 
    NULL AS DateTrunc_Output
UNION ALL
SELECT 
    'Hour', 
    DATEPART(hour, GETDATE()) AS DatePart_Output,
    DATENAME(hour, GETDATE()) AS DateName_Output, 
    DATETRUNC(hour, GETDATE()) AS DateTrunc_Output
UNION ALL
SELECT 
    'HH', 
    DATEPART(hh, GETDATE()) AS DatePart_Output,
    DATENAME(hh, GETDATE()) AS DateName_Output, 
    DATETRUNC(hh, GETDATE()) AS DateTrunc_Output
UNION ALL
SELECT 
    'Minute', 
    DATEPART(minute, GETDATE()) AS DatePart_Output,
    DATENAME(minute, GETDATE()) AS DateName_Output, 
    DATETRUNC(minute, GETDATE()) AS DateTrunc_Output
UNION ALL
SELECT 
    'MI', 
    DATEPART(mi, GETDATE()) AS DatePart_Output,
    DATENAME(mi, GETDATE()) AS DateName_Output, 
    DATETRUNC(mi, GETDATE()) AS DateTrunc_Output
UNION ALL
SELECT 
    'N', 
    DATEPART(n, GETDATE()) AS DatePart_Output,
    DATENAME(n, GETDATE()) AS DateName_Output, 
    DATETRUNC(n, GETDATE()) AS DateTrunc_Output
UNION ALL
SELECT 
    'Second', 
    DATEPART(second, GETDATE()) AS DatePart_Output,
    DATENAME(second, GETDATE()) AS DateName_Output, 
    DATETRUNC(second, GETDATE()) AS DateTrunc_Output
UNION ALL
SELECT 
    'SS', 
    DATEPART(ss, GETDATE()) AS DatePart_Output,
    DATENAME(ss, GETDATE()) AS DateName_Output, 
    DATETRUNC(ss, GETDATE()) AS DateTrunc_Output
UNION ALL
SELECT 
    'S', 
    DATEPART(s, GETDATE()) AS DatePart_Output,
    DATENAME(s, GETDATE()) AS DateName_Output, 
    DATETRUNC(s, GETDATE()) AS DateTrunc_Output
UNION ALL
SELECT 
    'Millisecond', 
    DATEPART(millisecond, GETDATE()) AS DatePart_Output,
    DATENAME(millisecond, GETDATE()) AS DateName_Output, 
    DATETRUNC(millisecond, GETDATE()) AS DateTrunc_Output
UNION ALL
SELECT 
    'MS', 
    DATEPART(ms, GETDATE()) AS DatePart_Output,
    DATENAME(ms, GETDATE()) AS DateName_Output, 
    DATETRUNC(ms, GETDATE()) AS DateTrunc_Output
UNION ALL
SELECT 
    'Microsecond', 
    DATEPART(microsecond, GETDATE()) AS DatePart_Output,
    DATENAME(microsecond, GETDATE()) AS DateName_Output, 
    NULL AS DateTrunc_Output
UNION ALL
SELECT 
    'MCS', 
    DATEPART(mcs, GETDATE()) AS DatePart_Output,
    DATENAME(mcs, GETDATE()) AS DateName_Output, 
    NULL AS DateTrunc_Output
UNION ALL
SELECT 
    'Nanosecond', 
    DATEPART(nanosecond, GETDATE()) AS DatePart_Output,
    DATENAME(nanosecond, GETDATE()) AS DateName_Output, 
    NULL AS DateTrunc_Output
UNION ALL
SELECT 
    'NS', 
    DATEPART(ns, GETDATE()) AS DatePart_Output,
    DATENAME(ns, GETDATE()) AS DateName_Output, 
    NULL AS DateTrunc_Output
UNION ALL
SELECT 
    'ISOWeek', 
    DATEPART(iso_week, GETDATE()) AS DatePart_Output,
    DATENAME(iso_week, GETDATE()) AS DateName_Output, 
    DATETRUNC(iso_week, GETDATE()) AS DateTrunc_Output
UNION ALL
SELECT 
    'ISOWK', 
    DATEPART(isowk, GETDATE()) AS DatePart_Output,
    DATENAME(isowk, GETDATE()) AS DateName_Output, 
    DATETRUNC(isowk, GETDATE()) AS DateTrunc_Output
UNION ALL
SELECT 
    'ISOWW', 
    DATEPART(isoww, GETDATE()) AS DatePart_Output,
    DATENAME(isoww, GETDATE()) AS DateName_Output, 
    DATETRUNC(isoww, GETDATE()) AS DateTrunc_Output;

--=====================================================================================
--Date Format
--====================================================================================

SELECT 
    'D' AS FormatType, 
    FORMAT(GETDATE(), 'D') AS FormattedValue,
    'Full date pattern' AS Description
UNION ALL
SELECT 
    'd', 
    FORMAT(GETDATE(), 'd'), 
    'Short date pattern'
UNION ALL
SELECT 
    'dd', 
    FORMAT(GETDATE(), 'dd'), 
    'Day of month with leading zero'
UNION ALL
SELECT 
    'ddd', 
    FORMAT(GETDATE(), 'ddd'), 
    'Abbreviated name of day'
UNION ALL
SELECT 
    'dddd', 
    FORMAT(GETDATE(), 'dddd'), 
    'Full name of day'
UNION ALL
SELECT 
    'M', 
    FORMAT(GETDATE(), 'M'), 
    'Month without leading zero'
UNION ALL
SELECT 
    'MM', 
    FORMAT(GETDATE(), 'MM'), 
    'Month with leading zero'
UNION ALL
SELECT 
    'MMM', 
    FORMAT(GETDATE(), 'MMM'), 
    'Abbreviated name of month'
UNION ALL
SELECT 
    'MMMM', 
    FORMAT(GETDATE(), 'MMMM'), 
    'Full name of month'
UNION ALL
SELECT 
    'yy', 
    FORMAT(GETDATE(), 'yy'), 
    'Two-digit year'
UNION ALL
SELECT 
    'yyyy', 
    FORMAT(GETDATE(), 'yyyy'), 
    'Four-digit year'
UNION ALL
SELECT 
    'hh', 
    FORMAT(GETDATE(), 'hh'), 
    'Hour in 12-hour clock with leading zero'
UNION ALL
SELECT 
    'HH', 
    FORMAT(GETDATE(), 'HH'), 
    'Hour in 24-hour clock with leading zero'
UNION ALL
SELECT 
    'm', 
    FORMAT(GETDATE(), 'm'), 
    'Minutes without leading zero'
UNION ALL
SELECT 
    'mm', 
    FORMAT(GETDATE(), 'mm'), 
    'Minutes with leading zero'
UNION ALL
SELECT 
    's', 
    FORMAT(GETDATE(), 's'), 
    'Seconds without leading zero'
UNION ALL
SELECT 
    'ss', 
    FORMAT(GETDATE(), 'ss'), 
    'Seconds with leading zero'
UNION ALL
SELECT 
    'f', 
    FORMAT(GETDATE(), 'f'), 
    'Tenths of a second'
UNION ALL
SELECT 
    'ff', 
    FORMAT(GETDATE(), 'ff'), 
    'Hundredths of a second'
UNION ALL
SELECT 
    'fff', 
    FORMAT(GETDATE(), 'fff'), 
    'Milliseconds'
UNION ALL
SELECT 
    'T', 
    FORMAT(GETDATE(), 'T'), 
    'Full AM/PM designator'
UNION ALL
SELECT 
    't', 
    FORMAT(GETDATE(), 't'), 
    'Single character AM/PM designator'
UNION ALL
SELECT 
    'tt', 
    FORMAT(GETDATE(), 'tt'), 
    'Two character AM/PM designator';
