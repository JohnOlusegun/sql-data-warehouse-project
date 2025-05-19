/*
  Start with data integration from the bronze layer.
  Perform:
  - Data Cleaning
  - Data Standardization
  - Data Normalization
  - Derived Columns
  - Data Enrichment
*/
====================================================================================================================================================
    CRM TABLES
====================================================================================================================================================

-- ============================================
-- Data Quality Checks on bronze.crm_cust_info
-- ============================================

-- 1. Check for NULLs or duplicate customer IDs
-- Expectation: No results (all cst_id values should be unique and not null)
SELECT 
    cst_id,
    COUNT(*) AS cnt
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;


-- 2. Check for unwanted leading/trailing spaces in customer first names
-- Expectation: No results
SELECT 
    cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);


-- 3. Check for unwanted leading/trailing spaces in customer last names
-- Expectation: No results
SELECT 
    cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);


-- 4. Check for standardization and consistency in gender values
-- Expectation: Only standardized values (e.g., 'M', 'F')
SELECT DISTINCT 
    cst_gndr
FROM bronze.crm_cust_info;

--==========================================================
-- Load Cleaned Data into the silver_crm_cust_info and 
-- Refreshing silver.crm_cust_info with cleaned data
-- ============================================

-- Step 1: Truncate the target table to remove existing records
TRUNCATE TABLE silver.crm_cust_info;
GO
-- Step 2: Insert cleaned and standardized data into silver.crm_cust_info
INSERT INTO silver.crm_cust_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_material_status,
    cst_gndr,
    cst_create_date
)
SELECT 
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,
    CASE 
        WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
        ELSE 'n/a'
    END AS cst_material_status,
    CASE 
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'n/a'
    END AS cst_gndr,
    cst_create_date
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY cst_id 
               ORDER BY cst_create_date DESC
           ) AS flag_last
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
) AS t
WHERE flag_last = 1;

-- Step 3: Review the results
SELECT * 
FROM silver.crm_cust_info;

--==============================================
  -- Checkmate the clean Data for verification:
--==============================================

-- Check for data quality issues (NULLs or duplicate customer IDs)
-- Expectation: No results
SELECT 
    cst_id,
    COUNT(*) AS cnt
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Check for unwanted leading/trailing spaces in first names
-- Expectation: No results
SELECT 
    cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

-- Check for unwanted leading/trailing spaces in last names
-- Expectation: No results
SELECT 
    cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

-- Check for standardization and consistency in gender values
-- Expectation: Only standardized values (e.g., 'M', 'F')
SELECT DISTINCT 
    cst_gndr
FROM silver.crm_cust_info;


-- ============================================
-- Data Cleaning on bronze.crm_prd_info
-- Before migration into silver.crm_prd_info
-- ============================================

-- 1. Preview raw product data
SELECT 
    prd_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
FROM bronze.crm_prd_info;


-- 2. Check for duplicate or NULL product IDs
-- Expectation: No results (each prd_id should be unique and not null)
SELECT 
    prd_id,
    COUNT(*) AS cnt
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;


-- 3. Check for unwanted leading/trailing spaces in product names
-- Expectation: No results
SELECT 
    prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);


-- 4. Check for NULL or negative product costs
-- Expectation: No results (prd_cost should be non-null and non-negative)
SELECT 
    prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0;


-- 5. Check for standardization and consistency in product lines
-- Expectation: Distinct values should be within an acceptable domain
SELECT DISTINCT 
    prd_line
FROM bronze.crm_prd_info;


-- 6. Check for invalid date ranges (end date earlier than start date)
-- Expectation: No results ( We confirmed that end_date was lesser than the start_date which should be corrected)
SELECT 
    *
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt;


-- ============================================================
-- Step 1: Drop and Recreate silver.crm_prd_info with Updated DDL
-- ============================================================

IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;

CREATE TABLE silver.crm_prd_info (
    prd_id INT,
    cat_id NVARCHAR(50),
    prd_key NVARCHAR(50),
    prd_nm NVARCHAR(50),
    prd_cost INT,
    prd_line NVARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);


-- ============================================================
-- Step 2: Insert Cleaned and Transformed Data
-- ============================================================

TRUNCATE TABLE silver.crm_prd_info;

INSERT INTO silver.crm_prd_info (
    prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)
SELECT 
    prd_id,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- Extract category ID
    SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key, -- Extract product key
    TRIM(prd_nm) AS prd_nm,
    ISNULL(prd_cost, 0) AS prd_cost,
    CASE 
        WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
        WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
        WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
        WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
        ELSE 'n/a'
    END AS prd_line, -- change product line code to a descriptive values 
    CAST(prd_start_dt AS DATE) AS prd_start_dt,
    CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt --Calculate end date as one day before the next start date
FROM bronze.crm_prd_info;


-- ============================================================
-- Step 3: Data Quality Checks on silver.crm_prd_info
-- ============================================================

-- 3.1 Check for Duplicate or NULL Product IDs
-- Expectation: No rows returned
SELECT 
    prd_id,
    COUNT(*) AS cnt
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;


-- 3.2 Check for Unwanted Leading/Trailing Spaces in Product Name
-- Expectation: No rows returned
SELECT 
    prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);


-- 3.3 Check for NULL or Negative Product Costs
-- Expectation: No rows returned
SELECT 
    prd_cost
FROM silver.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0;


-- 3.4 Check for Consistent Product Line Labels
-- Expectation: Only standardized labels returned
SELECT DISTINCT 
    prd_line
FROM silver.crm_prd_info;


-- 3.5 Check for Invalid Date Ranges (end date < start date)
-- Expectation: No rows returned
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;


-- ============================================================
-- Step 4: Final Review of the Cleaned Data
-- ============================================================

SELECT * 
FROM silver.crm_prd_info;

-- ========================================================
-- Step 1: Initial Review of Data in bronze.crm_sales_details
-- ========================================================

SELECT 
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
FROM bronze.crm_sales_details;


-- ========================================================
-- Step 2: Check for Invalid Order Dates
-- Criteria:
--   - Order dates should be 8-digit integers (yyyymmdd)
--   - Valid range: 19000101 to 20500101
--   - Must not be zero or malformed
-- Expectation: No rows returned
-- ========================================================

SELECT 
    NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 
    OR LEN(sls_order_dt) != 8
    OR sls_order_dt > 20500101 
    OR sls_order_dt < 19000101;


-- ========================================================
-- Step 3: Check for Invalid Date Relationships
-- Criteria:
--   - Order date must be earlier than or equal to ship and due dates
-- Expectation: No rows returned
-- ========================================================

SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt 
   OR sls_order_dt > sls_due_dt;


-- ========================================================
-- Step 4: Check Data Consistency Between Sales, Quantity, and Price
-- Logic:
--   - Sales = Quantity * Price
--   - No NULL, zero, or negative values allowed
--   - Price should be corrected to ABS (if needed)
-- ========================================================

SELECT DISTINCT
    sls_sales AS old_sls_sales,
    sls_quantity,
    sls_price AS old_sls_price,

    -- Recomputed sales if invalid
    CASE 
        WHEN sls_sales IS NULL 
          OR sls_sales <= 0 
          OR sls_sales != sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END AS recomputed_sales,

    -- Recomputed price if invalid
    CASE 
        WHEN sls_price IS NULL 
          OR sls_price <= 0 
        THEN sls_sales / NULLIF(sls_quantity, 0)
        ELSE sls_price
    END AS recomputed_price

FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
    OR sls_sales IS NULL 
    OR sls_quantity IS NULL 
    OR sls_price IS NULL 
    OR sls_sales <= 0 
    OR sls_quantity <= 0 
    OR sls_price <= 0
ORDER BY
    sls_sales,
    sls_quantity,
    sls_price;

-- =============================================================================
-- STEP 1: Drop and Recreate silver.crm_sales_details with Updated DDL
-- =============================================================================

IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;

CREATE TABLE silver.crm_sales_details (
    sls_ord_num     NVARCHAR(50),
    sls_prd_key     NVARCHAR(50),
    sls_cust_id     INT,
    sls_order_dt    DATE,
    sls_ship_dt     DATE,
    sls_due_dt      DATE,
    sls_sales       INT,
    sls_quantity    INT,
    sls_price       INT,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

-- =============================================================================
-- STEP 2: Truncate Target Table and Insert Cleaned Data from bronze.crm_sales_details
-- =============================================================================

TRUNCATE TABLE silver.crm_sales_details;

INSERT INTO silver.crm_sales_details (
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
)
SELECT 
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,

    -- Clean and convert order date
    CASE 
        WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 
        THEN NULL
        ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
    END AS sls_order_dt,

    -- Clean and convert ship date
    CASE 
        WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 
        THEN NULL
        ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
    END AS sls_ship_dt,

    -- Clean and convert due date
    CASE 
        WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 
        THEN NULL
        ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
    END AS sls_due_dt,

    -- Recalculate sales if missing or incorrect
    CASE 
        WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales 
    END AS sls_sales,

    -- Retain quantity
    sls_quantity,

    -- Derive price if original is invalid or non-positive
    CASE 
        WHEN sls_price IS NULL OR sls_price <= 0 
        THEN sls_sales / NULLIF(sls_quantity, 0)
        ELSE sls_price
    END AS sls_price

FROM bronze.crm_sales_details;


-- =============================================================================
-- STEP 3: View Final Cleaned Table
-- =============================================================================

SELECT * 
FROM silver.crm_sales_details;


--====================================================================================================================================================
   -- ERP TABLES
--====================================================================================================================================================
-- DATA CLEANING on [bronze].[erp_cust_az12] before migrating into [silver].[erp_cust_az12]
SELECT 
    CASE 
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
        ELSE cid
    END AS cid,
    
    CASE 
        WHEN bdate > GETDATE() THEN NULL
        ELSE bdate
    END AS bdate,
    
    CASE 
        WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
        WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
        ELSE 'n/a'
    END AS gen
FROM bronze.erp_cust_az12;


-- Identify out-of-range birthdates
SELECT 
    bdate 
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE();


-- Data standardization and consistency for gender
SELECT 
    DISTINCT gen,
    CASE 
        WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
        WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
        ELSE 'n/a'
    END AS standardized_gen
FROM bronze.erp_cust_az12;


-- Load the clean data into [silver].[erp_cust_az12]
TRUNCATE TABLE silver.erp_cust_az12;

INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
SELECT 
    CASE 
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
        ELSE cid
    END AS cid,

    CASE 
        WHEN bdate > GETDATE() THEN NULL
        ELSE bdate
    END AS bdate,

    CASE 
        WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
        WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
        ELSE 'n/a'
    END AS gen
FROM bronze.erp_cust_az12;


-- View cleaned customer table
SELECT * FROM silver.erp_cust_az12;


-- DATA CLEANING on [bronze].[erp_loc_a101] before migrating into [silver].[erp_loc_a101]
SELECT 
    REPLACE(cid, '-', '') AS cid,
    CASE 
        WHEN TRIM(cntry) = 'DE' THEN 'Germany'
        WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
        WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
        ELSE TRIM(cntry)
    END AS cntry
FROM bronze.erp_loc_a101;


-- Data standardization and consistency for country
SELECT 
    DISTINCT cntry,
    CASE 
        WHEN TRIM(cntry) = 'DE' THEN 'Germany'
        WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
        WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
        ELSE TRIM(cntry)
    END AS standardized_cntry
FROM bronze.erp_loc_a101;


-- Load the clean data into [silver].[erp_loc_a101]
TRUNCATE TABLE silver.erp_loc_a101;

INSERT INTO silver.erp_loc_a101 (cid, cntry)
SELECT 
    REPLACE(cid, '-', '') AS cid,
    CASE 
        WHEN TRIM(cntry) = 'DE' THEN 'Germany'
        WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
        WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
        ELSE TRIM(cntry)
    END AS cntry
FROM bronze.erp_loc_a101;


-- View cleaned location table
SELECT * FROM silver.erp_loc_a101;


-- DATA CLEANING on [bronze].[erp_px_cat_g1v2] before migrating into [silver].[erp_px_cat_g1v2]
SELECT * FROM bronze.erp_px_cat_g1v2;


-- Check for unwanted spaces in category and subcategory
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat);

SELECT * FROM bronze.erp_px_cat_g1v2
WHERE subcat != TRIM(subcat);


-- Data standardization checks
SELECT DISTINCT cat FROM bronze.erp_px_cat_g1v2;
SELECT DISTINCT maintenance FROM bronze.erp_px_cat_g1v2;


-- Load the clean data into [silver].[erp_px_cat_g1v2]
TRUNCATE TABLE silver.erp_px_cat_g1v2;

INSERT INTO silver.erp_px_cat_g1v2 (
    id,
    cat,
    subcat,
    maintenance
)
SELECT 
    id,
    cat,
    subcat,
    maintenance
FROM bronze.erp_px_cat_g1v2;


-- View cleaned category table
SELECT * FROM silver.erp_px_cat_g1v2;








