/*
  Start with data integration from the bronze layer.
  Perform:
  - Data Cleaning
  - Data Standardization
  - Data Normalization
  - Derived Columns
  - Data Enrichment
*/

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


