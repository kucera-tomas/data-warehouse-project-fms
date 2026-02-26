/*
===============================================================================
Quality Checks: Silver Layer Validation
===============================================================================
Script Purpose:
    This script runs data quality checks on the 'dw_silver' schema to ensure
    integrity and consistency before loading into Gold.

Key Checks Implemented:
    1. Primary Key Integrity: Ensuring no duplicates or NULLs in PK columns.
    2. Data Standardization: Verifying that 'type' and 'color' mapping logic 
       (e.g., HEX codes, full descriptions) worked correctly.
    3. Distribution Analysis: Checking for outliers in numerical fields (tonnage)
       to validate unit conversions (kg vs tons).

Current Status:
    - [x] fms_car_info: PK and Standardization checks active.
    - [ ] fms_driver_info: Pending.
    - [ ] fms_company_info: Pending.
    - [ ] Telematics Tables: Pending.

Usage:
    Run individual blocks to validate specific tables after the Silver load.
===============================================================================
*/


-- ====================================================================
-- Quality Checks: 'dw_silver.fms_car_info'
-- ====================================================================

-- 1. Primary Key Integrity Check
-- Purpose: Verify that 'car_key' is unique and not NULL.
-- Expectation: This query should return 0 rows.
SELECT car_key, COUNT(*)
FROM dw_silver.fms_car_info
GROUP BY car_key
HAVING COUNT(*) > 1 OR car_key IS NULL;


-- 2. Data Standardization: Vehicle Type
-- Purpose: Ensure all single-character codes (V, W, etc.) were successfully 
--          converted to full descriptions (Van, Lowdeck).
-- Expectation: Result list should NOT contain single letters like 'V' or 'L'.
SELECT DISTINCT type 
FROM dw_silver.fms_car_info;


-- 3. Data Standardization: Color Codes
-- Purpose: Verify that color names were normalized to lowercase and mapped to HEX codes.
-- Expectation: Results should look like '#ffffff', '#ecf0f6', or 'n/a'. 
--              No 'White' or 'Alpine White' strings should remain.
SELECT DISTINCT color
FROM dw_silver.fms_car_info;


-- 4. Distribution Analysis: Tonnage Outliers
-- Purpose: Validate that the unit conversion logic (kg -> tons) handled all edge cases.
-- Expectation: You should see a reasonable distribution (e.g., 1.5 - 40.0). 
--              If you see values > 100, the division logic failed for some records.
SELECT tonnage, COUNT(*)
FROM dw_silver.fms_car_info
GROUP BY tonnage
ORDER BY tonnage DESC;