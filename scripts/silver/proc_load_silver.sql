/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This script performs the ETL transformation from raw Bronze tables to the 
    cleaned Silver layer. It handles:
    1. CDC (Change Data Capture): Merging base snapshot tables with incremental 
       update (_upd) tables to create a single source of truth.
    2. Data Cleaning: Normalizing license plates, standardizing color codes (hex), 
       and fixing tonnage unit mismatches.
    3. Type Casting: Converting loose VARCHAR types to strict schema types (FLOAT, INT).

Current Status:
    - [x] fms_car_info: Implemented merge logic and cleaning rules.
    - [ ] fms_driver_info: Pending.
    - [ ] fms_company_info: Pending.
    - [ ] telco_operator_info: Pending.
    - [ ] telematics_tracking: Pending.
    - [ ] telematics_health: Pending.

Usage Example:
    Run this script to populate 'dw_silver.fms_car_info'.
===============================================================================
*/

TRUNCATE TABLE dw_silver.fms_car_info;

INSERT INTO dw_silver.fms_car_info (car_key, company_key, license_plate, make, color, tonnage, type)


WITH car_info_merged AS (
    -- New or updated rows
    SELECT * FROM dw_bronze.fms_car_info_upd
    
    UNION ALL
    
    -- Existing rows
    SELECT base.* FROM dw_bronze.fms_car_info base
    LEFT JOIN dw_bronze.fms_car_info_upd upd 
        ON base.car_key = upd.car_key
    WHERE upd.car_key IS NULL
)

-- 2. Apply transformations to the final dataset
SELECT 
	car_key,
    company_key,
    CASE
		WHEN license_plate LIKE "B66-0FV5375" THEN "0FV5375"
        ELSE REPLACE(license_plate, "-", "")
	END AS license_plate,
    CASE 
		WHEN make LIKE "MB" THEN "Mercedes-Benz"
		WHEN make IS NULL OR make = "" OR make = "-------" THEN "n/a"
		ELSE make
    END AS make,
    CASE
		WHEN color = "white" THEN "#ffffff"
        WHEN color = "alpine white" THEN "#ecf0f6"
        WHEN color = "" THEN "n/a"
        ELSE LOWER(color)
	END AS color,
	CASE
		WHEN tonnage = 111 THEN CAST(tonnage AS FLOAT) / 10.0
        WHEN tonnage > 200 THEN CAST(tonnage AS FLOAT) / 100.0 -- Values inputted as kilos instead of tons
        ELSE CAST(tonnage AS FLOAT)
    END AS tonnage,
    CASE type
		WHEN "V" THEN "Van"
		WHEN "W" THEN "Lowdeck"
		WHEN "N" THEN "Normal"
		WHEN "P" THEN "Passenger"
		WHEN "D" THEN "Dumper"
		WHEN "X" THEN "Special"
		WHEN "L" THEN "Tautliner"
        ELSE "n/a"
	END AS type
FROM car_info_merged;