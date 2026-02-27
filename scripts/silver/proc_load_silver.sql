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

Usage Example:
    Run this script to populate 'dw_silver' tables.
===============================================================================
*/

-- =======================================================
-- Source: FMS (Fleet Management System)
-- =======================================================

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

-- Apply transformations to the final car dataset
SELECT 
    car_key,
    company_key,
    -- Standardization: Remove prefixes like 'AB12-x' or simple dashes from license plates
    CASE
        WHEN license_plate REGEXP '^[a-zA-Z][0-9]{2}-' 
        THEN REGEXP_REPLACE(license_plate, '^[a-zA-Z][0-9]{2}-', '')
        ELSE REPLACE(license_plate, "-", "")
    END AS license_plate,
    -- Quality: Map abbreviations to full names, handle empty/null strings
    CASE 
        WHEN make LIKE "MB" THEN "Mercedes-Benz"
        WHEN make IS NULL OR make = "" OR make = "-------" THEN "n/a"
        ELSE make
    END AS make,
    -- Enrichment: Convert text colors to UI-friendly Hex codes
    CASE
        WHEN color = "white" THEN "#ffffff"
        WHEN color = "alpine white" THEN "#ecf0f6"
        WHEN color = "" THEN "n/a"
        ELSE LOWER(color)
    END AS color,
    -- Validation: Fix unit conversion errors (kilos to tons)
    CASE
        WHEN tonnage = 111 THEN CAST(tonnage AS FLOAT) / 10.0
        WHEN tonnage > 200 THEN CAST(tonnage AS FLOAT) / 100.0 
        ELSE CAST(tonnage AS FLOAT)
    END AS tonnage,
    -- Standardization: Expand 1-letter type codes to full descriptions
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


TRUNCATE TABLE dw_silver.fms_company_info;
INSERT INTO dw_silver.fms_company_info (company_key, company, city, country, region)
SELECT 
    company_key,
    company,
    city,
    country,
    region
FROM dw_bronze.fms_company_info;


TRUNCATE TABLE dw_silver.fms_driver_info;
INSERT INTO dw_silver.fms_driver_info (company_key, driver_key, name)
SELECT 
    company_key,
    driver_key,
    -- Standardization: Ensure names are properly capitalized (e.g., 'martin' -> 'Martin')
    CONCAT(
        UPPER(SUBSTRING(name, 1, 1)),
        LOWER(SUBSTRING(name, 2, LENGTH(name)))
    ) AS name
FROM dw_bronze.fms_driver_info;


-- =======================================================
-- Source: TELCO 
-- =======================================================

TRUNCATE TABLE dw_silver.telco_operator_info;
INSERT INTO dw_silver.telco_operator_info (mcc, mnc, country_code, country, network)
SELECT
    mcc,
    mnc,
    country_code,
    country,
    network
FROM dw_bronze.telco_operator_info;


-- =======================================================
-- Source: Telematics (IoT data)
-- CDC Strategy: Insert base, then Upsert updates
-- =======================================================

TRUNCATE TABLE dw_silver.telematics_health;

-- Step 1: Insert the base data 
INSERT IGNORE INTO dw_silver.telematics_health 
(service_key, car_key, driver_key, scan_time, app_run_time, dev_run_time, device_name, battery_level, pos_gps_lat, pos_gps_long)
SELECT 
     service_key, 
     car_key, 
     driver_key, 
     -- Type Casting: Clean string timestamp and convert to DATETIME
     CAST(LEFT(scan_time, 19) AS DATETIME), 
     CAST(app_run_time AS FLOAT), 
     CAST(dev_run_time AS FLOAT), 
     device_name, 
     CAST(battery_level AS FLOAT), 
     -- Feature Engineering: Strip parentheses and split GPS string into Lat and Long
     CAST(SUBSTRING_INDEX(REPLACE(REPLACE(pos_gps, '(', ''), ')', ''), ',', 1) AS DECIMAL(10, 6)), 
     CAST(SUBSTRING_INDEX(REPLACE(REPLACE(pos_gps, '(', ''), ')', ''), ',', -1) AS DECIMAL(10, 6))
FROM dw_bronze.telematics_health;

-- Step 2: Upsert the updated/new data
INSERT INTO dw_silver.telematics_health 
(service_key, car_key, driver_key, scan_time, app_run_time, dev_run_time, device_name, battery_level, pos_gps_lat, pos_gps_long)
SELECT 
     service_key, 
     car_key, 
     driver_key, 
     CAST(LEFT(scan_time, 19) AS DATETIME), 
     CAST(app_run_time AS FLOAT), 
     CAST(dev_run_time AS FLOAT), 
     device_name, 
     CAST(battery_level AS FLOAT), 
     CAST(SUBSTRING_INDEX(REPLACE(REPLACE(pos_gps, '(', ''), ')', ''), ',', 1) AS DECIMAL(10, 6)), 
     CAST(SUBSTRING_INDEX(REPLACE(REPLACE(pos_gps, '(', ''), ')', ''), ',', -1) AS DECIMAL(10, 6))
FROM dw_bronze.telematics_health_upd
-- CDC Logic: If Primary Key exists, overwrite with newest update file values
ON DUPLICATE KEY UPDATE 
    car_key = VALUES(car_key),
    driver_key = VALUES(driver_key),
    scan_time = VALUES(scan_time),
    app_run_time = VALUES(app_run_time),
    dev_run_time = VALUES(dev_run_time),
    device_name = VALUES(device_name),
    battery_level = VALUES(battery_level),
    pos_gps_lat = VALUES(pos_gps_lat),
    pos_gps_long = VALUES(pos_gps_long);


TRUNCATE TABLE dw_silver.telematics_tracking;

-- Step 1: Insert the base data 
INSERT IGNORE INTO dw_silver.telematics_tracking 
(pos_key, car_key, driver_key, scan_time, truck_status, pos_gps_lat, pos_gps_long, speed, distance, driving_time)
SELECT 
     pos_key,
     car_key,
     driver_key,
     CAST(LEFT(scan_time, 19) AS DATETIME),
     -- Standardization: Map obscure status codes to readable business logic
     CASE truck_status
        WHEN "M" THEN "Moving Car"
        WHEN "_" THEN "Start of Stationary State"
        WHEN "=" THEN "End of Stationary State"
        WHEN "*" THEN "Start of Driver in Rest"
        WHEN "X" THEN "GPS Unit was Offile During Driving"
        WHEN "F" THEN "Abroad Ferry"
        ELSE "n/a"
    END AS truck_status,
     CAST(SUBSTRING_INDEX(REPLACE(REPLACE(pos_gps, '(', ''), ')', ''), ',', 1) AS DECIMAL(10, 6)),
     CAST(SUBSTRING_INDEX(REPLACE(REPLACE(pos_gps, '(', ''), ')', ''), ',', -1) AS DECIMAL(10, 6)),
    -- Anomaly Correction: Handle GPS drift by recalculating speed based on physics (v=d/t)
    CASE 
        WHEN speed < 0 OR speed > 130 
             AND driving_time > 0 
        THEN ROUND(CAST(distance AS FLOAT) / (CAST(driving_time AS FLOAT) / 3600.0), 1)
        
        WHEN speed < 0 OR speed > 130 THEN NULL 
        ELSE CAST(speed AS FLOAT)
    END AS speed,
    CAST(distance AS FLOAT) AS distance,
    CAST(driving_time AS FLOAT) AS driving_time
FROM dw_bronze.telematics_tracking;

-- Step 2: Upsert the updated/new data
INSERT INTO dw_silver.telematics_tracking 
(pos_key, car_key, driver_key, scan_time, truck_status, pos_gps_lat, pos_gps_long, speed, distance, driving_time)
SELECT 
     pos_key,
     car_key,
     driver_key,
     CAST(LEFT(scan_time, 19) AS DATETIME),
     CASE truck_status
        WHEN "M" THEN "Moving Car"
        WHEN "_" THEN "Start of Stationary State"
        WHEN "=" THEN "End of Stationary State"
        WHEN "*" THEN "Start of Driver in Rest"
        WHEN "X" THEN "GPS Unit was Offile During Driving"
        WHEN "F" THEN "Abroad Ferry"
        ELSE "n/a"
    END AS truck_status,
     CAST(SUBSTRING_INDEX(REPLACE(REPLACE(pos_gps, '(', ''), ')', ''), ',', 1) AS DECIMAL(10, 6)),
     CAST(SUBSTRING_INDEX(REPLACE(REPLACE(pos_gps, '(', ''), ')', ''), ',', -1) AS DECIMAL(10, 6)),
    CASE 
        WHEN speed < 0 OR speed > 130 
             AND driving_time > 0 
        THEN ROUND(CAST(distance AS FLOAT) / (CAST(driving_time AS FLOAT) / 3600.0), 1)
        
        WHEN speed < 0 OR speed > 130 THEN NULL 
        ELSE CAST(speed AS FLOAT)
    END AS speed,
    CAST(distance AS FLOAT) AS distance,
    CAST(driving_time AS FLOAT) AS driving_time
FROM dw_bronze.telematics_tracking_upd
-- CDC Logic: Overwrite existing rows with the newest delta payload
ON DUPLICATE KEY UPDATE 
    car_key = VALUES(car_key),
    driver_key = VALUES(driver_key),
    scan_time = VALUES(scan_time),
    truck_status = VALUES(truck_status),
    pos_gps_lat = VALUES(pos_gps_lat),
    pos_gps_long = VALUES(pos_gps_long),
    speed = VALUES(speed),
    distance = VALUES(distance),
    driving_time = VALUES(driving_time);





-- 1. Disable safe mode for this session
SET SQL_SAFE_UPDATES = 0;

-- 2. Run script to fix empty country values in ref_gps_country
UPDATE dw_silver.ref_gps_country 
SET country = 'Unknown Region' 
WHERE country = '';

-- 3. Re-enable safe mode
SET SQL_SAFE_UPDATES = 1;