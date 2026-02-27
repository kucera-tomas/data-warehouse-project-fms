/*
===============================================================================
Quality Checks: Silver Layer Validation
===============================================================================
Script Purpose:
    This script runs data quality checks on the 'dw_silver' schema to ensure
    integrity and consistency before loading into Gold.

Key Checks Implemented:
    1. Primary Key Integrity: Ensuring no duplicates or NULLs in PK columns.
    2. Data Standardization: Verifying mapping logic and string formatting.
    3. Logical Constraints: Verifying physical realities (e.g. speeds > 130km/h).
    4. Anomaly Detection: Using Window Functions to catch IoT sensor drift/spikes.

Usage:
    Run individual blocks to validate specific tables after the Silver load.
===============================================================================
*/

-- ====================================================================
-- Quality Checks: 'dw_silver.fms_car_info'
-- ====================================================================

-- 1. Primary Key Integrity Check
-- Expectation: 0 rows (No duplicates or nulls)
SELECT car_key, COUNT(*)
FROM dw_silver.fms_car_info
GROUP BY car_key
HAVING COUNT(*) > 1 OR car_key IS NULL;

-- 2. Data Standardization: Vehicle Type Conversion
-- Expectation: No 1-letter codes should remain (e.g. 'V', 'L')
SELECT DISTINCT type FROM dw_silver.fms_car_info;

-- 3. Data Standardization: Color Mapping
-- Expectation: Output should be valid Hex Codes or 'n/a'
SELECT DISTINCT color FROM dw_silver.fms_car_info;

-- 4. Distribution Analysis: Tonnage
-- Expectation: Values should be strictly between ~1.0 and 50.0 tons.
SELECT tonnage, COUNT(*) FROM dw_silver.fms_car_info GROUP BY tonnage ORDER BY tonnage DESC;


-- ====================================================================
-- Quality Checks: 'dw_silver.fms_company_info'
-- ====================================================================

-- 1. Primary Key Integrity Check
-- Expectation: 0 rows (No duplicates or nulls)
SELECT company_key, COUNT(*)
FROM dw_silver.fms_company_info
GROUP BY company_key
HAVING COUNT(*) > 1 OR company_key IS NULL;


-- ====================================================================
-- Quality Checks: 'dw_silver.fms_driver_info'
-- ====================================================================

-- 1. Primary Key Integrity Check
-- Expectation: 0 rows (No duplicates or nulls)
SELECT driver_key, COUNT(*)
FROM dw_silver.fms_driver_info
GROUP BY driver_key
HAVING COUNT(*) > 1 OR driver_key IS NULL;

-- 2. Format Validation: Name Capitalization
-- Expectation: 0 rows (Finds names that are entirely lowercase, proving INITCAP failed)
SELECT name
FROM dw_silver.fms_driver_info
WHERE LOWER(name) LIKE name;


-- ====================================================================
-- Quality Checks: 'dw_silver.telco_operator_info'
-- ====================================================================
-- Manual visual validation check for operator metadata
SELECT * FROM dw_silver.telco_operator_info LIMIT 10;


-- ====================================================================
-- Quality Checks: 'dw_silver.telematics_health'
-- ====================================================================

-- 1. Logical Constraint: Application Time vs Device Time
-- Expectation: 0 rows (An app cannot run longer than the device it runs on)
SELECT * FROM dw_silver.telematics_health
WHERE app_run_time > dev_run_time;

-- 2. Logical Constraint: Impossible Negative Times
-- Expectation: 0 rows
SELECT * FROM dw_silver.telematics_health 
WHERE app_run_time < 0 OR dev_run_time < 0;

-- 3. Constraint: Battery Percentage Limits
-- Expectation: 0 rows (Battery must be 0-100)
SELECT * FROM dw_silver.telematics_health 
WHERE battery_level < 0 OR battery_level > 100;

-- 4. Anomaly Detection: Rapid Battery Drain/Spike (Hardware Malfunction)
-- Expectation: 0 rows (A battery should not jump or drop 40% in under 5 minutes)
WITH BatteryPings AS (
    SELECT 
        service_key, car_key, scan_time, battery_level,
        LAG(battery_level) OVER(PARTITION BY car_key, device_name ORDER BY scan_time) as prev_battery,
        LAG(scan_time) OVER(PARTITION BY car_key, device_name ORDER BY scan_time) as prev_time
    FROM dw_silver.telematics_health
)
SELECT * FROM BatteryPings
WHERE TIMESTAMPDIFF(MINUTE, prev_time, scan_time) <= 5
  AND ABS(battery_level - prev_battery) > 40;

-- 5. Data Profiling: Distinct Devices
SELECT DISTINCT device_name FROM dw_silver.telematics_health;


-- ====================================================================
-- Quality Checks: 'dw_silver.telematics_tracking'
-- ====================================================================

-- 1. Event Granularity Check
-- Expectation: 0 rows (A single truck should not send >2 pings at the exact same second)
SELECT car_key, scan_time, COUNT(*) 
FROM dw_silver.telematics_tracking
GROUP BY car_key, scan_time
HAVING COUNT(*) > 2;

-- 2. Geospatial Integrity: Earth Boundaries
-- Expectation: 0 rows (Latitudes must be -90/90, Longitudes -180/180)
SELECT * FROM dw_silver.telematics_tracking
WHERE pos_gps_lat NOT BETWEEN -90 AND 90
   OR pos_gps_long NOT BETWEEN -180 AND 180;

-- 3. Logical Constraint: Physics/Speed validation
-- Expectation: 0 rows (Validates that the v=d/t fix in the Silver load worked)
SELECT * FROM dw_silver.telematics_tracking WHERE speed < 0 OR speed > 130;

-- 4. Status vs. Telemetry Mismatch
-- Expectation: 0 rows (Finds trucks that claim to be parked but are doing > 5km/h, 
--              or claim to be driving but are at 0km/h)
SELECT * FROM telematics_tracking
WHERE (truck_status IN ('Start of Stationary State', 'End of Stationary State', 'Start of Driver in Rest') AND speed > 5) 
   OR (truck_status = 'Moving Car' AND speed = 0);

-- 5. Anomaly Detection: GPS Multipath / "Teleportation" Check
-- Expectation: 0 rows (A truck cannot move 0.1 decimal degrees [~11km] in under 60 seconds)
WITH OrderedPings AS (
    SELECT 
        car_key, scan_time, pos_gps_lat, pos_gps_long,
        LAG(scan_time) OVER(PARTITION BY car_key ORDER BY scan_time) as prev_time,
        LAG(pos_gps_lat) OVER(PARTITION BY car_key ORDER BY scan_time) as prev_lat,
        LAG(pos_gps_long) OVER(PARTITION BY car_key ORDER BY scan_time) as prev_long
    FROM telematics_tracking
)
SELECT * FROM OrderedPings
WHERE TIMESTAMPDIFF(second, prev_time, scan_time) < 60 
  AND (ABS(pos_gps_lat - prev_lat) > 0.1 OR ABS(pos_gps_long - prev_long) > 0.1);
  
-- 6. Logical Time Constraint
-- Expectation: 0 rows (A scan time cannot exist in the future)
SELECT * FROM telematics_tracking 
WHERE scan_time > NOW(); 
  
-- 7. Process Constraint: Concurrent Driving
-- Expectation: 0 rows (One driver cannot physically drive two different trucks at the exact same time)
SELECT driver_key, scan_time, COUNT(DISTINCT car_key) as trucks_driven
FROM telematics_tracking
GROUP BY driver_key, scan_time
HAVING COUNT(DISTINCT car_key) > 1;


-- ====================================================================
-- Quality Checks: 'dw_silver.ref_gps_country'
-- ====================================================================
-- 1. Geospatial Mapping Validation
SELECT country_code, country, COUNT(*)
FROM dw_silver.ref_gps_country
GROUP BY country_code, country
ORDER BY country_code ASC, COUNT(*) DESC;