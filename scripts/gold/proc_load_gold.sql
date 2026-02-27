/*
===============================================================================
Stored Procedure: Load Gold Layer (Silver -> Gold)
===============================================================================
Script Purpose:
    This script populates the Star Schema in the 'dw_gold' database.
    1. Loads Dimensions: Maps Silver attributes to Gold dimension tables.
    2. Generates Date Dimension: Dynamically creates a smart date table.
    3. Loads Facts: Performs lookups to replace Natural Keys (_id) with 
       Data Warehouse Surrogate Keys (_key).

Usage:
    Run this script after the Silver layer has been successfully populated.
===============================================================================
*/

USE dw_gold;


-- Disable Foreign Key checks temporarily to allow truncating tables
SET FOREIGN_KEY_CHECKS = 0;

-- ====================================================================
-- 1. LOAD DIMENSIONS
-- ====================================================================

-- 1A. Dim: Company
TRUNCATE TABLE dim_company;
INSERT INTO dim_company (company_id, company_name, city, country, region)
SELECT 
    company_key AS company_id, 
    company AS company_name, 
    city, 
    country, 
    region
FROM dw_silver.fms_company_info;

-- 1B. Dim: Vehicle
TRUNCATE TABLE dim_vehicle;
INSERT INTO dim_vehicle (car_id, company_id, license_plate, make, color, tonnage, vehicle_type)
SELECT 
    car_key AS car_id, 
    company_key AS company_id, 
    license_plate, 
    make, 
    color, 
    tonnage, 
    type AS vehicle_type
FROM dw_silver.fms_car_info;

-- 1C. Dim: Driver
TRUNCATE TABLE dim_driver;
INSERT INTO dim_driver (driver_id, company_id, driver_name)
SELECT 
    driver_key AS driver_id, 
    company_key AS company_id, 
    name AS driver_name
FROM dw_silver.fms_driver_info;

-- 1D. Dim: Geography (Unique list of regions from Python enrichment)
TRUNCATE TABLE dim_geography;
INSERT INTO dim_geography (country_code, region_name)
SELECT DISTINCT 
    country_code, 
    country AS region_name
FROM dw_silver.ref_gps_country
WHERE country_code IS NOT NULL;

-- 1E. Dim: Roaming Network
TRUNCATE TABLE dim_roaming_network;
INSERT INTO dim_roaming_network (mcc, mnc, country_code, country_name, network_name)
SELECT 
    mcc, 
    mnc, 
    country_code, 
    country AS country_name, 
    network AS network_name
FROM dw_silver.telco_operator_info;

-- 1F. Dim: Date (Generating 2 years of dates dynamically)
-- Using a recursive CTE (Works in MySQL 8.0+)
TRUNCATE TABLE dim_date;
INSERT INTO dim_date (date_key, full_date, year, quarter, month, month_name, day_of_month, day_of_week, day_name, is_weekend)
WITH RECURSIVE DateGenerator AS (
    SELECT CAST('2025-01-01' AS DATE) AS d_date
    UNION ALL
    SELECT DATE_ADD(d_date, INTERVAL 1 DAY)
    FROM DateGenerator
    WHERE d_date < '2026-12-31'
)
SELECT 
    CAST(DATE_FORMAT(d_date, '%Y%m%d') AS UNSIGNED) AS date_key,
    d_date AS full_date,
    YEAR(d_date) AS year,
    QUARTER(d_date) AS quarter,
    MONTH(d_date) AS month,
    MONTHNAME(d_date) AS month_name,
    DAY(d_date) AS day_of_month,
    DAYOFWEEK(d_date) AS day_of_week,
    DAYNAME(d_date) AS day_name,
    CASE WHEN DAYOFWEEK(d_date) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend
FROM DateGenerator;


-- ====================================================================
-- 2. LOAD FACTS (The Lookups)
-- ====================================================================

-- 2A. Fact: Device Health
TRUNCATE TABLE fact_device_health;
INSERT INTO fact_device_health (
    date_key, vehicle_key, driver_key, company_key, 
    scan_time, device_name, pos_gps_lat, pos_gps_long, 
    battery_level, app_run_time, dev_run_time
)
SELECT 
    CAST(DATE_FORMAT(s.scan_time, '%Y%m%d') AS UNSIGNED) AS date_key,
    v.vehicle_key,
    d.driver_key,
    c.company_key,
    s.scan_time,
    s.device_name,
    s.pos_gps_lat,
    s.pos_gps_long,
    s.battery_level,
    s.app_run_time,
    s.dev_run_time
FROM dw_silver.telematics_health s
-- LOOKUPS: Translating Natural Keys (_id) to Surrogate Keys (_key)
LEFT JOIN dim_vehicle v ON s.car_key = v.car_id
LEFT JOIN dim_driver d ON s.driver_key = d.driver_id
LEFT JOIN dim_company c ON v.company_id = c.company_id;


-- 2B. Fact: Tracking
TRUNCATE TABLE fact_tracking;
INSERT INTO fact_tracking (
    date_key, vehicle_key, driver_key, company_key, geo_key,
    scan_time, truck_status, pos_gps_lat, pos_gps_long, 
    speed, distance, driving_time
)
SELECT 
    CAST(DATE_FORMAT(s.scan_time, '%Y%m%d') AS UNSIGNED) AS date_key,
    v.vehicle_key,
    d.driver_key,
    c.company_key,
    g.geo_key,
    s.scan_time,
    s.truck_status,
    s.pos_gps_lat,
    s.pos_gps_long,
    s.speed,
    s.distance,
    s.driving_time
FROM dw_silver.telematics_tracking s
-- LOOKUPS: Translating Natural Keys (_id) to Surrogate Keys (_key)
LEFT JOIN dim_vehicle v ON s.car_key = v.car_id
LEFT JOIN dim_driver d ON s.driver_key = d.driver_id
LEFT JOIN dim_company c ON v.company_id = c.company_id
-- GEO BRIDGE LOOKUP: Reconstruct the original GPS string to join to the Python reference table, then join to dim_geography
LEFT JOIN dw_silver.ref_gps_country ref 
       ON CONCAT(s.pos_gps_lat, ',', s.pos_gps_long) = ref.pos_gps
LEFT JOIN dim_geography g 
       ON ref.country_code = g.country_code AND ref.country = g.region_name;


SET FOREIGN_KEY_CHECKS = 1;