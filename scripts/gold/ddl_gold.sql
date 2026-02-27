/*
===============================================================================
DDL Script: Create Gold Layer (Star Schema)
===============================================================================
Script Purpose:
    This script defines the physical structure of the Gold layer.
    It implements a Kimball Star Schema optimized for BI reporting and analytics.

Usage:
    Run this script to initialize or reset the 'dw_gold' database structure.
===============================================================================
*/

USE dw_gold;


-- ====================================================================
-- 1. CREATE DIMENSIONS
-- ====================================================================

-- Dim: Date
DROP TABLE IF EXISTS dim_date;
CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    day_of_month INT NOT NULL,
    day_of_week INT NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

-- Dim: Company
DROP TABLE IF EXISTS dim_company;
CREATE TABLE dim_company (
    company_key INT AUTO_INCREMENT PRIMARY KEY,
    company_id INT NOT NULL,
    company_name NVARCHAR(50),
    city NVARCHAR(50),
    country NVARCHAR(10),
    region NVARCHAR(10)
);

-- Dim: Vehicle
DROP TABLE IF EXISTS dim_vehicle;
CREATE TABLE dim_vehicle (
    vehicle_key INT AUTO_INCREMENT PRIMARY KEY,
    car_id INT NOT NULL,
    company_id INT,
    license_plate NVARCHAR(20),
    make NVARCHAR(50),
    color NVARCHAR(10),      
    tonnage FLOAT,
    vehicle_type NVARCHAR(50)
);

-- Dim: Driver
DROP TABLE IF EXISTS dim_driver;
CREATE TABLE dim_driver (
    driver_key INT AUTO_INCREMENT PRIMARY KEY,
    driver_id INT NOT NULL,
    company_id INT,
    driver_name NVARCHAR(50)
);

-- Dim: Geography
DROP TABLE IF EXISTS dim_geography;
CREATE TABLE dim_geography (
    geo_key INT AUTO_INCREMENT PRIMARY KEY,
    country_code NVARCHAR(10) NOT NULL,
    region_name NVARCHAR(100) NOT NULL
);

-- Dim: Roaming Network
DROP TABLE IF EXISTS dim_roaming_network;
CREATE TABLE dim_roaming_network (
    network_key INT AUTO_INCREMENT PRIMARY KEY,
    mcc INT,
    mnc INT,
    country_code NVARCHAR(10),
    country_name NVARCHAR(50),
    network_name NVARCHAR(50)
);


-- ====================================================================
-- 2. CREATE FACTS
-- ====================================================================

-- Fact: Tracking
DROP TABLE IF EXISTS fact_tracking;
CREATE TABLE fact_tracking (
    tracking_id INT AUTO_INCREMENT PRIMARY KEY,
    
    -- Foreign Keys
    date_key INT,
    vehicle_key INT,
    driver_key INT,
    company_key INT,
    geo_key INT,
    
    -- Degenerate Dimensions
    scan_time DATETIME NOT NULL,
    truck_status NVARCHAR(50),
    pos_gps_lat DECIMAL(10, 6),
    pos_gps_long DECIMAL(10, 6),
    
    -- Measures
    speed FLOAT,
    distance FLOAT,
    driving_time FLOAT,
    
    -- Constraints
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (vehicle_key) REFERENCES dim_vehicle(vehicle_key),
    FOREIGN KEY (driver_key) REFERENCES dim_driver(driver_key),
    FOREIGN KEY (company_key) REFERENCES dim_company(company_key),
    FOREIGN KEY (geo_key) REFERENCES dim_geography(geo_key)
);

-- Fact: Device Health
DROP TABLE IF EXISTS fact_device_health;
CREATE TABLE fact_device_health (
    health_id INT AUTO_INCREMENT PRIMARY KEY,
    
    -- Foreign Keys
    date_key INT,
    vehicle_key INT,
    driver_key INT,
    company_key INT,
    
    -- Degenerate Dimensions
    scan_time DATETIME NOT NULL,
    device_name NVARCHAR(50),
    pos_gps_lat DECIMAL(10, 6),
    pos_gps_long DECIMAL(10, 6),
    
    -- Measures
    battery_level FLOAT,
    app_run_time FLOAT,
    dev_run_time FLOAT,

    -- Constraints
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (vehicle_key) REFERENCES dim_vehicle(vehicle_key),
    FOREIGN KEY (driver_key) REFERENCES dim_driver(driver_key),
    FOREIGN KEY (company_key) REFERENCES dim_company(company_key)
);