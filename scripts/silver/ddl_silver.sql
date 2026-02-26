/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'dw_silver' database, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'silver' Tables
===============================================================================
*/

USE dw_silver;

-- =======================================================
-- Source: FMS (Fleet Management System)
-- =======================================================

-- 1. Car info
DROP TABLE IF EXISTS fms_car_info;
CREATE TABLE fms_car_info
(
	car_key INT,
    company_key INT,
    license_plate NVARCHAR(20),
    make NVARCHAR(50),
    color NVARCHAR(50),
    tonnage FLOAT,
    type NVARCHAR(50)
);


-- 3. Company info
DROP TABLE IF EXISTS fms_company_info;
CREATE TABLE fms_company_info
(
    company_key INT,
	company NVARCHAR(50),
    city NVARCHAR(50),
    country NVARCHAR(10),
    region NVARCHAR(10)
);

-- 4. Driver info
DROP TABLE IF EXISTS fms_driver_info;
CREATE TABLE fms_driver_info
(
    company_key INT,
	driver_key INT,
    name NVARCHAR(50)
);


-- =======================================================
-- Source: TELCO (Telecomunications)
-- =======================================================

-- 5. Operator info
DROP TABLE IF EXISTS telco_operator_info;
CREATE TABLE telco_operator_info
(
	mcc INT,
    mnc INT,
    country_code NVARCHAR(10),
    country NVARCHAR(50),
    network NVARCHAR(50)
);


-- =======================================================
-- Source: Telematics (IoT data)
-- =======================================================

-- 6. Health
DROP TABLE IF EXISTS telematics_health;
CREATE TABLE telematics_health
(
	service_key INT,
    car_key INT,
    driver_key INT,
    scan_time DATETIME,
    app_run_time FLOAT,
    dev_run_time FLOAT,
    device_name NVARCHAR(50),
    battery_level FLOAT,
    pos_gps_lat FLOAT,
    pos_gps_long FLOAT
);

-- 8. Tracking
DROP TABLE IF EXISTS telematics_tracking;
CREATE TABLE telematics_tracking
(
	pos_key INT,
    car_key INT,
    driver_key INT,
    scan_time DATETIME,
    truck_status NVARCHAR(20),
    pos_gps_lat FLOAT,
    pos_gps_long FLOAT,
	speed FLOAT,
    distance FLOAT, 
    driving_time FLOAT
);
