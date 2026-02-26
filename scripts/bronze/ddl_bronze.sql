/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'dw_bronze' database, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

USE dw_bronze;

-- =======================================================
-- Source: FMS (Fleet Management System)
-- =======================================================

-- 1. Car info (Base)
DROP TABLE IF EXISTS fms_car_info;
CREATE TABLE fms_car_info
(
	car_key INT,
    company_key INT,
    license_plate NVARCHAR(20),
    make NVARCHAR(50),
    color NVARCHAR(50),
    tonnage NVARCHAR(50),
    type NVARCHAR(50)
);

-- 2. Car info (Update)
DROP TABLE IF EXISTS fms_car_info_upd;
CREATE TABLE fms_car_info_upd
(
	car_key INT,
    company_key INT,
    license_plate NVARCHAR(20),
    make NVARCHAR(50),
    color NVARCHAR(50),
    tonnage NVARCHAR(50),
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

-- 6. Health (Base)
DROP TABLE IF EXISTS telematics_health;
CREATE TABLE telematics_health
(
	service_key INT,
    car_key INT,
    driver_key INT,
    scan_time NVARCHAR(50),
    app_run_time NVARCHAR(50),
    dev_run_time NVARCHAR(50),
    device_name NVARCHAR(50),
    battery_level NVARCHAR(50),
    pos_gps NVARCHAR(50)
);

-- 7. Health (Updates)
DROP TABLE IF EXISTS telematics_health_upd;
CREATE TABLE telematics_health_upd
(
	service_key INT,
    car_key INT,
    driver_key INT,
    scan_time NVARCHAR(50),
    app_run_time NVARCHAR(50),
    dev_run_time NVARCHAR(50),
    device_name NVARCHAR(50),
    battery_level NVARCHAR(50),
    pos_gps NVARCHAR(50)
);

-- 8. Tracking (Base)
DROP TABLE IF EXISTS telematics_tracking;
CREATE TABLE telematics_tracking
(
	pos_key INT,
    car_key INT,
    driver_key INT,
    scan_time NVARCHAR(50),
    truck_status NVARCHAR(2),
    pos_gps NVARCHAR(50),
	speed NVARCHAR(50),
    distance NVARCHAR(50), 
    driving_time NVARCHAR(50)
);

-- 9. Tracking (Updates)
DROP TABLE IF EXISTS telematics_tracking_upd;
CREATE TABLE telematics_tracking_upd
(
	pos_key INT,
    car_key INT,
    driver_key INT,
    scan_time NVARCHAR(50),
    truck_status NVARCHAR(2),
    pos_gps NVARCHAR(50),
	speed NVARCHAR(50),
    distance NVARCHAR(50), 
    driving_time NVARCHAR(50)
);