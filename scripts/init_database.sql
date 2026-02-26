/*
=============================================================
Data Warehouse Setup (Bronze, Silver and Gold)
=============================================================
Script Purpose:
	This script creates the three-layer database architecture for the Data Warehouse.
    Checks for existing databases and recreates them to ensure a clean state.
	
WARNING:
	Running this script will permanently delete all data in the 
    'dw_bronze', 'dw_silver' and 'dw_gold' databases.
*/

-- 1. Bronze
DROP DATABASE IF EXISTS dw_bronze;
CREATE DATABASE dw_bronze;

-- 2. Silver
DROP DATABASE IF EXISTS dw_silver;
CREATE DATABASE dw_silver;

-- 3. Gold
DROP DATABASE IF EXISTS dw_gold;
CREATE DATABASE dw_gold;