/*
===============================================================================
Batch Script: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This script loads data into the 'dw_bronze' database from external CSV files.
    It performs the following actions:
    - Creates a temporary log table to track progress and duration.
    - Truncates the bronze tables before loading data.
    - Uses 'LOAD DATA INFILE' to bulk insert data from CSV files.
    - Generates a final summary of rows loaded and execution time.

Parameters:
    None.
      This script uses User-Defined Variables (e.g., @batch_start) and 
      Temporary Tables valid only for the current session.

Usage Example:
    1. Open this script in MySQL Workbench or your SQL Client.
    2. Execute the entire script at once (Run All).
    3. View the final result grid for the execution log.

Notes:
    - Ensure 'local_infile' is enabled if loading from a client machine.
    - Update file paths in the 'LOAD DATA' commands to match your local directory.
===============================================================================
*/

USE dw_bronze;

-- 1. Setup: Create a temporary logging table (Resets every run)
DROP TEMPORARY TABLE IF EXISTS Job_Log;
CREATE TEMPORARY TABLE Job_Log (
    ID INT AUTO_INCREMENT PRIMARY KEY,
    StepName VARCHAR(100),
    Status VARCHAR(50),
    Rows_Affected INT,
    Duration_Seconds DECIMAL(10,2),
    LogTime DATETIME
);

-- Initialize Global Timer
SET @batch_start = NOW();

-- =======================================================
-- Loading source: FMS (Fleet Management System)
-- =======================================================

-- 1. fms_car_info
SET @t_start = NOW();
TRUNCATE TABLE fms_car_info;
LOAD DATA INFILE 'C:/sql/data-warehouse-project/datasets/source_fms/car_info.csv'
INTO TABLE fms_car_info 
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;
SET @t_end = NOW();
SELECT COUNT(*) INTO @rc FROM fms_car_info;
INSERT INTO Job_Log (StepName, Status, Rows_Affected, Duration_Seconds, LogTime)
VALUES ('fms_car_info', 'Success', @rc, TIMESTAMPDIFF(MICROSECOND, @t_start, @t_end)/1000000, NOW());

-- 2. fms_car_info_upd
SET @t_start = NOW();
TRUNCATE TABLE fms_car_info_upd;
LOAD DATA INFILE 'C:/sql/data-warehouse-project/datasets/source_fms/car_info_upd.csv'
INTO TABLE fms_car_info_upd 
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;
SET @t_end = NOW();
SELECT COUNT(*) INTO @rc FROM fms_car_info_upd;
INSERT INTO Job_Log (StepName, Status, Rows_Affected, Duration_Seconds, LogTime)
VALUES ('fms_car_info_upd', 'Success', @rc, TIMESTAMPDIFF(MICROSECOND, @t_start, @t_end)/1000000, NOW());

-- 3. fms_company_info
SET @t_start = NOW();
TRUNCATE TABLE fms_company_info;
LOAD DATA INFILE 'C:/sql/data-warehouse-project/datasets/source_fms/company_info.csv'
INTO TABLE fms_company_info 
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;
SET @t_end = NOW();
SELECT COUNT(*) INTO @rc FROM fms_company_info;
INSERT INTO Job_Log (StepName, Status, Rows_Affected, Duration_Seconds, LogTime)
VALUES ('fms_company_info', 'Success', @rc, TIMESTAMPDIFF(MICROSECOND, @t_start, @t_end)/1000000, NOW());

-- 4. fms_driver_info
SET @t_start = NOW();
TRUNCATE TABLE fms_driver_info;
LOAD DATA INFILE 'C:/sql/data-warehouse-project/datasets/source_fms/driver_info.csv'
INTO TABLE fms_driver_info 
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;
SET @t_end = NOW();
SELECT COUNT(*) INTO @rc FROM fms_driver_info;
INSERT INTO Job_Log (StepName, Status, Rows_Affected, Duration_Seconds, LogTime)
VALUES ('fms_driver_info', 'Success', @rc, TIMESTAMPDIFF(MICROSECOND, @t_start, @t_end)/1000000, NOW());


-- =======================================================
-- Loading source: TELCO
-- =======================================================

-- 5. telco_operator_info
SET @t_start = NOW();
TRUNCATE TABLE telco_operator_info;
LOAD DATA INFILE 'C:/sql/data-warehouse-project/datasets/source_telco/operator_info.csv'
INTO TABLE telco_operator_info 
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;
SET @t_end = NOW();
SELECT COUNT(*) INTO @rc FROM telco_operator_info;
INSERT INTO Job_Log (StepName, Status, Rows_Affected, Duration_Seconds, LogTime)
VALUES ('telco_operator_info', 'Success', @rc, TIMESTAMPDIFF(MICROSECOND, @t_start, @t_end)/1000000, NOW());


-- =======================================================
-- Loading source: TELEMATICS
-- =======================================================

-- 6. telematics_health
SET @t_start = NOW();
TRUNCATE TABLE telematics_health;
LOAD DATA INFILE 'C:/sql/data-warehouse-project/datasets/source_telematics/import_health.csv'
INTO TABLE telematics_health 
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;
SET @t_end = NOW();
SELECT COUNT(*) INTO @rc FROM telematics_health;
INSERT INTO Job_Log (StepName, Status, Rows_Affected, Duration_Seconds, LogTime)
VALUES ('telematics_health', 'Success', @rc, TIMESTAMPDIFF(MICROSECOND, @t_start, @t_end)/1000000, NOW());

-- 7. telematics_health_upd
SET @t_start = NOW();
TRUNCATE TABLE telematics_health_upd;
LOAD DATA INFILE 'C:/sql/data-warehouse-project/datasets/source_telematics/import_health_upd.csv'
INTO TABLE telematics_health_upd 
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;
SET @t_end = NOW();
SELECT COUNT(*) INTO @rc FROM telematics_health_upd;
INSERT INTO Job_Log (StepName, Status, Rows_Affected, Duration_Seconds, LogTime)
VALUES ('telematics_health_upd', 'Success', @rc, TIMESTAMPDIFF(MICROSECOND, @t_start, @t_end)/1000000, NOW());

-- 8. telematics_tracking
SET @t_start = NOW();
TRUNCATE TABLE telematics_tracking;
LOAD DATA INFILE 'C:/sql/data-warehouse-project/datasets/source_telematics/import_tracking.csv'
INTO TABLE telematics_tracking 
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;
SET @t_end = NOW();
SELECT COUNT(*) INTO @rc FROM telematics_tracking;
INSERT INTO Job_Log (StepName, Status, Rows_Affected, Duration_Seconds, LogTime)
VALUES ('telematics_tracking', 'Success', @rc, TIMESTAMPDIFF(MICROSECOND, @t_start, @t_end)/1000000, NOW());

-- 9. telematics_tracking_upd
SET @t_start = NOW();
TRUNCATE TABLE telematics_tracking_upd;
LOAD DATA INFILE 'C:/sql/data-warehouse-project/datasets/source_telematics/import_tracking_upd.csv'
INTO TABLE telematics_tracking_upd 
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;
SET @t_end = NOW();
SELECT COUNT(*) INTO @rc FROM telematics_tracking_upd;
INSERT INTO Job_Log (StepName, Status, Rows_Affected, Duration_Seconds, LogTime)
VALUES ('telematics_tracking_upd', 'Success', @rc, TIMESTAMPDIFF(MICROSECOND, @t_start, @t_end)/1000000, NOW());


-- =======================================================
-- FINAL SUMMARY
-- =======================================================
SET @batch_end = NOW();
SET @total_duration = TIMESTAMPDIFF(MICROSECOND, @batch_start, @batch_end) / 1000000;

SELECT SUM(Rows_Affected) INTO @total_rows FROM Job_Log;

INSERT INTO Job_Log (StepName, Status, Rows_Affected, Duration_Seconds, LogTime)
VALUES ('=== TOTAL BATCH ===', 'COMPLETE', @total_rows, @total_duration, NOW());

-- Show output logs
SELECT StepName, Status, Rows_Affected, Duration_Seconds FROM Job_Log;