-- =========================================================
-- 1. Index the Gold Dimensions (Natural Keys)
-- =========================================================
-- These are the columns your script is looking up (v.car_id, d.driver_id, c.company_id)
CREATE INDEX idx_dim_vehicle_car ON dw_gold.dim_vehicle(car_id);
CREATE INDEX idx_dim_driver_drv ON dw_gold.dim_driver(driver_id);
CREATE INDEX idx_dim_company_cmp ON dw_gold.dim_company(company_id);
CREATE INDEX idx_dim_geo_country ON dw_gold.dim_geography(country_code, region_name);

-- =========================================================
-- 2. Index the Silver Fact Table (Foreign Keys)
-- =========================================================
CREATE INDEX idx_silver_track_car ON dw_silver.telematics_tracking(car_key);
CREATE INDEX idx_silver_track_drv ON dw_silver.telematics_tracking(driver_key);

-- =========================================================
-- 3. THE LIFESAVER: Composite Indexes for the GPS Join
-- =========================================================
CREATE INDEX idx_silver_track_gps ON dw_silver.telematics_tracking(pos_gps_lat, pos_gps_long);
CREATE INDEX idx_silver_ref_gps ON dw_silver.ref_gps_country(pos_gps_lat, pos_gps_long);