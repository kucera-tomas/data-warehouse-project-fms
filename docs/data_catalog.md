# 📖 Enterprise Data Catalog

Welcome to the Fleet Telematics Data Catalog. This document serves as the primary data dictionary for the **Gold Layer (Data Mart)**, designed for business intelligence and analytics.

---

## 🏗️ Architecture Overview
* **Bronze Layer:** Raw, immutable history of ingested CSVs (FMS, Telematics, Telco). All fields are strings.
* **Silver Layer:** Cleaned, deduplicated (CDC applied), and strongly-typed data. Includes Python-based geospatial enrichment.
* **Gold Layer:** Star Schema optimized for read-heavy analytical queries.
---

## 📊 Data Dictionary: Facts

### `fact_tracking`
**Type:** Transactional Fact Table  
**Description:** The core business table recording the physical movement, speed, and status of the fleet.  
**Grain:** One row per GPS ping per vehicle.  

| Column Name | Data Type | Description | Example |
| :--- | :--- | :--- | :--- |
| `tracking_id` | `INT` | Primary Key. Auto-incremented unique identifier for the record. | `140592` |
| `date_key` | `INT` | Foreign Key -> `dim_date`. Smart key format (YYYYMMDD). | `20250128` |
| `vehicle_key` | `INT` | Foreign Key -> `dim_vehicle`. Surrogate key for the truck. | `15` |
| `driver_key` | `INT` | Foreign Key -> `dim_driver`. Surrogate key for the driver. | `42` |
| `company_key` | `INT` | Foreign Key -> `dim_company`. Surrogate key for the owning entity. | `3` |
| `geo_key` | `INT` | Foreign Key -> `dim_geography`. Surrogate key for the location. | `8` |
| `scan_time` | `DATETIME` | Exact chronological timestamp of the telemetry ping. | `2025-01-28 16:34:09` |
| `truck_status` | `VARCHAR` | Current operating state (e.g., Moving, Parked, Rest). | `Moving Car` |
| `pos_gps_lat` | `DECIMAL(10,6)` | Latitude coordinate of the vehicle. | `50.009400` |
| `pos_gps_long` | `DECIMAL(10,6)` | Longitude coordinate of the vehicle. | `11.330700` |
| `speed` | `FLOAT` | Speed in km/h. | `84.5` |
| `distance` | `FLOAT` | Distance driven in kilometers since the last ping. | `5.3` |
| `driving_time` | `FLOAT` | Time elapsed in seconds since the last ping. | `301` |

### `fact_device_health`
**Type:** Transactional Fact Table  
**Description:** IoT operational table monitoring the health, battery life, and software uptime of the telemetry devices installed in the trucks.  
**Grain:** One row per health heartbeat per device.  

| Column Name | Data Type | Description | Example |
| :--- | :--- | :--- | :--- |
| `health_id` | `INT` | Primary Key. Auto-incremented unique identifier. | `99302` |
| `date_key` | `INT` | Foreign Key -> `dim_date`. | `20250128` |
| `vehicle_key` | `INT` | Foreign Key -> `dim_vehicle`. | `15` |
| `driver_key` | `INT` | Foreign Key -> `dim_driver`. | `42` |
| `company_key` | `INT` | Foreign Key -> `dim_company`. | `3` |
| `scan_time` | `DATETIME` | Exact chronological timestamp of the health ping. | `2025-01-28 16:35:00` |
| `device_name` | `VARCHAR` | Hardware identifier/model of the IoT device. | `Teltonika FMB120` |
| `pos_gps_lat` | `DECIMAL(10,6)` | Latitude coordinate at the time of the health ping. | `50.009400` |
| `pos_gps_long` | `DECIMAL(10,6)` | Longitude coordinate at the time of the health ping. | `11.330700` |
| `battery_level` | `FLOAT` | Remaining battery percentage of the device (0-100). | `87.5` |
| `app_run_time` | `FLOAT` | Uptime of the tracking software application in seconds. | `36000` |
| `dev_run_time` | `FLOAT` | Total hardware uptime of the device in seconds. | `86400` |

---

## 🗂️ Data Dictionary: Dimensions

### `dim_date`
**Type:** Role-Playing Dimension  
**Description:** Standard calendar dimension to enable time-series reporting and aggregations.  

| Column Name | Data Type | Description | Example |
| :--- | :--- | :--- | :--- |
| `date_key` | `INT` | Primary Key. Smart Key format YYYYMMDD. | `20250128` |
| `full_date` | `DATE` | Standard ISO date format. | `2025-01-28` |
| `year` | `INT` | Calendar year. | `2025` |
| `quarter` | `INT` | Calendar quarter (1-4). | `1` |
| `month` | `INT` | Calendar month number (1-12). | `1` |
| `month_name` | `VARCHAR` | Full name of the month. | `January` |
| `day_of_month`| `INT` | Day of the month (1-31). | `28` |
| `day_of_week` | `INT` | Day of the week index (1=Sunday, 7=Saturday). | `3` |
| `day_name` | `VARCHAR` | Full name of the day. | `Tuesday` |
| `is_weekend` | `BOOLEAN` | Flag indicating if the day is Saturday or Sunday. | `FALSE` |

### `dim_vehicle`
**Type:** Conformed Dimension  
**Description:** Master data holding the descriptive attributes of the fleet vehicles.  

| Column Name | Data Type | Description | Example |
| :--- | :--- | :--- | :--- |
| `vehicle_key` | `INT` | Primary Key (Surrogate Key). Used for DW joins. | `15` |
| `car_id` | `INT` | Natural Business Key originating from the FMS source. | `49070` |
| `company_id` | `INT` | Natural Business Key linking to the source company. | `101` |
| `license_plate` | `VARCHAR` | Standardized vehicle registration plate (dashes removed). | `0FV5375` |
| `make` | `VARCHAR` | Vehicle manufacturer (standardized, e.g., 'MB' -> 'Mercedes-Benz'). | `Mercedes-Benz` |
| `color` | `VARCHAR` | Vehicle color normalized to UI-friendly Hex codes. | `#ffffff` |
| `tonnage` | `FLOAT` | Maximum carrying capacity in metric tons. | `12.5` |
| `vehicle_type` | `VARCHAR` | Descriptive vehicle categorization (e.g., Van, Lowdeck). | `Tautliner` |

### `dim_driver`
**Type:** Conformed Dimension  
**Description:** Master data holding information about the vehicle operators.  

| Column Name | Data Type | Description | Example |
| :--- | :--- | :--- | :--- |
| `driver_key` | `INT` | Primary Key (Surrogate Key). Used for DW joins. | `42` |
| `driver_id` | `INT` | Natural Business Key originating from the FMS source. | `70462` |
| `company_id` | `INT` | Natural Business Key linking to the source company. | `101` |
| `driver_name` | `VARCHAR` | Full name of the driver (standardized to Proper Case). | `Martin` |

### `dim_company`
**Type:** Conformed Dimension  
**Description:** Master data holding the organizational entities that own the vehicles and employ the drivers.  

| Column Name | Data Type | Description | Example |
| :--- | :--- | :--- | :--- |
| `company_key` | `INT` | Primary Key (Surrogate Key). Used for DW joins. | `3` |
| `company_id` | `INT` | Natural Business Key originating from the FMS source. | `101` |
| `company_name`| `VARCHAR` | Legal name of the transportation company. | `Logistics Corp CZ` |
| `city` | `VARCHAR` | City of the company's headquarters. | `Prague` |
| `country` | `VARCHAR` | Country of the company's headquarters. | `CZ` |
| `region` | `VARCHAR` | Regional designation of the headquarters. | `Central` |

### `dim_geography`
**Type:** Bridge Dimension (Python Enriched)  
**Description:** Geographic lookup table mapping exact coordinates to specific countries and administrative regions, generated via Python reverse-geocoding.  

| Column Name | Data Type | Description | Example |
| :--- | :--- | :--- | :--- |
| `geo_key` | `INT` | Primary Key (Surrogate Key). Used for DW joins. | `8` |
| `country_code`| `VARCHAR` | ISO 2-letter country code. | `CZ` |
| `region_name` | `VARCHAR` | Administrative province or state. | `Central Bohemia` |

### `dim_roaming_network`
**Type:** Outrigger Dimension (Telco Reference)  
**Description:** Reference table mapping Mobile Country Codes (MCC) and Mobile Network Codes (MNC) to actual telecom operators. Connects to the data model via `dim_geography.country_code` in a Many-to-Many reporting relationship.  

| Column Name | Data Type | Description | Example |
| :--- | :--- | :--- | :--- |
| `network_key` | `INT` | Primary Key (Surrogate Key). | `112` |
| `mcc` | `INT` | Mobile Country Code (ITU standard). | `230` |
| `mnc` | `INT` | Mobile Network Code. | `3` |
| `country_code`| `VARCHAR` | ISO 2-letter country code used to join to Geography. | `CZ` |
| `country_name`| `VARCHAR` | Full name of the country. | `Czechia` |
| `network_name`| `VARCHAR` | Commercial name of the mobile operator. | `Vodafone CZ` |