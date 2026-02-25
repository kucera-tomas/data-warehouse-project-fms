# Fleet Telematics Data Warehouse Project

Welcome to the **Fleet Telematics Data Warehouse** repository! 🚀  
This project demonstrates an end-to-end data engineering solution designed to process high-frequency IoT tracking data and fleet master records. Using a Medallion Architecture, we transform raw telemetry and asset data into actionable logistics insights.

---

## 🏗️ Data Architecture

The architecture follows the **Medallion Architecture** (Bronze, Silver, and Gold) to ensure data reliability and traceability:



1.  **Bronze Layer**: Stores raw snapshots from source systems. We ingest both "Base" snapshots and "Update" (`_upd`) files to capture the latest state of the fleet.
2.  **Silver Layer**: Merges base tables with updates to create a **Single Source of Truth**. This layer handles deduplication, GPS coordinate validation, and standardization of vehicle types.
3.  **Gold Layer**: Houses business-ready data modeled into a **Star Schema** (Facts and Dimensions) optimized for fleet utilization, fuel efficiency, and driver performance reporting.

---

## 📖 Project Overview

This project involves modeling and transforming data from three distinct functional domains:

* **Source FMS (Fleet Management System)**: Master data for vehicles (`car_info`), drivers, and companies. This is the "What" of our data.
* **Source Telematics (IoT)**: High-frequency transactional data (`import_tracking`, `import_health`) coming from GPS hardware. This is the "How" and "Where" of our data.
* **Source Telco**: Connectivity metadata (`operator_info`) providing geographical and network context via MCC/MNC codes. This is the "Context" of our data.

---

## 🛠️ Tools Used:

- **MySQL:** For hosting the Bronze, Silver, and Gold layers.
- **PHPMyAdmin:** For database management and SQL development.
- **DrawIO:** For designing the fleet data model and telematics flow.
- **Git:** Version control for SQL transformation scripts.

---

## 🚀 Project Requirements

### Building the Data Warehouse (Data Engineering)

#### Objective
Develop a modern data warehouse to consolidate fragmented fleet data, enabling real-time asset tracking and operational analysis.

#### Specifications
- **Data Integration**: Resolve the "Split Table" challenge by merging base records with incremental update files (`_upd`).
- **Data Quality**: Handle `NULL` values in GPS tracking and standardize vehicle tonnage and color formats.
- **System Separation**: Organize data into logical folders: `source_fms`, `source_telematics`, and `source_telco`.
- **Deduplication**: Implement logic in the Silver layer using `ROW_NUMBER()` or `UNION` logic to ensure only the most recent telemetry scan per `pos_key` is processed.

---

### BI: Analytics & Reporting (Data Analysis)

#### Objective
Deliver SQL-based analytics to provide visibility into:
- **Fleet Utilization**: Identify underused vehicles based on distance and driving time.
- **Hardware Health**: Monitor device battery levels and app runtimes to prevent data gaps.
- **Geographical Insights**: Map network operator codes to countries for international fleet tracking.

---

## 📂 Repository Structure
```text
fleet-telematics-dw/
│
├── datasets/                           
│   ├── source_fms/                     # Asset data (cars, drivers, companies)
│   ├── source_telematics/              # IoT logs (tracking, health, and updates)
│   └── source_telco/                   # Connectivity metadata (operator_info)
│
├── docs/                               # Project documentation and architecture details
│   ├── data_architecture.drawio        # Visual flow of the Medallion layers
│   ├── data_catalog.md                 # Metadata for MCC, MNC, and Telemetry fields
│   └── star_schema.drawio              # ERD for the Gold Layer
│
├── scripts/                            
│   ├── bronze/                         # Initial load scripts for all CSV sources
│   ├── silver/                         # Merge logic for _upd tables and cleaning
│   └── gold/                           # Final Analytical models (fact_tracking, dim_cars)
│
├── tests/                              # Test scripts and quality files
│
├── README.md                           # Project overview and instruction
└── .gitignore