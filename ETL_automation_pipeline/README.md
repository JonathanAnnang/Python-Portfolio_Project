📊 Survey Data Pipeline (Python + SQL Server)
Overview

This project simulates a real-world data engineering workflow for processing multi-country survey data. It demonstrates how raw data can be ingested, cleaned, transformed, and loaded into a structured SQL Server database for analytics.

The pipeline is designed with scalability, data quality, and automation in mind.

🚀 Key Features
End-to-end ETL pipeline (Extract, Transform, Load)
Incremental data loading (prevents duplicate records)
Automated data cleaning and validation
Multi-country dataset standardization
SQL Server integration (SSMS)
Data integrity enforcement using constraints
Modular pipeline design (separate scripts per stage)
🧱 Architecture
Raw Data (CSV)
   ↓
Ingestion (Python)
   ↓
Cleaning & Validation
   ↓
Transformation (feature engineering)
   ↓
SQL Server (Data Warehouse)

⚙️ Technologies Used
Python (Pandas, PyODBC)
SQL Server (SSMS)
Power BI (for future visualization)
Excel (data exploration)
🧠 Key Engineering Concepts Demonstrated
Incremental Loading

Only new records are inserted into the database by checking existing respondent_id, preventing duplication.

Data Cleaning
Handling missing values
Standardizing formats
Removing duplicates
Data Integrity
SQL constraints (UNIQUE keys)
Deduplication logic using SQL window functions

📌 Example SQL Deduplication
WITH CTE AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY respondent_id ORDER BY id) AS rn
    FROM survey_responses
)
DELETE FROM CTE
WHERE rn > 1;

📈 Future Improvements
Add orchestration (Airflow)
Deploy pipeline to cloud (Azure/AWS)
Add real-time ingestion
Build dashboards in Power BI
💡 Why This Project Matters

This project reflects real-world data engineering challenges:

messy data
duplicate records
inconsistent formats
need for automation
