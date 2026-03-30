

📊 Automated Survey Data Pipeline (ETL)
A modular Python-based ETL pipeline for multi-country survey data ingestion and SQL Server integration.

🎯 Overview
This project automates the extraction, transformation, and loading (ETL) of survey research data. It was designed to replace manual data cleaning processes, ensuring 100% schema consistency and reducing data preparation time by automating validation and feature engineering.

🛠️ Tech Stack
Language: Python 3.12+

Libraries: Pandas (Data Manipulation), PyODBC (Database Connection), Dotenv (Security)

Database: Microsoft SQL Server (SSMS)

Environment: Virtual Environments (venv) for dependency isolation

🚀 Key Features
Automated Data Cleaning: Handles outliers (e.g., age validation), standardizes country codes, and manages missing values.

Feature Engineering: Automatically generates age_groups, income_bands, and calculates weighted response_scores.

Enterprise Security: Implements .env for secrets management and Windows Authentication for secure SQL access.

Robust Logging: Centralized execution tracking with error handling and success verification.

Schema Validation: Strict column-matching to prevent "broken" loads into production tables.

📂 Project Structure
Plaintext
ETL_project/
├── data/
│   ├── raw/           # Original survey CSVs
│   └── processed/     # Cleaned and Transformed outputs
├── scripts/
│   ├── ingest.py      # Data Acquisition
│   ├── clean.py       # Validation & Deduplication
│   ├── transform.py   # Feature Engineering
│   └── load.py        # SQL Server Loading Logic
├── main_pipeline.py   # Orchestration Script
├── .env               # Database Credentials (Hidden)
└── requirements.txt   # Dependency List
⚙️ Setup & Installation
Clone the repo: git clone <your-repo-link>

Create Venv: python -m venv venv

Install Dependencies: pip install -r requirements.txt

Configure Environment: Create a .env file with your DB_SERVER and DB_NAME.

Run Pipeline: python main_pipeline.py
