import pandas as pd
import os
import logging

# Setup logging
base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
log_path = os.path.join(base_path, "logs", "pipeline.log")
os.makedirs(os.path.dirname(log_path), exist_ok=True)

logging.basicConfig(
    filename=log_path,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def ingest_data():
    try:
        logging.info("Ingestion started: Simulating API data pull...")

        raw_dir = os.path.join(base_path, "data", "raw")
        os.makedirs(raw_dir, exist_ok=True)

        # Simulated API data
        data = {
            "respondent_id": ["R100", "R101"],
            "country": ["Ghana", "Nigeria"],
            "age": [28, 35],
            "gender": ["Male", "Female"],
            "income": [2500, 3200],
            "survey_date": ["2024-01-01", "2024-01-02"]
        }

        df = pd.DataFrame(data)
        file_path = os.path.join(raw_dir, "api_data.csv")
        df.to_csv(file_path, index=False)

        logging.info(f"Ingestion successful: {file_path}")

    except Exception as e:
        logging.error(f"Ingestion failed: {e}")
        raise

if __name__ == "__main__":
    ingest_data()
