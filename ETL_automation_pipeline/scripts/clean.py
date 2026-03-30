import pandas as pd
import os
import logging

base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

input_path = os.path.join(base_path, "data", "raw", "survey_data.csv")
output_path = os.path.join(base_path, "data", "processed", "cleaned_data.csv")

log_path = os.path.join(base_path, "logs", "pipeline.log")
os.makedirs(os.path.dirname(log_path), exist_ok=True)

logging.basicConfig(
    filename=log_path,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def clean_data():
    try:
        logging.info("Cleaning started")

        df = pd.read_csv(input_path)

        # Standardize country
        df['country'] = df['country'].str.upper().str.strip()

        # Convert date safely
        df['survey_date'] = pd.to_datetime(df['survey_date'], errors='coerce')

        # Fix invalid ages
        median_age = int(df['age'].median())
        df.loc[(df['age'] <= 0) | (df['age'] > 100), 'age'] = median_age

        # Remove duplicates
        df = df.drop_duplicates(subset=['respondent_id'])

        # Fill missing income
        df['income'] = df['income'].fillna(df['income'].median())

        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_csv(output_path, index=False)

        logging.info(f"Cleaning successful: {len(df)} records processed")

    except Exception as e:
        logging.error(f"Cleaning failed: {e}")
        raise

if __name__ == "__main__":
    clean_data()
