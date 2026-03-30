import pandas as pd
import pyodbc
import os
import logging
from dotenv import load_dotenv

load_dotenv()

base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
data_path = os.path.join(base_path, "data", "processed", "transformed_data.csv")

log_path = os.path.join(base_path, "logs", "pipeline.log")

logging.basicConfig(
    filename=log_path,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

server = os.getenv('DB_SERVER')
database = os.getenv('DB_NAME')

import pandas as pd
import pyodbc
import os
import logging
from dotenv import load_dotenv

load_dotenv()

base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
data_path = os.path.join(base_path, "data", "processed", "transformed_data.csv")

log_path = os.path.join(base_path, "logs", "pipeline.log")

logging.basicConfig(
    filename=log_path,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

server = os.getenv('DB_SERVER')
database = os.getenv('DB_NAME')

def load_to_sql():
    try:
        logging.info("Incremental load started")

        if not os.path.exists(data_path):
            raise FileNotFoundError("Transformed data not found")

        df = pd.read_csv(data_path)

        # Validate critical fields
        if df['respondent_id'].isnull().any():
            raise ValueError("Null respondent_id found")

        conn = pyodbc.connect(
            f"DRIVER={{SQL Server}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"Trusted_Connection=yes;"
        )

        cursor = conn.cursor()

        # 🔍 Step 1: Get existing IDs from DB
        cursor.execute("SELECT respondent_id FROM survey_responses")
        existing_ids = set(row[0] for row in cursor.fetchall())

        logging.info(f"Existing records in DB: {len(existing_ids)}")

        # 🔍 Step 2: Filter new records
        new_df = df[~df['respondent_id'].isin(existing_ids)]

        if new_df.empty:
            logging.info("No new records to insert")
            print("No new records found.")
            return

        logging.info(f"New records to insert: {len(new_df)}")

        cursor.fast_executemany = True

        cols = [
            'respondent_id', 'country', 'age', 'gender', 'income',
            'response_score', 'age_group', 'income_band'
        ]

        insert_query = f"""
            INSERT INTO survey_responses ({', '.join(cols)})
            VALUES ({', '.join(['?' for _ in cols])})
        """

        data = [tuple(x) for x in new_df[cols].values]

        cursor.executemany(insert_query, data)
        conn.commit()

        logging.info(f"Inserted {len(data)} new records successfully")
        print(f"Inserted {len(data)} new records.")

    except Exception as e:
        logging.error(f"Incremental load failed: {e}")
        raise

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    load_to_sql()
