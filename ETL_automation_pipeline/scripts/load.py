import pandas as pd
import pyodbc
import os
import sys
from dotenv import load_dotenv

# Load variables from .env
load_dotenv()

# 1. Setup Paths
base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
data_path = os.path.join(base_path, "data", "processed", "cleaned_data.csv")

# 2. Get Credentials Securely
server = os.getenv('DB_SERVER')
database = os.getenv('DB_NAME')

if not server or not database:
    print("!!! ERROR: Database credentials missing from .env file.")
    sys.exit(1)

if not os.path.exists(data_path):
    print(f"!!! ERROR: {data_path} not found.")
    sys.exit(1)

# 3. Prepare Data
df = pd.read_csv(data_path)

# Define exactly what the database expects
required_cols = [
    'respondent_id', 'country', 'age', 'gender', 'income', 
    'response_score', 'age_group', 'income_band'
]

# Ensure we only take the columns we need and handle NaNs for SQL
df_to_load = df[required_cols].astype(object).where(pd.notnull(df[required_cols]), None)

# 4. DATABASE CONNECTION
try:
    conn = pyodbc.connect(
        f"DRIVER={{SQL Server}};"
        f"SERVER={server};" 
        f"DATABASE={database};"
        f"Trusted_Connection=yes;"
    )
    cursor = conn.cursor()
    cursor.fast_executemany = True 

    # Dynamic query using the required_cols list
    insert_query = f"""
        INSERT INTO survey_responses ({', '.join(required_cols)})
        VALUES ({', '.join(['?' for _ in required_cols])})
    """

    # Convert DataFrame to list of tuples
    data_to_insert = [tuple(x) for x in df_to_load.values]

    cursor.executemany(insert_query, data_to_insert)
    conn.commit()
    print(f"SUCCESS: {len(df_to_load)} rows loaded into SQL Server.")

except pyodbc.Error as e:
    print(f"!!! DATABASE ERROR: {e}")
    if 'conn' in locals(): conn.rollback()
except Exception as e:
    print(f"!!! UNEXPECTED ERROR: {e}")
finally:
    if 'cursor' in locals(): cursor.close()
    if 'conn' in locals(): conn.close()