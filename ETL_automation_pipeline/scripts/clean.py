import pandas as pd
import os

base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
input_path = os.path.join(base_path, "data", "raw", "survey_data.csv")
output_path = os.path.join(base_path, "data", "processed", "cleaned_data.csv")

def clean_data():
    print("Cleaning started: Applying automated validation...")
    
    df = pd.read_csv(input_path)

    df['country'] = df['country'].str.upper().str.strip()

    df['survey_date'] = pd.to_datetime(df['survey_date'], errors='coerce').dt.strftime('%Y-%m-%d')

    median_age = int(df['age'].median())
    df.loc[df['age'] > 100, 'age'] = median_age

    df = df.drop_duplicates(subset=['respondent_id'], keep='first')

    df['income'] = df['income'].fillna(0)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"Success: Cleaned data saved to {output_path}")

if __name__ == "__main__":
    clean_data()