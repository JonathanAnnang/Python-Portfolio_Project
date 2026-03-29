import pandas as pd
import os
import numpy as np

base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
input_path = os.path.join(base_path, "data", "processed", "cleaned_data.csv")
output_path = os.path.join(base_path, "data", "processed", "cleaned_data.csv") 

def transform_data():
    print(">>> Transformation started: Categorizing data for analytics...")
    
    if not os.path.exists(input_path):
        print(f"!!! Error: {input_path} not found. Ensure clean.py ran successfully.")
        return

    df = pd.read_csv(input_path)

    bins = [0, 18, 30, 50, 100]
    labels = ['Under 18', '19-30', '31-50', '50+']
    df['age_group'] = pd.cut(df['age'], bins=bins, labels=labels, right=False)

    def categorize_income(income):
        if income < 3000:
            return 'Low'
        elif income <= 7000:
            return 'Medium'
        else:
            return 'High'
    df['income_band'] = df['income'].apply(categorize_income)

    df['response_score'] = ((df['income'] / 10000) + (df['age'] / 100)).round(2)
    
    df['response_score'] = df['response_score'].clip(upper=1.0)

    df['gender'] = df['gender'].str.title()
   
    required_for_load = [
        'respondent_id', 'country', 'age', 'gender', 'income', 
        'response_score', 'age_group', 'income_band'
    ]
    
    missing = [col for col in required_for_load if col not in df.columns]
    
    if missing:
        print(f"!!! WARNING: Transformation failed to produce columns: {missing}")
    else:
     
        df.to_csv(output_path, index=False)
        print(f"Success: Data transformed and saved to {output_path}")
        print(f"Verified Columns: {df.columns.tolist()}")

if __name__ == "__main__":
    transform_data()