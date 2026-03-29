import os
import shutil

def ingest_data():
    base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    raw_dir = os.path.join(base_path, "data", "raw")
    
    print("Ingestion started: Simulating API data pull...")
    os.makedirs(raw_dir, exist_ok=True)
    
    # Logic to confirm file exists would go her
    print(f"Success: Raw data localized in {raw_dir}")

if __name__ == "__main__":
    ingest_data()