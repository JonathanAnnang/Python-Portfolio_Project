import subprocess
import sys
import os

def run_stage(script_name):
    script_path = os.path.join("scripts", script_name)
    print(f"\n>>> EXECUTION STAGE: {script_name}")
    
    
    result = subprocess.run([sys.executable, script_path], capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"!!! PIPELINE FAILED at {script_name} !!!")
        print(f"Error details: {result.stderr}")
        sys.exit(1)
    else:
        print(result.stdout)

if __name__ == "__main__":
    print("========================================")
    print("   STARTING DATA PIPELINE    ")
    print("========================================")
    
    # 1. Ingest (Extract)
    run_stage("ingest.py")
    
    # 2. Clean (Transform - Part 1)
    run_stage("clean.py")
    
    # 3. Transform (Transform - Part 2)
    run_stage("transform.py")
    
    # 4. Load (Load)
    run_stage("load.py")
    
    print("========================================")
    print("   PIPELINE COMPLETED SUCCESSFULLY      ")
    print("========================================")