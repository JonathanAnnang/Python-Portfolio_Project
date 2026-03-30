import subprocess
import sys
import os
import logging

base_path = os.path.dirname(os.path.abspath(__file__))
log_path = os.path.join(base_path, "logs", "pipeline.log")

logging.basicConfig(
    filename=log_path,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def run_stage(script_name):
    script_path = os.path.join("scripts", script_name)

    logging.info(f"Running stage: {script_name}")

    result = subprocess.run([sys.executable, script_path], capture_output=True, text=True)

    if result.returncode != 0:
        logging.error(f"Pipeline failed at {script_name}: {result.stderr}")
        print(result.stderr)
        sys.exit(1)
    else:
        logging.info(f"{script_name} completed successfully")
        print(result.stdout)

if __name__ == "__main__":
    print("Starting Data Pipeline...\n")

    run_stage("ingest.py")
    print("data injestion complete")
    run_stage("clean.py")
    print("data cleaning complete")
    run_stage("transform.py")
    print("data transformation complete")
    run_stage("load.py")
    print("data loading complete")

    print("\nPipeline completed successfully")
